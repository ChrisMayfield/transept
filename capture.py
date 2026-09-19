#!/usr/bin/env python3
"""
Audio capture, one interface over three backends.

parec is preferred on Linux where it exists, because PortAudio through
PipeWire's compatibility layer can be inconsistent about device names and
buffer sizes. sounddevice (PortAudio) is the fallback there and the only
backend on macOS and Windows. remote is neither: the audio arrives over
the room's control socket from a laptop somewhere else, which is what a
hosted server captures from.

All three deliver the same thing: 16 kHz mono signed 16-bit chunks, one
per CHUNK_MS, which is what the speech recognizer expects, and each says
what it yields, because a sender may hand over encoded audio that the
recognizer has to be told about.
"""

import array
import asyncio
import shutil
import subprocess
import sys
import time

SAMPLE_RATE = 16000
CHANNELS = 1
BYTES_PER_SAMPLE = 2
CHUNK_MS = 100
CHUNK_FRAMES = SAMPLE_RATE * CHUNK_MS // 1000
CHUNK_BYTES = CHUNK_FRAMES * CHANNELS * BYTES_PER_SAMPLE

# Not every input device will open at 16 kHz. 48 kHz is nearly universal and
# divides evenly, so the fallback is a clean 3:1 decimation.
FALLBACK_RATE = 48000

# Chunks a source may hold before the oldest is dropped. A reader who has
# fallen seconds behind is already lost, and unbounded memory would be
# worse.
QUEUE_CHUNKS = 64

# How often a remote source reports a gap rather than a chunk, so the
# caller can keep its own upstream connection warm. Well inside the ten
# seconds a recognizer socket waits before closing on silence.
GAP_TICK = 3.0


class CaptureError(RuntimeError):
    """The audio device failed or went away."""


def default_backend():
    """parec where it exists and is proven, sounddevice everywhere else."""
    if sys.platform.startswith("linux") and shutil.which("parec"):
        return "parec"
    return "sounddevice"


def resolve_backend(name):
    """Which backend a setting names. auto never chooses remote.

    remote is a deployment rather than a preference: it means the audio is
    coming from a sender in another building, so it has to be asked for.
    """
    if not name or name == "auto":
        return default_backend()
    if name not in ("parec", "sounddevice", "remote"):
        raise CaptureError(f"Unknown capture backend: {name}")
    return name


# -- device discovery --------------------------------------------------------


def list_devices(backend="auto"):
    """Return [{"name", "detail", "monitor", "default"}] for input devices."""
    backend = resolve_backend(backend)
    if backend == "remote":
        # The sound hardware is on the sender's laptop, which is the only
        # machine that can enumerate it. A hosted server has none of its
        # own and must not offer a list that would be the rented VM's.
        raise CaptureError("The audio devices are on the sender, not on "
                           "this server.")
    if backend == "parec":
        return _parec_devices()
    return _sounddevice_devices()


def choose_default(devices):
    """Which input to offer first, in the backend's own order.

    parec marks nothing as default, so without a rule here a PulseAudio
    machine offers its playback monitor first, and the subtitles would be
    whatever the laptop is playing rather than what was said in the room.
    """
    for device in devices:
        if device["default"]:
            return device["name"]
    for device in devices:
        if not device["monitor"]:
            return device["name"]
    return devices[0]["name"] if devices else ""


def default_device(backend="auto"):
    """The input to use when nobody named one. May be empty."""
    return choose_default(list_devices(backend))


def _parec_devices():
    try:
        output = subprocess.run(["pactl", "list", "short", "sources"],
                                capture_output=True, text=True,
                                check=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        raise CaptureError(f"pactl failed: {exc}") from exc
    devices = []
    for line in output.strip().splitlines():
        fields = line.split("\t")
        if len(fields) >= 2:
            devices.append({"name": fields[1], "detail": "",
                            "monitor": fields[1].endswith(".monitor"),
                            "default": False})
    return devices


def _sounddevice_devices():
    sd = _import_sounddevice()
    try:
        default_input = sd.default.device[0]
    except (TypeError, IndexError):
        default_input = None
    devices = []
    for index, info in enumerate(sd.query_devices()):
        if info["max_input_channels"] < 1:
            continue
        name = info["name"]
        devices.append({
            "name": name,
            "detail": f"{info['max_input_channels']} ch, "
                      f"{sd.query_hostapis(info['hostapi'])['name']}",
            # PortAudio surfaces PulseAudio monitors too, and picking one is
            # the most common setup mistake.
            "monitor": "monitor" in name.lower(),
            "default": index == default_input,
        })
    return devices


def _import_sounddevice():
    try:
        import sounddevice as sd
    except (ImportError, OSError) as exc:
        # OSError means the PortAudio library itself is missing, which on
        # Linux usually means: sudo apt install libportaudio2
        raise CaptureError(
            "sounddevice is unavailable. pip install sounddevice, and on "
            f"Linux also: sudo apt install libportaudio2  ({exc})") from exc
    return sd


# -- capture -----------------------------------------------------------------


async def open_capture(device, backend="auto"):
    backend = resolve_backend(backend)
    if backend == "remote":
        # A remote source has no device to open: the control socket hands
        # one over when a sender attaches, and the session asks the room
        # for it rather than coming through here.
        raise CaptureError("A remote source is opened by the control "
                           "socket, not by name.")
    if not device:
        raise CaptureError("No audio device configured.")
    if backend == "parec":
        return await ParecCapture.open(device)
    return await SoundDeviceCapture.open(device)


class ParecCapture:
    """Reads raw PCM from the parec subprocess."""

    backend = "parec"
    encoding = "pcm"

    def __init__(self, process):
        self.process = process

    @classmethod
    async def open(cls, device):
        try:
            process = await asyncio.create_subprocess_exec(
                "parec", "--device", str(device), "--format=s16le",
                f"--rate={SAMPLE_RATE}", f"--channels={CHANNELS}",
                "--latency-msec=50",
                stdout=asyncio.subprocess.PIPE,
                # Discarded, not piped: nothing drains a stderr pipe, so
                # enough diagnostics fill the buffer and parec blocks mid
                # write. Audio stops with no error and no end of stream,
                # which is the one failure the supervisor cannot see.
                stderr=asyncio.subprocess.DEVNULL,
            )
        except OSError as exc:
            raise CaptureError(f"Could not start parec: {exc}") from exc
        return cls(process)

    async def read(self):
        """One chunk, or empty bytes when the stream ends."""
        return await self.process.stdout.read(CHUNK_BYTES)

    async def close(self):
        try:
            self.process.terminate()
        except ProcessLookupError:
            # Ctrl-C reaches parec too, since it shares this process group,
            # so it is often already gone by the time cleanup runs.
            pass
        await self.process.wait()


class SoundDeviceCapture:
    """Reads from PortAudio, which hands chunks over on its own thread."""

    backend = "sounddevice"
    encoding = "pcm"

    def __init__(self, stream, queue):
        self.stream = stream
        self.queue = queue

    @classmethod
    async def open(cls, device):
        sd = _import_sounddevice()
        loop = asyncio.get_running_loop()
        queue = asyncio.Queue(maxsize=QUEUE_CHUNKS)

        def push(data):
            # Drop the oldest chunk rather than let the queue grow without
            # bound, for the reason QUEUE_CHUNKS gives.
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(data)

        def make_callback(ratio):
            def callback(indata, frames, time_info, status):
                data = bytes(indata)
                if ratio > 1:
                    data = _downsample(data, ratio)
                try:
                    loop.call_soon_threadsafe(push, data)
                except RuntimeError:
                    pass
            return callback

        def finished():
            # A device that disappears ends the stream. The sentinel turns
            # that into a normal end-of-stream for the caller, which lets the
            # session supervisor reconnect instead of hanging.
            try:
                loop.call_soon_threadsafe(push, b"")
            except RuntimeError:
                pass

        target = _device_index(device)
        last_error = None
        for rate, ratio in ((SAMPLE_RATE, 1), (FALLBACK_RATE,
                                               FALLBACK_RATE // SAMPLE_RATE)):
            stream = None
            try:
                stream = sd.RawInputStream(
                    samplerate=rate, channels=CHANNELS, dtype="int16",
                    blocksize=CHUNK_FRAMES * ratio, device=target,
                    callback=make_callback(ratio), finished_callback=finished)
                stream.start()
                return cls(stream, queue)
            except Exception as exc:
                last_error = exc
                # RawInputStream opens the device and start() can still
                # fail. Holding it would make the 48 kHz attempt fail as
                # busy, and the operator would see only that second error.
                if stream is not None:
                    try:
                        stream.close()
                    except Exception:
                        pass
        raise CaptureError(
            f"Could not open audio device {device!r}: {last_error}")

    async def read(self):
        return await self.queue.get()

    async def close(self):
        try:
            self.stream.stop()
            self.stream.close()
        except Exception:
            pass


class RemoteCapture:
    """Audio arriving over the room's control socket, from a sender.

    The same chunks the local backends produce, except that they come off a
    websocket rather than a device, which changes what silence means. A
    device that stops has died and the session should reconnect; a socket
    that stops has usually blinked, and the sentence in progress is not
    over. So a gap is reported as a gap, and only a gap longer than the
    grace period ends the stream.

    The encoding is whatever the sender declared, because a sender on a
    constrained uplink may be sending compressed audio, and the recognizer
    has to be told which it is getting.
    """

    backend = "remote"

    def __init__(self, grace, encoding="pcm", tick=GAP_TICK):
        self.grace = grace
        self.encoding = encoding
        # A parameter so a check can exercise a gap without waiting three
        # seconds for one. Nothing in a meeting passes it.
        self.tick = tick
        self.queue = asyncio.Queue(maxsize=QUEUE_CHUNKS)
        self.last_chunk = time.monotonic()
        self.closed = False

    def feed(self, chunk):
        """One chunk off the control socket. Never blocks, never raises.

        Called from the task draining the sender's socket, so anything
        that blocked here would stop the room's audio being read.
        """
        if self.closed:
            return
        if self.queue.full():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        self.queue.put_nowait(chunk)

    async def read(self):
        """A chunk, None for a gap, or empty bytes when the sender is gone.

        None is the middle case the local backends never produce: nothing
        has arrived just now, but the sender is still inside its grace
        period and the caller should hold its recognizer socket open rather
        than end the session or send silence, which is billed as audio.
        """
        while True:
            if self.closed:
                return b""
            try:
                chunk = await asyncio.wait_for(self.queue.get(),
                                               timeout=self.tick)
            except TimeoutError:
                if time.monotonic() - self.last_chunk >= self.grace:
                    return b""
                return None
            if chunk is None:
                return b""
            self.last_chunk = time.monotonic()
            return chunk

    async def close(self):
        self.closed = True
        # Wakes a read that is waiting, so the pump does not sit out the
        # rest of a tick after the session has already ended.
        try:
            self.queue.put_nowait(None)
        except asyncio.QueueFull:
            pass


def _device_index(device):
    """Accept an index, or a name that PortAudio will match on a substring."""
    text = str(device).strip()
    return int(text) if text.lstrip("-").isdigit() else text


def _downsample(data, ratio):
    """Average groups of samples down to 16 kHz.

    A boxcar average rather than plain decimation, because taking every third
    sample aliases everything above 8 kHz straight back into the speech band.
    Native byte order is fine: every platform this runs on is little-endian.
    """
    samples = array.array("h")
    samples.frombytes(data)
    reduced = array.array("h", [
        sum(samples[index:index + ratio]) // ratio
        for index in range(0, len(samples) - ratio + 1, ratio)
    ])
    return reduced.tobytes()


def print_devices(backend="auto"):
    """Human-readable device list for the command line."""
    resolved = resolve_backend(backend)
    try:
        devices = list_devices(resolved)
    except CaptureError as exc:
        sys.exit(str(exc))
    print(f"Input devices ({resolved}):\n")
    for device in devices:
        marks = []
        if device["default"]:
            marks.append("default")
        if device["monitor"]:
            marks.append("playback, not a microphone")
        suffix = f"  [{', '.join(marks)}]" if marks else ""
        detail = f"  ({device['detail']})" if device["detail"] else ""
        print(f"  {device['name']}{detail}{suffix}")
    print("\nPass the name to pipeline.py with --device, or pick it on the "
          "operator page.\nAnything marked playback captures what this "
          "computer is playing, not what the\nmicrophone hears.")
