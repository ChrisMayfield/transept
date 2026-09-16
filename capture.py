#!/usr/bin/env python3
"""
Audio capture, one interface over two backends.

sounddevice (PortAudio) works everywhere and is the default. parec is Linux
only and stays as a second path, because PortAudio through PipeWire's
compatibility layer can be inconsistent about device names and buffer sizes.

Both deliver the same thing: 16 kHz mono signed 16-bit chunks, one per
CHUNK_MS, which is what the speech recognizer expects.
"""

import array
import asyncio
import shutil
import subprocess
import sys

SAMPLE_RATE = 16000
CHANNELS = 1
BYTES_PER_SAMPLE = 2
CHUNK_MS = 100
CHUNK_FRAMES = SAMPLE_RATE * CHUNK_MS // 1000
CHUNK_BYTES = CHUNK_FRAMES * CHANNELS * BYTES_PER_SAMPLE

# Not every input device will open at 16 kHz. 48 kHz is nearly universal and
# divides evenly, so the fallback is a clean 3:1 decimation.
FALLBACK_RATE = 48000


class CaptureError(RuntimeError):
    """The audio device failed or went away."""


def default_backend():
    """parec where it exists and is proven, sounddevice everywhere else."""
    if sys.platform.startswith("linux") and shutil.which("parec"):
        return "parec"
    return "sounddevice"


def resolve_backend(name):
    if not name or name == "auto":
        return default_backend()
    if name not in ("parec", "sounddevice"):
        raise CaptureError(f"Unknown capture backend: {name}")
    return name


# -- device discovery --------------------------------------------------------


def list_devices(backend="auto"):
    """Return [{"name", "detail", "monitor", "default"}] for input devices."""
    backend = resolve_backend(backend)
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
    if not device:
        raise CaptureError("No audio device configured.")
    if backend == "parec":
        return await ParecCapture.open(device)
    return await SoundDeviceCapture.open(device)


class ParecCapture:
    """Reads raw PCM from the parec subprocess."""

    backend = "parec"

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

    def __init__(self, stream, queue, state, rate, ratio):
        self.stream = stream
        self.queue = queue
        self.state = state
        self.rate = rate
        self.ratio = ratio

    @classmethod
    async def open(cls, device):
        sd = _import_sounddevice()
        loop = asyncio.get_running_loop()
        queue = asyncio.Queue(maxsize=64)
        state = {"error": None, "dropped": 0}

        def push(data):
            # Drop the oldest chunk rather than let the queue grow without
            # bound. A reader who has fallen seconds behind is already lost;
            # unbounded memory would be worse.
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                state["dropped"] += 1
            queue.put_nowait(data)

        def make_callback(ratio):
            def callback(indata, frames, time_info, status):
                if status.input_overflow:
                    state["dropped"] += 1
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
                return cls(stream, queue, state, rate, ratio)
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
        if self.state["error"]:
            raise CaptureError(self.state["error"])
        return await self.queue.get()

    async def close(self):
        try:
            self.stream.stop()
            self.stream.close()
        except Exception:
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
