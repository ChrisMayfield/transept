#!/usr/bin/env python3
"""
The operator page and the routes behind it, shared by both entry points.

One page starts and stops a meeting, picks the microphone, forces a
language on or off, and shows the address to hand the room. Where that page
runs depends on where the audio is captured, which is the only thing that
differs between the two deployments:

    server.py   a sound card on this machine, so the page is here
    sender.py   a laptop in the room feeding a rented server, so the page
                is beside the microphone and the controls travel over the
                room's control socket

So the handlers live here rather than in either file, and they talk to one
object with four methods: start, stop, set_override, and status. Session is
that object in the default deployment; a sender supplies a proxy of the
same shape. That is the same "one interface, two implementations" that
capture backends and pipeline sinks already use, and copying these
handlers into the second entry point instead is the mistake this project
has already made once, when two copies of the pipeline loops drifted until
a session start raised NameError.

This module imports neither entry point, so nothing here drags a Hub and a
Session into a program that has neither.
"""

import asyncio
import hmac
import io
import json
import secrets
from pathlib import Path

from aiohttp import web

import capture

# Beside the code rather than in the working directory: the pages are part
# of the program, not part of a room's configuration.
STATIC = Path(__file__).parent / "static"
# Not a setting: the point of the operator listener is that a tunnel cannot
# be pointed at it by mistake.
OPERATOR_HOST = "127.0.0.1"


def authorized(request):
    """True if this request carries the token its listener was given.

    One rule, several tokens: each application holds its own under
    app["token"], so a reader link opens nothing on the operator port and
    the reverse. No tokenless mode on any of them. The reader port is on
    the public internet through the tunnel, and on the operator port
    loopback alone would still let a page in another browser tab post a
    cross-origin form at these routes.
    """
    token = request.app["token"]
    supplied = (request.headers.get("X-Subtitle-Token")
                or request.query.get("token") or "")
    # Bytes, because compare_digest on two str raises TypeError outside
    # ASCII and a query string can carry that.
    return hmac.compare_digest(supplied.encode(), token.encode())


def mint_token(pinned=""):
    """A token for one of the addresses this program prints.

    Minted for the run unless [keys] pins one, so a link that leaks expires
    when the program does. The overrides exist for development, where a new
    address every restart is a new link to click every restart, and for a
    room that prints its reader card once and wants it to keep working; a
    pinned operator token should be left empty for a meeting.
    """
    return pinned or secrets.token_urlsafe(32)


async def page(request, filename):
    return web.FileResponse(STATIC / filename)


async def read_body(request):
    """The posted JSON object, or an empty one.

    A malformed or absent body is not a server error. The operator page
    renders the {"ok": false} shape and renders a 500 traceback as nothing.
    """
    if not request.can_read_body:
        return {}
    try:
        body = await request.json()
    except (ValueError, json.JSONDecodeError):
        return {}
    return body if isinstance(body, dict) else {}


async def operator_page(request):
    if not authorized(request):
        return web.Response(status=403, text="Add ?token=... to this address.")
    return await page(request, "operator.html")


async def api_status(request):
    """Everything the operator page shows, which is more than a reader sees.

    Behind the token: it names the audio device and the model and carries
    raw exception text.
    """
    if not authorized(request):
        return web.json_response({"ok": False, "message": "Not authorized."},
                                 status=403)
    return web.json_response(request.app["session"].status())


async def api_devices(request):
    """Input devices, so the operator picks from a list rather than typing.

    Always this machine's own, which is the point of serving this page
    beside the microphone: the sound hardware is local knowledge, and a
    rented server has none of it to offer.
    """
    if not authorized(request):
        # The devices-and-error shape this route already returns, not the
        # ok-and-message shape: the page destructures the list and would
        # throw on a missing one.
        return web.json_response({"devices": [], "error": "Not authorized."},
                                 status=403)
    config = request.app["session"].config
    try:
        # In a thread: list_devices shells out to pactl with a five second
        # timeout, and this loop is also feeding the subtitles.
        devices = await asyncio.to_thread(capture.list_devices,
                                          config["capture"])
    except capture.CaptureError as exc:
        return web.json_response({"devices": [], "error": str(exc)})
    # Which one to pre-select, decided here rather than on the page: the
    # page sorts the list by name and so no longer knows the order the
    # backend offered them in. capture.choose_default holds the rule.
    return web.json_response({"devices": devices,
                              "suggested": capture.choose_default(devices)})


async def api_start(request):
    if not authorized(request):
        return web.json_response({"ok": False, "message": "Not authorized."},
                                 status=403)
    body = await read_body(request)
    ok, message = await request.app["session"].start(body.get("device"))
    return web.json_response({"ok": ok, "message": message})


async def api_stop(request):
    if not authorized(request):
        return web.json_response({"ok": False, "message": "Not authorized."},
                                 status=403)
    ok, message = await request.app["session"].stop()
    return web.json_response({"ok": ok, "message": message})


async def api_language(request):
    if not authorized(request):
        return web.json_response({"ok": False, "message": "Not authorized."},
                                 status=403)
    body = await read_body(request)
    ok, message = request.app["session"].set_override(
        body.get("language"), body.get("mode"))
    return web.json_response({"ok": ok, "message": message})


async def qr_code(request):
    """QR for the reader address, rendered on demand as SVG.

    Drawn from public_url rather than the bind address, because the address
    this server listens on is not the one a phone can reach, and carrying
    the reader token, because without it that address answers nothing. A
    sender holds neither, so it renders the address the server sent it when
    the control socket opened.

    Behind the operator token like the rest of this listener, now that the
    image encodes the reader token: a page in another tab of the operator's
    browser can point an <img> at a loopback port without asking anybody.
    """
    if not authorized(request):
        return web.Response(status=403, text="Not authorized.")
    url = request.app["session"].config.get("reader_url")
    if not url:
        return web.Response(status=404, text="No public_url configured.")
    try:
        import segno
    except ImportError:
        return web.Response(status=501, text="pip install segno")

    buffer = io.BytesIO()
    # Medium error correction, which survives a printed card getting scuffed.
    segno.make(url, error="m").save(
        buffer, kind="svg", scale=8, border=2, dark="#16181d", light="#ffffff")
    return web.Response(body=buffer.getvalue(),
                        content_type="image/svg+xml",
                        headers={"Cache-Control": "max-age=600"})


def build_operator_app(session, token):
    """The controls, on a listener the tunnel never sees.

    A separate listener rather than a check in the handlers: the tunnel
    daemon connects from this machine, so a public visitor and the
    operator both arrive from 127.0.0.1 and no check can tell them apart.
    A token of its own, not the reader's: the link the whole room is given
    must not be the link that can press Start.

    No Hub here, because none of these routes touches one. What the page
    knows about readers arrives inside the status.
    """
    app = web.Application()
    app["session"] = session
    app["token"] = token
    app.add_routes([
        web.get("/operator", operator_page),
        web.get("/api/status", api_status),
        web.get("/api/devices", api_devices),
        web.post("/api/start", api_start),
        web.post("/api/stop", api_stop),
        web.post("/api/language", api_language),
        web.get("/qr.svg", qr_code),
    ])
    return app
