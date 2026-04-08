#!/usr/bin/env python3
"""
mjpeg_server.py — MJPEG HTTP server for camera_app.py

Reads raw XRGB8888 frames from the Unix domain socket produced by
camera_app.py, JPEG-encodes them with OpenCV, and serves a standard
multipart/x-mixed-replace MJPEG stream over HTTP using aiohttp.

Any browser or media player that supports MJPEG can consume the stream:
    http://<pi-ip>:8080/stream

Multiple clients are supported simultaneously; each gets an independent
copy of every frame.

Usage:
    python mjpeg_server.py
    python mjpeg_server.py --socket /tmp/picam_frames.sock \
                           --width 1920 --height 1080 \
                           --host 0.0.0.0 --port 8080 \
                           --quality 80
"""

import argparse
import asyncio
import logging
import signal
import socket
import struct
import sys
from asyncio import Queue
from typing import Optional

import cv2
import numpy as np
from aiohttp import web

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("mjpeg")

# MIME boundary used in the multipart stream
BOUNDARY = "picamframe"


# ---------------------------------------------------------------------------
# Frame distributor
# ---------------------------------------------------------------------------

class FrameDistributor:
    """
    Holds the most recent JPEG-encoded frame and notifies all active HTTP
    clients whenever a new one arrives via an asyncio.Event per client.

    Clients register a Queue; the reader task pushes every new JPEG into
    every registered queue so each client streams independently.
    """

    def __init__(self):
        self._queues: list[Queue[bytes]] = []
        self._lock = asyncio.Lock()
        self.latest_jpeg: Optional[bytes] = None

    async def register(self) -> Queue:
        q: Queue[bytes] = Queue(maxsize=2)
        async with self._lock:
            self._queues.append(q)
        log.info("MJPEG client registered (total: %d).", len(self._queues))
        return q

    async def unregister(self, q: Queue):
        async with self._lock:
            try:
                self._queues.remove(q)
            except ValueError:
                pass
        log.info("MJPEG client unregistered (remaining: %d).", len(self._queues))

    async def publish(self, jpeg: bytes):
        """Push a new JPEG to every registered client queue."""
        self.latest_jpeg = jpeg
        async with self._lock:
            dead = []
            for q in self._queues:
                try:
                    # Drop the oldest frame if the client is falling behind
                    # rather than blocking the whole pipeline
                    if q.full():
                        try:
                            q.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    q.put_nowait(jpeg)
                except Exception:
                    dead.append(q)
            for q in dead:
                self._queues.remove(q)


# ---------------------------------------------------------------------------
# Unix socket reader (runs in a thread pool executor)
# ---------------------------------------------------------------------------

def _recv_exactly(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Unix socket closed by camera_app.")
        buf.extend(chunk)
    return bytes(buf)


async def socket_reader(
    loop: asyncio.AbstractEventLoop,
    distributor: FrameDistributor,
    socket_path: str,
    width: int,
    height: int,
    jpeg_quality: int,
    reconnect_delay: float = 2.0,
):
    """
    Async task that connects to the camera Unix socket, reads raw frames,
    JPEG-encodes them on a thread-pool executor (to keep the event loop free),
    and publishes to the FrameDistributor.

    Reconnects automatically if the camera process is restarted.
    """
    while True:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            log.info("Connecting to Unix socket '%s' …", socket_path)
            await loop.run_in_executor(None, sock.connect, socket_path)
            log.info("Connected to camera socket.")

            while True:
                # Read length-prefixed frame (blocking I/O on executor thread)
                header = await loop.run_in_executor(None, _recv_exactly, sock, 4)
                (payload_len,) = struct.unpack("<I", header)
                raw = await loop.run_in_executor(None, _recv_exactly, sock, payload_len)

                # Encode to JPEG on executor so the event loop stays responsive
                jpeg = await loop.run_in_executor(
                    None, _encode_jpeg, raw, width, height, jpeg_quality
                )

                await distributor.publish(jpeg)

        except (ConnectionError, ConnectionRefusedError, FileNotFoundError, OSError) as exc:
            log.warning("Socket error: %s — retrying in %.1f s.", exc, reconnect_delay)
        finally:
            try:
                sock.close()
            except OSError:
                pass

        await asyncio.sleep(reconnect_delay)


def _encode_jpeg(raw: bytes, width: int, height: int, quality: int) -> bytes:
    """Convert raw XRGB8888 bytes → JPEG bytes (runs on executor thread)."""
    arr = np.frombuffer(raw, dtype=np.uint8).reshape((height, width, 4))
    # XRGB8888: byte order is [B, G, R, X] on little-endian ARM
    bgr = arr[:, :, :3]
    ok, buf = cv2.imencode(".jpg", bgr, [cv2.IMWRITE_JPEG_QUALITY, quality])
    if not ok:
        raise RuntimeError("JPEG encoding failed")
    return buf.tobytes()


# ---------------------------------------------------------------------------
# HTTP handlers
# ---------------------------------------------------------------------------

async def handle_stream(request: web.Request) -> web.StreamResponse:
    """Serve the MJPEG multipart stream to one client."""
    distributor: FrameDistributor = request.app["distributor"]

    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": f"multipart/x-mixed-replace; boundary={BOUNDARY}",
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Connection": "close",
        },
    )
    await response.prepare(request)

    q = await distributor.register()
    log.info("Stream started for %s.", request.remote)

    try:
        while True:
            jpeg = await asyncio.wait_for(q.get(), timeout=10.0)
            part = (
                f"--{BOUNDARY}\r\n"
                f"Content-Type: image/jpeg\r\n"
                f"Content-Length: {len(jpeg)}\r\n"
                f"\r\n"
            ).encode() + jpeg + b"\r\n"
            await response.write(part)
    except (asyncio.TimeoutError, ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        await distributor.unregister(q)
        log.info("Stream ended for %s.", request.remote)

    return response


async def handle_snapshot(request: web.Request) -> web.Response:
    """Serve the most recent frame as a single JPEG."""
    distributor: FrameDistributor = request.app["distributor"]
    if distributor.latest_jpeg is None:
        raise web.HTTPServiceUnavailable(reason="No frame available yet.")
    return web.Response(
        body=distributor.latest_jpeg,
        content_type="image/jpeg",
        headers={"Cache-Control": "no-cache"},
    )


async def handle_index(request: web.Request) -> web.Response:
    """Minimal HTML page that embeds the MJPEG stream."""
    host = request.host
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>PiCam Live</title>
  <style>
    body {{ margin: 0; background: #111; display: flex; flex-direction: column;
            align-items: center; justify-content: center; min-height: 100vh; }}
    img  {{ max-width: 100%; border: 2px solid #333; }}
    p    {{ color: #aaa; font-family: monospace; margin-top: 0.5em; font-size: 0.85em; }}
  </style>
</head>
<body>
  <img src="/stream" alt="PiCam live stream">
  <p>stream: http://{host}/stream &nbsp;|&nbsp; snapshot: http://{host}/snapshot</p>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def make_app(distributor: FrameDistributor) -> web.Application:
    app = web.Application()
    app["distributor"] = distributor
    app.router.add_get("/", handle_index)
    app.router.add_get("/stream", handle_stream)
    app.router.add_get("/snapshot", handle_snapshot)
    return app


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PiCam MJPEG server — serves camera frames over HTTP.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--socket",  default="/tmp/picam_frames.sock", metavar="PATH",
                        help="Unix socket path from camera_app.py.")
    parser.add_argument("--width",   type=int, default=1280, metavar="PX",
                        help="Frame width (must match camera_app config).")
    parser.add_argument("--height",  type=int, default=720, metavar="PX",
                        help="Frame height (must match camera_app config).")
    parser.add_argument("--host",    default="0.0.0.0", metavar="ADDR",
                        help="Address to bind the HTTP server.")
    parser.add_argument("--port",    type=int, default=8080, metavar="PORT",
                        help="TCP port for the HTTP server.")
    parser.add_argument("--quality", type=int, default=80, metavar="1-100",
                        help="JPEG compression quality.")
    parser.add_argument("--reconnect-delay", type=float, default=2.0, metavar="SECS",
                        help="Seconds to wait before retrying a dropped socket connection.")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable DEBUG logging.")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def _main(args: argparse.Namespace):
    distributor = FrameDistributor()
    app = make_app(distributor)

    loop = asyncio.get_running_loop()

    # Start the Unix socket reader as a background task
    reader_task = asyncio.create_task(
        socket_reader(
            loop=loop,
            distributor=distributor,
            socket_path=args.socket,
            width=args.width,
            height=args.height,
            jpeg_quality=args.quality,
            reconnect_delay=args.reconnect_delay,
        ),
        name="socket-reader",
    )

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, args.host, args.port)
    await site.start()

    log.info(
        "MJPEG server running at http://%s:%d/  (stream: /stream  snapshot: /snapshot)",
        args.host, args.port,
    )

    # Graceful shutdown on SIGINT / SIGTERM
    stop_event = asyncio.Event()

    def _request_stop(*_):
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _request_stop)

    await stop_event.wait()

    log.info("Shutting down …")
    reader_task.cancel()
    try:
        await reader_task
    except asyncio.CancelledError:
        pass
    await runner.cleanup()
    log.info("Done.")


def main():
    args = parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if not (1 <= args.quality <= 100):
        sys.exit("--quality must be between 1 and 100.")

    asyncio.run(_main(args))


if __name__ == "__main__":
    main()