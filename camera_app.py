#!/usr/bin/env python3
"""
PiCam Recorder
Raspberry Pi 5 camera application with:
  - Config-file driven startup parameters
  - Continuous segmented recording via a custom Output subclass that rolls to
    a new UTC-timestamped file at the next keyframe — the encoder and camera
    pipeline never stop between segments
  - Uninterrupted raw frame streaming over a Unix domain socket
"""

import argparse
import json
import logging
import os
import signal
import socket
import struct
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from picamera2 import Picamera2
from picamera2.encoders import H264Encoder, Quality
from picamera2.outputs import Output, FfmpegOutput

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("picam")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_CONFIG: dict = {
    "resolution": {"width": 1920, "height": 1080},
    "framerate": 30,
    "format": "XRGB8888",        # lores / raw-frame pixel format sent over socket
    "output_dir": "recordings",
    "socket_path": "/tmp/picam_frames.sock",
    "controls": {
        "AwbMode": 0,            # 0 = Auto white balance
        "AeEnable": True,
        "AnalogueGain": 1.0,
        "Brightness": 0.0,       # -1.0 … 1.0
        "Contrast": 1.0,
        "Saturation": 1.0,
        "Sharpness": 1.0,
        "ExposureTime": 0,       # microseconds; 0 = auto
        "NoiseReductionMode": 1, # 0=Off 1=Fast 2=HighQuality
    },
}


def load_config(path: str) -> dict:
    """Load JSON config and deep-merge over built-in defaults."""
    cfg = dict(DEFAULT_CONFIG)
    cfg["controls"] = dict(DEFAULT_CONFIG["controls"])

    config_path = Path(path)
    if not config_path.exists():
        log.warning("Config '%s' not found — using built-in defaults.", path)
        return cfg

    with config_path.open() as fh:
        user_cfg = json.load(fh)

    for key, value in user_cfg.items():
        if key.startswith("_"):
            continue
        if key == "controls" and isinstance(value, dict):
            cfg["controls"].update(value)
        else:
            cfg[key] = value

    log.info("Loaded config from '%s'.", path)
    return cfg


# ---------------------------------------------------------------------------
# Unix-socket frame server
# ---------------------------------------------------------------------------

class FrameSocketServer:
    """
    Broadcasts raw lores frames to any number of Unix-socket clients.

    Wire format per frame:
        ┌──────────────────────┬──────────────────────────┬────────────────────────┐
        │ 4 bytes (uint32 LE)  │  8 bytes (uint64 LE)     │  N bytes (raw pixels)  │
        │ payload length       │  capture timestamp (µs)  │  format = cfg["format"]│
        │                      │  microseconds since epoch│                        │
        └──────────────────────┴──────────────────────────┴────────────────────────┘

    The timestamp is recorded immediately after capture_array() returns,
    giving the closest possible approximation to when the Pi received the frame.
    """

    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self._clients: list[socket.socket] = []
        self._lock = threading.Lock()
        self._server: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._running = False

    def start(self):
        if Path(self.socket_path).exists():
            os.unlink(self.socket_path)
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(self.socket_path)
        self._server.listen(8)
        self._running = True
        self._accept_thread = threading.Thread(
            target=self._accept_loop, daemon=True, name="sock-accept"
        )
        self._accept_thread.start()
        log.info("Frame socket listening at '%s'.", self.socket_path)

    def stop(self):
        self._running = False
        if self._server:
            try:
                self._server.close()
            except OSError:
                pass
        with self._lock:
            for client in self._clients:
                try:
                    client.close()
                except OSError:
                    pass
            self._clients.clear()
        if Path(self.socket_path).exists():
            os.unlink(self.socket_path)
        log.info("Frame socket server stopped.")

    def _accept_loop(self):
        while self._running:
            try:
                self._server.settimeout(1.0)
                conn, _ = self._server.accept()
                with self._lock:
                    self._clients.append(conn)
                log.info("New frame client (total: %d).", len(self._clients))
            except socket.timeout:
                continue
            except OSError:
                break

    def send_frame(self, frame_bytes: bytes, timestamp_us: int):
        """
        Broadcast one frame to all connected clients.

        Args:
            frame_bytes:   Raw pixel data.
            timestamp_us:  Capture time as microseconds since the Unix epoch
                           (int(time.time() * 1_000_000) at the capture site).
        """
        if not self._clients:
            return
        # Pack length (4 bytes) + timestamp (8 bytes) + pixels
        header = struct.pack("<IQ", len(frame_bytes), timestamp_us)
        payload = header + frame_bytes
        dead: list[socket.socket] = []
        with self._lock:
            for client in self._clients:
                try:
                    client.sendall(payload)
                except (BrokenPipeError, OSError):
                    dead.append(client)
            for d in dead:
                self._clients.remove(d)
                try:
                    d.close()
                except OSError:
                    pass
        if dead:
            log.info("%d client(s) dropped (remaining: %d).", len(dead), len(self._clients))


# ---------------------------------------------------------------------------
# Segmenting Output — the key piece
# ---------------------------------------------------------------------------

def _utc_filename(output_dir: str) -> str:
    name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + ".h264"
    return os.path.join(output_dir, name)


class SegmentingOutput(Output):
    """
    A custom Picamera2 Output that writes raw H.264 to a file and rolls to a
    new UTC-timestamped file every `segment_duration` seconds.

    The roll happens inside outputframe() at the next keyframe after the
    deadline, so the encoder is never stopped and no bytes are lost between
    segments. Both the old file is cleanly closed and the new one opened
    within the same callback.
    """

    def __init__(self, output_dir: str, segment_duration: float):
        super().__init__()
        self.output_dir = output_dir
        self.segment_duration = segment_duration

        os.makedirs(output_dir, exist_ok=True)

        self._file = None
        self._segment_start: float = 0.0
        self._lock = threading.Lock()

        self._open_new_file()

    # ------------------------------------------------------------------
    def _open_new_file(self):
        """Close the current file (if any) and open the next one."""
        if self._file is not None:
            try:
                self._file.close()
            except OSError as exc:
                log.warning("Error closing segment: %s", exc)

        path = _utc_filename(self.output_dir)
        self._file = open(path, "wb")
        self._segment_start = time.monotonic()
        log.info("New segment → '%s'", path)

    # ------------------------------------------------------------------
    def outputframe(self, frame: bytes, keyframe: bool = True, timestamp=None):
        """Called by the encoder for every encoded frame."""
        with self._lock:
            # Roll on the first keyframe after the segment deadline
            elapsed = time.monotonic() - self._segment_start
            if keyframe and elapsed >= self.segment_duration:
                self._open_new_file()

            if self._file and not self._file.closed:
                self._file.write(frame)

    # ------------------------------------------------------------------
    def stop(self):
        """Flush and close the current segment file."""
        with self._lock:
            if self._file and not self._file.closed:
                self._file.close()
                self._file = None
        super().stop()


# ---------------------------------------------------------------------------
# Main recorder
# ---------------------------------------------------------------------------

class SegmentedRecorder:
    def __init__(self, cfg: dict, segment_duration: float):
        self.cfg = cfg
        self.segment_duration = segment_duration
        self.camera = Picamera2()
        self.socket_server = FrameSocketServer(cfg["socket_path"])

        self._frame_thread: threading.Thread | None = None
        self._running = False

    # ------------------------------------------------------------------
    def configure(self):
        res = self.cfg["resolution"]
        w, h = res["width"], res["height"]
        fps = self.cfg["framerate"]
        fmt = self.cfg["format"]

        log.info("Configuring: %dx%d @ %d fps | socket format: %s", w, h, fps, fmt)

        video_config = self.camera.create_video_configuration(
            main={"size": (w, h), "format": "RGB888"},  # main → H264 encoder
            lores={"size": (w, h), "format": fmt},       # lores → Unix socket
            controls={"FrameRate": fps},
        )
        self.camera.configure(video_config)

        controls = {k: v for k, v in self.cfg["controls"].items() if not k.startswith("_")}
        if controls:
            log.info("Applying camera controls: %s", controls)
            self.camera.set_controls(controls)

    # ------------------------------------------------------------------
    def _frame_loop(self):
        log.info("Frame capture loop started.")
        while self._running:
            try:
                frame = self.camera.capture_array("lores")
                # Timestamp recorded immediately after capture returns —
                # this is as close as we can get to the actual sensor receive time.
                timestamp_us = int(time.time() * 1_000_000)
                self.socket_server.send_frame(frame.tobytes(), timestamp_us)
            except Exception as exc:
                if self._running:
                    log.debug("Frame grab error: %s", exc)
        log.info("Frame capture loop stopped.")

    # ------------------------------------------------------------------
    def run(self):
        self.socket_server.start()
        self.camera.start()

        self._running = True
        self._frame_thread = threading.Thread(
            target=self._frame_loop, daemon=True, name="frame-cap"
        )
        self._frame_thread.start()

        output = SegmentingOutput(self.cfg["output_dir"], self.segment_duration)
        encoder = H264Encoder()
        self.camera.start_recording(encoder, output)

        log.info(
            "Recording continuously. Segment length: %.1f s. "
            "Press Ctrl+C or send SIGTERM to stop.",
            self.segment_duration,
        )

        try:
            while self._running:
                time.sleep(0.5)
        except KeyboardInterrupt:
            log.info("Keyboard interrupt — shutting down.")

        self._stop(output)

    # ------------------------------------------------------------------
    def _stop(self, output: SegmentingOutput):
        log.info("Stopping recorder …")
        self._running = False

        if self._frame_thread and self._frame_thread.is_alive():
            self._frame_thread.join(timeout=3)

        try:
            self.camera.stop_recording()
        except Exception as exc:
            log.warning("stop_recording: %s", exc)

        output.stop()
        self.camera.stop()
        self.socket_server.stop()
        log.info("Shutdown complete.")

    # ------------------------------------------------------------------
    def request_stop(self):
        """Thread-safe stop trigger (used by signal handler)."""
        self._running = False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "PiCam Recorder — continuously records fixed-length H.264 segments "
            "labelled with UTC timestamps while streaming raw frames over a "
            "Unix domain socket. Runs indefinitely until Ctrl+C or SIGTERM."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--config", "-c", default="camera_config.json", metavar="FILE",
                        help="Path to JSON configuration file.")
    parser.add_argument("--segment-duration", "-d", type=float, default=60.0, metavar="SECONDS",
                        help="Length of each recorded video segment in seconds.")
    parser.add_argument("--socket", "-s", default=None, metavar="PATH",
                        help="Override the Unix socket path from config.")
    parser.add_argument("--output-dir", "-o", default=None, metavar="DIR",
                        help="Override the output directory from config.")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable DEBUG logging.")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.segment_duration <= 0:
        log.error("--segment-duration must be a positive number.")
        sys.exit(1)

    cfg = load_config(args.config)

    if args.socket:
        cfg["socket_path"] = args.socket
    if args.output_dir:
        cfg["output_dir"] = args.output_dir

    recorder = SegmentedRecorder(cfg, segment_duration=args.segment_duration)

    def _on_sigterm(signum, frame):
        log.info("SIGTERM received.")
        recorder.request_stop()

    signal.signal(signal.SIGTERM, _on_sigterm)

    try:
        recorder.configure()
        recorder.run()
    except Exception as exc:
        log.exception("Fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()