#!/usr/bin/env python3
"""
frame_client.py – example consumer of raw frames from camera_app.py

Connects to the Unix socket, reads length-prefixed frames, and displays
them with OpenCV. Swap the display call for your own processing pipeline.

Usage:
    python frame_client.py
    python frame_client.py --socket /tmp/picam_frames.sock --width 1920 --height 1080
"""

import argparse
import socket
import struct
import sys

import cv2
import numpy as np


def recv_exactly(conn: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from the socket, blocking until available."""
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed by server.")
        buf.extend(chunk)
    return bytes(buf)


def main():
    parser = argparse.ArgumentParser(description="PiCam raw frame display client.")
    parser.add_argument("--socket", default="/tmp/picam_frames.sock", metavar="PATH")
    parser.add_argument("--width",  type=int, default=1920)
    parser.add_argument("--height", type=int, default=1080)
    args = parser.parse_args()

    print(f"Connecting to {args.socket} …")
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        conn.connect(args.socket)
    except FileNotFoundError:
        sys.exit(f"Socket '{args.socket}' not found. Is camera_app.py running?")

    print("Connected. Press 'q' to quit.")

    try:
        while True:
            # Read 4-byte little-endian length prefix
            header = recv_exactly(conn, 4)
            (payload_len,) = struct.unpack("<I", header)

            # Read the raw frame bytes
            raw = recv_exactly(conn, payload_len)

            # Interpret as XRGB8888 → drop the X channel → BGR for OpenCV
            arr = np.frombuffer(raw, dtype=np.uint8).reshape(
                (args.height, args.width, 4)
            )
            bgr = arr[:, :, :3]  # drop alpha/X channel

            print(bgr)

            #cv2.imshow("PiCam live", bgr)
            #if cv2.waitKey(1) & 0xFF == ord("q"):
                #break
    except (ConnectionError, struct.error) as exc:
        print(f"Stream ended: {exc}")
    finally:
        conn.close()
        #cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
