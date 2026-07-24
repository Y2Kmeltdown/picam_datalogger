# PiCam Recorder

A Raspberry Pi 5 camera application that:
- **Records video** to disk for a CLI-specified duration
- **Streams raw frames** to any process via a Unix domain socket
- **Reads startup parameters** from a JSON config file

---

## Eventide module

This repository is an installable [Eventide](https://github.com/Y2Kmeltdown/eventide)
module — `eventide-module.json` at the repo root advertises **two** supervisor
services that install together from the dashboard's MODULES tab:

| Service | What it does |
| ------- | ------------ |
| `pi_camera_datalogger` | Runs `camera_app.py` — segmented H.264 recording into `<recordings_dir>/picam/`, raw frames published to `/tmp/picam_frames.sock`. |
| `pi_mjpeg_server` | Runs `mjpeg_server.py` — consumes the frame socket and serves an MJPEG live stream on port `8082` (nginx proxies it at `/stream/picam/`). |

Install: open the dashboard → **MODULES** → paste this repo's URL → INSTALL.
The installer creates a dedicated Python venv for the module, installs
`requirements.txt` into it, registers both services with supervisord, and
starts them. `python3-picamera2` and `ffmpeg` are installed system-wide via
apt (declared in the manifest); the venv uses `--system-site-packages` so
`picamera2` stays importable.

Manual usage below still works standalone — under Eventide, supervisord runs
the programs for you.

---

## Requirements

### Hardware
- Raspberry Pi 5
- Raspberry Pi Camera Module (v2, v3, or HQ)

### Software
```
# System packages (Raspberry Pi OS Bookworm)
sudo apt install python3-picamera2 python3-opencv ffmpeg

# Python packages (if not using system picamera2)
pip install picamera2 opencv-python numpy
```

---

## Project layout

```
picam/
├── camera_app.py        # Main application
├── camera_config.json   # Startup configuration
├── frame_client.py      # Example frame consumer (OpenCV display)
└── README.md
```

---

## Usage

### Basic recording
```bash
# Record 30 seconds using default config
python camera_app.py --duration 30

# Record 10 seconds with a custom output name
python camera_app.py --duration 10 --output test_clip.mp4

# Use a different config file
python camera_app.py --duration 60 --config /etc/picam/prod.json

# Override the socket path at runtime
python camera_app.py --duration 30 --socket /run/picam.sock

# Verbose / debug logging
python camera_app.py --duration 30 --verbose
```

### All CLI flags
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config` | `-c` | `camera_config.json` | JSON config file path |
| `--duration` | `-d` | *(required)* | Recording length in seconds |
| `--output` | `-o` | `capture_<timestamp>.mp4` | Output filename |
| `--socket` | `-s` | value from config | Unix socket path override |
| `--verbose` | `-v` | off | Enable DEBUG logging |

### Consuming frames from another process
While `camera_app.py` is running, connect to the Unix socket and read
length-prefixed frames:

```bash
# Run the bundled OpenCV display client
python frame_client.py

# Or pipe raw bytes into ffmpeg from the socket
python - <<'EOF'
import socket, struct, sys
conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
conn.connect("/tmp/picam_frames.sock")
while True:
    (n,) = struct.unpack("<I", conn.recv(4))
    sys.stdout.buffer.write(conn.recv(n))
EOF | ffmpeg -f rawvideo -pix_fmt rgb24 -s 1920x1080 -i - output.mp4
```

---

## Wire protocol (Unix socket)

Every frame is prefixed with its byte length:

```
┌─────────────────────┬──────────────────────────┐
│  4 bytes (uint32 LE)│  N bytes (raw frame data) │
│  payload length     │  XRGB8888 pixels           │
└─────────────────────┴──────────────────────────┘
```

Multiple clients can connect simultaneously; each receives every frame.

---

## Configuration file (`camera_config.json`)

```jsonc
{
  "record_resolution": { "width": 1280, "height": 720 },
  "socket_resolution": { "width": 1280, "height": 720 },
  "framerate": 120,
  "format": "XRGB8888",       // lores (raw-frame) pixel format
  "record_format": "h264",
  "output_dir": "recordings",
  "socket_path": "/tmp/picam_frames.sock",
  "controls": {
    "AwbMode": 0,             // 0 = Auto white balance
    "AeEnable": true,
    "AnalogueGain": 1.0,
    "Brightness": 0.0,        // -1.0 … 1.0
    "Contrast": 1.0,
    "Saturation": 1.0,
    "Sharpness": 1.0,
    "ExposureTime": 0,        // microseconds; 0 = auto
    "NoiseReductionMode": 1   // 0=Off 1=Fast 2=HighQuality
  }
}
```

Any key omitted from the file falls back to the built-in default.
The `controls` section maps directly to Picamera2 control names.

---

## Run as a systemd service (optional)

```ini
# /etc/systemd/system/picam.service
[Unit]
Description=PiCam Recorder
After=network.target

[Service]
ExecStart=/usr/bin/python3 /opt/picam/camera_app.py --duration 3600 --config /etc/picam/config.json
WorkingDirectory=/opt/picam
Restart=on-failure
User=pi

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now picam
```
