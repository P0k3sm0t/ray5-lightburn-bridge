# Ray5 LightBurn Bridge

Network bridge for using LightBurn with a Longer Ray5 over ESP32 HTTP APIs.

## Features

- LightBurn to Longer Ray5 network bridge
- ESP32 HTTP bridge workflow
- Upload/spool mode for jobs
- Live frame passthrough support
- RTSP camera capture
- Automatic startup camera capture
- `camera_captures/latest_raw.jpg` and `camera_captures/latest.jpg` outputs
- OpenCV deskew/perspective correction
- `rotate_degrees` postprocess support
- `final_size` postprocess support
- DPI metadata writing with Pillow
- Drag-and-drop LightBurn overlay workflow
- `calibrate_camera.py` four-point calibration tool
- M8/M9 air assist passthrough
- LightBurn layer-based Air Assist usage

## Install

```powershell
python -m pip install -r requirements.txt
```

## Configuration

Copy example config:

```powershell
copy config.example.json config.json
```

Edit `config.json`:

- set `ray5_host`
- set `camera.url`
- set `deskew.source_points` after calibration
- set `postprocess.scale`, `postprocess.final_size`, `postprocess.dpi`, `postprocess.rotate_degrees`

## Run

Run bridge:

```powershell
python ray5_lightburn_bridge.py
```

Run calibration:

```powershell
python calibrate_camera.py
```

## LightBurn Camera Overlay Workflow

1. Start bridge.
2. Bridge captures `camera_captures/latest_raw.jpg`.
3. Bridge writes corrected `camera_captures/latest.jpg`.
4. Drag `latest.jpg` into LightBurn.
5. Put image on a non-output layer.
6. Turn Output OFF for that image layer.
7. Lock the image.
8. Use image as placement reference.
9. Always Frame before Start.

## Air Assist

- Enable Air Assist only on desired LightBurn layers.
- LightBurn sends `M8` / `M9`.
- Bridge passes `M8` / `M9` through.
- Ray5 pump port toggles pump.

## Safety

- Never publish `config.json`.
- Do not commit passwords.
- Camera must stay fixed after calibration.
- Always frame before burning.
- Use proper relay/electrical safety for 120V pump control.
