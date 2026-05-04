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

### config.json Reference

- `ray5_host` / `ray5_port`: IP and port for the Ray5 HTTP command endpoint.
- `listen_host` / `listen_port`: local address and port LightBurn connects to.
- `protocol_type`: bridge transport mode (`http` for Ray5 ESP32 workflow).
- `connect_timeout_seconds` / `read_timeout_seconds`: request timing controls for upstream communication.
- `log_file` / `log_dir` / `log_retention_days`: bridge log filename, folder, and retention cleanup window.
- `newline`: line ending used by the bridge protocol layer.
- `status_poll_interval_seconds`: status poll pacing.
- `tcp.recv_chunk_size`: low-level socket receive buffer size.
- `websocket.url`: optional Ray5 sideband websocket URL. Leave empty to disable sideband.
- `pump_control.enabled`: master switch for pump handling features.
- `pump_control.passthrough_m8_m9`: when true, `M8` / `M9` from LightBurn are forwarded to Ray5.
- `pump_control.inject_on_upload_start`: optionally inject `M8` at start of uploaded job file.
- `pump_control.inject_off_upload_end`: optionally inject `M9` at end of uploaded job file.
- `camera.enabled`: enables camera capture pipeline.
- `camera.url`: RTSP (or HTTP stream) camera source URL.
- `camera.snapshot_url`: optional direct still-image endpoint (used first when set).
- `camera.capture_method`: capture strategy (`ffmpeg` recommended for RTSP).
- `camera.output_dir`: where camera outputs are written (for example `camera_captures`).
- `camera.filename_prefix`: prefix used for timestamped captures.
- `camera.keep_last`: how many timestamped captures to keep before cleanup.
- `camera.auto_capture_on_upload`: capture automatically when upload/spool event occurs.
- `camera.auto_capture_on_start`: capture once at bridge startup.
- `camera.open_capture_folder_on_start`: optionally open capture folder after startup capture.
- `camera.timeout_seconds`: camera capture timeout.
- `camera.deskew.enabled`: enables perspective correction.
- `camera.deskew.source_points`: 4-point calibration input in order TL, TR, BR, BL.
- `camera.deskew.output_size`: deskew output pixel size before postprocess.
- `camera.postprocess.enabled`: enables postprocess pipeline.
- `camera.postprocess.scale`: image scale factor before center-crop.
- `camera.postprocess.center_crop_margin`: optional center crop margin (pixels) before final resize.
- `camera.postprocess.rotate_degrees`: final rotation (`0`, `90`, `180`, `270`).
- `camera.postprocess.final_size`: final output resolution for `latest.jpg`.
- `camera.postprocess.dpi`: DPI metadata written to output image for LightBurn import sizing.
- `camera.postprocess.overlay_guides.enabled`: draw placement guides on final image.
- `camera.postprocess.overlay_guides.draw_center_cross`: draw center crosshair.
- `camera.postprocess.overlay_guides.draw_border`: draw border rectangle.
- `camera.postprocess.overlay_guides.draw_corner_marks`: draw corner guide marks.
- `http.url`: Ray5 command API endpoint.
- `http.method`: command API method (`GET` in this workflow).
- `http.synthetic_grbl_handshake`: GRBL-compatible handshake emulation for LightBurn.
- `http.startup_banner` / `http.synthetic_grbl_version_line` / `http.synthetic_grbl_options_line`: GRBL identity strings shown to LightBurn.
- `http.body_mode` / `http.command_field` / `http.body_template`: how bridge encodes outbound command requests.
- `http.debug_protocol`: verbose protocol logging toggle.
- `http.synthetic_limits`: synthetic GRBL machine limits reported to LightBurn.
- `http.synthetic_status_response`: fallback status line if upstream status is unavailable.
- `http.spool.*`: upload/spool behavior controls (job detection, upload URLs, file naming, run command template, frame passthrough options).
- `http.dual_mode.*`: live/upload mode tuning controls for command interpretation.

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
