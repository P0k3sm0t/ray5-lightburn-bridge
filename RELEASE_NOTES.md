# Ray5 LightBurn Bridge v1.1 – Camera Overlay, Deskew Calibration, and Air Assist

This release adds a full camera-assisted LightBurn workflow for the Longer Ray5 bridge.

## Highlights

- RTSP camera capture
- Automatic startup snapshot
- `latest_raw.jpg` and corrected `latest.jpg` output
- OpenCV perspective deskew
- Interactive 4-corner calibration tool
- Rotation and scaling postprocess
- DPI metadata for LightBurn drag-and-drop sizing
- Layer-based air assist support using `M8`/`M9`
- Cleaner `config.example.json` and safer `.gitignore`

## Notes

- `config.json` is intentionally ignored
- `camera_captures` and `logs` are ignored
- Copy `config.example.json` to `config.json` and edit locally
- Camera must remain fixed after calibration
