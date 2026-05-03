# RELEASE NOTES

## v0.1.0

### Summary
Ray5 LightBurn bridge for an upload-to-Ray5 workflow.

### What Works

- LightBurn connection through the bridge.
- Job upload workflow to Ray5 storage.
- Ray5 touchscreen frame/run workflow.
- Basic console/jog/homing passthrough where supported.

### Known Limitations

- LightBurn PC Frame button is not supported in upload-to-Ray5 mode.
- Not a complete live USB-GRBL replacement.
- Ray5 touchscreen should be used for Frame and Run.

### Safety / Caution

- Always verify framing on the Ray5 touchscreen before running.
- Keep laser power low during testing.
- Do not rely on PC Frame for positioning.
