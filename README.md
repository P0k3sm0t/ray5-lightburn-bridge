# Ray5 LightBurn Bridge

This bridge connects LightBurn to a Longer Ray5 over network APIs and is intended for an upload-to-Ray5 workflow.

## What This Bridge Does

- Accepts LightBurn GRBL-style connection on a local TCP port.
- Translates supported live commands (status, homing, basic jog/console moves) to Ray5 network endpoints.
- Buffers full job streams and uploads them to Ray5 storage/SD workflow.
- Keeps GRBL-compatible status responses for LightBurn polling.

## Intended Workflow (Recommended)

1. Create your job in LightBurn.
2. Send/start the job through this bridge so it uploads to the Ray5.
3. On the Ray5 touchscreen, select the uploaded file.
4. Use the Ray5 touchscreen Frame function.
5. Run the job from the Ray5 touchscreen.

## Important Limitation

WARNING: LightBurn PC Frame is not supported in upload-to-Ray5 mode. Use the Ray5 touchscreen Frame function after uploading the file.

This bridge is not a full live USB-GRBL replacement.

- Console/jog/homing may work depending on controller state.
- Production jobs should use upload plus touchscreen frame/run.

## Setup

1. Install Python 3.10+.
2. Install dependencies:
   - `pip install requests websockets`
3. Create a local config from template values (do not commit real machine values):
   - `RAY5_HOST=192.168.x.x`
   - `BRIDGE_PORT=9000`
4. Start the bridge:
   - `python ray5_lightburn_bridge.py --config config.json`
5. In LightBurn, add/connect a GRBL device to:
   - Host: `127.0.0.1`
   - Port: `9000`

## Debug Logging

Default mode is quiet.

- Enable verbose protocol logging via env var:
  - `DEBUG_PROTOCOL=true`
- Or in config:
  - `http.debug_protocol=true`

When enabled, raw protocol traffic is written to `lightburn_bridge_protocol.log`.

## Troubleshooting

- If LightBurn connects but jobs do not run, verify Ray5 IP/ports and websocket sideband connectivity.
- If status seems stale, confirm the bridge is receiving sideband status updates.
- If upload fails, check Ray5 storage availability and upload endpoint settings.
- If framing seems wrong, use Ray5 touchscreen Frame before running.

## Privacy / Safety

- Do not commit logs, IP addresses, local settings, or machine-specific config.
- Keep laser power low during validation.
- Always verify frame and origin on the Ray5 touchscreen before running.
