# Ray5 LightBurn Bridge

Ray5 LightBurn Bridge is a Python utility that lets LightBurn connect to a Longer Ray5 over Wi-Fi as if it were a normal GRBL `Ethernet/TCP` laser.

It was created for Ray5 machines that expose HTTP and WebSocket services through the onboard ESP32 interface, but do not behave like a plain raw GRBL TCP controller. The bridge sits between LightBurn and the laser, translates traffic, and makes the Ray5 more usable from LightBurn's standard GRBL network mode.

## Features

- Accepts LightBurn connections as a local GRBL `Ethernet/TCP` device
- Forwards commands to the Ray5 over its HTTP interface
- Normalizes Ray5 responses into GRBL-style text such as `ok`, `error:`, and status lines
- Handles frequent LightBurn `?` polling safely
- Includes a probing tool for Ray5 port and protocol discovery
- Includes an experimental SD spool mode that uploads a job to the Ray5 and starts it from onboard storage instead of live-streaming every line

## Why this exists

Longer LaserBurn can connect to the Ray5 over Wi-Fi, but LightBurn and LaserGRBL often expect a more standard GRBL-over-network behavior. This project bridges that gap by adapting LightBurn's raw TCP expectations to the Ray5's actual network interface.

## Observed Ray5 behavior

The tested Ray5 exposed:

- HTTP command endpoint on port `8848`
- WebSocket service on port `8849`
- File upload endpoint for SD-style jobs
- File listing endpoint
- SD run command using `$sd/runzip=/filename.gc.gz`

Observed HTTP command format:

```text
GET /command?commandText=<urlencoded gcode>
```

Observed file upload format:

```text
POST /upload?path=/
multipart/form-data
```

Observed SD run command:

```text
$sd/runzip=/filename.gc.gz
```

## Project files

- `ray5_probe.py`
  - Scans likely Ray5 ports and records safe probe results
- `ray5_lightburn_bridge.py`
  - Main LightBurn bridge
- `config.json`
  - IP, ports, upstream mode, and spool settings
- `capture_notes.md`
  - Notes from traffic capture and protocol discovery
- `ray5_probe_report.json`
  - Example probe output
- `bridge.log`
  - Runtime bridge log

## LightBurn setup

Use these settings in LightBurn:

- Device: `GRBL`
- Connection: `Ethernet/TCP`
- Address: `127.0.0.1`
- Port: `9000`

If LightBurn is running on a different machine than the bridge, use the bridge computer's LAN IP instead of `127.0.0.1`.

## Running the bridge

```powershell
python .\ray5_lightburn_bridge.py --config .\config.json
```

## Configuration

Important settings:

- `ray5_host`: Ray5 IP address
- `ray5_port`: usually `8848`
- `listen_host`: usually `127.0.0.1`
- `listen_port`: usually `9000`
- `protocol_type`: currently `http`

### HTTP mode

The bridge sends each command to:

```text
http://<ray5-ip>:8848/command?commandText=<urlencoded command>
```

It also normalizes unusual Ray5 responses so LightBurn sees more GRBL-like behavior.

### Experimental spool mode

Spool mode is intended to reduce jitter during long jobs.

Instead of streaming every command live, it:

1. Buffers likely job lines from LightBurn
2. Waits for a short idle gap
3. Compresses the job to `.gc.gz`
4. Uploads it to the Ray5
5. Starts it with `$sd/runzip=/filename.gc.gz`

This is still experimental, but it is the closest match so far to LaserBurn's "send to machine, then run from SD" workflow.

## Safe discovery notes

Only safe discovery commands were used during protocol testing:

- `?`
- `$$`
- `$I`
- `$G`

No laser power or movement test commands were intentionally used during initial protocol discovery without approval.

## Known limitations

- LightBurn expects live GRBL behavior, so some status values may be synthetic
- Ray5 HTTP responses are not always identical to a normal GRBL TCP controller
- Spool mode is experimental and may need tuning for different job patterns
- Real-time controls in SD spool mode may not exactly match true live-stream GRBL behavior

## Status

This project is a working prototype and research tool. It is usable now, but still evolving as more of the Ray5 network protocol is mapped.

## License

MIT is a good default choice for this kind of utility project if you want a permissive open source license.
