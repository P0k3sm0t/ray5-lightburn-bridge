# Ray5 LightBurn Bridge

Ray5 LightBurn Bridge is a Python utility that lets LightBurn talk to a Longer Ray5 over Wi-Fi as if it were a normal GRBL `Ethernet/TCP` laser.

It was built for Ray5 machines that expose HTTP and WebSocket services through the onboard ESP32 interface, but do not behave like a plain raw GRBL TCP controller. The bridge sits between LightBurn and the laser, translates network traffic, and makes the Ray5 much easier to use from LightBurn's standard GRBL network mode.

## Current status

The project now supports two practical workflows:

- live LightBurn control over a local GRBL `Ethernet/TCP` bridge
- upload-only SD spool mode that creates Ray5 touchscreen-friendly offline job files

The most useful proven workflow so far is upload-only mode. LightBurn sends the job to the bridge, the bridge uploads it to the Ray5 over Wi-Fi, and the file can then be selected from the Ray5 screen for border/frame and manual run.

## Why this exists

Longer LaserBurn can connect to the Ray5 over Wi-Fi, but LightBurn and LaserGRBL usually expect a more standard GRBL-over-network controller. This project bridges that gap by adapting LightBurn's raw TCP expectations to the Ray5's actual HTTP-based interface.

## What the bridge does

- accepts LightBurn connections as a GRBL `Ethernet/TCP` device
- forwards GRBL commands to the Ray5 HTTP interface
- normalizes Ray5 responses into GRBL-style text such as `ok`, `error:`, and status lines
- handles frequent LightBurn `?` polling safely
- includes a probe tool for Ray5 service discovery
- includes an SD spool mode for uploading jobs instead of live-streaming every line
- supports upload-only mode or upload-and-run mode

## Ray5 behavior observed

The tested Ray5 exposed:

- HTTP command endpoint on port `8848`
- WebSocket service on port `8849`
- file upload endpoint for SD-style jobs
- file listing endpoint
- SD run command using `$sd/runzip=/filename.gc.gz`

Observed command format:

```text
GET /command?commandText=<urlencoded gcode>
```

Observed upload format:

```text
POST /upload?path=/
multipart/form-data
```

Observed run command:

```text
$sd/runzip=/filename.gc.gz
```

## Important compatibility finding

The Ray5 touchscreen file browser appears to care a lot about the uploaded base filename.

Long timestamped names like:

```text
longer__20260430_175659_001.gc.gz
```

did not reliably appear on the machine screen.

Short LaserBurn-style names like:

```text
longer_001.gc.gz
```

did appear and were selectable from the Ray5 screen.

Because of that, the default spool filename mode now uses short counter-based names instead of long timestamped names.

## Current default offline settings

The default config is now aimed at Ray5 touchscreen selection:

- `http.spool.enabled: true`
- `http.spool.start_after_upload: false`
- `http.spool.upload_format: gc_gz`
- `http.spool.screen_compatible_rewrite: true`
- `http.spool.filename_prefix: longer`
- `http.spool.filename_mode: short_counter`

That means new offline uploads look like:

```text
longer_001.gc.gz
longer_002.gc.gz
```

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

## First-time configuration

The bundled `config.json` uses safe generic example values. Before using the bridge on a real machine, update these fields for your own network:

- `ray5_host`
  - set this to your Ray5's IP address
- `websocket.url`
  - usually `ws://YOUR_RAY5_IP:8849/ws`
- `http.url`
  - usually `http://YOUR_RAY5_IP:8848/command`
- `http.spool.upload_url`
  - usually `http://YOUR_RAY5_IP:8848/upload`
- `http.spool.files_url`
  - usually `http://YOUR_RAY5_IP:8848/files`

You may also want to adjust these local bridge settings:

- `listen_host`
  - use `127.0.0.1` if LightBurn runs on the same computer as the bridge
  - use your bridge computer's LAN IP if LightBurn runs on a different machine
- `listen_port`
  - default is `9000`
  - change it only if that port conflicts with something else on your system

### Example

If your Ray5 is at `192.168.1.77`, the important config values would look like:

```json
{
  "ray5_host": "192.168.1.77",
  "listen_host": "127.0.0.1",
  "listen_port": 9000,
  "websocket": {
    "url": "ws://192.168.1.77:8849/ws"
  },
  "http": {
    "url": "http://192.168.1.77:8848/command",
    "spool": {
      "upload_url": "http://192.168.1.77:8848/upload",
      "files_url": "http://192.168.1.77:8848/files"
    }
  }
}
```

After that, point LightBurn at:

- Address: `127.0.0.1`
- Port: `9000`

If LightBurn is on another computer, use the bridge computer's LAN IP instead of `127.0.0.1`.

## How spool mode works

Instead of streaming every command live, spool mode can:

1. buffer likely job lines from LightBurn
2. wait for a short idle gap
3. rewrite the job into a more Ray5/LaserBurn-friendly offline format
4. upload the file to the Ray5
5. either leave it on SD for manual selection or start it automatically

### Upload-only mode

This is the default mode now.

It is intended for the workflow:

1. send from LightBurn
2. walk to the Ray5
3. select the file on the touchscreen
4. frame/border
5. run manually

### Upload-and-run mode

If you want automatic start later, set:

```json
"start_after_upload": true
```

## Screen-compatible rewrite

For Ray5 touchscreen compatibility, the bridge rewrites uploaded offline jobs to look more like Longer LaserBurn output.

This currently includes:

- Longer-style comment header
- `M4` converted to `M3`
- removal of `M8`
- LaserBurn-style footer order
- short Ray5-friendly filenames

## Configuration notes

Important settings include:

- `ray5_host`: Ray5 IP address
- `ray5_port`: usually `8848`
- `listen_host`: usually `127.0.0.1`
- `listen_port`: usually `9000`
- `protocol_type`: currently `http`
- `http.spool.start_after_upload`: upload only vs auto-run
- `http.spool.upload_format`: `gc`, `gc_gz`, or `both`
- `http.spool.filename_prefix`: file name prefix
- `http.spool.filename_mode`: `short_counter` or `timestamp_counter`

### Upload format guidance

- `gc_gz`: best match for Ray5 touchscreen offline workflow
- `gc`: useful for ESP32 web page file playback
- `both`: uploads both variants

## Project files

- `ray5_probe.py`
  - scans likely Ray5 ports and records safe probe results
- `ray5_lightburn_bridge.py`
  - main LightBurn bridge
- `config.json`
  - IP, ports, bridge mode, and spool settings
- `capture_notes.md`
  - notes from protocol discovery and traffic capture
- `ray5_probe_report.json`
  - example probe output
- `bridge.log`
  - runtime bridge log

## Safe discovery notes

Only safe discovery commands were used during protocol testing:

- `?`
- `$$`
- `$I`
- `$G`

No laser power or movement test commands were intentionally used during initial protocol discovery without approval.

## Known limitations

- LightBurn expects live GRBL behavior, so some status values are synthetic
- Ray5 HTTP responses are not always identical to a normal GRBL TCP controller
- upload-and-run mode is still less battle-tested than upload-only mode
- the Ray5 touchscreen appears to be picky about offline file naming and formatting

## License

MIT
