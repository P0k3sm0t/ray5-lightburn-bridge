# Ray5 LaserBurn Capture Notes

Status: sanitized sample probe results are included for `192.168.1.50`, but no live LaserBurn packet capture is bundled in this repo. This file records the confirmed service layout plus the exact capture plan for the LaserBurn-specific handshake.

## Confirmed probe findings for 192.168.1.50

- Date: `2026-04-30`
- Open ports found:
  - `8848/tcp`: HTTP server returning `HTTP/1.1 200 OK`
  - `8849/tcp`: WebSocket-only service returning `HTTP/1.1 400 Bad Request` to plain HTTP with body `This is a Websocket server only!`
- Closed or nonresponsive during probe:
  - `80`, `23`, `8080`, `8847`, `8888`

### Port 8848 behavior

- `GET /` returns HTML with `Content-Encoding: gzip`
- Sending raw probe commands like `?`, `$$`, `$I`, `$G` over plain TCP returns:
  - `HTTP/1.1 400 Bad Request`
  - body: `Server unable to understand request due to invalid syntax`
- Conclusion:
  - `8848` is an HTTP service, not raw GRBL/Telnet

### Port 8849 behavior

- Plain HTTP request returns:
  - `HTTP/1.1 400 Bad Request`
  - `Server: arduino-WebSocket-Server`
  - body: `This is a Websocket server only!`
- WebSocket connections succeeded on:
  - `ws://192.168.1.50:8849/ws`
  - `ws://192.168.1.50:8849/websocket`
  - `ws://192.168.1.50:8849/socket`
  - `ws://192.168.1.50:8849/`
- Raw GRBL query strings sent as WebSocket text did not return GRBL responses. Observed responses included:
  - `CURRENT_ID:0`
  - `ACTIVE_ID:0`
  - `PING:0`
- Conclusion:
  - `8849` is a WebSocket endpoint, but not a plain raw-GRBL-over-WebSocket terminal as currently probed
  - LaserBurn likely performs a specific handshake, channel selection, or message framing before command traffic

## Capture setup

- Target filter: `ip.addr == RAY5_IP && (tcp.port == 8847 || tcp.port == 8848 || tcp.port == 8849)`
- Keep discovery commands limited to `?`, `$$`, `$I`, and `$G`
- Do not test `M3`, `M4`, `S`, or movement commands until manual approval
- Suggested tools:
  - Wireshark display filter: `ip.addr == RAY5_IP && (tcp.port == 8847 || tcp.port == 8848 || tcp.port == 8849)`
  - TShark capture example: `tshark -i <iface> -f "host RAY5_IP and tcp portrange 8847-8849" -w ray5-laserburn.pcapng`

## What to confirm from the live capture

- Destination port LaserBurn chooses first
- Whether the session starts with plain TCP text, HTTP, or TLS
- Whether LaserBurn sends `GET`, `POST`, or `Upgrade: websocket`
- Whether the Ray5 returns HTTP headers before command traffic
- Whether command traffic is plain text, JSON, or binary-framed
- Exact line endings: `\n` vs `\r\n`
- Any cookies, auth tokens, session ids, or CSRF-style headers
- Whether status requests use raw `?` or a wrapped message

## Extraction worksheet

### Connection summary

- Ray5 IP: `192.168.1.50`
- LaserBurn source IP:
- First destination port:
- Additional ports touched:
- Protocol classification:

### Handshake

- First client bytes:
- First server bytes:
- HTTP request line, if present:
- WebSocket path, if present:
- WebSocket subprotocol, if present:
- Cookies or auth headers:

### Command framing

- `?` on wire:
- `$$` on wire:
- `$I` on wire:
- `$G` on wire:
- Wrapped format:
- Line endings:

### Response framing

- Status response example:
- Config response example:
- Identity response example:
- Modal state response example:
- Whether responses already contain `ok` / `error:` / `ALARM:`:

## Bridge impact

- If LaserBurn uses raw TCP text:
  - Set `protocol_type` to `tcp`
  - Set `ray5_port` to the confirmed port
  - Adjust `newline` in `config.json` if the device insists on `\r\n`

- If LaserBurn upgrades to WebSocket:
  - Set `protocol_type` to `websocket`
  - Copy the exact `ws://` path into `config.json`
  - Add any required subprotocol string to `websocket.subprotocols`

- If LaserBurn posts HTTP:
  - Set `protocol_type` to `http`
  - Copy the exact endpoint, method, headers, and request body shape into `config.json`
  - Use `body_mode: "json"` if commands are wrapped as JSON

## Next live capture pass

1. Start Wireshark or TShark with the filter above.
2. Open LaserBurn and connect to the Ray5 over Wi-Fi.
3. Issue only `?`, `$$`, `$I`, and `$G`.
4. Save the pcap and fill the worksheet above.
5. Update `config.json` to match the observed transport.
