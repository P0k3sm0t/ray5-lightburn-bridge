#!/usr/bin/env python3
"""Probe common Ray5 network services without issuing motion commands."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_PORTS = [80, 23, 8080, 8847, 8848, 8849, 8888]
SAFE_COMMANDS = ["?", "$$", "$I", "$G"]
WEBSOCKET_PATHS = ["/ws", "/websocket", "/socket", "/"]


def iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def preview(data: bytes, limit: int = 240) -> str:
    text = data.decode("utf-8", errors="replace")
    return text[:limit]


@dataclass
class Exchange:
    command: str
    newline: str
    sent_b64: str
    received_b64: str
    received_preview: str
    elapsed_ms: float


@dataclass
class PortReport:
    port: int
    open: bool
    classification: str
    notes: list[str] = field(default_factory=list)
    banner_preview: str = ""
    http: dict[str, Any] | None = None
    websocket: dict[str, Any] | None = None
    raw_grbl: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "port": self.port,
            "open": self.open,
            "classification": self.classification,
            "notes": self.notes,
            "banner_preview": self.banner_preview,
        }
        if self.http is not None:
            payload["http"] = self.http
        if self.websocket is not None:
            payload["websocket"] = self.websocket
        if self.raw_grbl is not None:
            payload["raw_grbl"] = self.raw_grbl
        return payload


def tcp_connect(host: str, port: int, timeout: float) -> socket.socket:
    sock = socket.create_connection((host, port), timeout=timeout)
    sock.settimeout(timeout)
    return sock


def capture_banner(host: str, port: int, timeout: float) -> bytes:
    try:
        with tcp_connect(host, port, timeout) as sock:
            try:
                return sock.recv(512)
            except socket.timeout:
                return b""
    except OSError:
        return b""


def probe_http(host: str, port: int, timeout: float) -> dict[str, Any] | None:
    request = (
        f"GET / HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"User-Agent: ray5-probe/1.0\r\n"
        f"Connection: close\r\n\r\n"
    ).encode("ascii")
    try:
        with tcp_connect(host, port, timeout) as sock:
            sock.sendall(request)
            chunks: list[bytes] = []
            started = time.monotonic()
            while time.monotonic() - started < timeout:
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    break
                if not chunk:
                    break
                chunks.append(chunk)
                if sum(len(item) for item in chunks) >= 16384:
                    break
        data = b"".join(chunks)
    except OSError:
        return None

    if not data.startswith(b"HTTP/"):
        return None

    head, _, body = data.partition(b"\r\n\r\n")
    lines = head.decode("iso-8859-1", errors="replace").splitlines()
    status_line = lines[0] if lines else ""
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            key, value = line.split(":", 1)
            headers[key.strip()] = value.strip()

    return {
        "status_line": status_line,
        "headers": headers,
        "body_preview": preview(body),
        "raw_response_b64": b64(data),
    }


def probe_raw_grbl(host: str, port: int, timeout: float) -> dict[str, Any] | None:
    exchanges: list[dict[str, Any]] = []
    positive_hits = 0

    for newline in ("\n", "\r\n"):
        for command in SAFE_COMMANDS:
            payload = (command + newline).encode("ascii")
            started = time.monotonic()
            try:
                with tcp_connect(host, port, timeout) as sock:
                    banner = b""
                    try:
                        banner = sock.recv(512)
                    except socket.timeout:
                        banner = b""
                    sock.sendall(payload)
                    time.sleep(0.2)
                    chunks: list[bytes] = []
                    stop = time.monotonic() + timeout
                    while time.monotonic() < stop:
                        try:
                            chunk = sock.recv(4096)
                        except socket.timeout:
                            break
                        if not chunk:
                            break
                        chunks.append(chunk)
                    data = banner + b"".join(chunks)
            except OSError:
                continue

            text = data.decode("utf-8", errors="replace").lower()
            if any(token in text for token in ("ok", "error", "alarm", "<idle", "<run", "$0=")):
                positive_hits += 1

            exchanges.append(
                Exchange(
                    command=command,
                    newline=repr(newline),
                    sent_b64=b64(payload),
                    received_b64=b64(data),
                    received_preview=preview(data),
                    elapsed_ms=round((time.monotonic() - started) * 1000, 1),
                ).__dict__
            )

    if not exchanges:
        return None

    return {
        "probable_grbl": positive_hits > 0,
        "positive_hits": positive_hits,
        "exchanges": exchanges,
    }


async def probe_websocket(host: str, port: int, timeout: float) -> dict[str, Any] | None:
    try:
        from websockets.asyncio.client import connect
    except Exception:
        return None

    findings: list[dict[str, Any]] = []

    for path in WEBSOCKET_PATHS:
        url = f"ws://{host}:{port}{path}"
        try:
            async with connect(url, open_timeout=timeout, close_timeout=1) as ws:
                path_result: dict[str, Any] = {"url": url, "accepted": True, "messages": []}
                for command in SAFE_COMMANDS:
                    started = time.monotonic()
                    await ws.send(command)
                    messages: list[str] = []
                    deadline = time.monotonic() + timeout
                    while time.monotonic() < deadline:
                        remaining = max(0.05, deadline - time.monotonic())
                        try:
                            incoming = await asyncio.wait_for(ws.recv(), timeout=remaining)
                        except TimeoutError:
                            break
                        except Exception:
                            break
                        if isinstance(incoming, bytes):
                            rendered = incoming.decode("utf-8", errors="replace")
                        else:
                            rendered = incoming
                        messages.append(rendered)
                        lowered = rendered.lower()
                        if any(token in lowered for token in ("ok", "error", "alarm", "<idle", "<run", "$0=")):
                            break
                    path_result["messages"].append(
                        {
                            "command": command,
                            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
                            "responses": messages,
                        }
                    )
                findings.append(path_result)
        except Exception as exc:
            findings.append({"url": url, "accepted": False, "error": str(exc)})

    accepted = [item for item in findings if item.get("accepted")]
    if not accepted:
        return None
    return {"paths": findings}


async def scan_port(host: str, port: int, timeout: float) -> PortReport:
    try:
        with tcp_connect(host, port, timeout):
            pass
    except OSError as exc:
        return PortReport(port=port, open=False, classification="closed", notes=[str(exc)])

    report = PortReport(port=port, open=True, classification="unknown")
    banner = capture_banner(host, port, timeout)
    report.banner_preview = preview(banner)

    http_result = probe_http(host, port, timeout)
    if http_result is not None:
        report.http = http_result
        report.notes.append("HTTP response detected.")
        report.classification = "http"

    raw_result = probe_raw_grbl(host, port, timeout)
    if raw_result is not None:
        report.raw_grbl = raw_result
        if raw_result.get("probable_grbl"):
            report.notes.append("GRBL-style responses detected on raw TCP.")
            report.classification = "raw_grbl_telnet"
        elif report.classification == "unknown":
            report.notes.append("Raw TCP accepted test commands but did not look like GRBL.")
            report.classification = "custom_tcp"

    websocket_result = await probe_websocket(host, port, timeout)
    if websocket_result is not None:
        report.websocket = websocket_result
        report.notes.append("WebSocket upgrade or message exchange succeeded.")
        if report.classification == "http":
            report.classification = "http_plus_websocket"
        elif report.classification == "unknown":
            report.classification = "websocket"

    if report.classification == "unknown":
        report.notes.append("Port is open but did not match the current HTTP, WebSocket, or GRBL heuristics.")
        report.classification = "custom_laserburn_or_unknown"

    return report


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host", help="Ray5 IP or hostname")
    parser.add_argument("--ports", default=",".join(str(port) for port in DEFAULT_PORTS))
    parser.add_argument("--timeout", type=float, default=1.5)
    parser.add_argument(
        "--output",
        default="ray5_probe_report.json",
        help="JSON report destination",
    )
    args = parser.parse_args()

    ports = [int(item.strip()) for item in args.ports.split(",") if item.strip()]
    results = [await scan_port(args.host, port, args.timeout) for port in ports]
    report = {
        "generated_at": iso_now(),
        "host": args.host,
        "ports": ports,
        "results": [item.to_dict() for item in results],
    }

    output_path = Path(args.output)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"\nSaved report to {output_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
