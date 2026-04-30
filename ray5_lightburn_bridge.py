#!/usr/bin/env python3
"""Local GRBL-over-TCP bridge for Longer Ray5 network interfaces."""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import queue
import socket
import socketserver
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

REALTIME_BYTES = {b"?", b"!", b"~", b"\x18"}
TERMINAL_PREFIXES = ("ok", "error", "alarm")
STATUS_PREFIXES = ("<", "[")


def load_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def configure_logging(log_path: Path) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


class BridgeProtocolError(RuntimeError):
    pass


class CommandTracker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current: threading.Event | None = None

    def begin(self) -> threading.Event:
        event = threading.Event()
        with self._lock:
            self._current = event
        return event

    def finish(self) -> None:
        with self._lock:
            if self._current is not None:
                self._current.set()
                self._current = None


class LineNormalizer:
    def __init__(self) -> None:
        self._buffer = ""

    def feed(self, chunk: str) -> list[str]:
        self._buffer += chunk.replace("\r\n", "\n").replace("\r", "\n")
        if "\n" not in self._buffer:
            return []
        parts = self._buffer.split("\n")
        self._buffer = parts.pop()
        return [part for part in parts if part]

    def flush(self) -> list[str]:
        if not self._buffer:
            return []
        leftover = self._buffer
        self._buffer = ""
        return [leftover]


class UpstreamBase:
    def __init__(self, config: dict[str, Any], handler: "BridgeHandler") -> None:
        self.config = config
        self.handler = handler
        self.newline = config.get("newline", "\n")
        self.read_timeout = float(config.get("read_timeout_seconds", 1.0))
        self.connect_timeout = float(config.get("connect_timeout_seconds", 3.0))
        self.status_poll_interval = float(config.get("status_poll_interval_seconds", 0.0))
        self.log = logging.getLogger(self.__class__.__name__)

    def open(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def send_line(self, line: str) -> None:
        raise NotImplementedError

    def send_realtime(self, raw: bytes) -> None:
        raise NotImplementedError


class TcpUpstream(UpstreamBase):
    def __init__(self, config: dict[str, Any], handler: "BridgeHandler") -> None:
        super().__init__(config, handler)
        self.sock: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def open(self) -> None:
        self.sock = socket.create_connection(
            (self.config["ray5_host"], int(self.config["ray5_port"])),
            timeout=self.connect_timeout,
        )
        self.sock.settimeout(self.read_timeout)
        self._stop.clear()
        self._thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self.sock is not None:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()
            self.sock = None

    def send_line(self, line: str) -> None:
        if self.sock is None:
            raise BridgeProtocolError("TCP upstream is not connected.")
        payload = (line + self.newline).encode("utf-8")
        self.log.info("TX line %r", line)
        self.sock.sendall(payload)

    def send_realtime(self, raw: bytes) -> None:
        if self.sock is None:
            raise BridgeProtocolError("TCP upstream is not connected.")
        self.log.info("TX realtime %r", raw)
        self.sock.sendall(raw)

    def _reader_loop(self) -> None:
        assert self.sock is not None
        while not self._stop.is_set():
            try:
                data = self.sock.recv(int(self.config.get("tcp", {}).get("recv_chunk_size", 4096)))
            except socket.timeout:
                continue
            except OSError:
                break
            if not data:
                break
            self.handler.forward_upstream_data(data)
        self.handler.on_upstream_closed()


class WebSocketUpstream(UpstreamBase):
    def __init__(self, config: dict[str, Any], handler: "BridgeHandler") -> None:
        super().__init__(config, handler)
        self.ws = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def open(self) -> None:
        from websockets.sync.client import connect

        ws_config = self.config.get("websocket", {})
        self.ws = connect(
            ws_config["url"],
            subprotocols=ws_config.get("subprotocols") or None,
            origin=ws_config.get("origin"),
            open_timeout=float(ws_config.get("open_timeout_seconds", 5.0)),
            close_timeout=float(ws_config.get("close_timeout_seconds", 1.0)),
        )
        self._stop.clear()
        self._thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self.ws is not None:
            try:
                self.ws.close()
            except Exception:
                pass
            self.ws = None

    def send_line(self, line: str) -> None:
        if self.ws is None:
            raise BridgeProtocolError("WebSocket upstream is not connected.")
        ws_config = self.config.get("websocket", {})
        outbound = line
        if ws_config.get("append_newline"):
            outbound += self.newline
        self.log.info("TX line %r", line)
        self.ws.send(outbound)

    def send_realtime(self, raw: bytes) -> None:
        if self.ws is None:
            raise BridgeProtocolError("WebSocket upstream is not connected.")
        text = raw.decode("latin-1")
        self.log.info("TX realtime %r", raw)
        self.ws.send(text)

    def _reader_loop(self) -> None:
        assert self.ws is not None
        while not self._stop.is_set():
            try:
                data = self.ws.recv(timeout=self.read_timeout)
            except TimeoutError:
                continue
            except Exception:
                break
            if data is None:
                break
            if isinstance(data, str):
                payload = data.encode("utf-8", errors="replace")
            else:
                payload = data
            self.handler.forward_upstream_data(payload)
        self.handler.on_upstream_closed()


class HttpUpstream(UpstreamBase):
    def __init__(self, config: dict[str, Any], handler: "BridgeHandler") -> None:
        super().__init__(config, handler)
        self.session = requests.Session()
        self.session.trust_env = False
        self.http_config = config.get("http", {})
        self.spool_config = self.http_config.get("spool", {})
        self._spool_lock = threading.Lock()
        self._spool_lines: list[str] = []
        self._spool_started_at: float | None = None
        self._spool_last_line_at: float | None = None
        self._spool_job_started = False
        self._spool_filename: str | None = None
        self._spool_run_started_at: float | None = None
        self._spool_upload_thread: threading.Thread | None = None
        self._spool_counter = 0
        self._spool_enabled = bool(self.spool_config.get("enabled", False))

    def open(self) -> None:
        return None

    def close(self) -> None:
        self.session.close()

    def send_line(self, line: str) -> None:
        if self._spool_enabled:
            if self._handle_spooled_line(line):
                self.handler.complete_current_command()
                return
        if line == "?":
            status_line = self._build_status_line()
            self.log.info("TX HTTP synthetic status for '?'")
            self.handler.forward_upstream_data((status_line + "\n").encode("utf-8"))
            self.handler.complete_current_command()
            return
        response_text = self._issue_http_command(line)
        self.handler.forward_upstream_data(response_text.encode("utf-8"))
        self.handler.complete_current_command()

    def send_realtime(self, raw: bytes) -> None:
        command = raw.decode("latin-1")
        if command == "?":
            status_line = self._build_status_line()
            self.log.info("TX HTTP synthetic realtime status for '?'")
            self.handler.forward_upstream_data((status_line + "\n").encode("utf-8"))
            return
        response_text = self._issue_http_command(command)
        self.handler.forward_upstream_data(response_text.encode("utf-8"))

    def _issue_http_command(self, command: str) -> str:
        method = self.http_config.get("method", "GET").upper()
        headers = dict(self.http_config.get("headers") or {})
        content_type = self.http_config.get("content_type")
        if content_type and "Content-Type" not in headers:
            headers["Content-Type"] = content_type

        body_mode = self.http_config.get("body_mode", "raw")
        params = None
        if body_mode == "json":
            payload: Any = {self.http_config.get("command_field", "command"): command}
            data = None
            json_payload = payload
        elif body_mode == "query_param":
            data = None
            json_payload = None
            params = {
                self.http_config.get("command_field", "commandText"): command,
            }
        else:
            data = self.http_config.get("body_template", "{command}").format(command=command)
            if self.http_config.get("append_newline_to_body"):
                data += self.newline
            json_payload = None

        self.log.info("TX HTTP %s %r", method, command)
        try:
            response = self.session.request(
                method=method,
                url=self.http_config["url"],
                headers=headers,
                params=params,
                data=data,
                json=json_payload,
                timeout=self.connect_timeout,
            )
        except requests.RequestException as exc:
            raise BridgeProtocolError(f"http request failed: {exc}") from exc

        if not response.ok:
            text = response.text.strip() or response.reason or f"HTTP {response.status_code}"
            return f"error: ray5 http {response.status_code}: {text}\n"

        text = response.text
        response_field = self.http_config.get("response_field")
        if response_field:
            parsed = response.json()
            text = str(parsed[response_field])

        lines = self._normalize_http_response(command, text)
        return "\n".join(lines) + "\n"

    def _handle_spooled_line(self, line: str) -> bool:
        stripped = line.strip()
        if not stripped:
            return True
        if self._is_passthrough_command(stripped):
            return False

        if not self._should_spool_command(stripped):
            self.log.info("TX HTTP passthrough non-job line %r", stripped)
            response_text = self._issue_http_command(stripped)
            self.handler.forward_upstream_data(response_text.encode("utf-8"))
            return True

        with self._spool_lock:
            if not self._spool_job_started:
                self._spool_job_started = True
                self._spool_started_at = time.time()
                self._spool_lines = []
            self._spool_last_line_at = time.time()
            self._spool_lines.append(stripped)
            job_line_count = len(self._spool_lines)
            self._ensure_spool_monitor_locked()

        self.log.info("Spool buffered line %r (%d buffered)", stripped, job_line_count)
        self.handler.forward_upstream_data(b"ok\n")
        return True

    def _ensure_spool_monitor_locked(self) -> None:
        if self._spool_upload_thread is not None and self._spool_upload_thread.is_alive():
            return
        self._spool_upload_thread = threading.Thread(target=self._spool_monitor_loop, daemon=True)
        self._spool_upload_thread.start()

    def _spool_monitor_loop(self) -> None:
        idle_seconds = float(self.spool_config.get("idle_seconds", 1.5))
        while True:
            time.sleep(0.2)
            with self._spool_lock:
                if not self._spool_job_started:
                    return
                last_line_at = self._spool_last_line_at
                lines = list(self._spool_lines)
                if last_line_at is None:
                    continue
                if time.time() - last_line_at < idle_seconds:
                    continue
                self._spool_job_started = False
                self._spool_started_at = None
                self._spool_last_line_at = None
                self._spool_lines = []
                break

        min_lines = int(self.spool_config.get("minimum_job_lines", 10))
        if len(lines) < min_lines:
            self.log.info("Discarded buffered job with %d lines as likely handshake chatter", len(lines))
            return

        try:
            filename = self._upload_and_start_spooled_job(lines)
        except Exception as exc:
            self.log.exception("Failed to upload/start spooled job")
            self.handler.forward_upstream_data(f"error: spool upload failed: {exc}\n".encode("utf-8"))
            with self._spool_lock:
                self._spool_filename = None
                self._spool_run_started_at = None
            return

        with self._spool_lock:
            self._spool_filename = filename
            self._spool_run_started_at = time.time()

    def _upload_and_start_spooled_job(self, lines: list[str]) -> str:
        filename = self._next_spool_filename()
        body = ("\n".join(lines) + "\n").encode("utf-8")
        compressed = gzip.compress(body, compresslevel=int(self.spool_config.get("gzip_level", 6)))

        upload_url = self.spool_config.get("upload_url")
        if not upload_url:
            raise BridgeProtocolError("spool upload_url is not configured")
        upload_path = self.spool_config.get("upload_path", "/")
        files_url = self.spool_config.get("files_url")
        run_command_template = self.spool_config.get("run_command_template", "$sd/runzip=/{filename}")

        self.log.info("Spool uploading %s (%d lines, %d bytes compressed)", filename, len(lines), len(compressed))
        try:
            response = self.session.post(
                upload_url,
                params={"path": upload_path},
                data={"path": upload_path, "size": str(len(compressed))},
                files={"file": (filename, compressed, "application/octet-stream")},
                timeout=max(self.connect_timeout, float(self.spool_config.get("upload_timeout_seconds", 30.0))),
            )
        except requests.RequestException as exc:
            raise BridgeProtocolError(f"upload request failed: {exc}") from exc
        if not response.ok:
            raise BridgeProtocolError(f"upload failed with HTTP {response.status_code}: {response.text.strip()}")

        if files_url:
            self.log.info("Spool listing files via %s", files_url)
            try:
                listing = self.session.get(
                    files_url,
                    params={"path": upload_path},
                    timeout=max(self.connect_timeout, 10.0),
                )
                if listing.ok and filename not in listing.text:
                    self.log.warning("Uploaded filename %s not found in immediate file listing", filename)
            except requests.RequestException:
                self.log.warning("File list verification failed", exc_info=True)

        run_command = run_command_template.format(filename=filename)
        self.log.info("Spool starting uploaded file with %r", run_command)
        start_response = self._issue_http_command(run_command)
        self.log.info("Spool start response %r", start_response.strip())
        return filename

    def _next_spool_filename(self) -> str:
        prefix = self.spool_config.get("filename_prefix", "lightburn_job")
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        with self._spool_lock:
            self._spool_counter += 1
            counter = self._spool_counter
        return f"{prefix}_{timestamp}_{counter:03d}.gc.gz"

    def _build_status_line(self) -> str:
        with self._spool_lock:
            if self._spool_job_started:
                buffered = len(self._spool_lines)
                return f"<Run|Buf:{buffered}|FS:0,0|MSG:buffering>"
            filename = self._spool_filename
            started_at = self._spool_run_started_at

        if filename and started_at:
            elapsed = time.time() - started_at
            return f"<Run|MPos:0.000,0.000,0.000|FS:0,0|SD:0.00,/{filename}|time:{elapsed:.3f}>"

        return self.http_config.get(
            "synthetic_status_response",
            "<Idle|MPos:0.000,0.000,0.000|FS:0,0>",
        )

    def _is_passthrough_command(self, command: str) -> bool:
        return command in ("!", "~", "\x18", "?")

    def _should_spool_command(self, command: str) -> bool:
        if command.startswith("$"):
            return False
        if command.startswith("[") or command.startswith("<"):
            return False
        if command in {"G0", "G00", "G1", "G01"}:
            return False
        return True

    def _normalize_http_response(self, command: str, text: str) -> list[str]:
        normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip("\n ")
        lines = [line.strip() for line in normalized.split("\n") if line.strip()]

        if not lines:
            if command.startswith("?"):
                return ["error: empty response to status query"]
            return ["ok"]

        mapped_lines: list[str] = []
        saw_terminal = False
        for line in lines:
            lowered = line.lower()
            if lowered == "error":
                mapped_lines.append("error: ray5 rejected command")
                saw_terminal = True
            else:
                mapped_lines.append(line)
                if lowered.startswith(TERMINAL_PREFIXES):
                    saw_terminal = True

        if not saw_terminal and not command.startswith("?"):
            mapped_lines.append("ok")
        return mapped_lines


UPSTREAM_FACTORIES = {
    "tcp": TcpUpstream,
    "websocket": WebSocketUpstream,
    "http": HttpUpstream,
}


@dataclass
class QueuedCommand:
    line: str
    done: threading.Event


class BridgeHandler(socketserver.BaseRequestHandler):
    def setup(self) -> None:
        self.config = self.server.config  # type: ignore[attr-defined]
        self.log = logging.getLogger(f"BridgeHandler[{self.client_address[0]}:{self.client_address[1]}]")
        self.tracker = CommandTracker()
        self.line_normalizer = LineNormalizer()
        self.client_lock = threading.Lock()
        self.queue: queue.Queue[QueuedCommand] = queue.Queue()
        self.closed = threading.Event()
        self.worker = threading.Thread(target=self._command_worker, daemon=True)
        protocol_type = self.config.get("protocol_type", "tcp")
        self.upstream = UPSTREAM_FACTORIES[protocol_type](self.config, self)
        self.upstream.open()
        self.worker.start()
        self.log.info("Client connected using upstream mode %s", protocol_type)

    def handle(self) -> None:
        while not self.closed.is_set():
            try:
                data = self.request.recv(4096)
            except OSError:
                break
            if not data:
                break
            self._consume_client_bytes(data)
        for line in self.line_normalizer.flush():
            event = threading.Event()
            self.queue.put(QueuedCommand(line=line, done=event))

    def finish(self) -> None:
        self.closed.set()
        try:
            self.upstream.close()
        except Exception:
            pass
        self.log.info("Client disconnected")

    def _consume_client_bytes(self, data: bytes) -> None:
        self.log.info("RX client %r", data)
        for byte_value in data:
            raw = bytes([byte_value])
            if raw in REALTIME_BYTES:
                self.upstream.send_realtime(raw)
                continue
            text = raw.decode("utf-8", errors="ignore")
            for line in self.line_normalizer.feed(text):
                event = threading.Event()
                self.queue.put(QueuedCommand(line=line, done=event))

    def _command_worker(self) -> None:
        while not self.closed.is_set():
            try:
                item = self.queue.get(timeout=0.2)
            except queue.Empty:
                continue
            tracker_event = self.tracker.begin()
            try:
                self.upstream.send_line(item.line)
            except Exception as exc:
                self.log.exception("Failed to send line upstream")
                self._send_to_client(f"error: upstream send failed: {exc}\n".encode("utf-8"))
                self.tracker.finish()
                item.done.set()
                continue

            if not tracker_event.wait(timeout=10.0):
                self.log.warning("Timed out waiting for completion of %r", item.line)
                self._send_to_client(b"error: bridge timeout waiting for upstream response\n")
                self.tracker.finish()
            item.done.set()

    def forward_upstream_data(self, payload: bytes) -> None:
        self.log.info("RX upstream %r", payload)
        self._send_to_client(self._normalize_upstream_payload(payload))
        text = payload.decode("utf-8", errors="replace").replace("\r\n", "\n").replace("\r", "\n")
        lines = [line for line in text.split("\n") if line]
        if any(self._is_command_completion_line(line) for line in lines):
            self.tracker.finish()

    def on_upstream_closed(self) -> None:
        if not self.closed.is_set():
            self.log.warning("Upstream connection closed")
            self.closed.set()
            try:
                self.request.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.request.close()

    def complete_current_command(self) -> None:
        self.tracker.finish()

    def _normalize_upstream_payload(self, payload: bytes) -> bytes:
        text = payload.decode("utf-8", errors="replace").replace("\r\n", "\n").replace("\r", "\n")
        if not text.endswith("\n"):
            text += "\n"
        return text.encode("utf-8")

    def _is_command_completion_line(self, line: str) -> bool:
        lowered = line.strip().lower()
        return lowered.startswith(TERMINAL_PREFIXES)

    def _send_to_client(self, payload: bytes) -> None:
        with self.client_lock:
            try:
                self.request.sendall(payload)
            except OSError:
                self.closed.set()


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], handler_cls: type[BridgeHandler], config: dict[str, Any]) -> None:
        self.config = config
        super().__init__(server_address, handler_cls)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json", help="Path to bridge configuration JSON")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_config(config_path)
    log_path = Path(config.get("log_file", "bridge.log"))
    configure_logging(log_path)

    listen_host = config.get("listen_host", "127.0.0.1")
    listen_port = int(config.get("listen_port", 9000))
    logging.info(
        "Starting Ray5 LightBurn bridge on %s:%s -> %s://%s:%s",
        listen_host,
        listen_port,
        config.get("protocol_type", "tcp"),
        config.get("ray5_host"),
        config.get("ray5_port"),
    )
    spool_config = config.get("http", {}).get("spool", {})
    logging.info(
        "HTTP spool mode: enabled=%s idle_seconds=%s minimum_job_lines=%s",
        spool_config.get("enabled", False),
        spool_config.get("idle_seconds", "n/a"),
        spool_config.get("minimum_job_lines", "n/a"),
    )

    with ThreadedTCPServer((listen_host, listen_port), BridgeHandler, config) as server:
        try:
            server.serve_forever(poll_interval=0.2)
        except KeyboardInterrupt:
            logging.info("Bridge interrupted by user")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
