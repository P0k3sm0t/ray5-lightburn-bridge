#!/usr/bin/env python3
"""Local GRBL-over-TCP bridge for Longer Ray5 network interfaces."""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import queue
import re
import socket
import socketserver
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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


def extract_command_word(command: str) -> str:
    stripped = command.strip()
    if not stripped:
        return ""
    token = stripped.split(None, 1)[0]
    return token.upper()


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
        self._command_exchange_lock = threading.Lock()
        self._spool_lock = threading.Lock()
        self._spool_lines: list[str] = []
        self._spool_started_at: float | None = None
        self._spool_last_line_at: float | None = None
        self._spool_job_started = False
        self._spool_has_job_marker = False
        self._spool_filename: str | None = None
        self._spool_run_started_at: float | None = None
        self._spool_last_upload_only = False
        self._spool_run_observed_active = False
        self._spool_upload_thread: threading.Thread | None = None
        self._spool_counter = 0
        self._synthetic_run_until: float | None = None
        self._synthetic_run_message: str = "moving"
        self._live_motion_active = False
        self._spool_enabled = bool(self.spool_config.get("enabled", False))
        self._virtual_mpos = [0.0, 0.0, 0.0]
        self._virtual_wco = [0.0, 0.0, 0.0]
        self._absolute_mode = True
        self._units_mm = True
        self._current_feed = 0.0
        self._current_power = 0.0
        self._laser_mode = "M5"
        self._coordinate_system = "G54"
        self._last_controller_state = "UNKNOWN"
        self._last_real_status_query_at = 0.0
        self._last_sd_status_value: str | None = None
        self._manual_sd_run_passive = False
        self._sideband_ws = None
        self._sideband_thread: threading.Thread | None = None
        self._sideband_stop = threading.Event()
        self._sideband_lines: queue.Queue[str] = queue.Queue()
        self._sideband_page_id = ""
        self._sideband_active_id = ""

    def classify_command(self, command: str) -> str:
        stripped = command.strip()
        if not stripped:
            return "empty"
        if stripped == "?":
            return "status"
        if stripped.startswith("$"):
            return "settings"
        if stripped.startswith("[") or stripped.startswith("<"):
            return "status_payload"
        if self._looks_like_live_motion_command(stripped):
            return "live_motion_candidate"
        if self._spool_enabled and self._should_spool_command(stripped):
            return "spool_job"
        return "passthrough"

    def open(self) -> None:
        self._open_sideband_websocket()
        self._send_startup_banner()
        return None

    def close(self) -> None:
        self._close_sideband_websocket()
        self.session.close()

    def send_line(self, line: str) -> None:
        if not self._ensure_manual_sd_run_ready_for_command(line):
            self.handler.complete_current_command()
            return
        if self._spool_enabled:
            if self._handle_spooled_line(line):
                self.handler.complete_current_command()
                return
        if line == "?":
            response_text = self._issue_status_query()
            self.handler.forward_upstream_data(response_text.encode("utf-8"))
            self.handler.complete_current_command()
            return
        synthetic_response = None if self._sideband_available() else self._synthetic_grbl_response(line)
        if synthetic_response is not None:
            self.log.info("TX synthetic GRBL response for %r", line)
            self.handler.forward_upstream_data(synthetic_response.encode("utf-8"))
            self.handler.complete_current_command()
            return
        response_text = self._issue_http_command(line)
        self.handler.forward_upstream_data(response_text.encode("utf-8"))
        self.handler.complete_current_command()

    def send_realtime(self, raw: bytes) -> None:
        command = raw.decode("latin-1")
        if command == "?":
            response_text = self._issue_status_query()
            self.handler.forward_upstream_data(response_text.encode("utf-8"))
            return
        if command == "\x18":
            self.log.info("Handling GRBL soft-reset realtime byte %r", raw)
            self._reset_virtual_state()
            self._send_startup_banner()
            return
        synthetic_response = None if self._sideband_available() else self._synthetic_grbl_response(command)
        if synthetic_response is not None:
            self.log.info("TX synthetic GRBL realtime response for %r", command)
            self.handler.forward_upstream_data(synthetic_response.encode("utf-8"))
            return
        response_text = self._issue_http_command(command)
        self.handler.forward_upstream_data(response_text.encode("utf-8"))

    def _issue_status_query(self) -> str:
        synthetic_reason = self._synthetic_status_query_reason()
        if synthetic_reason is not None:
            status_line = self._build_status_line()
            self.log.info("Using synthetic status for '?' while %s", synthetic_reason)
            return status_line + "\n"
        response_text = self._issue_http_command("?")
        with self._spool_lock:
            self._last_real_status_query_at = time.time()
        return response_text

    def _ensure_manual_sd_run_ready_for_command(self, command: str) -> bool:
        classification = self.classify_command(command)
        if classification in {"empty", "status", "settings", "status_payload"}:
            return True

        with self._spool_lock:
            manual_sd_active = self._manual_sd_run_passive and self._spool_run_started_at is not None

        if not manual_sd_active:
            return True

        self.log.info("Refreshing manual SD run state before handling command %r", command)
        try:
            self._issue_http_command("?")
        except BridgeProtocolError as exc:
            self.log.warning("Failed to refresh manual SD run state before %r: %s", command, exc)
            self.handler.forward_upstream_data(
                f"error: could not verify manual SD job state before command: {exc}\n".encode("utf-8")
            )
            return False

        with self._spool_lock:
            manual_sd_active = self._manual_sd_run_passive and self._spool_run_started_at is not None
            controller_state = self._last_controller_state
            active_filename = self._spool_filename

        if not manual_sd_active:
            self.log.info("Manual SD run is no longer active; proceeding with command %r", command)
            return True

        filename_text = f" {active_filename}" if active_filename else ""
        self.log.info(
            "Refusing command %r while manual SD job%s is still active (state=%s)",
            command,
            filename_text,
            controller_state,
        )
        self.handler.forward_upstream_data(
            f"error: manual SD job{filename_text} is still active; wait for it to finish before sending commands\n".encode(
                "utf-8"
            )
        )
        return False

    def _synthetic_status_query_reason(self) -> str | None:
        with self._spool_lock:
            if self._live_motion_active:
                return "live motion sequence is active"
            if self._spool_run_started_at is not None:
                if self._manual_sd_run_passive:
                    manual_poll_interval = max(
                        1.0,
                        float(self.spool_config.get("manual_sd_status_query_interval_seconds", 30.0)),
                    )
                    if time.time() - self._last_real_status_query_at < manual_poll_interval:
                        return "manual SD job is active in upload-only mode (bridge is mostly passive)"
                    return None
                poll_interval = max(0.1, float(self.spool_config.get("sd_status_query_interval_seconds", 5.0)))
                if time.time() - self._last_real_status_query_at < poll_interval:
                    return "SD job is active (throttling controller status polls)"
        return None

    def _issue_http_command(self, command: str) -> str:
        with self._command_exchange_lock:
            method = self.http_config.get("method", "GET").upper()
            url = self.http_config["url"]
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
                if self._sideband_page_id:
                    params["PAGEID"] = self._sideband_page_id
            else:
                data = self.http_config.get("body_template", "{command}").format(command=command)
                if self.http_config.get("append_newline_to_body"):
                    data += self.newline
                json_payload = None

            self.log.info(
                "TX HTTP request classification=%s method=%s url=%s params=%r data=%r json=%r command=%r",
                self.classify_command(command),
                method,
                url,
                params,
                data,
                json_payload,
                command,
            )
            stale_sideband_lines = self._drain_sideband_lines()
            if stale_sideband_lines:
                self.log.info("Discarded stale websocket sideband lines before %r: %r", command, stale_sideband_lines)
            try:
                response = self.session.request(
                    method=method,
                    url=url,
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

            self.log.info(
                "RX HTTP response status=%s reason=%r body=%r for command=%r",
                response.status_code,
                response.reason,
                text,
                command,
            )
            ws_lines = self._collect_sideband_response(command)
            if ws_lines:
                lines = self._normalize_sideband_lines(ws_lines)
                self.log.info("RX websocket sideband lines for %r: %r", command, lines)
            elif command.startswith("?"):
                status_line = self._build_status_line()
                self.log.info("Falling back to synthetic status for '?' because websocket produced no status line")
                lines = [status_line]
            else:
                lines = self._normalize_http_response(command, text)

            self._apply_status_lines(lines)
            if self._response_indicates_success(lines):
                self._apply_virtual_command_state(command)
            if self._response_indicates_success(lines) and self._should_mark_synthetic_motion(command):
                self._mark_synthetic_motion(command)
            return "\n".join(lines) + "\n"

    def _sideband_available(self) -> bool:
        return self._sideband_ws is not None

    def _open_sideband_websocket(self) -> None:
        ws_config = self.config.get("websocket", {})
        ws_url = ws_config.get("url")
        if not ws_url:
            return
        try:
            from websockets.sync.client import connect

            self._sideband_ws = connect(
                ws_url,
                subprotocols=ws_config.get("subprotocols") or ["arduino"],
                origin=ws_config.get("origin"),
                open_timeout=float(ws_config.get("open_timeout_seconds", 5.0)),
                close_timeout=float(ws_config.get("close_timeout_seconds", 1.0)),
            )
        except Exception:
            self.log.warning("Unable to open Ray5 websocket sideband", exc_info=True)
            self._sideband_ws = None
            return

        self._sideband_stop.clear()
        self._sideband_thread = threading.Thread(target=self._sideband_reader_loop, daemon=True)
        self._sideband_thread.start()
        self.log.info("Connected Ray5 websocket sideband to %s", ws_url)

    def _close_sideband_websocket(self) -> None:
        self._sideband_stop.set()
        if self._sideband_ws is not None:
            try:
                self._sideband_ws.close()
            except Exception:
                pass
            self._sideband_ws = None

    def _sideband_reader_loop(self) -> None:
        assert self._sideband_ws is not None
        normalizer = LineNormalizer()
        while not self._sideband_stop.is_set():
            try:
                payload = self._sideband_ws.recv(timeout=self.read_timeout)
            except TimeoutError:
                continue
            except Exception:
                break
            if payload is None:
                break

            if isinstance(payload, str):
                if self._process_sideband_control_message(payload):
                    continue
                chunk = payload
            else:
                chunk = payload.decode("utf-8", errors="replace")

            for line in normalizer.feed(chunk):
                self._queue_sideband_line(line)

        for line in normalizer.flush():
            self._queue_sideband_line(line)
        self.log.info("Ray5 websocket sideband disconnected")

    def _process_sideband_control_message(self, payload: str) -> bool:
        message = payload.strip()
        parts = message.split(":", 2)
        if len(parts) < 2:
            return False
        prefix = parts[0].upper()
        if prefix == "CURRENT_ID":
            self._sideband_page_id = parts[1]
            self.log.info("Ray5 websocket CURRENT_ID=%s", self._sideband_page_id)
            return True
        if prefix == "ACTIVE_ID":
            self._sideband_active_id = parts[1]
            self.log.info("Ray5 websocket ACTIVE_ID=%s", self._sideband_active_id)
            return True
        if prefix == "PING":
            return True
        if prefix == "MSG":
            self.log.info("Ray5 websocket %s payload=%r", prefix, message)
            self._handle_sideband_msg(message)
            return True
        if prefix in {"DHT", "ERROR"}:
            self.log.info("Ray5 websocket %s payload=%r", prefix, message)
            return True
        return False

    def _handle_sideband_msg(self, message: str) -> None:
        lowered = message.lower()
        with self._spool_lock:
            if "sd card job running" in lowered:
                if self._spool_last_upload_only and self._spool_filename:
                    self.log.info("Detected uploaded SD job %s has started from controller message", self._spool_filename)
                    self._spool_run_started_at = time.time()
                    self._manual_sd_run_passive = True
                    self._spool_last_upload_only = False
                if self._spool_run_started_at is not None:
                    self._spool_run_observed_active = True
                return

            completion_markers = (
                "sd card job complete",
            )
            if any(marker in lowered for marker in completion_markers):
                if self._spool_run_started_at is not None or self._spool_last_upload_only:
                    self.log.info("Detected SD job completion message from controller: %s", message)
                    self._clear_spool_job_tracking_locked()
                return

            generic_completion_markers = (
                "engraving complete",
                "engraving finished",
                "job complete",
                "job finished",
            )
            if any(marker in lowered for marker in generic_completion_markers):
                if self._spool_run_started_at is not None or self._spool_last_upload_only:
                    self.log.info(
                        "Ignoring generic completion-style controller message until SD status/idleness confirms completion: %s",
                        message,
                    )

    def _queue_sideband_line(self, line: str) -> None:
        stripped = line.strip()
        if not stripped:
            return
        if stripped.lower().startswith("grbl "):
            with self._spool_lock:
                if self._spool_run_started_at is not None or self._spool_last_upload_only:
                    self.log.warning(
                        "Controller startup banner arrived while SD job tracking was active; treating this as a controller reset and clearing SD job state"
                    )
                    self._clear_spool_job_tracking_locked()
        elif stripped.upper().startswith("ALARM:"):
            with self._spool_lock:
                if self._spool_run_started_at is not None or self._spool_last_upload_only:
                    self.log.warning(
                        "Controller alarm line %r arrived while SD job tracking was active; clearing SD job state",
                        stripped,
                    )
                    self._clear_spool_job_tracking_locked()
        if stripped.startswith("<"):
            self._apply_status_line(stripped)
            with self._spool_lock:
                passive_manual_sd_run = self._manual_sd_run_passive and self._spool_run_started_at is not None
            if passive_manual_sd_run:
                return
        self._sideband_lines.put(stripped)

    def _drain_sideband_lines(self) -> list[str]:
        drained: list[str] = []
        while True:
            try:
                drained.append(self._sideband_lines.get_nowait())
            except queue.Empty:
                break
        return drained

    def _collect_sideband_response(self, command: str) -> list[str]:
        if not self._sideband_available():
            return []

        is_status_query = command.strip() == "?"
        timeout_seconds = self.read_timeout if is_status_query else float(self.http_config.get("sideband_timeout_seconds", 1.5))
        deadline = time.time() + max(timeout_seconds, 0.1)
        lines: list[str] = []

        while time.time() < deadline:
            remaining = max(0.01, deadline - time.time())
            try:
                line = self._sideband_lines.get(timeout=min(0.25, remaining))
            except queue.Empty:
                continue

            if is_status_query:
                if line.startswith("<"):
                    return [line]
                continue

            if line.startswith("<") and not lines:
                self._apply_status_line(line)
                continue

            lines.append(line)
            if line.lower().startswith(TERMINAL_PREFIXES):
                break

        return lines

    def _normalize_sideband_lines(self, lines: list[str]) -> list[str]:
        merged_lines: list[str] = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith(",") and merged_lines and merged_lines[-1].startswith("[OPT:"):
                merged_lines[-1] = merged_lines[-1][:-1] + stripped
                continue
            merged_lines.append(stripped)

        normalized_lines: list[str] = []
        for line in merged_lines:
            if line.startswith("<"):
                normalized_lines.append(self._normalize_sideband_status_line(line))
            else:
                normalized_lines.append(self._normalize_sideband_text_line(line))
        return normalized_lines

    def _normalize_sideband_text_line(self, line: str) -> str:
        content = line.strip()
        if not (content.startswith("[") and content.endswith("]")):
            return content

        body = content[1:-1]
        if ":" not in body:
            return content

        key, raw_value = body.split(":", 1)
        key = key.strip()
        upper_key = key.upper()
        raw_value = raw_value.strip()

        coordinate_keys = {"G54", "G55", "G56", "G57", "G58", "G59", "G28", "G30", "G92"}
        if upper_key in coordinate_keys:
            coords = self._parse_status_floats(raw_value)
            if coords:
                padded = self._pad_axis_values(coords, fill_from=[0.0, 0.0, 0.0])
                return f"[{key}:{padded[0]:.3f},{padded[1]:.3f},{padded[2]:.3f}]"
            return content

        if upper_key == "PRB":
            coords_part, sep, suffix = raw_value.rpartition(":")
            parse_value = coords_part if sep else raw_value
            coords = self._parse_status_floats(parse_value)
            if coords:
                padded = self._pad_axis_values(coords, fill_from=[0.0, 0.0, 0.0])
                suffix_text = f":{suffix}" if sep else ""
                return f"[{key}:{padded[0]:.3f},{padded[1]:.3f},{padded[2]:.3f}{suffix_text}]"
            return content

        return content

    def _normalize_sideband_status_line(self, status_line: str) -> str:
        content = status_line.strip()
        if not (content.startswith("<") and content.endswith(">")):
            return status_line.strip()

        fields = [field.strip() for field in content[1:-1].split("|") if field.strip()]
        if not fields:
            return status_line.strip()

        state = fields[0]
        raw_mpos: list[float] | None = None
        raw_wpos: list[float] | None = None
        raw_wco: list[float] | None = None
        raw_feed: float | None = None
        raw_power: float | None = None
        extras: list[str] = []

        for field in fields[1:]:
            if ":" not in field:
                extras.append(field)
                continue

            key, raw_value = field.split(":", 1)
            key = key.strip().upper()
            raw_value = raw_value.strip()
            if key == "MPOS":
                coords = self._parse_status_floats(raw_value)
                if coords:
                    raw_mpos = coords
                continue
            if key == "WPOS":
                coords = self._parse_status_floats(raw_value)
                if coords:
                    raw_wpos = coords
                continue
            if key == "WCO":
                coords = self._parse_status_floats(raw_value)
                if coords:
                    raw_wco = coords
                continue
            if key == "FS":
                fs_values = self._parse_status_floats(raw_value)
                if fs_values:
                    raw_feed = fs_values[0]
                    if len(fs_values) > 1:
                        raw_power = fs_values[1]
                continue
            if key == "HEAP":
                continue
            extras.append(field)

        with self._spool_lock:
            mpos = list(self._virtual_mpos)
            wco = list(self._virtual_wco)
            feed = self._current_feed
            power = self._current_power

        if raw_mpos is not None:
            mpos = self._pad_axis_values(raw_mpos, fill_from=mpos)
        if raw_wco is not None:
            wco = self._pad_axis_values(raw_wco, fill_from=wco)

        if raw_wpos is not None:
            wpos = self._pad_axis_values(raw_wpos, fill_from=[mpos[idx] - wco[idx] for idx in range(3)])
            if raw_wco is None:
                wco = [mpos[idx] - wpos[idx] for idx in range(3)]
        else:
            wpos = [mpos[idx] - wco[idx] for idx in range(3)]

        if raw_feed is not None:
            feed = raw_feed
        if raw_power is not None:
            power = raw_power

        normalized_fields = [
            state,
            f"MPos:{mpos[0]:.3f},{mpos[1]:.3f},{mpos[2]:.3f}",
            f"WPos:{wpos[0]:.3f},{wpos[1]:.3f},{wpos[2]:.3f}",
            f"WCO:{wco[0]:.3f},{wco[1]:.3f},{wco[2]:.3f}",
            f"FS:{feed:.0f},{power:.0f}",
        ]

        seen_extras: set[str] = set()
        for extra in extras:
            normalized_extra = extra.strip()
            if not normalized_extra or normalized_extra in seen_extras:
                continue
            seen_extras.add(normalized_extra)
            normalized_fields.append(normalized_extra)

        return "<" + "|".join(normalized_fields) + ">"

    def _handle_spooled_line(self, line: str) -> bool:
        stripped = line.strip()
        if not stripped:
            return True
        self.log.info(
            "Parsed command word=%r classification=%s line=%r",
            extract_command_word(stripped),
            self.classify_command(stripped),
            stripped,
        )
        if self._is_passthrough_command(stripped):
            return False

        synthetic_response = None if self._sideband_available() else self._synthetic_grbl_response(stripped)
        if synthetic_response is not None:
            self.log.info("TX synthetic GRBL response for %r", stripped)
            self.handler.forward_upstream_data(synthetic_response.encode("utf-8"))
            return True

        frame_live_passthrough = bool(self.spool_config.get("frame_live_passthrough", True))
        allow_zero_power_frame = bool(self.spool_config.get("zero_power_frame_passthrough", True))
        run_live_now = False
        live_lines: list[str] = []

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
                self._spool_has_job_marker = False
            self._spool_last_line_at = time.time()
            self._spool_lines.append(stripped)
            self._spool_has_job_marker = self._spool_has_job_marker or self._is_job_marker_command(stripped)
            job_line_count = len(self._spool_lines)
            self._ensure_spool_monitor_locked()
            if (
                frame_live_passthrough
                and stripped.upper() == "M2"
                and self._is_live_frame_sequence(self._spool_lines, allow_zero_power_frame=allow_zero_power_frame)
            ):
                live_lines = list(self._spool_lines)
                self._spool_job_started = False
                self._spool_started_at = None
                self._spool_last_line_at = None
                self._spool_lines = []
                self._spool_has_job_marker = False
                run_live_now = True

        if run_live_now:
            self.log.info("Running %d buffered lines as live frame/jog sequence immediately on terminal marker", len(live_lines))
            try:
                self._run_live_motion_sequence(live_lines)
            except Exception as exc:
                self.log.exception("Failed to run immediate live frame/jog sequence")
                self.handler.forward_upstream_data(f"error: live motion failed: {exc}\n".encode("utf-8"))
            else:
                # Hold the terminal M2 acknowledgment until the live sequence actually finishes.
                self.handler.forward_upstream_data(b"ok\n")
            return True

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
        frame_idle_seconds = float(self.spool_config.get("frame_idle_seconds", 0.35))
        frame_live_passthrough = bool(self.spool_config.get("frame_live_passthrough", True))
        while True:
            time.sleep(0.2)
            with self._spool_lock:
                if not self._spool_job_started:
                    return
                last_line_at = self._spool_last_line_at
                lines = list(self._spool_lines)
                has_job_marker = self._spool_has_job_marker
                if last_line_at is None:
                    continue
                effective_idle_seconds = idle_seconds if has_job_marker or not frame_live_passthrough else frame_idle_seconds
                if time.time() - last_line_at < effective_idle_seconds:
                    continue
                self._spool_job_started = False
                self._spool_started_at = None
                self._spool_last_line_at = None
                self._spool_lines = []
                self._spool_has_job_marker = False
                break

        min_lines = int(self.spool_config.get("minimum_job_lines", 10))
        allow_zero_power_frame = bool(self.spool_config.get("zero_power_frame_passthrough", True))
        if frame_live_passthrough and self._is_live_frame_sequence(lines, allow_zero_power_frame=allow_zero_power_frame):
            self.log.info("Running %d buffered lines as live frame/jog sequence", len(lines))
            try:
                self._run_live_motion_sequence(lines)
            except Exception as exc:
                self.log.exception("Failed to run live frame/jog sequence")
                self.handler.forward_upstream_data(f"error: live motion failed: {exc}\n".encode("utf-8"))
            return

        if not has_job_marker and len(lines) < min_lines:
            self.log.info("Discarded buffered job with %d lines as likely handshake chatter", len(lines))
            return

        try:
            filename, started = self._upload_and_maybe_start_spooled_job(lines)
        except Exception as exc:
            self.log.exception("Failed to upload/start spooled job")
            self.handler.forward_upstream_data(f"error: spool upload failed: {exc}\n".encode("utf-8"))
            with self._spool_lock:
                self._spool_filename = None
                self._spool_run_started_at = None
                self._spool_last_upload_only = False
            return

        with self._spool_lock:
            self._spool_filename = filename
            self._spool_run_started_at = time.time() if started else None
            self._spool_last_upload_only = not started
            self._spool_run_observed_active = False
            self._manual_sd_run_passive = False

    def _upload_and_maybe_start_spooled_job(self, lines: list[str]) -> tuple[str, bool]:
        with self._spool_lock:
            controller_state = self._last_controller_state
        if controller_state == "ALARM":
            raise BridgeProtocolError("controller is in Alarm state; clear the alarm and home/unlock the Ray5 before uploading")

        start_after_upload = bool(self.spool_config.get("start_after_upload", True))
        upload_format = str(self.spool_config.get("upload_format", "gc_gz")).lower()
        if upload_format not in {"gc", "gc_gz", "both"}:
            raise BridgeProtocolError(f"unsupported upload_format: {upload_format}")

        body_lines = list(lines)
        if bool(self.spool_config.get("screen_compatible_rewrite", True)):
            body_lines = self._rewrite_for_screen_compatibility(body_lines)
        body = ("\n".join(body_lines) + "\n").encode("utf-8")

        upload_url = self.spool_config.get("upload_url")
        if not upload_url:
            raise BridgeProtocolError("spool upload_url is not configured")
        upload_path = self.spool_config.get("upload_path", "/")
        files_url = self.spool_config.get("files_url")
        run_command_template = self.spool_config.get("run_command_template", "$sd/runzip=/{filename}")

        uploads: list[tuple[str, bytes, str]] = []
        if upload_format in {"gc", "both"}:
            plain_filename = self._next_spool_filename(False)
            uploads.append((plain_filename, body, "plain"))
        if upload_format in {"gc_gz", "both"}:
            compressed_filename = self._next_spool_filename(True)
            compressed_bytes = self._make_gzip_bytes(body)
            uploads.append((compressed_filename, compressed_bytes, "compressed"))

        uploaded_filenames: list[str] = []
        for filename, upload_bytes, upload_kind in uploads:
            self._upload_spool_file(upload_url, upload_path, filename, upload_bytes, upload_kind)
            uploaded_filenames.append(filename)

        if files_url:
            self.log.info("Spool listing files via %s", files_url)
            try:
                listing = self.session.get(
                    files_url,
                    params={"path": upload_path},
                    timeout=max(self.connect_timeout, 10.0),
                )
                if listing.ok:
                    for filename in uploaded_filenames:
                        if filename not in listing.text:
                            self.log.warning("Uploaded filename %s not found in immediate file listing", filename)
            except requests.RequestException:
                self.log.warning("File list verification failed", exc_info=True)

        run_filename = self._pick_run_filename(uploaded_filenames)
        if not start_after_upload:
            self.log.info("Spool uploaded %s and left it on SD without starting", ", ".join(uploaded_filenames))
            return run_filename, False

        run_command = run_command_template.format(filename=run_filename)
        self.log.info("Spool starting uploaded file with %r", run_command)
        start_response = self._issue_http_command(run_command)
        self.log.info("Spool start response %r", start_response.strip())
        return run_filename, True

    def _make_gzip_bytes(self, body: bytes) -> bytes:
        data = bytearray(gzip.compress(body, compresslevel=int(self.spool_config.get("gzip_level", 6))))
        os_byte = self.spool_config.get("gzip_os_byte")
        if os_byte is not None and len(data) >= 10:
            data[9] = int(os_byte) & 0xFF
        return bytes(data)

    def _upload_spool_file(self, upload_url: str, upload_path: str, filename: str, upload_bytes: bytes, upload_kind: str) -> None:
        multipart_filename = self.spool_config.get("multipart_filename_mode", "bare")
        posted_filename = f"/{filename}" if multipart_filename == "leading_slash" else filename
        use_query_path = bool(self.spool_config.get("upload_query_path", True))
        query_params = {"path": upload_path} if use_query_path else None
        self.log.info("Spool uploading %s (%d bytes, %s, query_path=%s, multipart_name=%s)", filename, len(upload_bytes), upload_kind, use_query_path, posted_filename)
        response = None
        try:
            response = self.session.post(
                upload_url,
                params=query_params,
                data={"path": upload_path, "size": str(len(upload_bytes))},
                files={"file": (posted_filename, upload_bytes, "application/octet-stream")},
                timeout=max(self.connect_timeout, float(self.spool_config.get("upload_timeout_seconds", 30.0))),
                stream=True,
            )
        except requests.RequestException as exc:
            raise BridgeProtocolError(f"upload request failed: {exc}") from exc
        try:
            self.log.info(
                "Spool upload response status=%s reason=%r headers=%r",
                response.status_code,
                response.reason,
                dict(response.headers),
            )
            if not response.ok:
                raise BridgeProtocolError(f"upload failed with HTTP {response.status_code}: {response.reason}")
        finally:
            response.close()

    def _run_live_motion_sequence(self, lines: list[str]) -> None:
        self._mark_synthetic_motion("FRAME")
        with self._spool_lock:
            self._live_motion_active = True
        try:
            for line in lines:
                response_text = self._issue_http_command(line)
                self.log.info("Live motion response for %r: %r", line, response_text.strip())
        finally:
            with self._spool_lock:
                self._live_motion_active = False

    def _mark_synthetic_motion(self, command: str) -> None:
        duration = float(self.spool_config.get("synthetic_motion_status_seconds", 2.0))
        lower = command.strip().lower()
        message = "moving"
        if lower in {"$h", "g28", "g28.2"}:
            message = "homing"
        elif lower == "frame":
            message = "framing"
        with self._spool_lock:
            self._synthetic_run_until = time.time() + max(duration, 0.1)
            self._synthetic_run_message = message

    def _should_mark_synthetic_motion(self, command: str) -> bool:
        upper = command.strip().upper()
        return upper.startswith(("G0", "G00", "G1", "G01", "G2", "G02", "G3", "G03", "$H", "G28"))

    def _is_job_marker_command(self, command: str) -> bool:
        upper = command.upper()
        if upper.startswith("M3") or upper.startswith("M4") or upper.startswith("M5"):
            return True
        if upper.startswith("M2") or upper.startswith("M8") or upper.startswith("M9"):
            return True
        return self._extract_numeric_token(upper, 'S') is not None

    def _is_live_frame_sequence(self, lines: list[str], allow_zero_power_frame: bool = True) -> bool:
        if not lines:
            return False
        saw_motion = False
        saw_positive_power = False
        for line in lines:
            upper = line.upper().strip()
            if upper.startswith("$") or upper.startswith("[") or upper.startswith("<"):
                return False
            s_value = self._extract_numeric_token(upper, 'S')
            if s_value is not None and s_value > 0:
                saw_positive_power = True
            if upper.startswith(("M3", "M4")):
                if not allow_zero_power_frame:
                    return False
                continue
            if upper.startswith(("M5", "M8", "M9", "M2")):
                continue
            if upper.startswith(("G0", "G00", "G1", "G01", "G90", "G91", "G20", "G21", "G53", "G54", "G92", "F")):
                if upper.startswith(("G0", "G00", "G1", "G01")):
                    saw_motion = True
                continue
            return False
        if not saw_motion:
            return False
        if saw_positive_power:
            return False
        return True

    def _pick_run_filename(self, filenames: list[str]) -> str:
        for filename in filenames:
            if filename.endswith('.gc.gz'):
                return filename
        if filenames:
            return filenames[0]
        raise BridgeProtocolError('no uploaded filenames available to run')

    def _rewrite_for_screen_compatibility(self, lines: list[str]) -> list[str]:
        normalized: list[str] = []
        convert_m4_to_m3 = bool(self.spool_config.get("convert_m4_to_m3", False))
        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith(';'):
                continue
            compact = self._compact_gcode_spacing(line)
            if compact == 'M8':
                continue
            if convert_m4_to_m3 and compact == 'M4':
                compact = 'M3'
            normalized.append(compact)

        footer_start = len(normalized)
        for idx in range(len(normalized) - 1, -1, -1):
            if normalized[idx] == 'M9':
                footer_start = idx
                break
        if footer_start == len(normalized):
            for idx in range(len(normalized) - 1, -1, -1):
                if normalized[idx] in {'M5', 'M2'}:
                    footer_start = idx
                    break

        motion_source = normalized[:footer_start]
        footer_source = normalized[footer_start:]

        motion_lines: list[str] = []
        current_feed: str | None = None
        laser_mode = 'M3'
        max_s = 0.0
        xs: list[float] = []
        ys: list[float] = []
        finish_move: str | None = None
        absolute_mode = True
        units_scale = 1.0
        position = {'X': 0.0, 'Y': 0.0, 'Z': 0.0}

        for line in motion_source:
            upper = line.upper()
            if upper in {'M3', 'M4'}:
                laser_mode = upper
                continue
            if upper in {'G00 G17 G40 G21 G54', 'G00G17G40G21G54'}:
                units_scale = 1.0
                continue
            if upper == 'G20':
                units_scale = 25.4
                continue
            if upper == 'G21':
                units_scale = 1.0
                continue
            if upper == 'G90':
                absolute_mode = True
                continue
            if upper == 'G91':
                absolute_mode = False
                continue
            if upper in {'M9', 'M5', 'M2'}:
                continue

            line, feed_value = self._extract_feed_token(line)
            if feed_value is not None:
                current_feed = feed_value
            if not line:
                continue

            upper = line.upper()
            s_value = self._extract_numeric_token(upper, 'S')
            if s_value is not None:
                max_s = max(max_s, s_value)

            if upper.startswith('G0') or upper.startswith('G1'):
                words = self._parse_word_values(upper)
                motion_code = 'G0' if upper.startswith('G0') else 'G1'
                absolute_words: list[str] = [motion_code]
                touched_xy = False
                for axis in ('X', 'Y', 'Z'):
                    if axis not in words:
                        continue
                    delta_or_value = words[axis] * units_scale
                    if absolute_mode:
                        position[axis] = delta_or_value
                    else:
                        position[axis] += delta_or_value
                    absolute_words.append(f'{axis}{self._format_gcode_number(position[axis])}')
                    if axis in {'X', 'Y'}:
                        touched_xy = True
                if 'S' in words:
                    absolute_words.append(f'S{self._format_gcode_number(words["S"])}')
                if touched_xy:
                    xs.append(position['X'])
                    ys.append(position['Y'])
                if len(absolute_words) > 1:
                    motion_lines.append(''.join(absolute_words))
                continue

            motion_lines.append(self._compact_gcode_spacing(line))

        footer_seen = set()
        footer_lines: list[str] = []
        for line in footer_source:
            compact = self._compact_gcode_spacing(line)
            if compact in {'M9', 'M5', 'G90'} and compact not in footer_seen:
                footer_lines.append(compact)
                footer_seen.add(compact)
                continue
            if compact in {'G1S0', 'G1 S0'} and 'G1 S0' not in footer_seen:
                footer_lines.append('G1 S0')
                footer_seen.add('G1 S0')
                continue
            if compact.startswith('G0') and 'X0' in compact and 'Y0' in compact:
                finish_move = compact

        bounds_line = '; Bounds: unknown'
        if xs and ys:
            bounds_line = f'; Bounds: X{min(xs):g} Y{min(ys):g} to X{max(xs):g} Y{max(ys):g}'

        feed_comment = current_feed or '0'
        power_percent = int(round(max_s / 10.0)) if max_s else 0

        result = [
            '; Longer Laser APP 2.0 - LibGcode Engine',
            '; GRBL device profile, absolute coords',
            bounds_line,
            'G00 G17 G40 G21 G54',
            '; Layer C00',
            f'; Line @ {feed_comment} mm/min, {power_percent}% power',
            'G90',
            laser_mode,
        ]
        if current_feed:
            result.append(f'F{current_feed}')
        result.extend(motion_lines)
        result.append('')
        result.append('M9' if 'M9' in footer_seen else 'M9')
        result.append('G1 S0')
        result.append('M5' if 'M5' in footer_seen else 'M5')
        result.append('G90' if 'G90' in footer_seen else 'G90')
        result.append('; return to user-defined finish pos')
        result.append(finish_move or 'G0X0Y0')
        result.append('M2')
        return result

    def _extract_feed_token(self, line: str) -> tuple[str, str | None]:
        if 'F' not in line:
            return line, None
        idx = line.rfind('F')
        end = idx + 1
        while end < len(line) and line[end] in '0123456789.+-':
            end += 1
        token = line[idx + 1:end]
        if not token:
            return line, None
        new_line = (line[:idx] + line[end:]).strip()
        return new_line, token

    def _extract_numeric_token(self, line: str, key: str) -> float | None:
        idx = line.find(key)
        if idx == -1:
            return None
        start = idx + 1
        end = start
        while end < len(line) and line[end] in '0123456789.+-':
            end += 1
        token = line[start:end]
        if not token:
            return None
        try:
            return float(token)
        except ValueError:
            return None

    def _parse_word_values(self, command: str) -> dict[str, float]:
        values: dict[str, float] = {}
        for match in re.finditer(r"([A-Za-z])([+-]?(?:\d+(?:\.\d*)?|\.\d+))", command):
            key = match.group(1).upper()
            try:
                values[key] = float(match.group(2))
            except ValueError:
                continue
        return values

    def _to_mm(self, value: float) -> float:
        return value if self._units_mm else value * 25.4

    def _response_indicates_success(self, lines: list[str]) -> bool:
        for line in lines:
            lowered = line.strip().lower()
            if lowered.startswith("error") or lowered.startswith("alarm"):
                return False
        return True

    def _apply_virtual_command_state(self, command: str) -> None:
        upper = command.strip().upper()
        values = self._parse_word_values(upper)
        with self._spool_lock:
            if upper.startswith("G20"):
                self._units_mm = False
            elif upper.startswith("G21"):
                self._units_mm = True

            if upper.startswith("G90"):
                self._absolute_mode = True
            elif upper.startswith("G91"):
                self._absolute_mode = False

            if "F" in values:
                self._current_feed = self._to_mm(values["F"])
            if "S" in values:
                self._current_power = values["S"]

            if upper.startswith("M3"):
                self._laser_mode = "M3"
            elif upper.startswith("M4"):
                self._laser_mode = "M4"
            elif upper.startswith("M5"):
                self._laser_mode = "M5"
                self._current_power = 0.0

            for code in ("G54", "G55", "G56", "G57", "G58", "G59"):
                if upper.startswith(code):
                    self._coordinate_system = code
                    break

            if upper == "$H":
                self._virtual_mpos = [0.0, 0.0, 0.0]
                self._virtual_wco = [0.0, 0.0, 0.0]
                self._absolute_mode = True
                return

            if upper.startswith("G92"):
                for axis, idx in (("X", 0), ("Y", 1), ("Z", 2)):
                    if axis in values:
                        self._virtual_wco[idx] = self._virtual_mpos[idx] - self._to_mm(values[axis])
                return

            if not upper.startswith(("G0", "G00", "G1", "G01", "G2", "G02", "G3", "G03")):
                return

            for axis, idx in (("X", 0), ("Y", 1), ("Z", 2)):
                if axis not in values:
                    continue
                numeric_value = self._to_mm(values[axis])
                if self._absolute_mode:
                    self._virtual_mpos[idx] = numeric_value + self._virtual_wco[idx]
                else:
                    self._virtual_mpos[idx] += numeric_value

    def _apply_status_lines(self, lines: list[str]) -> None:
        for line in lines:
            if line.startswith("<"):
                self._apply_status_line(line)

    def _apply_status_line(self, status_line: str) -> None:
        content = status_line.strip()
        if not (content.startswith("<") and content.endswith(">")):
            return
        fields = content[1:-1].split("|")
        if not fields:
            return

        state = fields[0].strip().upper()
        updates: dict[str, list[float]] = {}
        feed_value: float | None = None
        power_value: float | None = None
        sd_value: str | None = None

        for field in fields[1:]:
            if ":" not in field:
                continue
            key, raw_value = field.split(":", 1)
            key = key.strip().upper()
            raw_value = raw_value.strip()
            if key in {"MPOS", "WPOS", "WCO"}:
                coords = self._parse_status_floats(raw_value)
                if coords:
                    updates[key] = coords
                continue
            if key == "FS":
                fs_values = self._parse_status_floats(raw_value)
                if fs_values:
                    feed_value = fs_values[0]
                    if len(fs_values) > 1:
                        power_value = fs_values[1]
                continue
            if key == "SD":
                sd_value = raw_value

        with self._spool_lock:
            self._last_controller_state = state
            if "MPOS" in updates:
                self._virtual_mpos = self._pad_axis_values(updates["MPOS"], fill_from=self._virtual_mpos)
            if "WCO" in updates:
                self._virtual_wco = self._pad_axis_values(updates["WCO"], fill_from=self._virtual_wco)
            elif "WPOS" in updates and "MPOS" in updates:
                mpos = self._pad_axis_values(updates["MPOS"], fill_from=self._virtual_mpos)
                wpos = self._pad_axis_values(updates["WPOS"], fill_from=[0.0, 0.0, 0.0])
                self._virtual_wco = [mpos[idx] - wpos[idx] for idx in range(3)]
            if feed_value is not None:
                self._current_feed = feed_value
            if power_value is not None:
                self._current_power = power_value
            if sd_value is not None:
                self._last_sd_status_value = sd_value

            sd_filename = self._parse_sd_status_filename(sd_value) if sd_value is not None else None
            if sd_filename is not None:
                current_filename = self._spool_filename.lstrip("/") if self._spool_filename else None
                if self._spool_last_upload_only and current_filename and sd_filename == current_filename:
                    self.log.info("Detected uploaded SD job %s has started from controller status", current_filename)
                    self._spool_run_started_at = time.time()
                    self._manual_sd_run_passive = True
                    self._spool_last_upload_only = False
                if self._spool_run_started_at is not None:
                    self._spool_run_observed_active = True
                    self._spool_filename = sd_filename
            elif self._spool_run_started_at is not None:
                if state.startswith("ALARM"):
                    self.log.warning("Detected SD job abort from controller alarm state %s", state)
                    self._clear_spool_job_tracking_locked()
                    return
                active_state = state not in {"IDLE"}
                active_output = (feed_value is not None and feed_value > 0.0) or (power_value is not None and power_value > 0.0)
                if active_state or active_output:
                    self._spool_run_observed_active = True
                elif self._spool_run_observed_active:
                    self.log.info("Detected SD job completion from idle controller status")
                    self._clear_spool_job_tracking_locked()

    def _parse_status_floats(self, raw_value: str) -> list[float]:
        values: list[float] = []
        for token in raw_value.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                values.append(float(token))
            except ValueError:
                continue
        return values

    def _pad_axis_values(self, values: list[float], fill_from: list[float]) -> list[float]:
        padded = list(fill_from[:3])
        for idx in range(min(3, len(values))):
            padded[idx] = values[idx]
        return padded

    def _parse_sd_status_filename(self, raw_value: str | None) -> str | None:
        if not raw_value:
            return None
        parts = [part.strip() for part in raw_value.split(",", 1)]
        if len(parts) != 2 or not parts[1]:
            return None
        return parts[1].lstrip("/")

    def _send_startup_banner(self) -> None:
        banner = self.http_config.get("startup_banner", "Grbl 1.1h ['$' for help]")
        self.log.info("TX synthetic startup banner %r", banner)
        self.handler.forward_upstream_data((str(banner) + "\n").encode("utf-8"))

    def _reset_virtual_state(self) -> None:
        with self._spool_lock:
            self._virtual_mpos = [0.0, 0.0, 0.0]
            self._virtual_wco = [0.0, 0.0, 0.0]
            self._absolute_mode = True
            self._units_mm = True
            self._current_feed = 0.0
            self._current_power = 0.0
            self._laser_mode = "M5"
            self._coordinate_system = "G54"
            self._last_controller_state = "UNKNOWN"
            self._synthetic_run_until = None
            self._synthetic_run_message = "moving"
            self._live_motion_active = False
            self._spool_job_started = False
            self._spool_started_at = None
            self._spool_last_line_at = None
            self._spool_lines = []
            self._spool_has_job_marker = False
            self._manual_sd_run_passive = False
            self._clear_spool_job_tracking_locked()

    def _clear_spool_job_tracking_locked(self) -> None:
        self._spool_filename = None
        self._spool_run_started_at = None
        self._spool_last_upload_only = False
        self._spool_run_observed_active = False
        self._last_sd_status_value = None
        self._manual_sd_run_passive = False

    def _synthetic_grbl_response(self, command: str) -> str | None:
        upper = command.strip().upper()
        if upper == "$$":
            return "\n".join([
                "$0=10",
                "$1=25",
                "$2=0",
                "$3=0",
                "$4=0",
                "$5=0",
                "$6=0",
                "$10=3",
                "$11=0.010",
                "$12=0.002",
                "$13=0",
                "$20=0",
                "$21=1",
                "$22=1",
                "$23=0",
                "$24=1000.000",
                "$25=3000.000",
                "$26=250",
                "$27=3.000",
                "$30=1000",
                "$31=0",
                "$32=1",
                "$100=80.000",
                "$101=80.000",
                "$102=400.000",
                "$110=12000.000",
                "$111=12000.000",
                "$112=1000.000",
                "$120=400.000",
                "$121=400.000",
                "$122=50.000",
                "$130=400.000",
                "$131=400.000",
                "$132=50.000",
                "ok",
            ]) + "\n"
        if upper == "$#":
            with self._spool_lock:
                wco = list(self._virtual_wco)
            return "\n".join([
                f"[G54:{wco[0]:.3f},{wco[1]:.3f},{wco[2]:.3f}]",
                "[G55:0.000,0.000,0.000]",
                "[G56:0.000,0.000,0.000]",
                "[G57:0.000,0.000,0.000]",
                "[G58:0.000,0.000,0.000]",
                "[G59:0.000,0.000,0.000]",
                "[G28:0.000,0.000,0.000]",
                "[G30:0.000,0.000,0.000]",
                f"[G92:{wco[0]:.3f},{wco[1]:.3f},{wco[2]:.3f}]",
                "[TLO:0.000]",
                "[PRB:0.000,0.000,0.000:0]",
                "ok",
            ]) + "\n"
        if upper == "$G":
            with self._spool_lock:
                mode = "G90" if self._absolute_mode else "G91"
                units = "G21" if self._units_mm else "G20"
                laser_mode = self._laser_mode
                coord = self._coordinate_system
                feed = self._current_feed
                power = self._current_power
            return f"[GC:G0 {coord} G17 {units} {mode} G94 {laser_mode} M9 T0 F{feed:.3f} S{power:.3f}]\nok\n"
        if upper == "$I":
            return "[VER:1.1h.2026:Ray5 LightBurn Bridge]\n[OPT:VN,15,128]\nok\n"
        return None

    def _compact_gcode_spacing(self, line: str) -> str:
        parts = line.split()
        if not parts:
            return line
        if parts[0] in {'G0', 'G00', 'G1', 'G01'} and len(parts) > 1:
            return parts[0] + ''.join(parts[1:])
        return ''.join(parts) if parts[0].startswith(('X', 'Y', 'S', 'F')) else line

    def _format_gcode_number(self, value: float) -> str:
        if abs(value) < 5e-7:
            value = 0.0
        text = f'{value:.6f}'.rstrip('0').rstrip('.')
        return text or '0'

    def _next_spool_filename(self, compress_upload: bool) -> str:
        prefix = str(self.spool_config.get("filename_prefix", "longer")).strip()
        filename_mode = str(self.spool_config.get("filename_mode", "short_counter")).strip().lower()
        separator = "" if not prefix or prefix.endswith(("_", "-")) else "_"
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        if compress_upload:
            extension = ".gc.gz"
        else:
            extension = str(self.spool_config.get("plain_extension", ".gc")).strip() or ".gc"
            if not extension.startswith("."):
                extension = "." + extension
        with self._spool_lock:
            self._spool_counter += 1
            counter = self._spool_counter
        if filename_mode == "timestamp_counter":
            return f"{prefix}{separator}{timestamp}_{counter:03d}{extension}"
        return f"{prefix}{separator}{counter:03d}{extension}"

    def _build_status_line(self) -> str:
        with self._spool_lock:
            if self._spool_job_started:
                buffered = len(self._spool_lines)
                return f"<Run|Buf:{buffered}|FS:0,0|MSG:buffering>"
            filename = self._spool_filename
            started_at = self._spool_run_started_at
            upload_only = self._spool_last_upload_only
            synthetic_run_until = self._synthetic_run_until
            synthetic_run_message = self._synthetic_run_message
            live_motion_active = self._live_motion_active
            mpos = list(self._virtual_mpos)
            wco = list(self._virtual_wco)
            feed = self._current_feed
            power = self._current_power
            sd_status_value = self._last_sd_status_value

        wpos = [mpos[idx] - wco[idx] for idx in range(3)]
        base_fields = (
            f"MPos:{mpos[0]:.3f},{mpos[1]:.3f},{mpos[2]:.3f}|"
            f"WPos:{wpos[0]:.3f},{wpos[1]:.3f},{wpos[2]:.3f}|"
            f"WCO:{wco[0]:.3f},{wco[1]:.3f},{wco[2]:.3f}|"
            f"FS:{feed:.0f},{power:.0f}"
        )

        if filename and started_at:
            elapsed = time.time() - started_at
            sd_status = f"0.00,/{filename}"
            if sd_status_value:
                parts = sd_status_value.split(",", 1)
                progress = parts[0].strip() or "0.00"
                status_filename = filename
                if len(parts) > 1 and parts[1].strip():
                    status_filename = parts[1].strip().lstrip("/")
                sd_status = f"{progress},/{status_filename}"
            return f"<Run|{base_fields}|SD:{sd_status}|time:{elapsed:.3f}>"

        if live_motion_active:
            return f"<Run|{base_fields}|MSG:{synthetic_run_message}>"

        if synthetic_run_until and time.time() < synthetic_run_until:
            return f"<Run|{base_fields}|MSG:{synthetic_run_message}>"

        if filename and upload_only:
            return f"<Idle|{base_fields}|SD:0.00,/{filename}|MSG:uploaded>"

        return f"<Idle|{base_fields}>"

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

    def _looks_like_live_motion_command(self, command: str) -> bool:
        upper = command.upper().strip()
        live_prefixes = (
            "G0", "G00", "G1", "G01",
            "G90", "G91", "G20", "G21", "G53", "G54", "G92",
            "M3", "M4", "M5", "F", "S",
        )
        return upper.startswith(live_prefixes)

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
                self.log.info("Decoded client line %r", line)
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
                classification = "unknown"
                if hasattr(self.upstream, "classify_command"):
                    classification = getattr(self.upstream, "classify_command")(item.line)
                self.log.info(
                    "Dispatching command word=%r classification=%s line=%r",
                    extract_command_word(item.line),
                    classification,
                    item.line,
                )
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
        "HTTP spool mode: enabled=%s start_after_upload=%s upload_format=%s screen_compatible_rewrite=%s convert_m4_to_m3=%s idle_seconds=%s minimum_job_lines=%s",
        spool_config.get("enabled", False),
        spool_config.get("start_after_upload", False),
        spool_config.get("upload_format", "gc_gz"),
        spool_config.get("screen_compatible_rewrite", True),
        spool_config.get("convert_m4_to_m3", False),
        spool_config.get("idle_seconds", "n/a"),
        spool_config.get("minimum_job_lines", "n/a"),
    )
    logging.info(
        "HTTP live frame passthrough: enabled=%s frame_idle_seconds=%s",
        spool_config.get("frame_live_passthrough", True),
        spool_config.get("frame_idle_seconds", 0.35),
    )

    with ThreadedTCPServer((listen_host, listen_port), BridgeHandler, config) as server:
        try:
            server.serve_forever(poll_interval=0.2)
        except KeyboardInterrupt:
            logging.info("Bridge interrupted by user")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
