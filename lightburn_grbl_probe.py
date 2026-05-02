#!/usr/bin/env python3
"""Minimal GRBL-over-TCP probe for observing LightBurn behavior."""

from __future__ import annotations

import argparse
import logging
import socket
import socketserver
import threading
import time
from dataclasses import dataclass

REALTIME_BYTES = {b"?", b"!", b"~", b"\x18"}


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


@dataclass
class ProbeState:
    mpos: list[float]
    wco: list[float]
    absolute_mode: bool
    units_mm: bool
    feed: float
    power: float
    laser_mode: str
    coordinate_system: str
    run_until: float
    run_message: str
    lock: threading.Lock

    @classmethod
    def create(cls) -> "ProbeState":
        return cls(
            mpos=[0.0, 0.0, 0.0],
            wco=[0.0, 0.0, 0.0],
            absolute_mode=True,
            units_mm=True,
            feed=0.0,
            power=0.0,
            laser_mode="M5",
            coordinate_system="G54",
            run_until=0.0,
            run_message="moving",
            lock=threading.Lock(),
        )


def configure_logging(log_path: str | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_path:
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
    )


def parse_word_values(command: str) -> dict[str, float]:
    values: dict[str, float] = {}
    idx = 0
    while idx < len(command):
        char = command[idx]
        if not char.isalpha():
            idx += 1
            continue
        key = char.upper()
        idx += 1
        start = idx
        while idx < len(command) and command[idx] in "+-.0123456789":
            idx += 1
        if start == idx:
            continue
        token = command[start:idx]
        try:
            values[key] = float(token)
        except ValueError:
            continue
    return values


def to_mm(value: float, units_mm: bool) -> float:
    return value if units_mm else value * 25.4


def synthetic_settings() -> str:
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


def synthetic_offsets(state: ProbeState) -> str:
    with state.lock:
        wco = list(state.wco)
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


def synthetic_modal(state: ProbeState) -> str:
    with state.lock:
        coord = state.coordinate_system
        mode = "G90" if state.absolute_mode else "G91"
        units = "G21" if state.units_mm else "G20"
        laser_mode = state.laser_mode
        feed = state.feed
        power = state.power
    return f"[GC:G0 {coord} G17 {units} {mode} G94 {laser_mode} M9 T0 F{feed:.3f} S{power:.3f}]\nok\n"


def synthetic_status(state: ProbeState) -> str:
    with state.lock:
        mpos = list(state.mpos)
        wco = list(state.wco)
        feed = state.feed
        power = state.power
        run_until = state.run_until
        run_message = state.run_message
    wpos = [mpos[i] - wco[i] for i in range(3)]
    status = "Run" if time.time() < run_until else "Idle"
    fields = (
        f"MPos:{mpos[0]:.3f},{mpos[1]:.3f},{mpos[2]:.3f}|"
        f"WPos:{wpos[0]:.3f},{wpos[1]:.3f},{wpos[2]:.3f}|"
        f"WCO:{wco[0]:.3f},{wco[1]:.3f},{wco[2]:.3f}|"
        f"FS:{feed:.0f},{power:.0f}"
    )
    if status == "Run":
        return f"<Run|{fields}|MSG:{run_message}>"
    return f"<Idle|{fields}>"


def apply_virtual_state(command: str, state: ProbeState) -> None:
    upper = command.strip().upper()
    values = parse_word_values(upper)
    with state.lock:
        if upper.startswith("G20"):
            state.units_mm = False
        elif upper.startswith("G21"):
            state.units_mm = True

        if upper.startswith("G90"):
            state.absolute_mode = True
        elif upper.startswith("G91"):
            state.absolute_mode = False

        if "F" in values:
            state.feed = to_mm(values["F"], state.units_mm)
        if "S" in values:
            state.power = values["S"]

        if upper.startswith("M3"):
            state.laser_mode = "M3"
        elif upper.startswith("M4"):
            state.laser_mode = "M4"
        elif upper.startswith("M5"):
            state.laser_mode = "M5"
            state.power = 0.0

        for code in ("G54", "G55", "G56", "G57", "G58", "G59"):
            if upper.startswith(code):
                state.coordinate_system = code
                break

        if upper == "$H":
            state.mpos = [0.0, 0.0, 0.0]
            state.wco = [0.0, 0.0, 0.0]
            state.absolute_mode = True
            state.run_until = time.time() + 2.0
            state.run_message = "homing"
            return

        if upper.startswith("G92"):
            for axis, idx in (("X", 0), ("Y", 1), ("Z", 2)):
                if axis in values:
                    state.wco[idx] = state.mpos[idx] - to_mm(values[axis], state.units_mm)
            return

        if upper.startswith(("G0", "G00", "G1", "G01", "G2", "G02", "G3", "G03")):
            for axis, idx in (("X", 0), ("Y", 1), ("Z", 2)):
                if axis not in values:
                    continue
                numeric = to_mm(values[axis], state.units_mm)
                if state.absolute_mode:
                    state.mpos[idx] = numeric + state.wco[idx]
                else:
                    state.mpos[idx] += numeric
            state.run_until = time.time() + 1.0
            state.run_message = "moving"


class ProbeHandler(socketserver.BaseRequestHandler):
    def setup(self) -> None:
        self.state: ProbeState = self.server.state  # type: ignore[attr-defined]
        self.log = logging.getLogger(f"Probe[{self.client_address[0]}:{self.client_address[1]}]")
        self.normalizer = LineNormalizer()
        self.send_startup_banner()
        self.log.info("Client connected")

    def handle(self) -> None:
        while True:
            try:
                data = self.request.recv(4096)
            except OSError:
                break
            if not data:
                break
            self.log.info("RX client %r", data)
            for byte in data:
                raw = bytes([byte])
                if raw in REALTIME_BYTES:
                    self.handle_realtime(raw)
                    continue
                text = raw.decode("utf-8", errors="ignore")
                for line in self.normalizer.feed(text):
                    self.log.info("Decoded client line %r", line)
                    self.handle_line(line)

    def finish(self) -> None:
        self.log.info("Client disconnected")

    def handle_realtime(self, raw: bytes) -> None:
        if raw == b"?":
            self.send_payload((synthetic_status(self.state) + "\n").encode("utf-8"))
            return
        if raw == b"\x18":
            self.log.info("Handling GRBL soft-reset realtime byte %r", raw)
            self.reset_state()
            self.send_startup_banner()
            return
        self.log.info("Ignoring realtime byte %r", raw)

    def handle_line(self, line: str) -> None:
        stripped = line.strip()
        upper = stripped.upper()
        if not stripped:
            return

        if upper == "$I":
            self.send_payload(b"[VER:1.1h.2026:LightBurn GRBL Probe]\n[OPT:VN,15,128]\nok\n")
            return
        if upper == "$$":
            self.send_payload(synthetic_settings().encode("utf-8"))
            return
        if upper == "$#":
            self.send_payload(synthetic_offsets(self.state).encode("utf-8"))
            return
        if upper == "$G":
            self.send_payload(synthetic_modal(self.state).encode("utf-8"))
            return

        apply_virtual_state(stripped, self.state)
        self.send_payload(b"ok\n")

    def send_payload(self, payload: bytes) -> None:
        self.log.info("TX probe %r", payload)
        self.request.sendall(payload)

    def send_startup_banner(self) -> None:
        self.send_payload(b"Grbl 1.1h ['$' for help]\n")

    def reset_state(self) -> None:
        with self.state.lock:
            self.state.mpos = [0.0, 0.0, 0.0]
            self.state.wco = [0.0, 0.0, 0.0]
            self.state.absolute_mode = True
            self.state.units_mm = True
            self.state.feed = 0.0
            self.state.power = 0.0
            self.state.laser_mode = "M5"
            self.state.coordinate_system = "G54"
            self.state.run_until = 0.0
            self.state.run_message = "moving"


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal GRBL-over-TCP probe for LightBurn")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument("--log-file", default="")
    args = parser.parse_args()

    configure_logging(args.log_file or None)
    state = ProbeState.create()
    with ThreadedTCPServer((args.host, args.port), ProbeHandler) as server:
        server.state = state  # type: ignore[attr-defined]
        logging.info("Starting LightBurn GRBL probe on %s:%s", args.host, args.port)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            logging.info("Stopping probe")


if __name__ == "__main__":
    main()
