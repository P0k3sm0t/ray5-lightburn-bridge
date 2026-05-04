#!/usr/bin/env python3
"""Interactive one-time camera calibration tool for Ray5 LightBurn Bridge."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import cv2
import numpy as np


WINDOW_NAME = "Calibration"
PREVIEW_NAME = "Ray5 Camera Calibration - Deskew Preview"
FINAL_SIZE = (1200, 1200)  # width, height


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def draw_overlay(image: np.ndarray, points: list[tuple[int, int]]) -> np.ndarray:
    canvas = image.copy()
    labels = ["TL", "TR", "BR", "BL"]

    for idx, (x, y) in enumerate(points):
        cv2.circle(canvas, (x, y), 6, (0, 255, 0), -1)
        cv2.putText(
            canvas,
            f"{labels[idx]} ({x},{y})",
            (x + 10, y - 10),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            (0, 255, 0),
            1,
            cv2.LINE_AA,
        )

    if len(points) >= 2:
        for i in range(len(points) - 1):
            cv2.line(canvas, points[i], points[i + 1], (255, 255, 0), 2)
    if len(points) == 4:
        cv2.line(canvas, points[3], points[0], (255, 255, 0), 2)

    instructions = [
        "Click 4 points: TL, TR, BR, BL",
        "R = Reset   S = Save + Exit   Q = Quit",
    ]
    for i, text in enumerate(instructions):
        cv2.putText(
            canvas,
            text,
            (20, 30 + (i * 24)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.65,
            (255, 255, 255),
            2,
            cv2.LINE_AA,
        )
    return canvas


def warp_preview(image: np.ndarray, points: list[tuple[int, int]]) -> np.ndarray:
    src_pts = np.array(points, dtype="float32")
    dst_pts = np.array(
        [
            [0, 0],
            [FINAL_SIZE[0] - 1, 0],
            [FINAL_SIZE[0] - 1, FINAL_SIZE[1] - 1],
            [0, FINAL_SIZE[1] - 1],
        ],
        dtype="float32",
    )
    matrix = cv2.getPerspectiveTransform(src_pts, dst_pts)
    return cv2.warpPerspective(image, matrix, FINAL_SIZE)


def update_config(config_path: Path, points: list[tuple[int, int]]) -> None:
    config = load_json(config_path)
    camera = config.setdefault("camera", {})
    deskew = camera.setdefault("deskew", {})
    deskew["enabled"] = True
    deskew["source_points"] = [[int(x), int(y)] for x, y in points]
    deskew["output_size"] = [FINAL_SIZE[0], FINAL_SIZE[1]]
    save_json(config_path, config)


def save_points_to_config(config_path: Path, points: list[tuple[int, int]]) -> None:
    update_config(config_path, points)


def main() -> int:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    base_path = Path(base_dir)
    image_path = os.path.join(base_dir, "camera_captures", "latest_raw.jpg")
    raw_path = Path(image_path)
    config_path = base_path / "config.json"

    print(f"[CALIBRATION] Loading image: {image_path}")

    if not raw_path.exists():
        print(f"[CALIBRATION] Missing raw image: {raw_path}")
        return 1
    if not config_path.exists():
        print(f"[CALIBRATION] Missing config file: {config_path}")
        return 1

    raw_image = cv2.imread(image_path)
    if raw_image is None:
        print("[ERROR] Failed to load latest_raw.jpg")
        return 1

    points: list[tuple[int, int]] = []

    def on_mouse(event: int, x: int, y: int, flags: int, param: Any) -> None:
        del flags, param
        if event == cv2.EVENT_LBUTTONDOWN and len(points) < 4:
            points.append((int(x), int(y)))
            print(f"[CALIBRATION] Point added: ({x}, {y})")

    cv2.namedWindow(WINDOW_NAME, cv2.WINDOW_NORMAL)
    cv2.resizeWindow(WINDOW_NAME, 1200, 900)
    cv2.namedWindow(PREVIEW_NAME, cv2.WINDOW_NORMAL)
    try:
        cv2.setWindowProperty(WINDOW_NAME, cv2.WND_PROP_TOPMOST, 1)
    except Exception:
        pass
    cv2.setMouseCallback(WINDOW_NAME, on_mouse)

    while True:
        overlay = draw_overlay(raw_image, points)
        cv2.imshow(WINDOW_NAME, overlay)

        if len(points) == 4:
            preview = warp_preview(raw_image, points)
            cv2.imshow(PREVIEW_NAME, preview)
        else:
            blank = np.zeros((FINAL_SIZE[1], FINAL_SIZE[0], 3), dtype=np.uint8)
            cv2.putText(
                blank,
                "Select 4 points to preview deskew",
                (30, FINAL_SIZE[1] // 2),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.8,
                (200, 200, 200),
                2,
                cv2.LINE_AA,
            )
            cv2.imshow(PREVIEW_NAME, blank)

        key = cv2.waitKeyEx(50)

        if key in (ord("s"), ord("S")):
            if len(points) != 4:
                print("[CALIBRATION] Need exactly 4 points before saving")
                continue
            print(f"[CALIBRATION] Selected points: {points}")
            save_points_to_config(config_path, points)
            print("[CALIBRATION] Saved deskew points to config.json")
            cv2.destroyAllWindows()
            raise SystemExit(0)

        elif key in (ord("q"), ord("Q"), 27):
            print("[CALIBRATION] Quit without saving")
            cv2.destroyAllWindows()
            raise SystemExit(0)

        elif key in (ord("r"), ord("R")):
            points.clear()
            print("[CALIBRATION] Reset points")

    cv2.destroyAllWindows()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
