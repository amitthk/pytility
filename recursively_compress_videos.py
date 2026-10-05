#!/usr/bin/env python3
"""Recursively recompress MP4 files with ffmpeg.

A file is replaced only when the new encode is smaller. The larger original
is removed at that point. If encoding fails, or the new file is not smaller,
the original is left unchanged.

Uses the project venv (./venv) and ffmpeg. On macOS the VideoToolbox H.264
encoder is used; otherwise libx264 CRF 28.

Examples:
  ./venv/bin/python compress_mp4.py out_dir/local-ai*
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

_venv_python = Path(__file__).resolve().parent / "venv" / "bin" / "python"
if _venv_python.is_file() and Path(sys.executable).resolve() != _venv_python.resolve():
    os.execv(str(_venv_python), [str(_venv_python), *sys.argv])

try:
    from tqdm import tqdm
except ImportError:
    sys.exit(f"tqdm is missing. Run this with {_venv_python}")


def human_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "K", "M", "G", "T"):
        if size < 1024 or unit == "T":
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{num_bytes}B"


def require_tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        sys.exit(f"{name} was not found on PATH")
    return path


def videotoolbox_available(ffmpeg: str) -> bool:
    result = subprocess.run(
        [ffmpeg, "-hide_banner", "-encoders"],
        capture_output=True,
        text=True,
        check=False,
    )
    return "h264_videotoolbox" in result.stdout


def video_height(ffprobe: str, path: Path) -> int | None:
    result = subprocess.run(
        [
            ffprobe,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=height",
            "-of",
            "csv=p=0",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    line = result.stdout.strip().splitlines()
    if not line:
        return None
    value = line[0].strip().split(",")[0]
    if not value.isdigit():
        return None
    return int(value)


def bitrate_for_height(height: int | None) -> int:
    """Target video bitrate in kbps. Tuned for lecture recordings."""
    if height is None or height <= 0:
        return 1400
    if height <= 720:
        return 800
    if height <= 1080:
        return 1400
    if height <= 1440:
        return 2200
    return 3200


def encode_command(ffmpeg: str, src: Path, dst: Path, height: int | None, hardware: bool) -> list[str]:
    command = [
        ffmpeg,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(src),
    ]
    if hardware:
        kbps = bitrate_for_height(height)
        command += [
            "-c:v",
            "h264_videotoolbox",
            "-b:v",
            f"{kbps}k",
            "-maxrate",
            f"{kbps * 3 // 2}k",
            "-bufsize",
            f"{kbps * 2}k",
            "-pix_fmt",
            "yuv420p",
        ]
    else:
        command += [
            "-c:v",
            "libx264",
            "-crf",
            "28",
            "-preset",
            "veryfast",
            "-pix_fmt",
            "yuv420p",
        ]
    command += ["-c:a", "copy", "-movflags", "+faststart", "-f", "mp4", str(dst)]
    return command


def collect_mp4s(directories: list[Path]) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for directory in directories:
        for path in directory.rglob("*.mp4"):
            if not path.is_file() or path.name.endswith(".part"):
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            files.append(path)
    files.sort()
    return files


def compress_one(
    ffmpeg: str,
    ffprobe: str,
    path: Path,
    hardware: bool,
    dry_run: bool,
) -> tuple[str, int]:
    """Return (status, bytes_saved). Status is replaced, kept, failed, or dry-run."""
    height = video_height(ffprobe, path)
    old_size = path.stat().st_size
    kbps = bitrate_for_height(height) if hardware else None
    target = f"{kbps}k" if kbps is not None else "crf 28"

    if height is None:
        tqdm.write(f"failed (no video stream): {path}")
        return "failed", 0

    if dry_run:
        tqdm.write(f"dry-run {human_size(old_size)} height={height} target={target}  {path}")
        return "dry-run", 0

    tmp = path.with_name(path.name + ".part")
    try:
        result = subprocess.run(
            encode_command(ffmpeg, path, tmp, height, hardware),
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0 or not tmp.is_file():
            message = (result.stderr or result.stdout or "ffmpeg failed").strip()
            tqdm.write(f"failed: {path}\n{message}")
            return "failed", 0

        new_size = tmp.stat().st_size
        if new_size <= 0 or new_size >= old_size:
            tqdm.write(
                f"kept {human_size(old_size)} (encode was {human_size(new_size)})  {path}"
            )
            return "kept", 0

        os.replace(tmp, path)
        saved = old_size - new_size
        tqdm.write(f"{human_size(old_size)} -> {human_size(new_size)}  {path}")
        return "replaced", saved
    finally:
        if tmp.exists():
            tmp.unlink()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recursively recompress MP4 files and replace each original when the new file is smaller."
    )
    parser.add_argument(
        "directories",
        nargs="+",
        type=Path,
        help="Directories to search recursively for .mp4 files",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="List files and target settings without encoding",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    missing = [str(path) for path in args.directories if not path.is_dir()]
    if missing:
        print("Not a directory:", ", ".join(missing), file=sys.stderr)
        return 2

    ffmpeg = require_tool("ffmpeg")
    ffprobe = require_tool("ffprobe")
    hardware = videotoolbox_available(ffmpeg)
    files = collect_mp4s(args.directories)
    encoder = "h264_videotoolbox" if hardware else "libx264"
    print(f"Found {len(files)} mp4 file(s). Encoder: {encoder}")
    if not files:
        return 0

    counts = {"replaced": 0, "kept": 0, "failed": 0, "dry-run": 0}
    saved = 0
    try:
        for path in tqdm(files, unit="file"):
            status, bytes_saved = compress_one(ffmpeg, ffprobe, path, hardware, args.dry_run)
            counts[status] += 1
            saved += bytes_saved
    except KeyboardInterrupt:
        print("\nStopped. Files already replaced stay replaced.", file=sys.stderr)
        print_summary(counts, saved)
        return 130

    print_summary(counts, saved)
    return 1 if counts["failed"] else 0


def print_summary(counts: dict[str, int], saved: int) -> None:
    print(
        "replaced={replaced} kept={kept} failed={failed} dry-run={dry-run} saved={saved}".format(
            saved=human_size(saved),
            **counts,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
