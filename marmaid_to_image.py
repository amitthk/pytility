#!/usr/bin/env python3
"""
mermaid_to_image.py
────────────────────
Convert every .mmd file in a directory to SVG and/or PNG.

WHY TWO STAGES?
  Mermaid v11 renders node labels inside <foreignObject> HTML elements.
  cairosvg / Inkscape silently drop <foreignObject>, producing blank boxes.
  The only renderer that handles <foreignObject> correctly is Chromium
  (used by mmdc/puppeteer).  So this utility uses mmdc for BOTH SVG and PNG.

PIPELINE
  Stage 1 — SVG
    mmdc -i file.mmd -o file.svg -w <viewport>
    A large viewport (default 16 000 px) gives mermaid room to spread nodes
    without crowding them.  SVG output has no screenshot size limit.

  Stage 2 — PNG  (from the SVG produced in stage 1)
    Read the SVG viewBox to find the natural pixel width of the rendered diagram.
    Calculate a scale factor so the output PNG is at least <min-width> pixels
    wide while staying under Chromium's screenshot limit (~14 000 px).
    Re-invoke mmdc with the same .mmd source at that scale.

Usage
─────
  python mermaid_to_image.py [INPUT_DIR] [options]

Examples
  python mermaid_to_image.py ./diagrams
  python mermaid_to_image.py ./diagrams --out ./out
  python mermaid_to_image.py ./diagrams --min-width 6000
  python mermaid_to_image.py ./diagrams --width-map 01:10000,09:8000
  python mermaid_to_image.py ./diagrams --svg-only --keep-svg
  python mermaid_to_image.py ./diagrams --skip-existing --theme forest -v

Dependencies
────────────
  node / npx  with  @mermaid-js/mermaid-cli  (mmdc)
    npm install -g @mermaid-js/mermaid-cli
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from math import ceil
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# Chromium won't screenshot beyond this many pixels in either dimension.
CHROMIUM_MAX_PX = 14_000

# Viewport passed to mmdc for SVG rendering (generous space for node layout).
SVG_VIEWPORT = 16_000


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def find_mmdc() -> str:
    if shutil.which("mmdc"):
        return "mmdc"
    if shutil.which("npx"):
        return "npx"
    raise RuntimeError(
        "Neither 'mmdc' nor 'npx' found on PATH.\n"
        "Install with:  npm install -g @mermaid-js/mermaid-cli"
    )


def build_cmd(runner: str, args: list[str]) -> list[str]:
    if runner == "npx":
        return ["npx", "--yes", "@mermaid-js/mermaid-cli"] + args
    return [runner] + args


def strip_existing_init(src: str) -> str:
    return re.sub(r"^%%\{init:.*?%%\s*\n?", "", src.strip(), flags=re.DOTALL)


def make_init_header(theme: str, font_size: str, font_family: str) -> str:
    return (
        f"%%{{init: {{'theme': '{theme}', "
        f"'themeVariables': {{'fontSize': '{font_size}', "
        f"'fontFamily': '{font_family}'}}}}}}%%\n"
    )


def parse_width_map(raw: str) -> dict[str, int]:
    result: dict[str, int] = {}
    if not raw:
        return result
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" not in entry:
            continue
        prefix, width = entry.split(":", 1)
        try:
            result[prefix.strip()] = int(width.strip())
        except ValueError:
            print(f"  [warn] bad width-map entry ignored: {entry!r}")
    return result


def svg_viewbox_width(svg_path: Path) -> float:
    """Return the rendered width (px) from the SVG's viewBox attribute."""
    try:
        with open(svg_path, encoding="utf-8") as f:
            head = f.read(4096)
        # viewBox="minX minY width height"
        m = re.search(r'viewBox="[^"]*?\s+([0-9.]+)\s+[0-9.]+\s*"', head)
        if not m:
            # fallback: max-width style
            m2 = re.search(r'max-width:\s*([0-9.]+)px', head)
            if m2:
                return float(m2.group(1))
        if m:
            return float(m.group(1))
    except Exception:
        pass
    return 0.0


def run_mmdc(runner: str, mmdc_args: list[str], label: str) -> tuple[bool, str]:
    """Run mmdc and return (success, error_message)."""
    cmd = build_cmd(runner, mmdc_args)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        return False, err[:300]
    return True, ""


def write_patched_mmd(source_path: Path, init_header: str) -> str:
    """Write a temp .mmd file with the init header prepended. Returns temp path."""
    src = strip_existing_init(source_path.read_text(encoding="utf-8"))
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".mmd", delete=False, encoding="utf-8"
    )
    tmp.write(init_header + src)
    tmp.close()
    return tmp.name


# ─────────────────────────────────────────────────────────────────────────────
# Core conversion
# ─────────────────────────────────────────────────────────────────────────────

def render_svg(mmd_path: Path, svg_path: Path,
               init_header: str, viewport: int,
               background: str, theme: str,
               runner: str) -> tuple[bool, str]:
    """Stage 1 — render .mmd → .svg via mmdc at a generous viewport."""
    tmp = write_patched_mmd(mmd_path, init_header)
    try:
        ok, err = run_mmdc(runner, [
            "-i", tmp,
            "-o", str(svg_path),
            "-b", background,
            "-w", str(viewport),
            "-t", theme,
        ], mmd_path.name)
        return ok, err
    finally:
        os.unlink(tmp)


def render_png(mmd_path: Path, png_path: Path,
               init_header: str, natural_w: float,
               min_width: int, background: str, theme: str,
               runner: str) -> tuple[bool, str, int]:
    """
    Stage 2 — render .mmd → .png via mmdc.

    Scale is chosen so the PNG is at least min_width pixels wide
    while keeping natural_w × scale under CHROMIUM_MAX_PX.

    Returns (success, error_message, actual_output_width).
    """
    if natural_w <= 0:
        natural_w = 1000.0   # safe fallback

    # Desired scale to reach min_width
    desired_scale = max(1.0, min_width / natural_w)

    # Cap so output stays under Chromium's screenshot limit
    max_scale = CHROMIUM_MAX_PX / natural_w
    scale = min(desired_scale, max_scale)

    # Round to 2 dp; mmdc accepts float scale
    scale = round(scale, 2)

    # The viewport for PNG rendering = natural width so mermaid
    # uses its already-computed layout from the SVG stage.
    viewport = max(int(natural_w) + 200, SVG_VIEWPORT)

    tmp = write_patched_mmd(mmd_path, init_header)
    try:
        ok, err = run_mmdc(runner, [
            "-i", tmp,
            "-o", str(png_path),
            "-b", background,
            "-w", str(viewport),
            "-s", str(scale),
            "-t", theme,
        ], mmd_path.name)
        actual_w = int(natural_w * scale)
        return ok, err, actual_w
    finally:
        os.unlink(tmp)


def convert_file(mmd_path: Path, out_dir: Path, *,
                 init_header: str,
                 min_width: int,
                 background: str,
                 theme: str,
                 runner: str,
                 keep_svg: bool,
                 svg_only: bool,
                 skip_existing: bool,
                 verbose: bool) -> dict:

    stem     = mmd_path.stem
    svg_path = out_dir / f"{stem}.svg"
    png_path = out_dir / f"{stem}.png"

    result = {"svg": False, "png": False, "width": 0}

    if skip_existing:
        need_svg = not svg_path.exists()
        need_png = not svg_only and not png_path.exists()
        if not need_svg and not need_png:
            result["svg"] = result["png"] = True
            return result | {"skipped": True}

    # ── Stage 1: SVG ──────────────────────────────────────────────────────────
    ok, err = render_svg(
        mmd_path, svg_path, init_header,
        SVG_VIEWPORT, background, theme, runner
    )
    if not ok:
        result["error"] = f"SVG render failed: {err}"
        return result
    result["svg"] = True

    if verbose:
        nat_w = svg_viewbox_width(svg_path)
        print(f"    SVG  natural width: {nat_w:.0f} px")

    if svg_only:
        result["png"] = True   # not required
        return result

    # ── Stage 2: PNG ──────────────────────────────────────────────────────────
    natural_w = svg_viewbox_width(svg_path)

    ok, err, actual_w = render_png(
        mmd_path, png_path, init_header,
        natural_w, min_width, background, theme, runner
    )
    if ok:
        result["png"]  = True
        result["width"] = actual_w
        if verbose:
            kb = png_path.stat().st_size // 1024
            print(f"    PNG  {actual_w} px wide  ({kb} KB)")
    else:
        result["error"] = f"PNG render failed: {err}"

    # Clean up SVG unless caller wants to keep it
    if not keep_svg and svg_path.exists():
        svg_path.unlink()

    return result


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Convert .mmd files → SVG + PNG using mmdc (Chromium renderer)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "input_dir", nargs="?", default=".",
        help="Directory containing .mmd files  (default: current directory)",
    )
    p.add_argument(
        "--out", "-o", default=None, metavar="DIR",
        help="Output directory  (default: same as input_dir)",
    )
    p.add_argument(
        "--min-width", "-w", type=int, default=4000, metavar="PX",
        help="Minimum PNG output width in pixels  (default: 4000). "
             "Scale is raised until the PNG is at least this wide, "
             "capped at Chromium's screenshot limit.",
    )
    p.add_argument(
        "--width-map", default="", metavar="PREFIX:PX,...",
        help="Per-file min-width overrides by filename prefix, "
             "e.g. '01:8000,09:6000'",
    )
    p.add_argument(
        "--font-size", default="20px", metavar="SIZE",
        help="Mermaid font size  (default: 20px)",
    )
    p.add_argument(
        "--font-family", default="trebuchet ms, verdana, arial",
        metavar="FAMILY",
        help="Font family  (default: 'trebuchet ms, verdana, arial')",
    )
    p.add_argument(
        "--theme", default="default",
        choices=["default", "forest", "dark", "neutral", "base"],
        help="Mermaid theme  (default: default)",
    )
    p.add_argument(
        "--background", "-b", default="white",
        help="Background colour  (default: white)",
    )
    p.add_argument(
        "--keep-svg", action="store_true",
        help="Keep intermediate .svg files  (deleted by default)",
    )
    p.add_argument(
        "--svg-only", action="store_true",
        help="Produce SVG only, skip PNG",
    )
    p.add_argument(
        "--skip-existing", action="store_true",
        help="Skip files whose outputs already exist",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print natural SVG width and PNG size per file",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    in_dir  = Path(args.input_dir).resolve()
    out_dir = Path(args.out).resolve() if args.out else in_dir
    if not in_dir.is_dir():
        print(f"[error] directory not found: {in_dir}", file=sys.stderr)
        return 1
    out_dir.mkdir(parents=True, exist_ok=True)

    mmd_files = sorted(in_dir.glob("*.mmd"))
    if not mmd_files:
        print(f"[warn] no .mmd files in {in_dir}")
        return 0

    try:
        runner = find_mmdc()
    except RuntimeError as e:
        print(f"[error] {e}", file=sys.stderr)
        return 1

    width_map   = parse_width_map(args.width_map)
    init_header = make_init_header(args.theme, args.font_size, args.font_family)

    print(f"\nMermaid → {'SVG' if args.svg_only else 'SVG + PNG'}")
    print(f"  input      : {in_dir}")
    print(f"  output     : {out_dir}")
    print(f"  runner     : {runner}")
    print(f"  theme      : {args.theme}   font: {args.font_size}")
    print(f"  min width  : {args.min_width} px   "
          f"(capped at {CHROMIUM_MAX_PX} px by Chromium limit)")
    print(f"  files      : {len(mmd_files)}")
    print()

    ok_count = fail_count = skip_count = 0

    for mmd_path in mmd_files:
        prefix    = mmd_path.stem[:2]
        min_width = width_map.get(prefix, args.min_width)

        print(f"  {mmd_path.name}")
        res = convert_file(
            mmd_path, out_dir,
            init_header   = init_header,
            min_width     = min_width,
            background    = args.background,
            theme         = args.theme,
            runner        = runner,
            keep_svg      = args.keep_svg or args.svg_only,
            svg_only      = args.svg_only,
            skip_existing = args.skip_existing,
            verbose       = args.verbose,
        )

        if res.get("skipped"):
            print("    – skipped (outputs exist)")
            skip_count += 1
            continue

        if res["svg"] and res["png"]:
            tag = "SVG" if args.svg_only else f"PNG ({res['width']} px wide)"
            print(f"    ✓  {tag}")
            ok_count += 1
        else:
            print(f"    ✗  {res.get('error', 'unknown error')}")
            fail_count += 1

    print()
    print(f"Done — {ok_count} ok,  {fail_count} failed,  {skip_count} skipped")
    print(f"Output: {out_dir}")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
