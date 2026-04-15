"""
Download a minimal set of files for a Hugging Face model to a local folder.

Example:
  python download_hf_minimal.py --repo-id facebook/mms-tts-eng --output-dir model_tts
"""

import argparse
import os
from pathlib import Path

from huggingface_hub import HfApi, snapshot_download

# ------------------ CONFIG ------------------
DEFAULT_INCLUDE = [
    "config.json",
    "generation_config.json",
    "preprocessor_config.json",
    "tokenizer.json",
    "tokenizer_config.json",
    "special_tokens_map.json",
    "vocab.json",
    "vocab.txt",
    "merges.txt",
    "spiece.model",
    "*.model",
    "*.safetensors",
    "pytorch_model.bin",
    "model.safetensors",
]
# -------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download minimal files for a Hugging Face model."
    )
    parser.add_argument("--repo-id", required=True, help="Model repo, e.g. facebook/mms-tts-eng")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Local directory to store the model files.",
    )
    parser.add_argument(
        "--include",
        nargs="*",
        default=DEFAULT_INCLUDE,
        help="Optional allow-list of file patterns.",
    )
    return parser.parse_args()


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


def get_hf_token() -> str | None:
    for name in ("HF_TOKEN", "HUGGINGFACE_HUB_TOKEN", "HUGGING_FACE_HUB_TOKEN"):
        token = os.environ.get(name)
        if token:
            return token
    return None


def main() -> None:
    args = parse_args()
    load_dotenv()
    token = get_hf_token()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        HfApi().model_info(repo_id=args.repo_id, token=token)
    except Exception as exc:
        raise SystemExit(
            f"Could not reach Hugging Face repo '{args.repo_id}'. "
            "Check network access and your HF_TOKEN in .env."
        ) from exc

    snapshot_download(
        repo_id=args.repo_id,
        local_dir=str(output_dir),
        allow_patterns=args.include,
        token=token,
    )

    print(f"Downloaded minimal files to {output_dir}")


if __name__ == "__main__":
    main()
