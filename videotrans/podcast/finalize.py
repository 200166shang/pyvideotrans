from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class FinalizedAudio:
    path: Path
    fingerprint: str
    duration_ms: int
    bitrate_kbps: int
    channels: int
    sample_rate_hz: int


def _run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, capture_output=True, text=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _concat_line(path: Path) -> str:
    escaped = path.resolve().as_posix().replace("'", "'\\''")
    return f"file '{escaped}'\n"


class Mp3Finalizer:
    """Concatenate ordered lossless chunks and encode one final MP3."""

    def __init__(self, runner: CommandRunner = _run) -> None:
        self._runner = runner

    def finalize(self, chunks: Sequence[Path], output: Path) -> FinalizedAudio:
        ordered = [Path(chunk) for chunk in chunks]
        if not ordered:
            raise ValueError("at least one audio chunk is required")
        missing = [chunk for chunk in ordered if not chunk.is_file() or chunk.stat().st_size == 0]
        if missing:
            raise FileNotFoundError("one or more committed audio chunks are missing")

        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        concat_file = output.with_suffix(output.suffix + ".concat.txt")
        temporary = output.with_suffix(output.suffix + ".partial")
        concat_file.write_text("".join(_concat_line(chunk) for chunk in ordered), encoding="utf-8")
        try:
            self._runner(
                [
                    "ffmpeg", "-v", "error", "-y", "-f", "concat", "-safe", "0",
                    "-i", str(concat_file), "-vn", "-ac", "1", "-ar", "48000",
                    "-b:a", "64k", "-f", "mp3", str(temporary),
                ]
            )
            if not temporary.is_file() or temporary.stat().st_size == 0:
                raise RuntimeError("ffmpeg did not create a valid MP3")
            with temporary.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(temporary, output)
            metadata = self._probe(output)
            return FinalizedAudio(
                path=output,
                fingerprint=_sha256(output),
                duration_ms=round(float(metadata["format"]["duration"]) * 1000),
                bitrate_kbps=round(int(metadata["format"]["bit_rate"]) / 1000),
                channels=int(metadata["streams"][0]["channels"]),
                sample_rate_hz=int(metadata["streams"][0]["sample_rate"]),
            )
        finally:
            concat_file.unlink(missing_ok=True)
            temporary.unlink(missing_ok=True)

    def _probe(self, path: Path) -> dict:
        result = self._runner(
            [
                "ffprobe", "-v", "error", "-select_streams", "a:0",
                "-show_entries", "stream=sample_rate,channels:format=duration,bit_rate",
                "-of", "json", str(path),
            ]
        )
        data = json.loads(result.stdout)
        if not data.get("streams") or not data.get("format"):
            raise RuntimeError("ffprobe returned incomplete audio metadata")
        return data
