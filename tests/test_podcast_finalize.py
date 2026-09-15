import json
import shutil
import struct
import wave
from pathlib import Path
from subprocess import CompletedProcess

import pytest

from videotrans.podcast.finalize import Mp3Finalizer


def test_finalizer_orders_chunks_and_returns_probed_metadata(tmp_path: Path) -> None:
    first = tmp_path / "0001.wav"
    second = tmp_path / "0002.wav"
    first.write_bytes(b"one")
    second.write_bytes(b"two")
    commands: list[list[str]] = []

    def runner(command):
        command = list(command)
        commands.append(command)
        if command[0] == "ffmpeg":
            Path(command[-1]).write_bytes(b"mp3-result")
            return CompletedProcess(command, 0, "", "")
        return CompletedProcess(
            command,
            0,
            json.dumps(
                {
                    "streams": [{"sample_rate": "48000", "channels": 1}],
                    "format": {"duration": "12.345", "bit_rate": "64000"},
                }
            ),
            "",
        )

    result = Mp3Finalizer(runner).finalize([first, second], tmp_path / "edition.mp3")

    assert result.path.read_bytes() == b"mp3-result"
    assert result.duration_ms == 12345
    assert result.bitrate_kbps == 64
    assert result.channels == 1
    assert result.sample_rate_hz == 48000
    assert result.fingerprint
    assert commands[0][0] == "ffmpeg"
    assert commands[1][0] == "ffprobe"
    assert not (tmp_path / "edition.mp3.concat.txt").exists()


def test_finalizer_rejects_missing_chunks(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        Mp3Finalizer().finalize([tmp_path / "missing.wav"], tmp_path / "edition.mp3")


def test_finalizer_requires_at_least_one_chunk(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        Mp3Finalizer().finalize([], tmp_path / "edition.mp3")


@pytest.mark.skipif(
    shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None,
    reason="ffmpeg and ffprobe are required",
)
def test_finalizer_runs_real_ffmpeg_for_extensionless_audio_chunks(tmp_path: Path) -> None:
    chunks = [tmp_path / "000000.audio", tmp_path / "000001.audio"]
    for chunk in chunks:
        with wave.open(str(chunk), "wb") as output:
            output.setnchannels(1)
            output.setsampwidth(2)
            output.setframerate(16000)
            output.writeframes(b"".join(struct.pack("<h", 0) for _ in range(1600)))

    result = Mp3Finalizer().finalize(chunks, tmp_path / "edition.mp3")

    assert result.path.stat().st_size > 0
    assert result.duration_ms >= 150
    assert result.bitrate_kbps in range(60, 70)
    assert result.channels == 1
    assert result.sample_rate_hz == 48000
