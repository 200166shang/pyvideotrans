from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from videotrans.podcast.cli_runner import run_from_args
from videotrans.podcast.orchestrator import PodcastRunResult


class FakeCoordinator:
    created = None
    resumed = None

    def __init__(self, runtime, profile):
        self.runtime = runtime
        self.profile = profile

    def create(self, source, run_directory, *, report_path, cache_mode):
        self.__class__.created = (source, run_directory, report_path, cache_mode)
        return _result(Path(run_directory))

    def resume(self, run_directory, *, report_path):
        self.__class__.resumed = (run_directory, report_path)
        return _result(Path(run_directory))


def _result(run_directory: Path) -> PodcastRunResult:
    run_directory.mkdir(parents=True, exist_ok=True)
    output = run_directory / "podcast.zh-CN.mp3"
    report = run_directory / "production-report.json"
    output.write_bytes(b"ID3")
    report.write_text(
        json.dumps(
            {
                "run": {"id": "sha256:" + "a" * 64},
                "totals": {"wall_clock_ms": 74_000},
                "listening_quality_gate": {"status": "pending"},
            }
        ),
        encoding="utf-8",
    )
    return PodcastRunResult(run_directory, output, report, "awaiting_review")


def _args(tmp_path: Path, **changes):
    values = {
        "task": "podcast",
        "podcast_profile": "alibaba-podcast-v1",
        "name": str(tmp_path / "source.m4a"),
        "output_dir": str(tmp_path / "run"),
        "resume": None,
        "report": None,
        "cache_mode": "cold",
    }
    values.update(changes)
    return Namespace(**values)


def test_cli_runner_creates_new_run(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr("videotrans.podcast.cli_runner.PodcastCoordinator", FakeCoordinator)
    args = _args(tmp_path)

    assert run_from_args(args, runtime_factory=lambda _: object()) == 0

    assert FakeCoordinator.created == (
        Path(args.name),
        Path(args.output_dir),
        None,
        "cold",
    )
    assert "Output MP3:" in capsys.readouterr().out


def test_benchmark_writes_small_target_summary(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("videotrans.podcast.cli_runner.PodcastCoordinator", FakeCoordinator)
    args = _args(tmp_path, task="benchmark")

    run_from_args(args, runtime_factory=lambda _: object())

    summary = json.loads(
        (Path(args.output_dir) / "benchmark-summary.json").read_text(encoding="utf-8")
    )
    assert summary["meets_first_release_target"] is True
    assert summary["meets_engineering_target"] is True


def test_cli_runner_resumes_without_source(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("videotrans.podcast.cli_runner.PodcastCoordinator", FakeCoordinator)
    run_dir = tmp_path / "existing"
    args = _args(tmp_path, name=None, output_dir=None, resume=str(run_dir))

    run_from_args(args, runtime_factory=lambda _: object())

    assert FakeCoordinator.resumed == (run_dir, None)

