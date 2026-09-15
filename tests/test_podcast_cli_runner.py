from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest

from videotrans.podcast.cli_runner import _validate_benchmark_identity, run_from_args
from videotrans.podcast.orchestrator import PodcastPipelineError, PodcastRunResult


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
        "review": None,
        "review_note": None,
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
    monkeypatch.setattr(
        "videotrans.podcast.cli_runner._validate_benchmark_identity", lambda *_: None
    )
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 300_010
    )
    monkeypatch.setattr(
        "videotrans.podcast.cli_runner.fingerprint_file",
        lambda _: "sha256:" + "a" * 64,
    )
    args = _args(tmp_path, task="benchmark")

    run_from_args(args, runtime_factory=lambda _: object())

    summary = json.loads(
        (Path(args.output_dir) / "benchmark-summary.json").read_text(encoding="utf-8")
    )
    assert summary["meets_first_release_target"] is True
    assert summary["meets_engineering_target"] is True
    assert summary["fixed_sample_validated"] is True


def test_cli_runner_resumes_without_source(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("videotrans.podcast.cli_runner.PodcastCoordinator", FakeCoordinator)
    monkeypatch.setattr("videotrans.podcast.cli_runner.load_terminal_run", lambda *_args, **_kwargs: None)
    run_dir = tmp_path / "existing"
    args = _args(tmp_path, name=None, output_dir=None, resume=str(run_dir))

    run_from_args(args, runtime_factory=lambda _: object())

    assert FakeCoordinator.resumed == (run_dir, None)


def test_review_does_not_construct_cloud_runtime(tmp_path, monkeypatch) -> None:
    called = []
    monkeypatch.setattr(
        "videotrans.podcast.cli_runner._record_review",
        lambda run_directory, status, *, profile, note, report_path: called.append(
            (run_directory, status, profile.id, note, report_path)
        )
        or tmp_path / "report.json",
    )
    args = _args(
        tmp_path,
        name=None,
        output_dir=None,
        resume=str(tmp_path / "run"),
        review="accepted",
        review_note="sounds natural",
    )

    assert run_from_args(
        args,
        runtime_factory=lambda _: (_ for _ in ()).throw(AssertionError("cloud runtime")),
    ) == 0

    assert called == [
        (
            tmp_path / "run",
            "accepted",
            "alibaba-podcast-v1",
            "sounds natural",
            None,
        )
    ]


def test_benchmark_rejects_non_fixed_input_before_cloud() -> None:
    with pytest.raises(PodcastPipelineError, match="fixed five-minute sample"):
        _validate_benchmark_identity("sha256:" + "0" * 64, 300_010)


def test_benchmark_review_validates_fixed_input_before_mutation(
    tmp_path, monkeypatch
) -> None:
    run_dir = tmp_path / "not-a-benchmark"
    run_dir.mkdir()
    (run_dir / "run.private.json").write_text(
        json.dumps(
            {
                "input_fingerprint": "sha256:" + "0" * 64,
                "input_duration_ms": 300_010,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "videotrans.podcast.cli_runner._record_review",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("review mutated before validation")
        ),
    )
    args = _args(
        tmp_path,
        task="benchmark",
        name=None,
        output_dir=None,
        resume=str(run_dir),
        review="accepted",
    )

    with pytest.raises(PodcastPipelineError, match="fixed five-minute sample"):
        run_from_args(args, runtime_factory=lambda _: object())


def test_terminal_resume_does_not_construct_cloud_runtime(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "accepted"
    result = _result(run_dir)
    result = PodcastRunResult(
        result.run_directory, result.output_path, result.report_path, "accepted"
    )
    monkeypatch.setattr(
        "videotrans.podcast.cli_runner.load_terminal_run", lambda *_args, **_kwargs: result
    )
    args = _args(tmp_path, name=None, output_dir=None, resume=str(run_dir))

    assert run_from_args(
        args,
        runtime_factory=lambda _: (_ for _ in ()).throw(AssertionError("cloud runtime")),
    ) == 0
