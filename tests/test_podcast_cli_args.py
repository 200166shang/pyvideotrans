from pathlib import Path

import pytest

from cli import build_parser, validate_task_params


def test_parser_accepts_podcast_profile_and_report(tmp_path: Path) -> None:
    source = tmp_path / "source.m4a"
    source.touch()
    parser = build_parser()
    args = parser.parse_args(
        [
            "--task", "podcast", "--name", str(source),
            "--podcast-profile", "alibaba-podcast-v1",
            "--report", str(tmp_path / "report.json"),
        ]
    )

    validate_task_params(args, parser)
    assert args.podcast_profile == "alibaba-podcast-v1"
    assert args.cache_mode == "cold"


def test_resume_does_not_require_source_name(tmp_path: Path) -> None:
    parser = build_parser()
    args = parser.parse_args(["--task", "podcast", "--resume", str(tmp_path)])

    validate_task_params(args, parser)


def test_resume_rejects_source_name(tmp_path: Path) -> None:
    source = tmp_path / "source.m4a"
    source.touch()
    parser = build_parser()
    args = parser.parse_args(
        ["--task", "podcast", "--resume", str(tmp_path), "--name", str(source)]
    )

    with pytest.raises(SystemExit):
        validate_task_params(args, parser)


def test_review_requires_resume(tmp_path: Path) -> None:
    source = tmp_path / "source.m4a"
    source.touch()
    parser = build_parser()
    args = parser.parse_args(
        ["--task", "podcast", "--name", str(source), "--review", "accepted"]
    )

    with pytest.raises(SystemExit):
        validate_task_params(args, parser)
