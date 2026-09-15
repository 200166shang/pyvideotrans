"""CLI entry point for the resumable Chinese podcast pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from .finalize import Mp3Finalizer
from .manifest import atomic_write_json
from .orchestrator import PodcastCoordinator, PodcastRunResult, PodcastRuntime
from .profiles import PodcastProfile, get_profile
from .providers import create_alibaba_podcast_providers


FIRST_RELEASE_FIVE_MINUTE_MS = 115_000
ENGINEERING_FIVE_MINUTE_MS = 75_000


def create_runtime(profile: PodcastProfile) -> PodcastRuntime:
    providers = create_alibaba_podcast_providers(profile=profile)
    return PodcastRuntime(
        asr=providers.asr,
        translator=providers.translation,
        tts=providers.tts,
        upload_audio=providers.upload_audio,
        finalizer=Mp3Finalizer(),
    )


def run_from_args(
    args: Any,
    *,
    runtime_factory: Callable[[PodcastProfile], PodcastRuntime] = create_runtime,
) -> int:
    """Execute podcast or benchmark mode and print only safe local paths."""

    profile = get_profile(args.podcast_profile)
    runtime = runtime_factory(profile)
    coordinator = PodcastCoordinator(runtime, profile)
    report_path = Path(args.report) if args.report else None

    if args.resume:
        result = coordinator.resume(Path(args.resume), report_path=report_path)
    else:
        source = Path(args.name)
        run_directory = (
            Path(args.output_dir)
            if args.output_dir
            else source.resolve().parent / f"{source.stem}-zh-podcast"
        )
        result = coordinator.create(
            source,
            run_directory,
            report_path=report_path,
            cache_mode=args.cache_mode,
        )

    if args.task == "benchmark":
        _write_benchmark_summary(result)

    print(f"Output MP3: {result.output_path}")
    print(f"Production report: {result.report_path}")
    print("Status: awaiting listener review")
    return 0


def _write_benchmark_summary(result: PodcastRunResult) -> Path:
    with result.report_path.open("r", encoding="utf-8") as handle:
        report = json.load(handle)
    elapsed_ms = int(report["totals"]["wall_clock_ms"])
    summary = {
        "schema_version": 1,
        "run_id": report["run"]["id"],
        "elapsed_ms": elapsed_ms,
        "first_release_target_ms": FIRST_RELEASE_FIVE_MINUTE_MS,
        "engineering_target_ms": ENGINEERING_FIVE_MINUTE_MS,
        "meets_first_release_target": elapsed_ms <= FIRST_RELEASE_FIVE_MINUTE_MS,
        "meets_engineering_target": elapsed_ms <= ENGINEERING_FIVE_MINUTE_MS,
        "listening_quality_status": report["listening_quality_gate"]["status"],
    }
    destination = result.run_directory / "benchmark-summary.json"
    atomic_write_json(destination, summary)
    return destination


__all__ = ["create_runtime", "run_from_args"]
