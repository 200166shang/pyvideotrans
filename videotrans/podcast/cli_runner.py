"""CLI entry point for the resumable Chinese podcast pipeline."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .finalize import Mp3Finalizer
from .manifest import ManifestStore, atomic_write_json
from .orchestrator import (
    PodcastCoordinator,
    PodcastPipelineError,
    PodcastRunResult,
    PodcastRuntime,
    _resolve_report_destination,
    load_terminal_run,
)
from .profiles import PodcastProfile, get_profile
from .providers import create_alibaba_podcast_providers
from .report import fingerprint_file, validate_report, write_report

FIRST_RELEASE_FIVE_MINUTE_MS = 115_000
ENGINEERING_FIVE_MINUTE_MS = 75_000
PIPELINE_OVERLAP_FIVE_MINUTE_MS = 45_000
COST_WATCHLINE_CNY_PER_SOURCE_HOUR = 3.85
BENCHMARK_INPUT_SHA256 = (
    "fba898bc3ff430ef7a2a78516600e6db7d00c33afa4f8b24fda20a1c1387501f"
)
BENCHMARK_INPUT_DURATION_MS = 300_010


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
    if args.task == "benchmark" and args.resume:
        _validate_benchmark_run(Path(args.resume))

    review = getattr(args, "review", None)
    if review:
        destination = _record_review(
            Path(args.resume),
            review,
            profile=profile,
            note=getattr(args, "review_note", None),
            report_path=Path(args.report) if args.report else None,
        )
        print(f"Listener review recorded: {review}")
        print(f"Production report: {destination}")
        if args.task == "benchmark":
            _write_benchmark_summary_from_report(Path(args.resume), destination)
        return 0

    if args.task == "benchmark" and not args.resume:
        source = Path(args.name)
        from .orchestrator import probe_duration_ms

        _validate_benchmark_identity(
            fingerprint_file(source), probe_duration_ms(source)
        )

    report_path = Path(args.report) if args.report else None
    if args.resume:
        terminal = load_terminal_run(
            Path(args.resume), profile, report_path=report_path
        )
        if terminal is not None:
            if args.task == "benchmark":
                _write_benchmark_summary(terminal)
            print(f"Output MP3: {terminal.output_path}")
            print(f"Production report: {terminal.report_path}")
            print(f"Status: {terminal.status}")
            return 0

    runtime = runtime_factory(profile)
    coordinator = PodcastCoordinator(runtime, profile)

    if args.resume:
        result = coordinator.resume(Path(args.resume), report_path=report_path)
    else:
        source = Path(args.name)
        run_directory = (
            Path(args.output_dir) if args.output_dir else _default_run_directory(source)
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
    print(f"Status: {result.status}")
    return 0


def _write_benchmark_summary(result: PodcastRunResult) -> Path:
    return _write_benchmark_summary_from_report(
        result.run_directory, result.report_path
    )


def _write_benchmark_summary_from_report(
    run_directory: Path, report_path: Path
) -> Path:
    with report_path.open("r", encoding="utf-8") as handle:
        report = json.load(handle)
    elapsed_ms = int(report["totals"]["wall_clock_ms"])
    duration_ms = int(report["input"]["duration_ms"])
    estimated_cost = float(report["totals"]["cost_confirmed_cny"]) + float(
        report["totals"]["cost_unconfirmed_cny"]
    )
    estimated_cost_per_hour = round(estimated_cost * 3_600_000 / duration_ms, 8)
    timing_basis = str(report["totals"].get("timing_basis", "wall-clock"))
    state_path = run_directory / "run.private.json"
    if state_path.is_file():
        with state_path.open("r", encoding="utf-8") as handle:
            private_basis = str(json.load(handle).get("timing_basis", "wall-clock"))
        if private_basis != timing_basis:
            raise PodcastPipelineError(
                "benchmark_state_invalid", "Benchmark timing basis does not match"
            )
    if timing_basis not in {"wall-clock", "active-process"}:
        raise PodcastPipelineError(
            "benchmark_state_invalid", "Benchmark timing basis is invalid"
        )
    uninterrupted = timing_basis == "wall-clock"
    summary = {
        "schema_version": 1,
        "run_id": report["run"]["id"],
        "elapsed_ms": elapsed_ms,
        "first_release_target_ms": FIRST_RELEASE_FIVE_MINUTE_MS,
        "engineering_target_ms": ENGINEERING_FIVE_MINUTE_MS,
        "pipeline_overlap_target_ms": PIPELINE_OVERLAP_FIVE_MINUTE_MS,
        "meets_first_release_target": elapsed_ms <= FIRST_RELEASE_FIVE_MINUTE_MS,
        "meets_engineering_target": elapsed_ms <= ENGINEERING_FIVE_MINUTE_MS,
        "meets_pipeline_overlap_target": uninterrupted
        and elapsed_ms <= PIPELINE_OVERLAP_FIVE_MINUTE_MS,
        "timing_basis": timing_basis,
        "uninterrupted_process": uninterrupted,
        "estimated_cost_per_source_hour_cny": estimated_cost_per_hour,
        "cost_watchline_per_source_hour_cny": COST_WATCHLINE_CNY_PER_SOURCE_HOUR,
        "exceeds_cost_watchline": estimated_cost_per_hour
        > COST_WATCHLINE_CNY_PER_SOURCE_HOUR,
        "listening_quality_status": report["listening_quality_gate"]["status"],
        "fixed_sample_validated": True,
    }
    destination = run_directory / "benchmark-summary.json"
    atomic_write_json(destination, summary)
    return destination


def _validate_benchmark_identity(fingerprint: str, duration_ms: int) -> None:
    expected = f"sha256:{BENCHMARK_INPUT_SHA256}"
    if fingerprint != expected or abs(duration_ms - BENCHMARK_INPUT_DURATION_MS) > 20:
        raise PodcastPipelineError(
            "benchmark_input_mismatch",
            "Benchmark mode requires the fixed five-minute sample",
        )


def _validate_benchmark_run(run_directory: Path) -> None:
    try:
        with (run_directory / "run.private.json").open("r", encoding="utf-8") as handle:
            state = json.load(handle)
        fingerprint = str(state.get("input_fingerprint", ""))
        duration_ms = int(state.get("input_duration_ms", -1))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise PodcastPipelineError(
            "benchmark_state_invalid",
            "Benchmark run is missing valid fixed-sample identity",
        ) from error
    _validate_benchmark_identity(fingerprint, duration_ms)


def _default_run_directory(source: Path) -> Path:
    timestamp = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d-%H%M%S")
    base = source.resolve().parent / f"{source.stem}-zh-podcast-{timestamp}"
    candidate = base
    suffix = 2
    while candidate.exists():
        candidate = base.with_name(f"{base.name}-{suffix}")
        suffix += 1
    return candidate


def _record_review(
    run_directory: Path,
    status: str,
    *,
    profile: PodcastProfile,
    note: str | None,
    report_path: Path | None,
) -> Path:
    run_directory = run_directory.expanduser().resolve()
    store = ManifestStore(run_directory / "manifest.json", lock_timeout=0)
    state_path = run_directory / "run.private.json"
    if not store.path.is_file() or not state_path.is_file():
        raise ValueError("Run directory is missing reviewable state")
    with state_path.open("r", encoding="utf-8") as handle:
        state = json.load(handle)
    if (
        state.get("profile_id") != profile.id
        or state.get("profile_fingerprint") != profile.fingerprint
    ):
        raise PodcastPipelineError(
            "profile_mismatch", "Saved run uses a different production profile"
        )
    destination = _resolve_report_destination(run_directory, state, report_path)
    with store.run_lock():
        manifest = store.load()
        manifest.record_review(status)
        with destination.open("r", encoding="utf-8") as handle:
            report = json.load(handle)
        validate_report(report)
        # The manifest is authoritative. If report replacement is interrupted,
        # repeating the idempotent review command repairs the derivative report.
        store.save(manifest)
        report["run"]["status"] = status
        report["listening_quality_gate"] = {
            "status": status,
            "decided_at": datetime.now(timezone.utc).isoformat(),
            "note": note,
        }
        write_report(report, destination)
    return destination


__all__ = ["create_runtime", "run_from_args"]
