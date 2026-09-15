import json
import math
from copy import deepcopy

import pytest

from videotrans.podcast.report import (
    SCHEMA_VERSION,
    STAGE_ORDER,
    ReportValidationError,
    build_report,
    calculate_totals,
    fingerprint_bytes,
    fingerprint_file,
    fingerprint_profile,
    make_chunk,
    make_stage,
    public_list_price_total,
    validate_report,
    write_report,
)


def _stages():
    stages = []
    for name in STAGE_ORDER:
        chunks = []
        if name == "tts":
            chunks = [
                make_chunk(
                    "tts-0001",
                    status="succeeded",
                    attempts=2,
                    retries=1,
                    elapsed_ms=900,
                    cache_hit=False,
                    cost_confirmed_cny=0.02,
                    cost_unconfirmed_cny=0.01,
                    artifact_fingerprint=fingerprint_bytes(b"chunk"),
                )
            ]
        stages.append(
            make_stage(
                name,
                provider="local"
                if name in {"prepare", "finalize"}
                else f"provider-{name}",
                status="succeeded",
                attempts=1,
                retries=0,
                elapsed_ms=1_000,
                cache_hit=False,
                cost_confirmed_cny=0.02 if name == "asr" else 0,
                cost_unconfirmed_cny=0.03 if name == "tts" else 0,
                chunks=chunks,
                artifact_fingerprint=fingerprint_bytes(name.encode()),
            )
        )
    return stages


def _report(cache_mode="cold"):
    profile = {
        "id": "alibaba-podcast-v1",
        "target_language": "zh-CN",
        "region": "cn-beijing",
        "providers": {
            "asr": "qwen-audio-3.0-asr-flash-filetrans",
            "translation": "qwen-mt-flash",
            "tts": "qwen3-tts-flash-2025-11-27",
            "voice": "Andre",
        },
    }
    return build_report(
        run_id="run-001",
        run_status="awaiting_review",
        cache_mode=cache_mode,
        input_fingerprint=fingerprint_bytes(b"source audio"),
        input_duration_ms=300_010,
        input_language="en",
        profile=profile,
        stages=_stages(),
        output={
            "format": "mp3",
            "bitrate_kbps": 64,
            "channels": 1,
            "duration_ms": 290_000,
            "fingerprint": fingerprint_bytes(b"output audio"),
        },
        listening_quality_gate={
            "status": "pending",
            "decided_at": None,
            "note": None,
        },
    )


def test_builds_schema_versioned_privacy_safe_contract():
    report = _report()

    assert report["schema_version"] == SCHEMA_VERSION
    assert list(report) == [
        "schema_version",
        "run",
        "input",
        "profile",
        "stages",
        "totals",
        "output",
        "listening_quality_gate",
        "privacy",
    ]
    assert report["run"]["cache_mode"] == "cold"
    assert report["profile"]["providers"]["voice"] == "Andre"
    assert report["profile"]["fingerprint"].startswith("sha256:")
    assert report["output"]["fingerprint"].startswith("sha256:")
    assert report["privacy"] == {
        "contains_credentials": False,
        "contains_transcript": False,
        "contains_absolute_source_path": False,
    }
    assert [stage["name"] for stage in report["stages"]] == list(STAGE_ORDER)


def test_stage_and_chunk_metrics_keep_confirmed_and_unconfirmed_costs():
    report = _report()
    tts = report["stages"][3]

    assert tts["cost_confirmed_cny"] == 0
    assert tts["cost_unconfirmed_cny"] == 0.03
    assert tts["chunks"][0] == {
        "id": "tts-0001",
        "status": "succeeded",
        "attempts": 2,
        "retries": 1,
        "elapsed_ms": 900,
        "cache_hit": False,
        "cost_confirmed_cny": 0.02,
        "cost_unconfirmed_cny": 0.01,
        "artifact_fingerprint": fingerprint_bytes(b"chunk"),
        "error_code": None,
    }
    assert report["totals"]["cost_confirmed_cny"] == 0.02
    assert report["totals"]["cost_unconfirmed_cny"] == 0.03


@pytest.mark.parametrize("cache_mode", ["cold", "warm"])
def test_accepts_cold_and_warm_cache_modes(cache_mode):
    assert _report(cache_mode)["run"]["cache_mode"] == cache_mode


def test_rejects_invalid_cache_mode():
    with pytest.raises(ReportValidationError, match="cache_mode"):
        _report("lukewarm")


def test_rejects_missing_or_out_of_order_stages():
    missing = _stages()[:-1]
    with pytest.raises(ReportValidationError, match="ordered stages"):
        build_report(
            run_id="run-001",
            run_status="running",
            cache_mode="cold",
            input_fingerprint=fingerprint_bytes(b"input"),
            input_duration_ms=1,
            input_language="en",
            profile={
                "id": "profile",
                "target_language": "zh-CN",
                "region": "cn-beijing",
                "providers": {
                    "asr": "a",
                    "translation": "b",
                    "tts": "c",
                    "voice": "Andre",
                },
            },
            stages=missing,
        )

    report = _report()
    report["stages"][1], report["stages"][2] = report["stages"][2], report["stages"][1]
    with pytest.raises(ReportValidationError, match="ordered stages"):
        validate_report(report)


@pytest.mark.parametrize(
    ("container", "unsafe_key", "unsafe_value"),
    [
        ("profile", "api_key", "secret-value"),
        ("input", "source_path", "episode.m4a"),
        ("output", "transcript", "private spoken words"),
    ],
)
def test_rejects_unsafe_fields(container, unsafe_key, unsafe_value):
    report = _report()
    report[container][unsafe_key] = unsafe_value

    with pytest.raises(ReportValidationError, match="unsafe field"):
        validate_report(report)


@pytest.mark.parametrize(
    "unsafe_value",
    [
        "/Users/person/private/source.m4a",
        "file:///tmp/source.m4a",
        r"C:\\private\\source.m4a",
    ],
)
def test_rejects_absolute_paths_in_any_string_value(unsafe_value):
    report = _report()
    report["listening_quality_gate"]["note"] = unsafe_value

    with pytest.raises(ReportValidationError, match="absolute path"):
        validate_report(report)


def test_rejects_false_privacy_claim_becoming_true():
    report = _report()
    report["privacy"]["contains_transcript"] = True

    with pytest.raises(ReportValidationError, match="privacy"):
        validate_report(report)


def test_fingerprints_are_stable_and_profile_excludes_its_fingerprint(tmp_path):
    source = tmp_path / "source.m4a"
    source.write_bytes(b"same bytes")

    assert fingerprint_file(source) == fingerprint_bytes(b"same bytes")
    first = fingerprint_profile({"id": "p", "voice": "Andre"})
    second = fingerprint_profile(
        {"voice": "Andre", "id": "p", "fingerprint": "ignored"}
    )
    assert first == second
    assert first.startswith("sha256:")


def test_public_list_price_total_uses_only_numeric_usage():
    usage = {"input_tokens": 2_000, "output_tokens": 500, "characters": 2_500}
    prices = {
        "input_tokens": {"price_cny": 0.70, "per_units": 1_000_000},
        "output_tokens": {"price_cny": 1.95, "per_units": 1_000_000},
        "characters": {"price_cny": 0.8, "per_units": 10_000},
    }

    assert public_list_price_total(usage, prices) == 0.202375


@pytest.mark.parametrize(
    "usage", [{"tokens": "10"}, {"tokens": True}, {"tokens": math.inf}]
)
def test_public_list_price_rejects_non_numeric_or_non_finite_usage(usage):
    with pytest.raises((TypeError, ValueError)):
        public_list_price_total(usage, {"tokens": 0.1})


def test_calculate_totals_aggregates_stage_values_not_chunk_values_twice():
    totals = calculate_totals(_stages(), wall_clock_ms=4_321)

    assert totals == {
        "wall_clock_ms": 4_321,
        "timing_basis": "wall-clock",
        "cost_confirmed_cny": 0.02,
        "cost_unconfirmed_cny": 0.03,
        "retries": 0,
        "cache_hits": 0,
    }


def test_write_report_atomically_replaces_existing_json(tmp_path, monkeypatch):
    destination = tmp_path / "nested" / "report.json"
    destination.parent.mkdir()
    destination.write_text('{"old": true}', encoding="utf-8")
    replacements = []

    import videotrans.podcast.report as report_module

    real_replace = report_module.os.replace

    def recording_replace(source, target):
        replacements.append((source, target))
        real_replace(source, target)

    monkeypatch.setattr(report_module.os, "replace", recording_replace)
    write_report(_report(), destination)

    assert json.loads(destination.read_text(encoding="utf-8")) == _report()
    assert len(replacements) == 1
    assert replacements[0][0].parent == destination.parent
    assert replacements[0][1] == destination
    assert list(destination.parent.glob("*.tmp")) == []


def test_write_report_validates_before_replacing_existing_file(tmp_path):
    destination = tmp_path / "report.json"
    destination.write_text('{"old": true}', encoding="utf-8")
    unsafe = deepcopy(_report())
    unsafe["run"]["access_token"] = "do-not-write"

    with pytest.raises(ReportValidationError):
        write_report(unsafe, destination)

    assert destination.read_text(encoding="utf-8") == '{"old": true}'
