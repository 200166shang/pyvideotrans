import json
from itertools import pairwise
from pathlib import Path

import pytest

from videotrans.podcast.throughput_replay import main, replay_tts_concurrency
from videotrans.podcast.trace import TraceError

_OBSERVED_TTS = (
    (18.995729542, 19.397785292, 40.361736708),
    (19.469049875, 19.797762667, 29.859607750),
    (21.117674458, 21.117931542, 22.271791000),
    (21.911474250, 21.911664792, 35.936021458),
    (22.708123875, 22.708323708, 24.374465167),
    (23.657886875, 24.374623333, 30.912545542),
    (25.371506500, 29.859746667, 32.140007500),
    (25.591994292, 30.912827708, 50.680576167),
    (26.694943625, 32.140164625, 33.501478042),
    (27.481641167, 33.501609375, 37.770614208),
    (28.688527042, 35.936139917, 36.844246542),
    (28.690200750, 36.844390583, 42.070992375),
)


def _event(timestamp: float, event: str, **fields: object) -> dict[str, object]:
    return {
        "schema_version": 1,
        "process_segment": 1,
        "monotonic_ns": round(timestamp * 1_000_000_000),
        "event": event,
        **fields,
    }


def _trace_events(
    rows: tuple[tuple[float, float, float], ...], *, tail: float = 1.0
) -> list[dict[str, object]]:
    events = [
        _event(0.0, "start", stage_id="prepare"),
        _event(0.1, "commit", stage_id="prepare"),
        _event(0.1, "start", stage_id="asr"),
        _event(0.2, "commit", stage_id="asr"),
    ]
    for chunk_id, (release, _start, _commit) in enumerate(rows):
        events.extend(
            (
                _event(
                    0.21,
                    "queue",
                    stage_id="translate",
                    chunk_id=chunk_id,
                    limits={"concurrency": 4, "requests_per_minute": 60},
                ),
                _event(
                    0.22,
                    "start",
                    stage_id="translate",
                    chunk_id=chunk_id,
                    limits={"concurrency": 4, "requests_per_minute": 60},
                ),
                _event(
                    release - 0.01,
                    "commit",
                    stage_id="translate",
                    chunk_id=chunk_id,
                ),
            )
        )
    for chunk_id, (release, start, commit) in enumerate(rows):
        dependency = [chunk_id, chunk_id]
        events.extend(
            (
                _event(
                    release,
                    "queue",
                    stage_id="tts",
                    chunk_id=chunk_id,
                    dependency_range=dependency,
                    limits={"concurrency": 4, "requests_per_minute": 150},
                ),
                _event(
                    start,
                    "start",
                    stage_id="tts",
                    chunk_id=chunk_id,
                    dependency_range=dependency,
                    limits={"concurrency": 4, "requests_per_minute": 150},
                ),
                _event(
                    commit,
                    "commit",
                    stage_id="tts",
                    chunk_id=chunk_id,
                    dependency_range=dependency,
                ),
            )
        )
    last_commit = max(row[2] for row in rows)
    total = last_commit + tail
    events.extend(
        (
            _event(last_commit, "start", stage_id="finalize"),
            _event(total, "commit", stage_id="finalize"),
            {
                **_event(total, "completion"),
                "duration_ns": round(total * 1_000_000_000),
            },
        )
    )
    return sorted(events, key=lambda event: int(event["monotonic_ns"]))


def _write_trace(path: Path, events: list[dict[str, object]]) -> Path:
    path.write_text(
        "".join(
            json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n"
            for event in events
        )
    )
    return path


def test_observed_trace_calibrates_and_reports_bounded_candidates(tmp_path: Path):
    events = _trace_events(_OBSERVED_TTS)
    # Use the complete accepted-run timing rather than the helper's one-second tail.
    finalize_start = next(
        event
        for event in events
        if event.get("stage_id") == "finalize" and event["event"] == "start"
    )
    finalize_commit = next(
        event
        for event in events
        if event.get("stage_id") == "finalize" and event["event"] == "commit"
    )
    completion = events[-1]
    finalize_start["monotonic_ns"] = 50_693_403_125
    finalize_commit["monotonic_ns"] = 52_008_945_292
    completion["monotonic_ns"] = completion["duration_ns"] = 52_009_369_458
    events.sort(key=lambda event: int(event["monotonic_ns"]))

    result = replay_tts_concurrency(_write_trace(tmp_path / "trace.jsonl", events))

    assert result["calibration"] == {
        "concurrency": 4,
        "predicted_total_seconds": pytest.approx(52.008729),
        "absolute_residual_seconds": pytest.approx(0.000271),
        "tolerance_seconds": 0.5,
        "passed": True,
    }
    candidates = {item["concurrency"]: item for item in result["candidates"]}
    assert candidates[6]["predicted_total_seconds"] == pytest.approx(46.868048)
    assert candidates[8]["predicted_total_seconds"] == pytest.approx(46.868048)
    assert candidates[6]["relative_improvement_fraction"] == pytest.approx(0.098847)
    assert not candidates[6]["meets_ten_percent_gate"]


def test_concurrency_helps_only_when_released_work_has_queueing_headroom(
    tmp_path: Path,
):
    bottleneck = tuple(
        (1.0, start, start + 5.0) for start in (1.4, 1.8, 2.2, 2.6, 6.4, 6.8)
    )
    bottleneck_result = replay_tts_concurrency(
        _write_trace(tmp_path / "bottleneck.jsonl", _trace_events(bottleneck)),
        concurrencies=(4, 6),
        latency_multipliers=(1.0,),
        measured_baseline_seconds=12.8,
    )
    bottleneck_totals = {
        item["concurrency"]: item["predicted_total_seconds"]
        for item in bottleneck_result["candidates"]
    }
    assert bottleneck_totals == {4: pytest.approx(12.8), 6: pytest.approx(9.4)}

    no_headroom = ((1.0, 1.4, 2.4), (7.0, 7.0, 8.0), (13.0, 13.0, 14.0))
    no_headroom_result = replay_tts_concurrency(
        _write_trace(tmp_path / "no-headroom.jsonl", _trace_events(no_headroom)),
        concurrencies=(4, 8),
        latency_multipliers=(1.0,),
        measured_baseline_seconds=15.0,
    )
    assert [
        item["predicted_total_seconds"] for item in no_headroom_result["candidates"]
    ] == [pytest.approx(15.0), pytest.approx(15.0)]


def test_replay_preserves_chunk_order_and_smooth_start_rate(tmp_path: Path):
    rows = tuple((1.0, start, start + 5.0) for start in (1.4, 1.8, 2.2, 2.6, 6.4, 6.8))
    result = replay_tts_concurrency(
        _write_trace(tmp_path / "trace.jsonl", _trace_events(rows)),
        concurrencies=(4, 6),
        latency_multipliers=(1.0,),
        measured_baseline_seconds=12.8,
    )
    schedule = result["candidates"][1]["schedule"]

    assert [item["chunk_id"] for item in schedule] == list(range(6))
    chronological_starts = sorted(item["start_seconds"] for item in schedule)
    assert all(
        later - earlier >= 0.4 - 1e-6
        for earlier, later in pairwise(chronological_starts)
    )


def test_latency_sensitivity_scales_each_observed_service_time(tmp_path: Path):
    rows = tuple((1.0, start, start + 5.0) for start in (1.4, 1.8, 2.2, 2.6, 6.4, 6.8))
    result = replay_tts_concurrency(
        _write_trace(tmp_path / "trace.jsonl", _trace_events(rows)),
        concurrencies=(4, 6),
        measured_baseline_seconds=12.8,
    )
    six_worker_totals = [
        row["candidates"][1]["predicted_total_seconds"] for row in result["sensitivity"]
    ]
    assert six_worker_totals == [
        pytest.approx(9.4),
        pytest.approx(9.9),
        pytest.approx(10.4),
    ]


def test_malformed_json_is_rejected_by_the_strict_trace_reader(tmp_path: Path):
    path = tmp_path / "trace.jsonl"
    path.write_bytes(b"{not json}\n")
    with pytest.raises(TraceError, match="invalid JSON"):
        replay_tts_concurrency(path)


@pytest.mark.parametrize(
    "mutation, message",
    (
        ("incomplete", "process completion"),
        ("resumed", "resumed"),
        ("translation_retry", "duplicate translate"),
        ("missing_finalize", "complete finalize"),
        ("missing_tts_commit", "observations must match"),
        ("chunk_gap", "contiguous"),
    ),
)
def test_ambiguous_or_incomplete_observations_are_rejected(
    tmp_path: Path, mutation: str, message: str
):
    rows = ((1.0, 1.4, 2.4), (2.0, 2.0, 3.0), (3.0, 3.0, 4.0))
    events = _trace_events(rows)
    if mutation == "incomplete":
        events.pop()
    elif mutation == "resumed":
        second_segment = [dict(event, process_segment=2) for event in events]
        events.extend(second_segment)
    elif mutation == "translation_retry":
        duplicate = next(
            dict(event)
            for event in events
            if event.get("stage_id") == "translate" and event["event"] == "start"
        )
        events.append(duplicate)
        events.sort(key=lambda event: int(event["monotonic_ns"]))
    elif mutation == "missing_finalize":
        events = [event for event in events if event.get("stage_id") != "finalize"]
    elif mutation == "missing_tts_commit":
        events = [
            event
            for event in events
            if not (
                event.get("stage_id") == "tts"
                and event["event"] == "commit"
                and event.get("chunk_id") == 1
            )
        ]
    elif mutation == "chunk_gap":
        events = [
            event
            for event in events
            if not (event.get("stage_id") == "tts" and event.get("chunk_id") == 1)
        ]

    with pytest.raises(TraceError, match=message):
        replay_tts_concurrency(_write_trace(tmp_path / f"{mutation}.jsonl", events))


def test_module_cli_prints_json_without_echoing_the_trace_path(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    rows = ((1.0, 1.4, 2.4),)
    path = _write_trace(
        tmp_path / "private-name.jsonl", _trace_events(rows, tail=49.609)
    )

    assert main(["--trace", str(path)]) == 0

    output = capsys.readouterr().out
    assert json.loads(output)["observations"]["chunk_count"] == 1
    assert str(path) not in output


@pytest.mark.parametrize("delayed_translation", [0, 1])
def test_rejects_tts_release_before_any_required_prefix_commit(
    tmp_path: Path, delayed_translation: int
):
    events = _trace_events(((1.0, 1.4, 2.0), (1.0, 1.8, 2.4)))
    # A direct dependency and an earlier out-of-order prefix dependency
    # must both have committed before release, even if their IDs exist.
    for event in events:
        if event.get("stage_id") == "tts":
            event["dependency_range"] = [1, 1]
        if (
            event.get("stage_id") == "translate"
            and event.get("chunk_id") == delayed_translation
            and event["event"] == "commit"
        ):
            event["monotonic_ns"] = 1_100_000_000
    events.sort(key=lambda event: int(event["monotonic_ns"]))
    with pytest.raises(TraceError, match="committed translation prefix"):
        replay_tts_concurrency(_write_trace(tmp_path / "invalid.jsonl", events))
