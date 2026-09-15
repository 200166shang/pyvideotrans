from __future__ import annotations

import json

import pytest

from videotrans.podcast import orchestrator as production_orchestrator
from videotrans.podcast import scheduler as production_scheduler
from videotrans.podcast.replay import replay_second_round_performance


def test_replay_reproduces_baselines_and_meets_overlap_target() -> None:
    replay = replay_second_round_performance()

    observations = replay["observations"]
    calibration = replay["calibration"]
    prediction = replay["prediction"]

    assert calibration["translation"]["replayed_standalone_seconds"] == pytest.approx(
        observations["translation_standalone_seconds"], abs=0.001
    )
    assert calibration["tts"]["replayed_standalone_seconds"] == pytest.approx(
        observations["tts_standalone_seconds"], abs=0.001
    )
    assert calibration["translation"]["absolute_error_seconds"] <= 0.001
    assert calibration["tts"]["absolute_error_seconds"] <= 0.001
    assert prediction["wall_clock_seconds"] <= 45.0
    assert prediction["meets_target"] is True


def test_replay_uses_one_to_one_ordered_dependencies() -> None:
    replay = replay_second_round_performance()

    assert replay["sample"]["dependency_map"] == [[index, index] for index in range(12)]
    translation_rows = replay["schedule"]["translation_overlap"]
    tts_rows = replay["schedule"]["tts_overlap"]
    assert len(translation_rows) == len(tts_rows) == 12
    for translation, tts in zip(translation_rows, tts_rows):
        assert tts[1] == translation[3]
        assert tts[2] >= tts[1]


def test_replay_is_deterministic_json_compatible_and_content_free() -> None:
    first = replay_second_round_performance()
    second = replay_second_round_performance()

    assert first == second
    encoded = json.dumps(first, sort_keys=True, allow_nan=False)
    assert json.loads(encoded) == first

    forbidden_keys = {
        "audio",
        "content",
        "source_text",
        "text",
        "transcript",
        "translation_text",
    }

    def assert_content_free(value: object) -> None:
        if isinstance(value, dict):
            assert forbidden_keys.isdisjoint(value)
            for child in value.values():
                assert_content_free(child)
        elif isinstance(value, list):
            for child in value:
                assert_content_free(child)
        else:
            assert not isinstance(value, str)

    assert_content_free(first)


def test_replay_executes_the_production_scheduler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = production_scheduler.AsyncScheduler
    original_overlap = (
        production_orchestrator.PodcastCoordinator._translate_and_tts_async
    )
    calls = {"run": 0, "overlap": 0}

    class SchedulerSpy(original):
        async def run(self, items, operation):
            calls["run"] += 1
            return await super().run(items, operation)

    async def overlap_spy(self, *args, **kwargs):
        calls["overlap"] += 1
        return await original_overlap(self, *args, **kwargs)

    monkeypatch.setattr(production_scheduler, "AsyncScheduler", SchedulerSpy)
    monkeypatch.setattr(
        production_orchestrator.PodcastCoordinator,
        "_translate_and_tts_async",
        overlap_spy,
    )

    replay = replay_second_round_performance()

    assert replay["prediction"]["meets_target"] is True
    assert calls["run"] > 0
    assert calls["overlap"] == 1
