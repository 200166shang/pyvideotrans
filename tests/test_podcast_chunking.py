from dataclasses import dataclass

import pytest

from videotrans.podcast.chunking import (
    DEFAULT_TTS_VOICE,
    PodcastTextRow,
    chunk_translation_rows,
    chunk_tts_rows,
    estimate_token_units,
)


def test_token_estimate_is_deterministic_and_language_aware():
    text = "A deterministic tokenizer 测试!"

    assert estimate_token_units(text) == estimate_token_units(text)
    assert estimate_token_units("中文") == 2
    assert estimate_token_units("abcdefgh") == 2


def test_translation_chunks_never_cross_speaker_turns():
    rows = [
        {"id": 1, "speaker": "a", "text": "a" * 300},
        {"id": 2, "speaker": "a", "text": "b" * 300},
        {"id": 3, "speaker": "b", "text": "c" * 300},
        {"id": 4, "speaker": "c", "text": "d" * 300},
    ]

    chunks = chunk_translation_rows(rows, token_estimator=len)

    assert [chunk.row_sequence_ids for chunk in chunks] == [
        ("1", "2"),
        ("3",),
        ("4",),
    ]
    assert chunks[0].speaker_ids == ("a",)
    assert chunks[1].speaker_ids == ("b",)
    assert all(chunk.token_estimate <= 1_000 for chunk in chunks)


def test_translation_chunks_split_at_max_and_have_stable_ids():
    rows = [
        PodcastTextRow("row-a", "One. " + "x" * 1_100, "speaker"),
        PodcastTextRow("row-b", "tail", "speaker"),
    ]

    first = chunk_translation_rows(rows, token_estimator=len)
    second = chunk_translation_rows(rows, token_estimator=len)

    assert first == second
    assert [chunk.sequence_id for chunk in first] == [
        f"translation-{index:06d}" for index in range(1, len(first) + 1)
    ]
    assert all(chunk.token_estimate <= 1_000 for chunk in first)
    assert first[0].rows[0].sequence_id.startswith("row-a.part-")


def test_translation_chunks_use_minimum_as_same_turn_packing_target():
    rows = [
        PodcastTextRow(f"row-{index}", str(index) * 300, "host")
        for index in range(1, 5)
    ]

    chunks = chunk_translation_rows(rows, token_estimator=len)

    assert [chunk.row_sequence_ids for chunk in chunks] == [
        ("row-1", "row-2"),
        ("row-3", "row-4"),
    ]


def test_tts_coalesces_same_speaker_and_always_uses_andre():
    rows = [
        {"id": 1, "speaker_id": "host", "translated_text": "甲" * 240 + "。", "voice": "Cherry"},
        {"id": 2, "speaker_id": "host", "translated_text": "乙" * 240 + "。"},
        {"id": 3, "speaker_id": "host", "translated_text": "丙" * 240 + "。"},
        {"id": 4, "speaker_id": "guest", "translated_text": "最后一句。"},
    ]

    chunks = chunk_tts_rows(rows)

    assert [chunk.speaker_id for chunk in chunks] == ["host", "host", "guest"]
    assert chunks[0].row_sequence_ids == ("1", "2", "3")
    assert all(chunk.voice == DEFAULT_TTS_VOICE == "Andre" for chunk in chunks)
    assert all(chunk.character_count <= 500 for chunk in chunks)


def test_tts_splits_an_overlong_sentence_without_losing_text():
    source = "沉" * 1_101

    chunks = chunk_tts_rows([{"line": 7, "speaker": "host", "text": source}])

    assert [chunk.sequence_id for chunk in chunks] == [
        "tts-000001",
        "tts-000002",
        "tts-000003",
    ]
    assert "".join(chunk.text for chunk in chunks) == source
    assert all(len(chunk.text) <= 500 for chunk in chunks)


@dataclass
class ObjectRow:
    sequence_id: str
    text: str
    speaker_id: str


def test_rows_can_be_objects_and_duplicate_ids_are_rejected():
    chunks = chunk_tts_rows([ObjectRow("stable", "你好。", "host")])
    assert chunks[0].row_sequence_ids == ("stable",)

    with pytest.raises(ValueError, match="duplicate row sequence_id"):
        chunk_translation_rows(
            [
                {"id": "same", "text": "one"},
                {"id": "same", "text": "two"},
            ]
        )
