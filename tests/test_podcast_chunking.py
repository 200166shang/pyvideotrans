from dataclasses import dataclass

import pytest

from videotrans.podcast.chunking import (
    DEFAULT_TTS_VOICE,
    IncrementalTTSChunker,
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
        {
            "id": 1,
            "speaker_id": "host",
            "translated_text": "甲" * 240 + "。",
            "voice": "Cherry",
        },
        {"id": 2, "speaker_id": "host", "translated_text": "乙" * 240 + "。"},
        {"id": 3, "speaker_id": "host", "translated_text": "丙" * 240 + "。"},
        {"id": 4, "speaker_id": "guest", "translated_text": "最后一句。"},
    ]

    chunks = chunk_tts_rows(rows)

    assert [chunk.speaker_id for chunk in chunks] == ["host", "host", "guest"]
    assert [chunk.row_sequence_ids for chunk in chunks] == [
        ("1", "2"),
        ("3",),
        ("4",),
    ]
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


def test_incremental_tts_seals_an_exact_boundary_immediately():
    chunker = IncrementalTTSChunker()

    sealed = chunker.add(PodcastTextRow("row-1", "甲" * 500, "host"))

    assert [chunk.text for chunk in sealed] == ["甲" * 500]
    assert sealed[0].row_sequence_ids == ("row-1",)
    assert chunker.finish() == []


def test_incremental_tts_does_not_seal_ambiguous_exact_sentence_boundary():
    source = "甲。" * 250
    chunker = IncrementalTTSChunker()

    assert chunker.add(PodcastTextRow("row-1", source, "host")) == []

    assert [chunk.text for chunk in chunker.finish()] == [source]


def test_incremental_tts_maps_inserted_sentence_spacing_to_source_rows():
    chunker = IncrementalTTSChunker(max_characters=5)

    sealed = chunker.add(PodcastTextRow("row-1", "甲。乙。丙。", "host"))

    assert [(chunk.text, chunk.row_sequence_ids) for chunk in sealed] == [
        ("甲。 乙。", ("row-1",))
    ]
    assert [chunk.text for chunk in chunker.finish()] == ["丙。"]


def test_incremental_tts_flushes_tail_before_new_speaker():
    chunker = IncrementalTTSChunker()

    assert chunker.add(PodcastTextRow("host-1", "主持人的尾句。", "host")) == []
    sealed = chunker.add(PodcastTextRow("guest-1", "嘉宾开场。", "guest"))

    assert [(chunk.text, chunk.speaker_id) for chunk in sealed] == [
        ("主持人的尾句。", "host")
    ]
    assert [(chunk.text, chunk.speaker_id) for chunk in chunker.finish()] == [
        ("嘉宾开场。", "guest")
    ]


def test_incremental_tts_keeps_tail_across_translation_chunks():
    chunker = IncrementalTTSChunker()
    first_translation_chunk = [
        PodcastTextRow("row-1", "甲" * 200 + "。", "host"),
        PodcastTextRow("row-2", "乙" * 200 + "。", "host"),
    ]
    next_translation_chunk = [PodcastTextRow("row-3", "丙" * 200 + "。", "host")]

    assert [
        chunk for row in first_translation_chunk for chunk in chunker.add(row)
    ] == []
    sealed = [chunk for row in next_translation_chunk for chunk in chunker.add(row)]

    assert [chunk.text for chunk in sealed] == [f"{'甲' * 200}。 {'乙' * 200}。"]
    assert sealed[0].row_sequence_ids == ("row-1", "row-2")
    assert [chunk.row_sequence_ids for chunk in chunker.finish()] == [("row-3",)]


def test_incremental_tts_seals_overlong_sentence_prefixes():
    chunker = IncrementalTTSChunker()
    source = "沉" * 1_101

    sealed = chunker.add(PodcastTextRow("long", source, "host"))
    tail = chunker.finish()

    assert [len(chunk.text) for chunk in sealed] == [500, 500]
    assert [len(chunk.text) for chunk in tail] == [101]
    assert "".join(chunk.text for chunk in [*sealed, *tail]) == source
    assert all(chunk.row_sequence_ids == ("long",) for chunk in [*sealed, *tail])


def test_incremental_tts_empty_input_and_finish_are_idempotent():
    chunker = IncrementalTTSChunker()

    assert chunker.flush() == []
    assert chunker.finish() == []
    with pytest.raises(RuntimeError, match="after the TTS chunker is finished"):
        chunker.add(PodcastTextRow("late", "too late", "host"))


@pytest.mark.parametrize(
    "rows,max_characters",
    [
        ([], 500),
        ([PodcastTextRow("one", "短句。", "host")], 500),
        (
            [
                PodcastTextRow("one", "A" * 240 + ".", "host"),
                PodcastTextRow("two", "B" * 240 + ".", "host"),
                PodcastTextRow("three", "C" * 240 + ".", "host"),
                PodcastTextRow("four", "换人。", "guest"),
            ],
            500,
        ),
        (
            [
                PodcastTextRow("one", "x" * 51, None),
                PodcastTextRow("two", "y" * 11, None),
            ],
            25,
        ),
    ],
)
def test_incremental_tts_matches_batch_for_complete_input(rows, max_characters):
    chunker = IncrementalTTSChunker(max_characters=max_characters)
    incremental = [chunk for row in rows for chunk in chunker.add(row)]
    incremental.extend(chunker.finish())

    assert incremental == chunk_tts_rows(rows, max_characters=max_characters)


def test_throughput_chunks_preserve_turns_and_text_across_translation_boundaries():
    from videotrans.podcast.profiles import ALIBABA_PODCAST_TTS_THROUGHPUT

    limit = ALIBABA_PODCAST_TTS_THROUGHPUT.tts_soft_char_limit
    rows = [
        PodcastTextRow("one", "这是一段完整的话。" * 29, "host"),
        PodcastTextRow("two", "后面的内容保持顺序。" * 31, "host"),
        PodcastTextRow("three", "来宾接着回应。" * 41, "guest"),
        PodcastTextRow("four", "主持人重新发言。" * 37, "host"),
    ]
    chunker = IncrementalTTSChunker(max_characters=limit)
    actual = [chunk for row in rows for chunk in chunker.add(row)]
    actual.extend(chunker.finish())
    assert actual == chunk_tts_rows(rows, max_characters=limit)
    expected_chars = [
        (row.speaker_id, char)
        for row in rows
        for char in row.text
        if not char.isspace()
    ]
    actual_chars = [
        (chunk.speaker_id, char)
        for chunk in actual
        for char in chunk.text
        if not char.isspace()
    ]
    assert actual_chars == expected_chars
    assert all(len(chunk.text) <= limit and chunk.voice == "Andre" for chunk in actual)


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
