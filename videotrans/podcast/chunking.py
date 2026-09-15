"""Deterministic text chunking for the audio-only podcast pipeline.

The module deliberately has no provider dependencies.  It turns transcript-like
rows into stable translation and TTS request units while retaining enough source
identity for checkpointing and ordered assembly.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence


DEFAULT_TTS_VOICE = "Andre"
TRANSLATION_MIN_TOKEN_UNITS = 600
TRANSLATION_MAX_TOKEN_UNITS = 1_000
TTS_SOFT_MAX_CHARACTERS = 500

TokenEstimator = Callable[[str], int]

_TOKEN_PARTS = re.compile(r"[\u3400-\u9fff\uf900-\ufaff]|[A-Za-z0-9_]+|[^\s]")
_STRONG_SENTENCE_END = frozenset("。！？!?；;")
_CLOSING_PUNCTUATION = frozenset("\"'”’）)]】》」』")
_SOFT_SPLIT_PUNCTUATION = frozenset(",，、:：")


@dataclass(frozen=True, slots=True)
class PodcastTextRow:
    """A normalized source or translated transcript row."""

    sequence_id: str
    text: str
    speaker_id: str | None = None


@dataclass(frozen=True, slots=True)
class TranslationChunk:
    """One independently translatable request payload."""

    sequence_id: str
    text: str
    rows: tuple[PodcastTextRow, ...]
    row_sequence_ids: tuple[str, ...]
    speaker_ids: tuple[str, ...]
    token_estimate: int


@dataclass(frozen=True, slots=True)
class TTSChunk:
    """One independently synthesizable request payload."""

    sequence_id: str
    text: str
    speaker_id: str | None
    row_sequence_ids: tuple[str, ...]
    character_count: int
    voice: str = DEFAULT_TTS_VOICE


def estimate_token_units(text: str) -> int:
    """Return a small, deterministic tokenizer-independent size estimate.

    CJK characters and punctuation count as one unit.  Latin/digit runs count as
    one unit per four characters, rounded up.  The estimate is intentionally
    conservative and stable across model/tokenizer upgrades.
    """

    units = 0
    for part in _TOKEN_PARTS.findall(text):
        if part.isascii() and (part[0].isalnum() or part[0] == "_"):
            units += max(1, math.ceil(len(part) / 4))
        else:
            units += 1
    return units


def chunk_translation_rows(
    rows: Iterable[PodcastTextRow | Mapping[str, Any] | Any],
    *,
    min_token_units: int = TRANSLATION_MIN_TOKEN_UNITS,
    max_token_units: int = TRANSLATION_MAX_TOKEN_UNITS,
    token_estimator: TokenEstimator = estimate_token_units,
    text_key: str | None = None,
) -> list[TranslationChunk]:
    """Create deterministic, turn-aware translation chunks.

    A known speaker change is used as a chunk boundary once the current chunk is
    at least ``min_token_units``.  Crossing that boundary is allowed only when it
    helps an undersized chunk approach the target.  ``max_token_units`` is kept as
    a hard request bound; exceptionally long rows are split at sentence, then
    whitespace/punctuation, boundaries before a character-level fallback.
    """

    _validate_limits(min_token_units, max_token_units, "token units")
    normalized = _normalize_rows(rows, text_key=text_key)
    expanded: list[PodcastTextRow] = []
    for row in normalized:
        parts = _split_to_budget(row.text, max_token_units, token_estimator)
        if len(parts) == 1:
            expanded.append(row)
            continue
        expanded.extend(
            PodcastTextRow(
                sequence_id=f"{row.sequence_id}.part-{part_index:04d}",
                text=part,
                speaker_id=row.speaker_id,
            )
            for part_index, part in enumerate(parts, start=1)
        )

    chunks: list[TranslationChunk] = []
    current: list[PodcastTextRow] = []

    def flush() -> None:
        if not current:
            return
        text = "\n".join(row.text for row in current)
        speakers = tuple(
            dict.fromkeys(
                row.speaker_id for row in current if row.speaker_id is not None
            )
        )
        chunks.append(
            TranslationChunk(
                sequence_id=f"translation-{len(chunks) + 1:06d}",
                text=text,
                rows=tuple(current),
                row_sequence_ids=tuple(row.sequence_id for row in current),
                speaker_ids=speakers,
                token_estimate=_measure(text, token_estimator),
            )
        )
        current.clear()

    for row in expanded:
        if not current:
            current.append(row)
            continue

        current_text = "\n".join(item.text for item in current)
        current_size = _measure(current_text, token_estimator)
        speaker_changed = row.speaker_id != current[-1].speaker_id

        # Once the minimum target is met, a turn boundary is the cleanest cut.
        if speaker_changed and current_size >= min_token_units:
            flush()
            current.append(row)
            continue

        candidate_text = f"{current_text}\n{row.text}"
        if _measure(candidate_text, token_estimator) > max_token_units:
            flush()
        current.append(row)

    flush()
    return chunks


def chunk_tts_rows(
    rows: Iterable[PodcastTextRow | Mapping[str, Any] | Any],
    *,
    max_characters: int = TTS_SOFT_MAX_CHARACTERS,
    text_key: str | None = None,
) -> list[TTSChunk]:
    """Coalesce contiguous same-speaker rows into Andre TTS chunks.

    Speaker IDs still define turn boundaries for natural assembly, but never
    select the voice: every returned chunk explicitly uses ``Andre``.  Sentence
    boundaries are preferred, with whitespace/punctuation and finally exact
    Unicode character boundaries used for an overlong sentence.
    """

    if max_characters <= 0:
        raise ValueError("max_characters must be greater than zero")

    normalized = _normalize_rows(
        rows,
        text_key=text_key,
        preferred_text_keys=("translated_text", "target_text", "text"),
    )
    turns: list[list[PodcastTextRow]] = []
    for row in normalized:
        if not turns or turns[-1][-1].speaker_id != row.speaker_id:
            turns.append([row])
        else:
            turns[-1].append(row)

    chunks: list[TTSChunk] = []
    for turn in turns:
        turn_text = " ".join(row.text for row in turn)
        row_ids = tuple(row.sequence_id for row in turn)
        for text in _split_to_budget(turn_text, max_characters, len):
            chunks.append(
                TTSChunk(
                    sequence_id=f"tts-{len(chunks) + 1:06d}",
                    text=text,
                    speaker_id=turn[0].speaker_id,
                    row_sequence_ids=row_ids,
                    character_count=len(text),
                    voice=DEFAULT_TTS_VOICE,
                )
            )
    return chunks


def _normalize_rows(
    rows: Iterable[PodcastTextRow | Mapping[str, Any] | Any],
    *,
    text_key: str | None,
    preferred_text_keys: Sequence[str] = ("text", "source_text", "source"),
) -> list[PodcastTextRow]:
    normalized: list[PodcastTextRow] = []
    seen_ids: set[str] = set()
    keys = (text_key,) if text_key else preferred_text_keys

    for position, value in enumerate(rows, start=1):
        if isinstance(value, PodcastTextRow):
            row = PodcastTextRow(
                sequence_id=str(value.sequence_id),
                text=_normalize_text(value.text),
                speaker_id=(
                    str(value.speaker_id) if value.speaker_id is not None else None
                ),
            )
        else:
            text_value = _first_value(value, keys)
            if text_value is None:
                raise ValueError(f"row {position} has no text value")
            source_id = _first_value(
                value, ("sequence_id", "id", "source_id", "line")
            )
            speaker = _first_value(value, ("speaker_id", "speaker"))
            row = PodcastTextRow(
                sequence_id=(
                    str(source_id)
                    if source_id is not None
                    else f"row-{position:06d}"
                ),
                text=_normalize_text(str(text_value)),
                speaker_id=str(speaker) if speaker is not None else None,
            )

        if not row.text:
            continue
        if row.sequence_id in seen_ids:
            raise ValueError(f"duplicate row sequence_id: {row.sequence_id}")
        seen_ids.add(row.sequence_id)
        normalized.append(row)
    return normalized


def _first_value(value: Mapping[str, Any] | Any, keys: Sequence[str]) -> Any:
    for key in keys:
        if isinstance(value, Mapping):
            if key in value:
                return value[key]
        elif hasattr(value, key):
            return getattr(value, key)
    return None


def _normalize_text(text: str) -> str:
    return " ".join(text.split())


def _validate_limits(minimum: int, maximum: int, unit_name: str) -> None:
    if minimum <= 0 or maximum <= 0:
        raise ValueError(f"{unit_name} limits must be greater than zero")
    if minimum > maximum:
        raise ValueError(f"minimum {unit_name} cannot exceed maximum")


def _measure(text: str, estimator: TokenEstimator) -> int:
    measured = int(estimator(text))
    if measured < 0:
        raise ValueError("text size estimator cannot return a negative value")
    return measured


def _split_to_budget(
    text: str, maximum: int, estimator: TokenEstimator
) -> list[str]:
    """Split normalized text while preferring complete sentences."""

    text = _normalize_text(text)
    if not text:
        return []
    if _measure(text, estimator) <= maximum:
        return [text]

    atomic_parts: list[str] = []
    for sentence in _sentence_parts(text):
        if _measure(sentence, estimator) <= maximum:
            atomic_parts.append(sentence)
        else:
            atomic_parts.extend(_split_overlong_part(sentence, maximum, estimator))

    packed: list[str] = []
    current = ""
    for part in atomic_parts:
        candidate = part if not current else f"{current} {part}"
        if current and _measure(candidate, estimator) > maximum:
            packed.append(current)
            current = part
        else:
            current = candidate
    if current:
        packed.append(current)
    return packed


def _sentence_parts(text: str) -> list[str]:
    parts: list[str] = []
    start = 0
    index = 0
    length = len(text)
    while index < length:
        character = text[index]
        boundary = character in _STRONG_SENTENCE_END
        if character == ".":
            boundary = index + 1 == length or text[index + 1].isspace()
            if 0 < index < length - 1:
                boundary = boundary and not (
                    text[index - 1].isdigit() and text[index + 1].isdigit()
                )
        if not boundary:
            index += 1
            continue

        end = index + 1
        while end < length and text[end] in _CLOSING_PUNCTUATION:
            end += 1
        part = text[start:end].strip()
        if part:
            parts.append(part)
        while end < length and text[end].isspace():
            end += 1
        start = end
        index = end
    remainder = text[start:].strip()
    if remainder:
        parts.append(remainder)
    return parts or [text]


def _split_overlong_part(
    text: str, maximum: int, estimator: TokenEstimator
) -> list[str]:
    parts: list[str] = []
    remaining = text.strip()
    while remaining:
        if _measure(remaining, estimator) <= maximum:
            parts.append(remaining)
            break

        low, high = 1, len(remaining)
        while low < high:
            middle = (low + high + 1) // 2
            if _measure(remaining[:middle], estimator) <= maximum:
                low = middle
            else:
                high = middle - 1
        cut = max(1, low)

        # Prefer a readable boundary in the latter half of the legal prefix.
        preferred = 0
        for offset in range(cut - 1, max(0, cut // 2) - 1, -1):
            if (
                remaining[offset].isspace()
                or remaining[offset] in _SOFT_SPLIT_PUNCTUATION
            ):
                preferred = offset + 1
                break
        if preferred:
            cut = preferred

        part = remaining[:cut].strip()
        if not part:
            part = remaining[:1]
            cut = 1
        parts.append(part)
        remaining = remaining[cut:].strip()
    return parts


__all__ = [
    "DEFAULT_TTS_VOICE",
    "PodcastTextRow",
    "TTSChunk",
    "TTS_SOFT_MAX_CHARACTERS",
    "TRANSLATION_MAX_TOKEN_UNITS",
    "TRANSLATION_MIN_TOKEN_UNITS",
    "TranslationChunk",
    "chunk_translation_rows",
    "chunk_tts_rows",
    "estimate_token_units",
]
