"""Deterministic text chunking for the audio-only podcast pipeline.

The module deliberately has no provider dependencies.  It turns transcript-like
rows into stable translation and TTS request units while retaining enough source
identity for checkpointing and ordered assembly.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

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


class IncrementalTTSChunker:
    """Seal deterministic TTS chunks from source-ordered translated rows.

    ``add`` returns only chunks whose text can no longer change.  The final
    mutable tail remains buffered until a later row fixes its boundary, the
    speaker changes, or ``finish`` is called.
    """

    def __init__(self, *, max_characters: int = TTS_SOFT_MAX_CHARACTERS) -> None:
        if max_characters <= 0:
            raise ValueError("max_characters must be greater than zero")

        self._max_characters = max_characters
        self._speaker_id: str | None = None
        self._pending_text = ""
        self._pending_origins: list[str | None] = []
        self._seen_ids: set[str] = set()
        self._next_sequence = 1
        self._finished = False
        self._turn_requires_sentence_splitting = False

    def add(self, row: PodcastTextRow) -> list[TTSChunk]:
        """Add one source-ordered row and return newly sealed chunks."""

        if self._finished:
            raise RuntimeError("cannot add rows after the TTS chunker is finished")
        if not isinstance(row, PodcastTextRow):
            raise TypeError("row must be a PodcastTextRow")

        normalized = PodcastTextRow(
            sequence_id=str(row.sequence_id),
            text=_normalize_text(row.text),
            speaker_id=(str(row.speaker_id) if row.speaker_id is not None else None),
        )
        if not normalized.text:
            return []
        if normalized.sequence_id in self._seen_ids:
            raise ValueError(f"duplicate row sequence_id: {normalized.sequence_id}")
        self._seen_ids.add(normalized.sequence_id)

        sealed: list[TTSChunk] = []
        if self._pending_text and normalized.speaker_id != self._speaker_id:
            sealed.extend(self._seal_all())
            self._turn_requires_sentence_splitting = False

        if not self._pending_text:
            self._speaker_id = normalized.speaker_id
        else:
            self._pending_text += " "
            self._pending_origins.append(None)

        self._pending_text += normalized.text
        self._pending_origins.extend(normalized.sequence_id for _ in normalized.text)
        if len(self._pending_text) > self._max_characters:
            self._turn_requires_sentence_splitting = True
        sealed.extend(self._seal_stable_prefix())
        return sealed

    def finish(self) -> list[TTSChunk]:
        """Seal the final mutable tail and end the incremental input."""

        if self._finished:
            return []
        self._finished = True
        return self._seal_all()

    def flush(self) -> list[TTSChunk]:
        """Alias for ``finish`` for callers that model end-of-input as flush."""

        return self.finish()

    def _seal_stable_prefix(self) -> list[TTSChunk]:
        if not self._turn_requires_sentence_splitting:
            if len(self._pending_text) != self._max_characters:
                return []
            parts = _split_normalized_to_budget(
                self._pending_text, self._max_characters, len
            )
            if parts != [self._pending_text]:
                return []
            self._turn_requires_sentence_splitting = True
        else:
            parts = _split_normalized_to_budget(
                self._pending_text, self._max_characters, len
            )

        stable_count = max(0, len(parts) - 1)
        if parts and len(parts[-1]) == self._max_characters:
            stable_count += 1
        return [self._seal_prefix(part) for part in parts[:stable_count]]

    def _seal_all(self) -> list[TTSChunk]:
        if self._turn_requires_sentence_splitting:
            parts = _split_normalized_to_budget(
                self._pending_text, self._max_characters, len
            )
        else:
            parts = _split_to_budget(self._pending_text, self._max_characters, len)
        return [self._seal_prefix(part) for part in parts]

    def _seal_prefix(self, text: str) -> TTSChunk:
        consumed = self._consumed_prefix_length(text)
        row_ids = tuple(
            dict.fromkeys(
                origin
                for origin in self._pending_origins[:consumed]
                if origin is not None
            )
        )
        while (
            consumed < len(self._pending_text)
            and self._pending_text[consumed].isspace()
        ):
            consumed += 1

        self._pending_text = self._pending_text[consumed:]
        del self._pending_origins[:consumed]
        chunk = TTSChunk(
            sequence_id=f"tts-{self._next_sequence:06d}",
            text=text,
            speaker_id=self._speaker_id,
            row_sequence_ids=row_ids,
            character_count=len(text),
            voice=DEFAULT_TTS_VOICE,
        )
        self._next_sequence += 1
        return chunk

    def _consumed_prefix_length(self, rendered_text: str) -> int:
        """Map sentence-normalized output back to its pending source prefix."""

        return _rendered_prefix_length(self._pending_text, rendered_text)


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

    Known speaker turn boundaries are hard. Within one turn, the first row
    boundary after ``min_token_units`` is the preferred packing cut.
    ``max_token_units`` remains a hard request bound; exceptionally long rows
    are split at sentence, then whitespace/punctuation, boundaries before a
    character-level fallback.
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

        # A known turn boundary is hard: translated text must remain mappable
        # to one speaker turn for deterministic TTS chunking.
        if speaker_changed:
            flush()
            current.append(row)
            continue

        if current_size >= min_token_units:
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
        pending_text = ""
        pending_origins: list[str | None] = []
        for row in turn:
            if pending_text:
                pending_text += " "
                pending_origins.append(None)
            pending_text += row.text
            pending_origins.extend(row.sequence_id for _ in row.text)

        for text in _split_to_budget(pending_text, max_characters, len):
            consumed = _rendered_prefix_length(pending_text, text)
            row_ids = tuple(
                dict.fromkeys(
                    origin
                    for origin in pending_origins[:consumed]
                    if origin is not None
                )
            )
            while consumed < len(pending_text) and pending_text[consumed].isspace():
                consumed += 1
            pending_text = pending_text[consumed:]
            del pending_origins[:consumed]
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


def _rendered_prefix_length(source: str, rendered: str) -> int:
    """Map normalized sentence-packed output back to its source prefix."""

    source_index = 0
    for character in rendered:
        if character.isspace():
            while source_index < len(source) and source[source_index].isspace():
                source_index += 1
            continue
        while source_index < len(source) and source[source_index].isspace():
            source_index += 1
        if source_index >= len(source) or source[source_index] != character:
            raise RuntimeError("TTS chunk does not match its canonical source prefix")
        source_index += 1
    return source_index


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
            source_id = _first_value(value, ("sequence_id", "id", "source_id", "line"))
            speaker = _first_value(value, ("speaker_id", "speaker"))
            row = PodcastTextRow(
                sequence_id=(
                    str(source_id) if source_id is not None else f"row-{position:06d}"
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


def _split_to_budget(text: str, maximum: int, estimator: TokenEstimator) -> list[str]:
    """Split normalized text while preferring complete sentences."""

    text = _normalize_text(text)
    if not text:
        return []
    if _measure(text, estimator) <= maximum:
        return [text]

    return _split_normalized_to_budget(text, maximum, estimator)


def _split_normalized_to_budget(
    text: str, maximum: int, estimator: TokenEstimator
) -> list[str]:
    """Split normalized over-budget text, including sentence rendering."""

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
    "TRANSLATION_MAX_TOKEN_UNITS",
    "TRANSLATION_MIN_TOKEN_UNITS",
    "TTS_SOFT_MAX_CHARACTERS",
    "IncrementalTTSChunker",
    "PodcastTextRow",
    "TTSChunk",
    "TranslationChunk",
    "chunk_translation_rows",
    "chunk_tts_rows",
    "estimate_token_units",
]
