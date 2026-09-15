"""Privacy-safe production reports for the podcast pipeline.

The report deliberately contains measurements and stable fingerprints, but no
source path, credentials, transcript, subtitles, or provider response bodies.
It is a small, dependency-free contract so the CLI can also use it while
recovering from a partially completed run.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0"
STAGE_ORDER = ("prepare", "asr", "translate", "tts", "finalize")

_TOP_LEVEL_KEYS = {
    "schema_version",
    "run",
    "input",
    "profile",
    "stages",
    "totals",
    "output",
    "listening_quality_gate",
    "privacy",
}
_RUN_KEYS = {"id", "status", "cache_mode"}
_INPUT_KEYS = {"fingerprint", "duration_ms", "language"}
_PROFILE_KEYS = {
    "id",
    "fingerprint",
    "target_language",
    "region",
    "providers",
}
_PROVIDER_KEYS = {"asr", "translation", "tts", "voice"}
_STAGE_KEYS = {
    "name",
    "provider",
    "status",
    "attempts",
    "retries",
    "elapsed_ms",
    "cache_hit",
    "cost_confirmed_cny",
    "cost_unconfirmed_cny",
    "chunks",
    "artifact_fingerprint",
    "error_code",
}
_CHUNK_KEYS = {
    "id",
    "status",
    "attempts",
    "retries",
    "elapsed_ms",
    "cache_hit",
    "cost_confirmed_cny",
    "cost_unconfirmed_cny",
    "artifact_fingerprint",
    "error_code",
}
_TOTAL_KEYS = {
    "wall_clock_ms",
    "cost_confirmed_cny",
    "cost_unconfirmed_cny",
    "retries",
    "cache_hits",
}
_OUTPUT_KEYS = {"format", "bitrate_kbps", "channels", "duration_ms", "fingerprint"}
_QUALITY_KEYS = {"status", "decided_at", "note"}
_PRIVACY_KEYS = {
    "contains_credentials",
    "contains_transcript",
    "contains_absolute_source_path",
}

_RUN_STATUSES = {
    "draft",
    "running",
    "failed",
    "awaiting_review",
    "accepted",
    "rejected",
}
_WORK_STATUSES = {"pending", "running", "succeeded", "cached", "failed", "skipped"}
_QUALITY_STATUSES = {"pending", "accepted", "rejected"}
_FINGERPRINT_RE = re.compile(r"^(?:sha256:)?[0-9a-f]{64}$")
_ERROR_CODE_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
_WINDOWS_ABSOLUTE_PATH_RE = re.compile(r"^[A-Za-z]:[\\/]")

# Exact or normalized key matches keep numeric usage fields such as
# ``input_tokens`` valid while rejecting actual bearer-token fields.
_UNSAFE_EXACT_KEYS = {
    "access_key",
    "access_key_id",
    "access_key_secret",
    "access_token",
    "api_key",
    "apikey",
    "authorization",
    "bearer_token",
    "client_secret",
    "credential",
    "credentials",
    "password",
    "secret",
    "secret_key",
    "source_path",
    "absolute_source_path",
    "input_path",
    "transcript",
    "transcript_text",
    "subtitle",
    "subtitles",
    "caption",
    "captions",
    "token",
}
_UNSAFE_KEY_PARTS = ("password", "credential", "api_key", "access_key", "transcript")


class ReportValidationError(ValueError):
    """Raised when a production report violates its schema or privacy rules."""


def fingerprint_bytes(data: bytes) -> str:
    """Return a stable, explicitly labelled SHA-256 fingerprint."""

    if not isinstance(data, bytes):
        raise TypeError("fingerprint data must be bytes")
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def fingerprint_file(
    path: str | os.PathLike[str], *, block_size: int = 1024 * 1024
) -> str:
    """Stream a file into a SHA-256 fingerprint without retaining its path."""

    if isinstance(block_size, bool) or not isinstance(block_size, int) or block_size <= 0:
        raise ValueError("block_size must be a positive integer")
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        while block := source.read(block_size):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


def fingerprint_profile(profile: Mapping[str, Any]) -> str:
    """Fingerprint canonical profile settings, ignoring an existing fingerprint."""

    if not isinstance(profile, Mapping):
        raise TypeError("profile must be a mapping")
    safe_profile = {key: value for key, value in profile.items() if key != "fingerprint"}
    _validate_privacy(safe_profile)
    try:
        canonical = json.dumps(
            safe_profile,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ReportValidationError("profile must contain JSON-safe values") from error
    return fingerprint_bytes(canonical)


def public_list_price_total(
    usage: Mapping[str, int | float | Decimal],
    prices: Mapping[str, int | float | Decimal | Mapping[str, int | float | Decimal]],
) -> float:
    """Calculate an unconfirmed CNY estimate from numeric provider usage.

    A numeric price means CNY per single usage unit. A mapping can specify
    ``price_cny`` and ``per_units``; for example, ``0.70`` per ``1_000_000``
    input tokens. The helper intentionally accepts no provider response object
    or text, and its result belongs in ``cost_unconfirmed_cny`` until billing
    confirms the amount.
    """

    if not isinstance(usage, Mapping) or not isinstance(prices, Mapping):
        raise TypeError("usage and prices must be mappings")

    total = Decimal(0)
    for metric, raw_usage in usage.items():
        if not isinstance(metric, str) or not metric:
            raise TypeError("usage metric names must be non-empty strings")
        used = _decimal_number(raw_usage, f"usage[{metric!r}]")
        if used < 0:
            raise ValueError(f"usage[{metric!r}] must be non-negative")
        if metric not in prices:
            raise ValueError(f"missing public list price for usage metric {metric!r}")

        price_spec = prices[metric]
        if isinstance(price_spec, Mapping):
            unknown = set(price_spec) - {"price_cny", "per_units"}
            if unknown or "price_cny" not in price_spec:
                raise ValueError(
                    f"price[{metric!r}] must contain price_cny and optional per_units"
                )
            price = _decimal_number(price_spec["price_cny"], f"price[{metric!r}].price_cny")
            per_units = _decimal_number(
                price_spec.get("per_units", 1), f"price[{metric!r}].per_units"
            )
        else:
            price = _decimal_number(price_spec, f"price[{metric!r}]")
            per_units = Decimal(1)
        if price < 0:
            raise ValueError(f"price[{metric!r}] must be non-negative")
        if per_units <= 0:
            raise ValueError(f"price[{metric!r}].per_units must be positive")
        total += used * price / per_units

    return float(total)


def make_chunk(
    chunk_id: str,
    *,
    status: str = "pending",
    attempts: int = 0,
    retries: int = 0,
    elapsed_ms: int = 0,
    cache_hit: bool = False,
    cost_confirmed_cny: float | Decimal = 0,
    cost_unconfirmed_cny: float | Decimal = 0,
    artifact_fingerprint: str | None = None,
    error_code: str | None = None,
) -> dict[str, Any]:
    """Create and validate a privacy-safe chunk measurement."""

    chunk = {
        "id": chunk_id,
        "status": status,
        "attempts": attempts,
        "retries": retries,
        "elapsed_ms": elapsed_ms,
        "cache_hit": cache_hit,
        "cost_confirmed_cny": _json_number(cost_confirmed_cny, "cost_confirmed_cny"),
        "cost_unconfirmed_cny": _json_number(cost_unconfirmed_cny, "cost_unconfirmed_cny"),
        "artifact_fingerprint": artifact_fingerprint,
        "error_code": error_code,
    }
    _validate_chunk(chunk, "chunk")
    return chunk


def make_stage(
    name: str,
    *,
    provider: str,
    status: str = "pending",
    attempts: int = 0,
    retries: int = 0,
    elapsed_ms: int = 0,
    cache_hit: bool = False,
    cost_confirmed_cny: float | Decimal = 0,
    cost_unconfirmed_cny: float | Decimal = 0,
    chunks: Sequence[Mapping[str, Any]] = (),
    artifact_fingerprint: str | None = None,
    error_code: str | None = None,
) -> dict[str, Any]:
    """Create and validate one stage measurement."""

    stage = {
        "name": name,
        "provider": provider,
        "status": status,
        "attempts": attempts,
        "retries": retries,
        "elapsed_ms": elapsed_ms,
        "cache_hit": cache_hit,
        "cost_confirmed_cny": _json_number(cost_confirmed_cny, "cost_confirmed_cny"),
        "cost_unconfirmed_cny": _json_number(cost_unconfirmed_cny, "cost_unconfirmed_cny"),
        "chunks": [deepcopy(dict(chunk)) for chunk in chunks],
        "artifact_fingerprint": artifact_fingerprint,
        "error_code": error_code,
    }
    _validate_stage(stage, f"stage {name!r}")
    return stage


def calculate_totals(
    stages: Sequence[Mapping[str, Any]], *, wall_clock_ms: int | None = None
) -> dict[str, int | float]:
    """Aggregate stage-level totals without counting child chunks twice."""

    if isinstance(stages, (str, bytes)) or not isinstance(stages, Sequence):
        raise TypeError("stages must be a sequence")
    for index, stage in enumerate(stages):
        _validate_stage(stage, f"stages[{index}]")

    if wall_clock_ms is None:
        wall_clock_ms = sum(stage["elapsed_ms"] for stage in stages)
    _non_negative_int(wall_clock_ms, "wall_clock_ms")
    confirmed = sum(
        (_decimal_number(stage["cost_confirmed_cny"], "cost") for stage in stages),
        Decimal(0),
    )
    unconfirmed = sum(
        (_decimal_number(stage["cost_unconfirmed_cny"], "cost") for stage in stages),
        Decimal(0),
    )
    return {
        "wall_clock_ms": wall_clock_ms,
        "cost_confirmed_cny": float(confirmed),
        "cost_unconfirmed_cny": float(unconfirmed),
        "retries": sum(stage["retries"] for stage in stages),
        "cache_hits": sum(1 for stage in stages if stage["cache_hit"]),
    }


def build_report(
    *,
    run_id: str,
    run_status: str,
    cache_mode: str,
    input_fingerprint: str,
    input_duration_ms: int,
    input_language: str,
    profile: Mapping[str, Any],
    stages: Sequence[Mapping[str, Any]],
    output: Mapping[str, Any] | None = None,
    listening_quality_gate: Mapping[str, Any] | None = None,
    wall_clock_ms: int | None = None,
    schema_version: str = SCHEMA_VERSION,
) -> dict[str, Any]:
    """Build the complete report contract and reject unsafe data."""

    profile_copy = deepcopy(dict(profile))
    profile_copy["fingerprint"] = profile_copy.get("fingerprint") or fingerprint_profile(
        profile_copy
    )
    stage_copies = [deepcopy(dict(stage)) for stage in stages]
    quality = deepcopy(
        dict(
            listening_quality_gate
            or {"status": "pending", "decided_at": None, "note": None}
        )
    )
    report = {
        "schema_version": schema_version,
        "run": {"id": run_id, "status": run_status, "cache_mode": cache_mode},
        "input": {
            "fingerprint": input_fingerprint,
            "duration_ms": input_duration_ms,
            "language": input_language,
        },
        "profile": profile_copy,
        "stages": stage_copies,
        "totals": calculate_totals(stage_copies, wall_clock_ms=wall_clock_ms),
        "output": deepcopy(dict(output)) if output is not None else None,
        "listening_quality_gate": quality,
        "privacy": {
            "contains_credentials": False,
            "contains_transcript": False,
            "contains_absolute_source_path": False,
        },
    }
    validate_report(report)
    return report


def validate_report(report: Mapping[str, Any]) -> None:
    """Validate the strict schema and scan every field for privacy hazards."""

    if not isinstance(report, Mapping):
        raise ReportValidationError("report must be a mapping")
    _validate_privacy(report)
    _require_exact_keys(report, _TOP_LEVEL_KEYS, "report")
    if report["schema_version"] != SCHEMA_VERSION:
        raise ReportValidationError(f"schema_version must be {SCHEMA_VERSION!r}")

    run = _mapping(report["run"], "run")
    _require_exact_keys(run, _RUN_KEYS, "run")
    _safe_identifier(run["id"], "run.id")
    if run["status"] not in _RUN_STATUSES:
        raise ReportValidationError("run.status is invalid")
    if run["cache_mode"] not in {"cold", "warm"}:
        raise ReportValidationError("run.cache_mode must be 'cold' or 'warm'")

    input_data = _mapping(report["input"], "input")
    _require_exact_keys(input_data, _INPUT_KEYS, "input")
    _fingerprint(input_data["fingerprint"], "input.fingerprint")
    _non_negative_int(input_data["duration_ms"], "input.duration_ms")
    _safe_identifier(input_data["language"], "input.language")

    profile = _mapping(report["profile"], "profile")
    _require_exact_keys(profile, _PROFILE_KEYS, "profile")
    _safe_identifier(profile["id"], "profile.id")
    _fingerprint(profile["fingerprint"], "profile.fingerprint")
    _safe_identifier(profile["target_language"], "profile.target_language")
    _safe_identifier(profile["region"], "profile.region")
    providers = _mapping(profile["providers"], "profile.providers")
    _require_exact_keys(providers, _PROVIDER_KEYS, "profile.providers")
    for provider_name, provider in providers.items():
        _safe_identifier(provider, f"profile.providers.{provider_name}")

    stages = report["stages"]
    if isinstance(stages, (str, bytes)) or not isinstance(stages, Sequence):
        raise ReportValidationError("stages must be a sequence")
    names = [stage.get("name") if isinstance(stage, Mapping) else None for stage in stages]
    if names != list(STAGE_ORDER):
        raise ReportValidationError(
            "report must contain ordered stages prepare/asr/translate/tts/finalize"
        )
    for index, stage in enumerate(stages):
        _validate_stage(stage, f"stages[{index}]")

    totals = _mapping(report["totals"], "totals")
    _require_exact_keys(totals, _TOTAL_KEYS, "totals")
    _non_negative_int(totals["wall_clock_ms"], "totals.wall_clock_ms")
    _non_negative_number(totals["cost_confirmed_cny"], "totals.cost_confirmed_cny")
    _non_negative_number(totals["cost_unconfirmed_cny"], "totals.cost_unconfirmed_cny")
    _non_negative_int(totals["retries"], "totals.retries")
    _non_negative_int(totals["cache_hits"], "totals.cache_hits")
    expected = calculate_totals(stages, wall_clock_ms=totals["wall_clock_ms"])
    for key in ("cost_confirmed_cny", "cost_unconfirmed_cny", "retries", "cache_hits"):
        if _decimal_number(totals[key], f"totals.{key}") != _decimal_number(
            expected[key], f"expected.{key}"
        ):
            raise ReportValidationError(f"totals.{key} does not match the stages")

    output = report["output"]
    if output is not None:
        output = _mapping(output, "output")
        _require_exact_keys(output, _OUTPUT_KEYS, "output")
        _safe_identifier(output["format"], "output.format")
        _non_negative_int(output["bitrate_kbps"], "output.bitrate_kbps")
        _non_negative_int(output["channels"], "output.channels")
        _non_negative_int(output["duration_ms"], "output.duration_ms")
        _fingerprint(output["fingerprint"], "output.fingerprint")

    quality = _mapping(report["listening_quality_gate"], "listening_quality_gate")
    _require_exact_keys(quality, _QUALITY_KEYS, "listening_quality_gate")
    if quality["status"] not in _QUALITY_STATUSES:
        raise ReportValidationError("listening_quality_gate.status is invalid")
    for key in ("decided_at", "note"):
        value = quality[key]
        if value is not None and not isinstance(value, str):
            raise ReportValidationError(
                f"listening_quality_gate.{key} must be a string or null"
            )
    if isinstance(quality["note"], str) and len(quality["note"]) > 1_000:
        raise ReportValidationError("listening_quality_gate.note is too long")

    privacy = _mapping(report["privacy"], "privacy")
    _require_exact_keys(privacy, _PRIVACY_KEYS, "privacy")
    if privacy != {key: False for key in _PRIVACY_KEYS}:
        raise ReportValidationError("privacy flags must all be false")


def write_report(report: Mapping[str, Any], target_path: str | os.PathLike[str]) -> Path:
    """Validate and atomically write a report as UTF-8 JSON.

    The temporary file is created beside the destination, flushed, and fsynced
    before ``os.replace``. Thus readers see either the previous complete report
    or the new complete report, never a partially written document.
    """

    validate_report(report)
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=target.parent,
        prefix=f".{target.name}.",
        suffix=".tmp",
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(report, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    except Exception:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    return target


# Explicit aliases make call sites read naturally and preserve the important
# distinction that public-list-price values are estimates, not confirmed bills.
atomic_write_report = write_report
calculate_public_list_price_total = public_list_price_total


def _validate_stage(stage: Mapping[str, Any], location: str) -> None:
    stage = _mapping(stage, location)
    _require_exact_keys(stage, _STAGE_KEYS, location)
    _safe_identifier(stage["name"], f"{location}.name")
    _safe_identifier(stage["provider"], f"{location}.provider")
    _validate_work_metrics(stage, location)
    chunks = stage["chunks"]
    if isinstance(chunks, (str, bytes)) or not isinstance(chunks, Sequence):
        raise ReportValidationError(f"{location}.chunks must be a sequence")
    seen_chunk_ids: set[str] = set()
    for index, chunk in enumerate(chunks):
        _validate_chunk(chunk, f"{location}.chunks[{index}]")
        if chunk["id"] in seen_chunk_ids:
            raise ReportValidationError(f"{location}.chunks contains duplicate ids")
        seen_chunk_ids.add(chunk["id"])


def _validate_chunk(chunk: Mapping[str, Any], location: str) -> None:
    chunk = _mapping(chunk, location)
    _require_exact_keys(chunk, _CHUNK_KEYS, location)
    _safe_identifier(chunk["id"], f"{location}.id")
    _validate_work_metrics(chunk, location)


def _validate_work_metrics(item: Mapping[str, Any], location: str) -> None:
    if item["status"] not in _WORK_STATUSES:
        raise ReportValidationError(f"{location}.status is invalid")
    _non_negative_int(item["attempts"], f"{location}.attempts")
    _non_negative_int(item["retries"], f"{location}.retries")
    if item["retries"] > max(item["attempts"] - 1, 0):
        raise ReportValidationError(f"{location}.retries cannot exceed attempts minus one")
    _non_negative_int(item["elapsed_ms"], f"{location}.elapsed_ms")
    if not isinstance(item["cache_hit"], bool):
        raise ReportValidationError(f"{location}.cache_hit must be boolean")
    _non_negative_number(item["cost_confirmed_cny"], f"{location}.cost_confirmed_cny")
    _non_negative_number(item["cost_unconfirmed_cny"], f"{location}.cost_unconfirmed_cny")
    artifact = item["artifact_fingerprint"]
    if artifact is not None:
        _fingerprint(artifact, f"{location}.artifact_fingerprint")
    if item["status"] in {"succeeded", "cached"} and artifact is None:
        raise ReportValidationError(
            f"{location}.artifact_fingerprint is required for completed work"
        )
    error_code = item["error_code"]
    if error_code is not None and (
        not isinstance(error_code, str) or not _ERROR_CODE_RE.fullmatch(error_code)
    ):
        raise ReportValidationError(
            f"{location}.error_code must be a safe machine code or null"
        )


def _validate_privacy(value: Any, location: str = "report") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ReportValidationError(f"{location} contains a non-string field name")
            normalized = key.lower().replace("-", "_")
            is_declared_privacy_flag = location == "report.privacy" and key in _PRIVACY_KEYS
            if not is_declared_privacy_flag and (
                normalized in _UNSAFE_EXACT_KEYS
                or any(part in normalized for part in _UNSAFE_KEY_PARTS)
            ):
                raise ReportValidationError(f"unsafe field {key!r} is not allowed in reports")
            _validate_privacy(child, f"{location}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _validate_privacy(child, f"{location}[{index}]")
    elif isinstance(value, str) and _looks_like_absolute_path(value):
        raise ReportValidationError(f"absolute path is not allowed at {location}")


def _looks_like_absolute_path(value: str) -> bool:
    stripped = value.strip()
    return (
        stripped.startswith("/")
        or stripped.lower().startswith("file://")
        or bool(_WINDOWS_ABSOLUTE_PATH_RE.match(stripped))
    )


def _mapping(value: Any, location: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReportValidationError(f"{location} must be an object")
    return value


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], location: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        details = []
        if missing:
            details.append(f"missing {missing}")
        if extra:
            details.append(f"unexpected {extra}")
        raise ReportValidationError(f"{location} fields are invalid: {', '.join(details)}")


def _safe_identifier(value: Any, location: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ReportValidationError(
            f"{location} must be a non-empty string up to 256 characters"
        )
    if any(character in value for character in ("\n", "\r", "\x00")):
        raise ReportValidationError(f"{location} contains control characters")


def _fingerprint(value: Any, location: str) -> None:
    if not isinstance(value, str) or not _FINGERPRINT_RE.fullmatch(value):
        raise ReportValidationError(f"{location} must be a SHA-256 fingerprint")


def _non_negative_int(value: Any, location: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ReportValidationError(f"{location} must be a non-negative integer")


def _non_negative_number(value: Any, location: str) -> None:
    number = _decimal_number(value, location)
    if number < 0:
        raise ReportValidationError(f"{location} must be non-negative")


def _decimal_number(value: Any, location: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise TypeError(f"{location} must be numeric")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{location} must be finite")
    try:
        number = Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError(f"{location} must be finite") from error
    if not number.is_finite():
        raise ValueError(f"{location} must be finite")
    return number


def _json_number(value: Any, location: str) -> int | float:
    number = _decimal_number(value, location)
    if number < 0:
        raise ValueError(f"{location} must be non-negative")
    if number == number.to_integral_value():
        return int(number)
    return float(number)


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "SCHEMA_VERSION",
    "STAGE_ORDER",
    "ReportValidationError",
    "atomic_write_report",
    "build_report",
    "calculate_public_list_price_total",
    "calculate_totals",
    "fingerprint_bytes",
    "fingerprint_file",
    "fingerprint_profile",
    "make_chunk",
    "make_stage",
    "public_list_price_total",
    "validate_report",
    "write_report",
]
