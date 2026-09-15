"""Durable, privacy-safe state for Chinese podcast production runs.

The manifest deliberately stores identities instead of source paths, source text,
translation text, or provider configuration.  Callers keep those values in their
own artifacts and use this module only to coordinate durable progress.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from filelock import FileLock

MANIFEST_VERSION = 2
MANIFEST_SCHEMA = "pyvideotrans.podcast.production-run"

STAGE_NAMES = ("prepare", "asr", "translate", "tts", "finalize")
CHUNKED_STAGES = frozenset(("translate", "tts"))

RUN_STATUSES = frozenset(
    (
        "pending",
        "running",
        "interrupted",
        "needs_attention",
        "awaiting_review",
        "accepted",
        "rejected",
        "failed",
    )
)
STAGE_STATUSES = frozenset(("pending", "in_flight", "completed", "failed"))
CHUNK_STATUSES = frozenset(("pending", "submission_uncertain", "committed", "failed"))
PLAN_STATUSES = frozenset(("open", "sealed"))

_SHA256_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_RUN_KEYS = frozenset(
    (
        "identity",
        "status",
        "input_identity",
        "profile_identity",
        "active_stages",
        "resume_count",
    )
)
_STAGE_KEYS = frozenset(
    (
        "identity",
        "name",
        "status",
        "attempts",
        "retries",
        "artifact",
        "task_id",
        "plan_status",
        "chunks",
    )
)
_CHUNK_KEYS = frozenset(
    (
        "identity",
        "index",
        "status",
        "attempts",
        "retries",
        "attempt_identity",
        "artifact",
    )
)
_ARTIFACT_KEYS = frozenset(("identity", "path", "size_bytes"))
_ROOT_KEYS = frozenset(("schema", "manifest_version", "revision", "run", "stages"))


class ManifestError(ValueError):
    """Raised when a manifest is malformed or an invalid transition is requested."""


class UnsupportedManifestVersion(ManifestError):
    """Raised when a manifest version cannot be read by this implementation."""


def _canonical_json(value: Any) -> bytes:
    """Return a stable JSON encoding suitable for identity calculation."""

    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ManifestError("identity input must be finite JSON data") from exc
    return encoded.encode("utf-8")


def sha256_identity(namespace: str, value: Any) -> str:
    """Build a deterministic, namespaced SHA-256 identity.

    The supplied value is never retained by the manifest.  Namespacing prevents
    the same JSON value from accidentally identifying two different concepts.
    """

    digest = hashlib.sha256()
    digest.update(namespace.encode("utf-8"))
    digest.update(b"\0")
    digest.update(_canonical_json(value))
    return f"sha256:{digest.hexdigest()}"


def profile_identity(profile: Mapping[str, Any]) -> str:
    """Return the stable identity of a production profile without storing it."""

    if not isinstance(profile, Mapping):
        raise ManifestError("profile must be a mapping")
    return sha256_identity("podcast-profile-v2", profile)


def stage_identity(run_identity: str, stage_name: str) -> str:
    """Return the stable identity for one stage in a production run."""

    _require_identity(run_identity, "run identity")
    _require_stage_name(stage_name)
    return sha256_identity(
        "podcast-stage-v2",
        {"run_identity": run_identity, "stage": stage_name},
    )


def chunk_identity(stage_id: str, index: int, source: Any) -> str:
    """Return the stable identity for a chunk and its logical source payload."""

    _require_identity(stage_id, "stage identity")
    if not isinstance(index, int) or isinstance(index, bool) or index < 0:
        raise ManifestError("chunk index must be a non-negative integer")
    return sha256_identity(
        "podcast-chunk-v2",
        {"stage_identity": stage_id, "index": index, "source": source},
    )


def _require_identity(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA256_ID_RE.fullmatch(value):
        raise ManifestError(f"{label} must be a sha256:<64 lowercase hex> identity")
    return value


def _require_stage_name(stage_name: str) -> str:
    if stage_name not in STAGE_NAMES:
        raise ManifestError(f"unknown podcast stage: {stage_name!r}")
    return stage_name


def _artifact(
    identity: str,
    path: str | None = None,
    size_bytes: int | None = None,
) -> dict[str, Any]:
    _require_identity(identity, "artifact identity")
    if path is not None:
        if not isinstance(path, str) or not path:
            raise ManifestError("artifact path must be a non-empty relative path")
        relative = PurePosixPath(path.replace("\\", "/"))
        if relative.is_absolute() or ".." in relative.parts:
            raise ManifestError(
                "artifact path must remain inside the production run directory"
            )
        path = relative.as_posix()
    if size_bytes is not None and (
        not isinstance(size_bytes, int)
        or isinstance(size_bytes, bool)
        or size_bytes < 0
    ):
        raise ManifestError("artifact size must be a non-negative integer")
    return {"identity": identity, "path": path, "size_bytes": size_bytes}


def _new_chunk(stage_id: str, index: int, source: Any) -> dict[str, Any]:
    return {
        "identity": chunk_identity(stage_id, index, source),
        "index": index,
        "status": "pending",
        "attempts": 0,
        "retries": 0,
        "attempt_identity": None,
        "artifact": None,
    }


def _new_stage(run_id: str, name: str, sources: Sequence[Any]) -> dict[str, Any]:
    identity = stage_identity(run_id, name)
    return {
        "identity": identity,
        "name": name,
        "status": "pending",
        "attempts": 0,
        "retries": 0,
        "artifact": None,
        "task_id": None,
        "plan_status": (
            "sealed"
            if name in CHUNKED_STAGES and sources
            else "open"
            if name in CHUNKED_STAGES
            else None
        ),
        "chunks": [
            _new_chunk(identity, index, source) for index, source in enumerate(sources)
        ],
    }


class PodcastManifest:
    """Mutable production state with validated, privacy-safe serialization."""

    def __init__(self, data: Mapping[str, Any]):
        self._data = copy.deepcopy(dict(data))
        stages = self._data.get("stages")
        if isinstance(stages, dict) and set(stages) == set(STAGE_NAMES):
            # JSON objects are unordered and ``sort_keys`` writes them
            # alphabetically.  Keep the in-memory pipeline order canonical.
            self._data["stages"] = {name: stages[name] for name in STAGE_NAMES}
        self._validate()

    @classmethod
    def create(
        cls,
        *,
        source: Any,
        profile: Mapping[str, Any],
        chunk_sources: Mapping[str, Sequence[Any]] | None = None,
    ) -> PodcastManifest:
        """Create a pending production run.

        ``source``, ``profile``, and chunk source values contribute only to
        deterministic identities.  Their raw values are not stored.
        """

        chunks = dict(chunk_sources or {})
        unknown = set(chunks) - CHUNKED_STAGES
        if unknown:
            raise ManifestError(
                f"chunks are only supported for: {sorted(CHUNKED_STAGES)}"
            )

        input_id = sha256_identity("podcast-source-v2", source)
        profile_id = profile_identity(profile)
        run_id = sha256_identity(
            "podcast-run-v2",
            {
                "input_identity": input_id,
                "profile_identity": profile_id,
                "manifest_version": MANIFEST_VERSION,
            },
        )
        stages = {
            name: _new_stage(run_id, name, chunks.get(name, ())) for name in STAGE_NAMES
        }
        return cls(
            {
                "schema": MANIFEST_SCHEMA,
                "manifest_version": MANIFEST_VERSION,
                "revision": 0,
                "run": {
                    "identity": run_id,
                    "status": "pending",
                    "input_identity": input_id,
                    "profile_identity": profile_id,
                    "active_stages": [],
                    "resume_count": 0,
                },
                "stages": stages,
            }
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PodcastManifest:
        return cls(data)

    @property
    def version(self) -> int:
        return self._data["manifest_version"]

    @property
    def revision(self) -> int:
        return self._data["revision"]

    @property
    def run(self) -> dict[str, Any]:
        return self._data["run"]

    @property
    def stages(self) -> dict[str, dict[str, Any]]:
        return self._data["stages"]

    def stage(self, name: str) -> dict[str, Any]:
        return self.stages[_require_stage_name(name)]

    def to_dict(self) -> dict[str, Any]:
        """Return a validated copy containing only the versioned schema fields."""

        self._validate()
        return copy.deepcopy(self._data)

    def to_json(self) -> str:
        """Serialize deterministically without source content or provider config."""

        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def start_stage(self, name: str) -> None:
        stage = self.stage(name)
        if stage["status"] == "completed":
            raise ManifestError("a completed stage cannot be restarted")
        stage["status"] = "in_flight"
        stage["attempts"] += 1
        self.run["status"] = "running"
        self._activate_stage(name)
        self._changed()

    def fail_stage(self, name: str) -> None:
        stage = self.stage(name)
        if stage["status"] == "completed":
            raise ManifestError("a completed stage cannot be failed")
        stage["status"] = "failed"
        stage["retries"] += 1
        self.run["status"] = "needs_attention"
        self._activate_stage(name)
        self._changed()

    def set_asr_task_id(self, task_id: str) -> None:
        """Persist the provider task handle needed to resume ASR polling."""

        if not isinstance(task_id, str) or not task_id.strip():
            raise ManifestError("ASR task_id must be a non-empty string")
        asr = self.stage("asr")
        if asr["status"] == "completed":
            raise ManifestError("a completed ASR stage cannot change task_id")
        if asr["task_id"] is not None and asr["task_id"] != task_id:
            raise ManifestError("ASR task_id is immutable once recorded")
        asr["task_id"] = task_id
        asr["status"] = "in_flight"
        self.run["status"] = "running"
        self._activate_stage("asr")
        self._changed()

    def start_chunk(self, stage_name: str, chunk_id: str) -> str:
        """Durably mark a new provider attempt as submission-uncertain."""

        chunk = self._chunk(stage_name, chunk_id)
        if chunk["status"] != "pending":
            raise ManifestError("only a pending chunk can start a provider attempt")
        attempt_number = chunk["attempts"] + 1
        attempt_identity = sha256_identity(
            "podcast-provider-attempt-v2",
            {"chunk_identity": chunk_id, "attempt": attempt_number},
        )
        chunk["status"] = "submission_uncertain"
        chunk["attempts"] += 1
        chunk["attempt_identity"] = attempt_identity
        stage = self.stage(stage_name)
        stage["status"] = "in_flight"
        self.run["status"] = "running"
        self._activate_stage(stage_name)
        self._changed()
        return attempt_identity

    def define_chunks(self, stage_name: str, sources: Sequence[Any]) -> None:
        """Define a chunked stage exactly once before any chunk is started.

        Chunk source payloads contribute only to identities; the raw text is not
        retained in the manifest. Repeating the same definition is idempotent,
        which makes coordinator restart boundaries straightforward.
        """

        if stage_name not in CHUNKED_STAGES:
            raise ManifestError("only translate and tts stages contain chunks")
        stage = self.stage(stage_name)
        proposed = [
            _new_chunk(stage["identity"], index, source)
            for index, source in enumerate(sources)
        ]
        if stage["chunks"]:
            if [chunk["identity"] for chunk in stage["chunks"]] != [
                chunk["identity"] for chunk in proposed
            ]:
                raise ManifestError("chunk identities cannot change after definition")
            if stage["plan_status"] != "sealed":
                raise ManifestError("an existing chunk plan must be sealed")
            return
        if stage["status"] != "pending":
            raise ManifestError("chunks must be defined before their stage starts")
        stage["chunks"] = proposed
        stage["plan_status"] = "sealed"
        self._changed()

    def append_chunk(self, stage_name: str, source: Any) -> dict[str, Any]:
        """Append one final chunk to an open dynamic plan."""

        if stage_name not in CHUNKED_STAGES:
            raise ManifestError("only translate and tts stages contain chunks")
        stage = self.stage(stage_name)
        if stage["plan_status"] != "open":
            raise ManifestError("chunks can only be appended to an open plan")
        if stage["status"] == "completed":
            raise ManifestError("a completed stage cannot append chunks")
        item = _new_chunk(stage["identity"], len(stage["chunks"]), source)
        stage["chunks"].append(item)
        self._changed()
        return copy.deepcopy(item)

    def seal_chunks(self, stage_name: str) -> None:
        """Seal an append-only plan after its final chunk has been defined."""

        if stage_name not in CHUNKED_STAGES:
            raise ManifestError("only translate and tts stages contain chunks")
        stage = self.stage(stage_name)
        if stage["plan_status"] == "sealed":
            return
        stage["plan_status"] = "sealed"
        self._changed()

    def retry_chunk(self, stage_name: str, chunk_id: str) -> None:
        """Record an explicit no-success response that is safe to retry."""

        chunk = self._chunk(stage_name, chunk_id)
        if chunk["status"] != "submission_uncertain":
            raise ManifestError("only an uncertain submission can become retryable")
        chunk["status"] = "pending"
        chunk["retries"] += 1
        chunk["attempt_identity"] = None
        self._changed()

    def fail_chunk(self, stage_name: str, chunk_id: str) -> None:
        chunk = self._chunk(stage_name, chunk_id)
        if chunk["status"] != "submission_uncertain":
            raise ManifestError("only an uncertain submission can fail")
        chunk["status"] = "failed"
        self.stage(stage_name)["status"] = "failed"
        self.run["status"] = "needs_attention"
        self._activate_stage(stage_name)
        self._changed()

    def commit_chunk(
        self,
        stage_name: str,
        chunk_id: str,
        *,
        artifact_identity: str,
        artifact_path: str | None = None,
        size_bytes: int | None = None,
    ) -> None:
        """Commit a chunk artifact once; a committed reference is immutable."""

        chunk = self._chunk(stage_name, chunk_id)
        artifact = _artifact(artifact_identity, artifact_path, size_bytes)
        if chunk["status"] == "committed":
            if chunk["artifact"] != artifact:
                raise ManifestError("a committed chunk artifact cannot be replaced")
            return
        if chunk["status"] != "submission_uncertain":
            raise ManifestError("only an uncertain submission can commit")
        chunk["artifact"] = artifact
        chunk["status"] = "committed"
        stage = self.stage(stage_name)
        if (
            stage["plan_status"] == "sealed"
            and stage["chunks"]
            and all(item["status"] == "committed" for item in stage["chunks"])
        ):
            stage["status"] = "completed"
        self._changed()

    def commit_stage(
        self,
        name: str,
        *,
        artifact_identity: str,
        artifact_path: str | None = None,
        size_bytes: int | None = None,
    ) -> None:
        """Commit a stage artifact once; a committed reference is immutable."""

        stage = self.stage(name)
        artifact = _artifact(artifact_identity, artifact_path, size_bytes)
        if stage["status"] == "completed":
            if stage["artifact"] is None and name in CHUNKED_STAGES:
                stage["artifact"] = artifact
                self._deactivate_stage(name)
                self._changed()
                return
            if stage["artifact"] != artifact:
                raise ManifestError("a committed stage artifact cannot be replaced")
            return
        if name in CHUNKED_STAGES:
            if stage["plan_status"] != "sealed":
                raise ManifestError("a chunked stage plan must be sealed before commit")
            if any(chunk["status"] != "committed" for chunk in stage["chunks"]):
                raise ManifestError("all chunks must be committed before their stage")
        stage["artifact"] = artifact
        stage["status"] = "completed"
        if name == "finalize":
            self.run["status"] = "awaiting_review"
            self.run["active_stages"] = []
        else:
            self._deactivate_stage(name)
        self._changed()

    def normalize_for_resume(self) -> None:
        """Normalize retry-safe local work without resubmitting uncertain work.

        The ASR stage remains in flight when it has a task id, because that id
        represents an already-submitted remote operation that should be polled,
        not submitted again.
        """

        terminal_run = self.run["status"] in {"awaiting_review", "accepted", "rejected"}
        for name in STAGE_NAMES:
            stage = self.stage(name)
            for chunk in stage["chunks"]:
                if chunk["status"] == "failed":
                    chunk["status"] = "pending"
                    chunk["attempt_identity"] = None
            if stage["status"] != "completed":
                if name == "asr" and stage["task_id"]:
                    stage["status"] = "in_flight"
                elif any(
                    chunk["status"] == "submission_uncertain"
                    for chunk in stage["chunks"]
                ):
                    stage["status"] = "failed"
                elif stage["status"] == "in_flight" or (
                    name in CHUNKED_STAGES
                    and any(chunk["status"] == "pending" for chunk in stage["chunks"])
                ):
                    stage["status"] = "pending"

        if not terminal_run:
            uncertain_stages = [
                name
                for name in CHUNKED_STAGES
                if any(
                    chunk["status"] == "submission_uncertain"
                    for chunk in self.stage(name)["chunks"]
                )
            ]
            self.run["status"] = (
                "needs_attention" if uncertain_stages else "interrupted"
            )
            self.run["active_stages"] = uncertain_stages
            if (
                self.stage("asr")["task_id"]
                and self.stage("asr")["status"] != "completed"
            ):
                self._activate_stage("asr")
        self.run["resume_count"] += 1
        self._changed()

    def record_review(self, status: str) -> None:
        """Record the listener-owned terminal quality decision."""

        if status not in {"accepted", "rejected"}:
            raise ManifestError("review status must be accepted or rejected")
        current = self.run["status"]
        if current == status:
            return
        if current != "awaiting_review":
            raise ManifestError("only a run awaiting review can be reviewed")
        self.run["status"] = status
        self.run["active_stages"] = []
        self._changed()

    def uncertain_chunks(self) -> list[tuple[str, dict[str, Any]]]:
        """Return privacy-safe copies of requests that must not be resent."""

        return [
            (stage_name, copy.deepcopy(chunk))
            for stage_name in CHUNKED_STAGES
            for chunk in self.stage(stage_name)["chunks"]
            if chunk["status"] == "submission_uncertain"
        ]

    def _activate_stage(self, name: str) -> None:
        _require_stage_name(name)
        if name not in self.run["active_stages"]:
            self.run["active_stages"].append(name)
            self.run["active_stages"].sort(key=STAGE_NAMES.index)

    def _deactivate_stage(self, name: str) -> None:
        if name in self.run["active_stages"]:
            self.run["active_stages"].remove(name)

    def _chunk(self, stage_name: str, chunk_id: str) -> dict[str, Any]:
        if stage_name not in CHUNKED_STAGES:
            raise ManifestError("only translate and tts stages contain chunks")
        _require_identity(chunk_id, "chunk identity")
        for chunk in self.stage(stage_name)["chunks"]:
            if chunk["identity"] == chunk_id:
                return chunk
        raise ManifestError(f"unknown chunk for {stage_name}: {chunk_id}")

    def _changed(self) -> None:
        self._data["revision"] += 1
        self._validate()

    def _validate(self) -> None:
        data = self._data
        if set(data) != _ROOT_KEYS:
            raise ManifestError("manifest contains unknown or missing root fields")
        if data.get("schema") != MANIFEST_SCHEMA:
            raise ManifestError("unexpected manifest schema")
        if data.get("manifest_version") != MANIFEST_VERSION:
            raise UnsupportedManifestVersion(
                f"unsupported manifest version: {data.get('manifest_version')!r}"
            )
        if (
            not isinstance(data.get("revision"), int)
            or isinstance(data["revision"], bool)
            or data["revision"] < 0
        ):
            raise ManifestError("revision must be a non-negative integer")

        run = data.get("run")
        if not isinstance(run, dict) or set(run) != _RUN_KEYS:
            raise ManifestError("run contains unknown or missing fields")
        _require_identity(run["identity"], "run identity")
        _require_identity(run["input_identity"], "input identity")
        _require_identity(run["profile_identity"], "profile identity")
        expected_run_identity = sha256_identity(
            "podcast-run-v2",
            {
                "input_identity": run["input_identity"],
                "profile_identity": run["profile_identity"],
                "manifest_version": MANIFEST_VERSION,
            },
        )
        if run["identity"] != expected_run_identity:
            raise ManifestError(
                "run identity does not match its input and profile identities"
            )
        if run["status"] not in RUN_STATUSES:
            raise ManifestError(f"invalid run status: {run['status']!r}")
        active_stages = run["active_stages"]
        if not isinstance(active_stages, list) or len(active_stages) != len(
            set(active_stages)
        ):
            raise ManifestError("active_stages must be unique and pipeline ordered")
        for active_stage in active_stages:
            _require_stage_name(active_stage)
        if active_stages != sorted(active_stages, key=STAGE_NAMES.index):
            raise ManifestError("active_stages must be unique and pipeline ordered")
        if (
            not isinstance(run["resume_count"], int)
            or isinstance(run["resume_count"], bool)
            or run["resume_count"] < 0
        ):
            raise ManifestError("resume_count must be a non-negative integer")

        stages = data.get("stages")
        if not isinstance(stages, dict) or tuple(stages) != STAGE_NAMES:
            raise ManifestError("manifest must contain the five ordered podcast stages")
        for name, stage in stages.items():
            self._validate_stage(name, stage)

    def _validate_stage(self, name: str, stage: Any) -> None:
        if not isinstance(stage, dict) or set(stage) != _STAGE_KEYS:
            raise ManifestError(f"stage {name!r} contains unknown or missing fields")
        if stage["name"] != name:
            raise ManifestError("stage key and name must match")
        expected_identity = stage_identity(self.run["identity"], name)
        if stage["identity"] != expected_identity:
            raise ManifestError(f"stage {name!r} has an invalid identity")
        if stage["status"] not in STAGE_STATUSES:
            raise ManifestError(f"invalid stage status: {stage['status']!r}")
        for counter in ("attempts", "retries"):
            if (
                not isinstance(stage[counter], int)
                or isinstance(stage[counter], bool)
                or stage[counter] < 0
            ):
                raise ManifestError(f"stage {counter} must be a non-negative integer")
        self._validate_artifact(stage["artifact"])
        if name in CHUNKED_STAGES:
            if stage["plan_status"] not in PLAN_STATUSES:
                raise ManifestError("chunked stage plan_status is invalid")
        elif stage["plan_status"] is not None:
            raise ManifestError("only chunked stages have a plan_status")
        if name == "asr":
            if stage["task_id"] is not None and (
                not isinstance(stage["task_id"], str) or not stage["task_id"].strip()
            ):
                raise ManifestError("ASR task_id must be null or a non-empty string")
        elif stage["task_id"] is not None:
            raise ManifestError("only the ASR stage may store task_id")

        chunks = stage["chunks"]
        if not isinstance(chunks, list):
            raise ManifestError("stage chunks must be a list")
        if name not in CHUNKED_STAGES and chunks:
            raise ManifestError("only translate and tts stages may contain chunks")
        seen: set[str] = set()
        for expected_index, chunk in enumerate(chunks):
            self._validate_chunk(chunk, expected_index)
            if chunk["identity"] in seen:
                raise ManifestError("chunk identities must be unique within a stage")
            seen.add(chunk["identity"])

    @staticmethod
    def _validate_chunk(chunk: Any, expected_index: int) -> None:
        if not isinstance(chunk, dict) or set(chunk) != _CHUNK_KEYS:
            raise ManifestError("chunk contains unknown or missing fields")
        _require_identity(chunk["identity"], "chunk identity")
        if chunk["index"] != expected_index:
            raise ManifestError("chunk indexes must be contiguous and zero-based")
        if chunk["status"] not in CHUNK_STATUSES:
            raise ManifestError(f"invalid chunk status: {chunk['status']!r}")
        for counter in ("attempts", "retries"):
            if (
                not isinstance(chunk[counter], int)
                or isinstance(chunk[counter], bool)
                or chunk[counter] < 0
            ):
                raise ManifestError(f"chunk {counter} must be a non-negative integer")
        PodcastManifest._validate_artifact(chunk["artifact"])
        if chunk["attempt_identity"] is not None:
            _require_identity(chunk["attempt_identity"], "attempt identity")
            expected_attempt_identity = sha256_identity(
                "podcast-provider-attempt-v2",
                {
                    "chunk_identity": chunk["identity"],
                    "attempt": chunk["attempts"],
                },
            )
            if chunk["attempt_identity"] != expected_attempt_identity:
                raise ManifestError("chunk attempt identity is invalid")
        if chunk["status"] == "pending" and chunk["attempt_identity"] is not None:
            raise ManifestError("a pending chunk cannot retain an attempt identity")
        if chunk["status"] != "pending" and chunk["attempt_identity"] is None:
            raise ManifestError("a started chunk must retain its attempt identity")
        if chunk["attempts"] == 0 and chunk["attempt_identity"] is not None:
            raise ManifestError("an unattempted chunk cannot have an attempt identity")
        if chunk["retries"] > chunk["attempts"]:
            raise ManifestError("chunk retries cannot exceed attempts")
        if chunk["status"] == "committed" and chunk["artifact"] is None:
            raise ManifestError("a committed chunk must reference its artifact")
        if chunk["status"] != "committed" and chunk["artifact"] is not None:
            raise ManifestError("only a committed chunk can reference an artifact")
        if (
            chunk["status"] == "submission_uncertain"
            and chunk["attempt_identity"] is None
        ):
            raise ManifestError("an uncertain chunk must retain its attempt identity")

    @staticmethod
    def _validate_artifact(artifact: Any) -> None:
        if artifact is None:
            return
        if not isinstance(artifact, dict) or set(artifact) != _ARTIFACT_KEYS:
            raise ManifestError("artifact contains unknown or missing fields")
        expected = _artifact(
            artifact["identity"], artifact["path"], artifact["size_bytes"]
        )
        if artifact != expected:
            raise ManifestError("artifact is not in canonical form")


def atomic_write_json(path: str | os.PathLike[str], data: Mapping[str, Any]) -> None:
    """Durably replace a JSON file using temp-write, fsync, and os.replace."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                data,
                handle,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    except BaseException:  # noqa: BLE001 - cleanup must also run on interruption
        try:
            temporary.unlink(missing_ok=True)
        finally:
            raise


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        # Some platforms/filesystems do not support syncing directory handles.
        pass
    finally:
        os.close(descriptor)


class ManifestStore:
    """Atomic manifest persistence and the single lock for one production run."""

    def __init__(self, path: str | os.PathLike[str], *, lock_timeout: float = -1):
        self.path = Path(path)
        self.lock_path = Path(f"{self.path}.lock")
        self._lock = FileLock(str(self.lock_path), timeout=lock_timeout)

    def run_lock(self) -> FileLock:
        """Return the one process lock to hold for the production run lifetime."""

        return self._lock

    def save(self, manifest: PodcastManifest) -> None:
        with self._lock:
            atomic_write_json(self.path, manifest.to_dict())

    def load(self, *, resume: bool = False) -> PodcastManifest:
        """Load state, optionally normalize and durably record a resume."""

        with self._lock:
            try:
                with self.path.open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
            except json.JSONDecodeError as exc:
                raise ManifestError("manifest is not valid JSON") from exc
            manifest = PodcastManifest.from_dict(data)
            if resume:
                manifest.normalize_for_resume()
                atomic_write_json(self.path, manifest.to_dict())
            return manifest


def iter_pending_chunks(
    manifest: PodcastManifest, stage_name: str
) -> Iterator[dict[str, Any]]:
    """Yield copies of chunks that are safe to submit."""

    if stage_name not in CHUNKED_STAGES:
        raise ManifestError("only translate and tts stages contain chunks")
    for chunk in manifest.stage(stage_name)["chunks"]:
        if chunk["status"] in ("pending", "failed"):
            yield copy.deepcopy(chunk)
