import json
import os

import pytest
from filelock import Timeout

from videotrans.podcast.manifest import (
    MANIFEST_VERSION,
    STAGE_NAMES,
    ManifestError,
    ManifestStore,
    PodcastManifest,
    UnsupportedManifestVersion,
    atomic_write_json,
    chunk_identity,
    profile_identity,
    sha256_identity,
    stage_identity,
)


def test_define_chunks_is_private_idempotent_and_immutable() -> None:
    manifest = PodcastManifest.create(
        source={"fingerprint": "source"}, profile={"id": "v1"}
    )

    manifest.define_chunks("translate", [{"text_hash": "a"}, {"text_hash": "b"}])
    first = [chunk["identity"] for chunk in manifest.stage("translate")["chunks"]]
    manifest.define_chunks("translate", [{"text_hash": "a"}, {"text_hash": "b"}])

    assert [
        chunk["identity"] for chunk in manifest.stage("translate")["chunks"]
    ] == first
    assert "text_hash" not in manifest.to_json()
    with pytest.raises(ManifestError, match="cannot change"):
        manifest.define_chunks("translate", [{"text_hash": "different"}])


def test_finalize_moves_run_to_awaiting_review() -> None:
    manifest = PodcastManifest.create(
        source={"fingerprint": "source"}, profile={"id": "v1"}
    )
    manifest.commit_stage("finalize", artifact_identity="sha256:" + "a" * 64)

    assert manifest.run["status"] == "awaiting_review"

    manifest.record_review("accepted")
    manifest.record_review("accepted")
    assert manifest.run["status"] == "accepted"

    with pytest.raises(ManifestError, match="awaiting review"):
        manifest.record_review("rejected")


def make_manifest():
    return PodcastManifest.create(
        source={"content_sha256": "source-fingerprint", "path": "/private/source.m4a"},
        profile={
            "asr": "qwen-audio-3.0-asr-flash-filetrans",
            "translation": "qwen-mt-flash",
            "tts": "qwen3-tts-flash-2025-11-27",
            "voice": "Andre",
            "api_key": "must-never-be-serialized",
        },
        chunk_sources={
            "translate": ["private English sentence", "second private sentence"],
            "tts": ["隐私中文文本", "第二段隐私中文文本"],
        },
    )


def artifact_id(label):
    return sha256_identity("test-artifact", label)


def test_create_has_versioned_run_and_ordered_five_stage_state():
    manifest = make_manifest()

    assert manifest.version == MANIFEST_VERSION
    assert manifest.revision == 0
    assert manifest.run["status"] == "pending"
    assert tuple(manifest.stages) == STAGE_NAMES
    assert all(stage["status"] == "pending" for stage in manifest.stages.values())
    assert all(
        chunk["status"] == "pending" for chunk in manifest.stage("tts")["chunks"]
    )
    assert manifest.stage("asr")["task_id"] is None


def test_identities_are_deterministic_sha256_and_domain_separated():
    first = profile_identity({"voice": "Andre", "region": "cn-beijing"})
    reordered = profile_identity({"region": "cn-beijing", "voice": "Andre"})
    changed = profile_identity({"voice": "Cherry", "region": "cn-beijing"})

    assert first == reordered
    assert first != changed
    assert first.startswith("sha256:") and len(first) == 71

    run_id = sha256_identity("run", {"input": "same"})
    translate = stage_identity(run_id, "translate")
    assert translate != stage_identity(run_id, "tts")
    assert chunk_identity(translate, 0, "same") != chunk_identity(translate, 1, "same")


def test_serialization_is_privacy_safe():
    serialized = make_manifest().to_json()

    assert "/private/source.m4a" not in serialized
    assert "private English sentence" not in serialized
    assert "隐私中文文本" not in serialized
    assert "must-never-be-serialized" not in serialized
    assert "api_key" not in serialized
    assert "Andre" not in serialized


def test_atomic_store_round_trip_and_replace_leaves_no_temp_file(tmp_path):
    path = tmp_path / "run" / "manifest.json"
    store = ManifestStore(path)
    first = make_manifest()
    store.save(first)

    first.start_stage("prepare")
    store.save(first)
    loaded = store.load()

    assert loaded.to_dict() == first.to_dict()
    assert json.loads(path.read_text(encoding="utf-8"))["manifest_version"] == 2
    assert not list(path.parent.glob(".manifest.json.*.tmp"))


def test_atomic_write_fsyncs_file_before_replace(tmp_path, monkeypatch):
    events = []
    real_fsync = os.fsync
    real_replace = os.replace

    def recording_fsync(descriptor):
        events.append("fsync")
        return real_fsync(descriptor)

    def recording_replace(source, destination):
        events.append("replace")
        return real_replace(source, destination)

    monkeypatch.setattr(os, "fsync", recording_fsync)
    monkeypatch.setattr(os, "replace", recording_replace)
    atomic_write_json(tmp_path / "manifest.json", {"value": 1})

    assert events[0:2] == ["fsync", "replace"]
    assert events[-1] == "fsync"  # parent directory durability


def test_resume_preserves_uncertain_chunks_and_commits(tmp_path):
    store = ManifestStore(tmp_path / "manifest.json")
    manifest = make_manifest()
    translate_chunks = manifest.stage("translate")["chunks"]

    manifest.start_chunk("translate", translate_chunks[0]["identity"])
    manifest.commit_chunk(
        "translate",
        translate_chunks[0]["identity"],
        artifact_identity=artifact_id("translated-0"),
        artifact_path="translate/0000.json",
        size_bytes=42,
    )
    manifest.start_chunk("translate", translate_chunks[1]["identity"])
    committed_before = manifest.stage("translate")["chunks"][0].copy()
    committed_before["artifact"] = committed_before["artifact"].copy()
    store.save(manifest)

    resumed = store.load(resume=True)
    chunks = resumed.stage("translate")["chunks"]

    assert chunks[0] == committed_before
    assert chunks[0]["status"] == "committed"
    assert chunks[1]["status"] == "submission_uncertain"
    assert chunks[1]["attempt_identity"].startswith("sha256:")
    assert chunks[1]["artifact"] is None
    assert resumed.stage("translate")["status"] == "failed"
    assert resumed.run["status"] == "needs_attention"
    assert resumed.run["resume_count"] == 1
    assert ManifestStore(store.path).load().to_dict() == resumed.to_dict()


def test_resume_preserves_asr_task_id_for_polling(tmp_path):
    store = ManifestStore(tmp_path / "manifest.json")
    manifest = make_manifest()
    manifest.start_stage("asr")
    manifest.set_asr_task_id("provider-task-123")
    store.save(manifest)

    resumed = store.load(resume=True)

    assert resumed.stage("asr")["task_id"] == "provider-task-123"
    assert resumed.stage("asr")["status"] == "in_flight"
    assert resumed.run["active_stages"] == ["asr"]


def test_dynamic_tts_plan_is_append_only_and_completes_only_after_seal():
    manifest = PodcastManifest.create(source="source", profile={"id": "v2"})
    first = manifest.append_chunk("tts", {"text_hash": "a"})
    manifest.start_chunk("tts", first["identity"])
    manifest.commit_chunk(
        "tts", first["identity"], artifact_identity=artifact_id("tts-a")
    )

    assert manifest.stage("tts")["status"] == "in_flight"
    assert manifest.stage("tts")["plan_status"] == "open"

    manifest.seal_chunks("tts")
    assert manifest.stage("tts")["status"] == "in_flight"
    manifest.commit_stage("tts", artifact_identity=artifact_id("tts-stage"))
    assert manifest.stage("tts")["status"] == "completed"

    with pytest.raises(ManifestError, match="open plan"):
        manifest.append_chunk("tts", {"text_hash": "b"})


def test_multiple_pipeline_stages_can_be_active():
    manifest = PodcastManifest.create(source="source", profile={"id": "v2"})

    manifest.start_stage("tts")
    manifest.start_stage("translate")

    assert manifest.run["active_stages"] == ["translate", "tts"]


def test_explicit_no_success_response_allows_new_attempt_identity():
    manifest = PodcastManifest.create(
        source="source",
        profile={"id": "v2"},
        chunk_sources={"translate": [{"text_hash": "a"}]},
    )
    item = manifest.stage("translate")["chunks"][0]

    first_attempt = manifest.start_chunk("translate", item["identity"])
    manifest.retry_chunk("translate", item["identity"])
    second_attempt = manifest.start_chunk("translate", item["identity"])

    assert first_attempt != second_attempt
    assert item["attempts"] == 2
    assert item["retries"] == 1


def test_committed_artifacts_are_immutable():
    manifest = make_manifest()
    chunk = manifest.stage("tts")["chunks"][0]
    manifest.start_chunk("tts", chunk["identity"])
    manifest.commit_chunk(
        "tts",
        chunk["identity"],
        artifact_identity=artifact_id("voice-0"),
        artifact_path="tts/0000.wav",
    )

    # Idempotent replay is safe.
    manifest.commit_chunk(
        "tts",
        chunk["identity"],
        artifact_identity=artifact_id("voice-0"),
        artifact_path="tts/0000.wav",
    )
    with pytest.raises(ManifestError, match="cannot be replaced"):
        manifest.commit_chunk(
            "tts",
            chunk["identity"],
            artifact_identity=artifact_id("different"),
            artifact_path="tts/replacement.wav",
        )


def test_chunk_transitions_require_durable_pre_send_marker():
    manifest = make_manifest()
    chunk = manifest.stage("tts")["chunks"][0]

    with pytest.raises(ManifestError, match="uncertain submission can commit"):
        manifest.commit_chunk(
            "tts", chunk["identity"], artifact_identity=artifact_id("voice-0")
        )
    with pytest.raises(ManifestError, match="uncertain submission can fail"):
        manifest.fail_chunk("tts", chunk["identity"])

    manifest.start_chunk("tts", chunk["identity"])
    with pytest.raises(ManifestError, match="pending chunk can start"):
        manifest.start_chunk("tts", chunk["identity"])


def test_manifest_rejects_forged_attempt_identity():
    manifest = make_manifest()
    chunk = manifest.stage("tts")["chunks"][0]
    manifest.start_chunk("tts", chunk["identity"])
    data = manifest.to_dict()
    data["stages"]["tts"]["chunks"][0]["attempt_identity"] = artifact_id("forged")

    with pytest.raises(ManifestError, match="attempt identity is invalid"):
        PodcastManifest.from_dict(data)


@pytest.mark.parametrize(
    "path", ["/tmp/result.wav", "../result.wav", "safe/../../result.wav"]
)
def test_artifacts_cannot_escape_run_directory(path):
    manifest = make_manifest()

    with pytest.raises(ManifestError, match="inside the production run directory"):
        manifest.commit_stage(
            "prepare",
            artifact_identity=artifact_id("prepared"),
            artifact_path=path,
        )


def test_unknown_fields_and_versions_are_rejected():
    data = make_manifest().to_dict()
    data["secret"] = "do not persist arbitrary metadata"
    with pytest.raises(ManifestError, match="root fields"):
        PodcastManifest.from_dict(data)

    data = make_manifest().to_dict()
    data["manifest_version"] = MANIFEST_VERSION + 1
    with pytest.raises(UnsupportedManifestVersion):
        PodcastManifest.from_dict(data)


def test_only_one_run_lock_can_be_held(tmp_path):
    path = tmp_path / "manifest.json"
    first = ManifestStore(path, lock_timeout=0)
    second = ManifestStore(path, lock_timeout=0)

    with first.run_lock(), pytest.raises(Timeout), second.run_lock():
        pass
