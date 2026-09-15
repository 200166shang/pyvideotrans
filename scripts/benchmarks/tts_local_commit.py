"""Offline diagnostic of real TTS commit work; never constructs cloud clients.

Run from the repository root with PYTHONPATH=. and an existing private run.
The skip-validation control is diagnostic only, never a production proposal.
Audio is read locally, never printed; all new packages live in a temporary dir.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import tempfile
import time
from collections import defaultdict
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import videotrans.podcast.orchestrator as module
from videotrans.podcast.manifest import ManifestStore, PodcastManifest
from videotrans.podcast.profiles import DEFAULT_PODCAST_PROFILE


class OfflineCoordinator(module.PodcastCoordinator):
    async def _call_tts(self, text):
        # A ready provider result still yields, like the production thread call.
        await asyncio.sleep(0)
        return SimpleNamespace(audio=self.audio[int(text)], usage=0)


def timed(original, samples, name):
    def wrapped(*args, **kwargs):
        start = time.perf_counter()
        try:
            return original(*args, **kwargs)
        finally:
            samples[name].append((time.perf_counter() - start) * 1000)

    return wrapped


async def measure(audio, root, *, skip_validation, spacing):
    profile = DEFAULT_PODCAST_PROFILE
    coordinator = OfflineCoordinator(None, profile)
    coordinator.audio = audio
    manifest = PodcastManifest.create(
        source={"offline": True}, profile=asdict(profile),
        chunk_sources={"tts": list(range(len(audio)))},
    )
    manifest.start_stage("tts")
    store = ManifestStore(root / "manifest.json")
    store.save(manifest)
    state = {}
    samples = defaultdict(list)
    lags = []
    done = False

    async def heartbeat():
        while not done:
            start = time.perf_counter()
            await asyncio.sleep(0.005)
            lags.append(max(0, (time.perf_counter() - start - 0.005) * 1000))

    semaphore = asyncio.Semaphore(profile.tts_concurrency)
    origin = time.perf_counter()

    async def commit(index):
        await asyncio.sleep(max(0, origin + index * spacing - time.perf_counter()))
        async with semaphore:
            chunk = SimpleNamespace(sequence_id=f"tts-{index + 1}", text=str(index))
            return await coordinator._tts_one(chunk, root, manifest, state, store, root)

    with ExitStack() as stack:
        for name in ("_write_bytes_artifact_package", "_validate_tts_audio",
                     "_atomic_write_bytes", "atomic_write_json", "_fsync_directory"):
            original = getattr(module, name)
            if skip_validation and name == "_validate_tts_audio":
                original = lambda _: None
            stack.enter_context(patch.object(module, name, timed(original, samples, name)))
        stack.enter_context(patch.object(store, "save", timed(store.save, samples, "manifest_save")))
        monitor = asyncio.create_task(heartbeat())
        started = time.perf_counter()
        paths = await asyncio.gather(*(commit(i) for i in range(len(audio))))
        elapsed = (time.perf_counter() - started) * 1000
        done = True
        await monitor

    saved = store.load()
    assert all(item["status"] == "committed" for item in saved.stage("tts")["chunks"])
    assert [p.read_bytes() for p in paths] == audio
    return {
        "elapsed_ms": round(elapsed, 3),
        "max_heartbeat_lag_ms": round(max(lags, default=0), 3),
        "timings": {
            name: {"count": len(values), "sum_ms": round(sum(values), 3),
                   "median_ms": round(statistics.median(values), 3),
                   "max_ms": round(max(values), 3)}
            for name, values in samples.items()
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("repeats must be positive")
    paths = sorted(args.run_directory.glob("**/tts/[0-9]*/artifact.audio"))
    if not paths:
        parser.error("no saved TTS artifacts found")
    audio = [path.read_bytes() for path in paths]
    results = []
    with tempfile.TemporaryDirectory(prefix="podcast-local-commit-") as temporary:
        for repeat in range(args.repeats):
            # Alternate order to reduce systematic warm-cache/order bias.
            for spacing in (0.4, 0.0):
                for skip in ((False, True) if repeat % 2 == 0 else (True, False)):
                    root = Path(temporary) / str(len(results))
                    root.mkdir()
                    result = asyncio.run(measure(audio, root, skip_validation=skip, spacing=spacing))
                    results.append({"repeat": repeat, "release_spacing_seconds": spacing,
                                    "skip_validation_control": skip, **result})
    output = {
        "schema_version": 1, "chunk_count": len(audio), "audio_bytes": sum(map(len, audio)),
        "cloud_calls": 0, "concurrency": DEFAULT_PODCAST_PROFILE.tts_concurrency,
        "limitations": [
            "Warm saved audio; synthetic ready-provider releases, not a production replay.",
            "0.4s spaces ready results, not requests; burst models clustered completions.",
            "Nested timing sums overlap and must not be added together.",
            "_fsync_directory covers package directories only; file fsync is inside write timers.",
            "atomic_write_json covers receipt/private state; manifest writes are in manifest_save.",
            "Validation-free control is unsafe for production and only isolates local cost.",
            "No end-to-end cloud speedup or long-programme performance claim.",
        ],
        "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
