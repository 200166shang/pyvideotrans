# Offline TTS commit measurement — 2026-09-15

## Decision

Audio validation blocks the event loop, but the measured local cost does not justify treating it as the next major end-to-end speed improvement. Keep production validation and durability unchanged. Prioritize investigation of the ASR stage before a performance implementation; do not yet select segmented ASR or change polling policy.

The accepted historical five-minute run took 38.471 seconds, including 17.627 seconds in the ASR stage. That stage includes upload, submission, polling and result handling; it is not a measurement of provider inference alone. The existing trace cannot separate those costs. Further offline code inspection can identify instrumentation seams; a new cloud measurement still needs its own agreed scope and cost authorization.

## Method and scope

- Fixed production revision: `3667370852059f8bf95174a2e02d6114aa744d94`.
- Local machine: Darwin arm64, Python 3.10.19, FFmpeg 8.1.1. Temporary files use the machine's default temporary filesystem; no cold-cache or storage durability benchmark is claimed.
- Reused 14 accepted-run audio artifacts (16,239,976 bytes) read-only. No transcript, credentials or audio contents are included in the results. Zero cloud calls.
- Run the real `PodcastCoordinator._tts_one`, with an offline override returning ready audio after a scheduling yield. Actual artifact writes, validation, receipts, hashes, manifest saves and private timing-state writes remain active.
- Use a fresh temporary directory and synthetic sealed manifest for every case. Assert all 14 chunks are committed and every written audio byte matches its input; remove generated copies afterwards.
- Compare real validation against a temporary no-op validator solely as a diagnostic control. This is unsafe as production behavior and is not an implementation proposal.
- Three repetitions per case, alternating control order. Two release shapes: ready results spaced by 0.4 seconds, and a burst of ready results under concurrency 8. These are synthetic completion shapes, not cloud request replay or a reproduction of the production rate limiter.
- A 5 ms async heartbeat measures scheduling delay while the real commit code runs. Its maximum is descriptive, not a service guarantee.

## Results

Values are medians across three repetitions. Nested timers overlap and must not be summed.

| Ready-result shape | Real validation total | Control total | Difference of medians | Real maximum heartbeat lag | Control maximum heartbeat lag |
| --- | ---: | ---: | ---: | ---: | ---: |
| Spaced by 0.4 s | 5,275.467 ms | 5,211.715 ms | 63.752 ms | 90.152 ms | 14.059 ms |
| Burst | 726.534 ms | 46.450 ms | 680.084 ms | 442.353 ms | 32.606 ms |

| Inclusive component, summed across 14 chunks | Spaced, real validation | Burst, real validation |
| --- | ---: | ---: |
| Complete artifact package | 998.749 ms | 693.394 ms |
| FFprobe and full FFmpeg decode, inside package | 959.560 ms | 670.541 ms |
| Audio write and file fsync, inside package | 13.620 ms | 5.627 ms |
| Manifest saves, before and after provider result | 42.997 ms | 20.491 ms |
| Receipt and private timing-state JSON writes | 12.485 ms | 8.572 ms |

The package directory-fsync timer does not include file fsync or manifest-directory fsync. Manifest JSON writes are included in the manifest-save timer rather than the receipt/private-state JSON timer.

In real-validation burst runs, total time ranged from 713.423 to 753.718 ms and maximum heartbeat delay ranged from 417.656 to 721.385 ms. In spaced runs, total time ranged from 5,274.554 to 5,278.709 ms. The sharp heartbeat delay confirms synchronous local work can delay other ready coroutine work.

## Interpretation

Validation dominates the local commit path. Durable file/state writes are a smaller share on this machine. In the spaced scenario most validation fits between incoming results, so eliminating it saves only the final tail; in a burst it accumulates on the event loop.

The measured validation sum is roughly 1.7–2.5% of the historical 38.471-second production total. This is only a scale comparison across different runs, not a predicted speedup or rigorous upper bound. Offloading validation would retain its CPU/I/O cost and introduce scheduling/resource contention; this diagnostic does not measure that implementation.

No long-programme extrapolation is warranted. More or longer chunks, a slower disk, or different completion clustering may change the result. If responsiveness or long-programme evidence later motivates an offload, preserve artifact-before-manifest commit, crash adoption and failure draining, and test them before any release.

## Reproduction and verification

From the worktree root, use the project Python environment with FFmpeg and FFprobe on PATH:

```sh
PYTHONPATH=. python scripts/benchmarks/tts_local_commit.py \
  --run-directory /path/to/private/completed-run \
  --output /tmp/tts-local-commit.json --repeats 3
```

Content-free raw observations are in [podcast-local-commit-results.json](podcast-local-commit-results.json). The script passed all 12 measured cases, including real committed-manifest and byte-integrity assertions. Ruff and compilation/diff checks passed. Independent review found no blocking Standards or scope finding; timer coverage is clarified above. Production code and defaults are unchanged.
