# Podcast TTS throughput investigation — 2026-09-15

## Evidence and constraints

The reference is the completed cold v2 run documented in [issue #12](https://github.com/200166shang/pyvideotrans/issues/12), 52.009 seconds for the fixed 300.010-second source. Its private trace remains in the previous pipeline-perf worktree; neither private transcript nor audio is published here. The active branch starts from merged `ba4076d`, not the older chunking-only checkout.

The listener approved [issue #14](https://github.com/200166shang/pyvideotrans/issues/14): at least 10% improvement, one paid cold candidate only, retain existing models/Andre, no confirmation or long-programme validation. The timing gate is 46.8081 seconds. This historical comparison cannot isolate cloud variability or establish stable performance.

## Provider quota check

Alibaba documents Beijing `qwen3-tts-flash-2025-11-27` at 180 RPM. The existing 150 RPM smooth start policy is retained (one request every 0.4 seconds); no explicit in-flight concurrency quota is published for that model. This is not proof of the account's available headroom. Limits aggregate across the main account, may operate at second granularity and include burst protection.

Sources, consulted 2026-09-15:

- [Qwen3-TTS-Flash model](https://help.aliyun.com/zh/model-studio/qwen3-tts-flash)
- [Model Studio rate limits](https://help.aliyun.com/zh/model-studio/rate-limit)
- [Rate-limiting best practices](https://help.aliyun.com/zh/model-studio/rate-limiting-best-practices)

## Method

Replay the original per-chunk TTS queue/start/commit events through the production scheduler with a virtual clock. Keep observed translation release times and the finalization tail fixed. Service times include the observed call/artifact interval; they are hypothetical when changing concurrency. Check replay4 against measured total before interpreting other configurations. Sensitivity to slower calls is a scenario, not a probability or confidence interval.

If concurrency alone fails the 10% gate, use the existing sentence-first incremental chunker to explore smaller ceilings offline. New chunk durations necessarily require a model of service time versus text allocation and per-request overhead; these are weaker predictions than unchanged-chunk replay. Preserve ordered full text and speaker turns, and compare incremental results with the independent batch oracle.

## Unchanged-chunk concurrency result

| TTS concurrency | Predicted total seconds | Reduction vs historical baseline |
| --- | ---: | ---: |
| 4 | 52.008729 | calibration residual 0.000271 s |
| 6 | 46.868048 | 9.8847% |
| 8 | 46.868048 | 9.8847% |

Both higher-concurrency candidates narrowly fail the agreed 10% gate. They must not be rounded up into a pass. A 20% service-latency increase raises both predictions to 50.821598 seconds. The next authorized branch is smaller sentence-first TTS chunks, rather than spending the one paid slot on concurrency alone.

Reproduce unchanged-chunk replay with the project Python environment:

```sh
python -m videotrans.podcast.throughput_replay --trace /path/to/private/performance-trace.jsonl
```

## Smaller-chunk fallback and selection

The scratch sweep uses actual saved translation rows locally. It checks equality with the independent batch chunker and preserves the exact sequence of `(non-whitespace character, speaker)` pairs. Character ownership maps original measured variable work onto new chunks without a text-matching fallback. Releases use contiguous translation-prefix completion, and the first request pays the existing 0.4-second limiter delay.

A candidate duration is fixed overhead plus allocated original variable work, multiplied by a latency factor. Overhead scenarios are 0.5/1/2 seconds; factors are 1/1.1/1.2. Subtracting overhead from an original shorter call clamps variable work to zero; at 2 seconds four original calls are clamped. This deliberately changes the model of short-call time and is an uncertainty, not a fitted provider law. The finalization tail is held fixed despite additional output chunks; real finalization is measured in the paid run.

| Ceiling / concurrency | Predicted total range, seconds | Reduction range vs 52.009 s |
| --- | ---: | ---: |
| 250 / 4 | 45.119–52.746 | -1.42%–13.25% |
| 250 / 6 | 40.380–44.488 | 14.46%–22.36% |
| **250 / 8** | **38.500–41.562** | **20.09%–25.97%** |

These are scenario envelopes, not confidence intervals. 250/8 is selected because it beats 250/6 by 1.880–2.926 seconds throughout the explored grid. Both smaller ceilings produce 14 chunks on these saved translations versus 12 at 500; earlier sealing matters as well as request count. No model, voice, rate limit or production default changes.

## Single paid candidate result

Candidate `tts-throughput-20260915-165255`, code commit `f16acd6`, completed once with the fixed source and explicit `alibaba-podcast-tts-throughput` profile:

- End-to-end cold wall clock: **38.471 seconds**, versus historical **52.009 seconds**; **26.030% reduction**, passing the 46.8081-second gate.
- Estimated cost: **CNY 0.30199705**, or **CNY 3.6238 per source hour**, below the informational 3.85 watchline. These are usage-based estimates, not reconciled invoices.
- 12 translation and 14 synthesis chunks committed; zero retries; each logical chunk has one observed start and one attempt.
- Single process, cold cache, zero cache hits; fixed input fingerprint and experimental profile verified.
- Contiguous sealed synthesis plan, exact batch-oracle chunk identities, artifact receipts/hashes and final report fingerprint verified. Final MP3 is 48 kHz mono 64 kbps and passes complete FFmpeg decode.
- Listening review is **pending**. The local review audio concatenates the first, centered and last 60 seconds. The agent does not infer listener acceptance from automated integrity.
- Exactly one paid candidate was used. No new baseline, confirmation or long-programme run was performed. This historical comparison supports a promising single sample, not a causal/stable or long-programme performance claim. The accepted v2 CLI default remains unchanged.

## Verification and review

Before the paid run, the full podcast suite passed **193 tests**. Review fixes then passed **18 targeted replay tests** (including new dependency-time regression coverage). Changed Python files pass Ruff and compilation; full-directory Ruff identified 11 existing base-revision findings, one import-order finding was removed while touching the shared replay module, leaving unrelated legacy findings outside this scope.

Standards review found missing dependency-prefix timing validation and a private clock coupling. The implementation now rejects TTS releases before their translation prefix has committed and exposes the shared `VirtualClock`; both changes were reverified. Spec review checked the opt-in profile delta, historical timing threshold, one-run limit and pending listener gate. No unresolved blocking finding remains for this experimental slice.
