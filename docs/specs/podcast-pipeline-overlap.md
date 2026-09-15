# Translation–TTS Pipeline Overlap

Tracking issue: [200166shang/pyvideotrans#12](https://github.com/200166shang/pyvideotrans/issues/12)

This is a delta specification over [`docs/specs/chinese-podcast-cli.md`](https://github.com/200166shang/pyvideotrans/blob/main/docs/specs/chinese-podcast-cli.md). Its fixed-sample target, overlapping coordinator, manifest v2, trace, and paid-run rules are normative where they differ; the base specification remains normative for the fixed fixture, CLI directory rules, deterministic normalization and chunk splitting, retry classification and backoff, provider configuration, pricing, report privacy, finalization, and listening-window calculation.

## Outcome

Reduce the fixed five-minute cold benchmark from 53.826 seconds (52.514-second confirmation) to at most 45 seconds by overlapping translation and speech synthesis. Keep the current Alibaba ASR, translation, and TTS models, the Andre narrator, 48 kHz mono 64 kbps MP3 output, resumability, privacy boundaries, and protection against duplicate billing.

The CNY 3.85 per source-hour cost watchline is informational rather than a release blocker. A run above it must make the increase visible and must still prove that no request was duplicated.

## Scope

This round changes only orchestration, recovery state, content-free timing telemetry, and benchmark evaluation. It does not change providers, models, credentials, translation prompts, narrator voice, audio format, ASR behavior, or configured provider concurrency and rate limits.

The new profile ID is `alibaba-podcast-v2`; changing the ID and manifest/chunk-policy versions deliberately changes its fingerprint. Its provider settings remain:

- ASR: `qwen-audio-3.0-asr-flash-filetrans`;
- translation: `qwen-mt-flash`, concurrency 4, 60 request starts per minute;
- TTS: `qwen3-tts-flash-2025-11-27`, voice `Andre`, concurrency 4, 150 request starts per minute;
- translation packing target 600 units and hard limit 1,000 units;
- sentence-preferred TTS soft ceiling 500 Unicode code points and existing hard rejection above 600;
- MP3 at 48 kHz, mono, 64 kbps.

There is no active production run to preserve. The optimized coordinator adopts manifest v2 directly and does not implement v1 resume migration or retain a selectable v1 coordinator. An incomplete v1 run must be abandoned and restarted in a new v2 run directory. Existing completed outputs remain untouched. A new run still requires an empty output directory; `--resume <run-directory>` is the only operation allowed to open a recognized existing v2 directory and holds its existing run lock.

This specification supersedes the candidate/confirmation protocol in `chinese-podcast-cli.md` only for the second-round fixed five-minute optimization. Passing it supports a five-minute cold-sample claim and enables the new default; it does not establish a representative two-hour performance claim.

## Pipeline

After whole-file ASR completes, translation remains bounded by its existing scheduler. A translation result is one existing deterministic translation-plan chunk, identified by its zero-based plan index. Each durably committed result enters an index-ordered reorder buffer. Only the contiguous source-order prefix advances the incremental synthesis chunker; ASR and translation do not overlap in this round.

The chunker:

1. Preserves speaker-turn boundaries.
2. Uses the existing sentence-preferred 500-character synthesis limit.
3. Holds only the mutable tail of the current speaker turn.
4. Appends a stable synthesis chunk when its text can no longer be affected by a later translation result: the chunk is full under the existing sentence-first rule, or its speaker turn has ended.
5. Flushes the tail when the speaker changes or translation ends.

The base specification and `videotrans/podcast/chunking.py` define normalization, joining, sentence and punctuation precedence, overlong-sentence fallback, speaker boundaries, and identities. In v2, each synthesis chunk's row IDs are the ordered translated rows actually consumed by that chunk, rather than every row in its complete speaker turn; this makes the dependency closed when the chunk seals and is an intentional metadata-only change from v1. Incremental output must equal the independent canonical batch implementation over the same complete ordered translated rows; this equality, including text, speaker ID, consumed row IDs, voice, and sequence order, is the chunking oracle. Only provider audio bytes and their resulting timing may differ.

Sealed synthesis chunks enter the existing bounded TTS scheduler immediately. TTS requests may finish out of order, but artifacts and final assembly remain source ordered.

The normal successful path must preserve translated text, order, speaker ownership, narrator voice, and output specification. Compared with v1 evidence, provider audio bytes and timing may differ, but v2 TTS boundaries must match the canonical batch-chunking oracle defined above.

## Manifest v2 and recovery

Manifest v2 supports:

- multiple simultaneously active stages;
- an append-only synthesis plan with an explicit open/sealed state;
- stable input identities for translated and synthesized chunks;
- provider-submission uncertainty distinct from a known retryable failure;
- atomic artifact commit with hash, size, and local path;
- content-free pipeline event timing.

The TTS stage stores `plan_status: open|sealed`. It does not persist a mutable chunk record: the current tail is reconstructed from committed translation artifacts on resume. Each finalized chunk is appended once with its final zero-based index and stable logical request identity. Sealing the whole plan changes only `plan_status` from `open` to `sealed`; the stage completes only when the plan is sealed and every appended chunk is committed.

Each provider chunk has a stable logical identity and one or more numbered attempt identities. Legal transitions are:

```text
pending -> submission_uncertain -> committed
pending -> submission_uncertain -> pending     # explicit no-success retry response
pending -> submission_uncertain -> failed      # explicit permanent no-success response
pending -> submission_uncertain -> failed      # retryable no-success response at retry limit
```

A second network send for a logical identity is allowed only after an explicit provider response classified as no-success by the base retry policy. Such a send receives a new attempt identity and counts as a retry, not a duplicate. Any second send after an ambiguous or successful attempt is a duplicate and fails acceptance.

Before a provider request can be sent, its durable state records both the new attempt identity and that submission may become uncertain. A successful provider result is written as a private artifact package: write and `fsync` the artifact in a temporary chunk directory; validate it and compute its size and SHA-256; write and `fsync` a content-free receipt containing the logical identity, attempt identity, size, and SHA-256; atomically rename the directory to the stable logical-identity location; then `fsync` its parent. Only then atomically commit the package path, size, hash, and attempt identity to the manifest. A verified artifact moves the chunk to committed. A malformed, empty, or corrupt provider success leaves the item submission-uncertain and fails the run because it may already have been billed.

On resume, committed packages are verified and reused. A stable package present after rename but before manifest commit is an orphan and may be adopted when its receipt matches the expected logical and attempt identities and its size and SHA-256 validate. Incomplete temporary directories are deleted without provider work. A manifest reference to a missing or invalid package stops with `artifact_invalid`. Final MP3 assembly uses the same write/probe/hash/atomic-rename ordering; an uncommitted final file may be discarded and rebuilt from committed TTS packages without a cloud call.

If a request may have been billed but no verified artifact exists, the run stops and reports the uncertain submission instead of automatically resending it. “Never submitted” means the durable state did not cross the pre-send uncertainty marker. “Definitively rejected” means the provider returned an explicit retryable response without a successful result; connection resets and timeouts after the marker remain uncertain unless the provider contract proves otherwise.

When any pipeline item fails permanently inside a live process, the coordinator stops all new network attempts, including retries for other logical items, waits through the provider adapters' existing bounded call timeouts for attempts already on the network, and durably commits successful results before reporting the failure. A process crash or termination cannot drain remote work; any marked request without a verified package becomes an uncertain submission on resume. Rate-limit state is process-local and restarts with the base profile's empty smooth bucket, preventing a restart burst.

Failure behavior not changed above remains the base behavior: explicit 408/429/5xx and throttling responses proven to contain no success use bounded retries; a post-marker connection reset or timeout is an uncertain submission and is not automatically retried. Authentication, invalid model/voice/input, malformed or empty success, exhausted quota, corrupt audio, disk or permission errors, failed hashing/probing, manifest-write errors, and final assembly errors fail without provider substitution. Signals request orderly drain when the process remains alive. If the process cannot complete that drain, resume applies the uncertainty rules.

## Performance trace

Each run stores schema-versioned `performance-trace.jsonl` inside its private run directory with owner-only permissions where the platform supports them. Every line is a validated JSON event containing a monotonically increasing process-segment number and a segment-relative monotonic timestamp, plus only:

- monotonic relative timestamps;
- stage and chunk identifiers;
- dependency ranges expressed only as inclusive numeric translation-plan indices;
- queue, start, completion, and commit events;
- effective concurrency and rate-limit settings;
- end-to-end wall-clock duration.

The trace excludes source media, transcript or translated text, credentials, request headers, provider response bodies, absolute paths, arbitrary exception strings, text-derived identifiers, and media or text lengths. It is retained and protected with the rest of the private run directory. Resume scans through the last complete valid newline and discards only an invalid trailing partial line before appending a new process segment; any complete invalid event fails recovery with `trace_invalid`.

Cold wall-clock measurement uses `time.monotonic_ns()`. It starts immediately before the prepare stage begins and ends after the final MP3 has been written, probed, and durably committed to the manifest. It excludes CLI startup, process downtime, and human review. A resumed diagnostic report uses the sum of completed process-segment durations and labels the value active-process time; it is not candidate evidence. An uninterrupted cold benchmark uses its single segment's direct elapsed wall clock rather than summing stage durations.

## Verification before paid use

No cloud call is made while developing the candidate. The implementation must first pass:

- deterministic producer-consumer and source-order tests;
- exact text, speaker-turn, and audio-order equivalence tests;
- incremental chunk sealing and end-of-input tests;
- crash/restart tests at every provider submission and artifact commit boundary;
- drain-on-failure and no-new-launch tests;
- uncertain-submission tests proving that automatic resend does not occur;
- privacy tests proving that the performance trace contains no content;
- a synthetic latency replay using the previous stage timings and the real scheduler.

The synthetic replay must predict at most 45 seconds before paid benchmarking begins. If more than one offline variant passes every recovery and equivalence test, select the one with the lowest predicted wall clock; break a tie by fewer provider submissions and then the smaller implementation surface. No paid comparison is used to select a variant.

The replay is deterministic: seed 0, the fixed 12-translation/12-TTS shape from the accepted five-minute run, its content-free one-to-one dependency ranges `(0,0)` through `(11,11)`, the accepted aggregate observations (2.24-second local prepare/finalize envelope, 13.20-second ASR, 11.68-second translation stage, and 26.06-second TTS stage), and the real production scheduler driven by a fake monotonic clock. For each cloud chunked stage, solve one constant fake service latency such that running that stage alone through its real concurrency and request-start pacing reproduces the observed aggregate stage duration; then run those calibrated operations through the overlapping coordinator. The pass value is the simulated direct wall clock from prepare start through final commit. This is a qualification gate for paid use, not evidence of cloud performance by itself.

## Paid benchmark protocol

Only the final offline winner receives paid runs:

1. Run one cold five-minute candidate.
2. Require at most 45 seconds, zero duplicate provider submissions under the logical/attempt rule above, the complete automated integrity suite defined below, and a passed beginning/middle/end listening review.
3. If the candidate fails, do not run a confirmation.
4. If it passes, run exactly one cold confirmation.
5. Require at most 45 seconds, `abs(confirmation_ms - candidate_ms) / candidate_ms <= 0.15`, zero duplicate provider submissions, and the same complete automated integrity suite as the candidate.
6. Listen to the confirmation only if an automated check emits a non-failing quality warning. The same three-window listening gate must then pass; a hard integrity failure fails the confirmation directly.
7. If confirmation fails, do not run a third paid attempt; return to offline diagnosis. Any later paid round requires a new explicit authorization from the listener.

Both paid runs use the fixed private fixture defined by the base specification, `alibaba-podcast-v2`, the same Git commit, the same local host, Beijing region, cold cache, fresh empty run directories, and uninterrupted processes. A resumed run is never candidate or confirmation evidence. Once a run sends its first cloud request it consumes that paid slot even if a provider, network, process, or local infrastructure failure later invalidates it; failure before the first cloud request does not consume the slot.

After both runs pass, stage overlap becomes the default production path. Estimated cost per source hour is `total_estimated_cny / (input_duration_ms / 3_600_000)` under the base specification's price calculation. A value above CNY 3.85 is reported prominently but does not by itself block acceptance.

“Valid structure and automated integrity” require a valid privacy-safe report and manifest, one contiguous sequence of sealed and committed TTS chunk identities with no gap or duplicate, matching profile and input fingerprints, a non-empty final artifact whose fingerprint matches the manifest, and FFprobe confirmation of MP3, 48 kHz, mono, and 64 kbps. The listening gate uses the first 60 seconds, a centered 60-second window, and the last 60 seconds; acceptance means no material translation-clarity, speaker-turn, voice-naturalness, or rhythm defect is heard.
