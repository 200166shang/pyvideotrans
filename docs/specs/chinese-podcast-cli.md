# Alibaba Chinese Podcast CLI Specification

Status: implementation-ready  
Decision source: [Map a faster Chinese podcast production line](https://github.com/200166shang/pyvideotrans/issues/1)

## Outcome

Add a single-user CLI path that turns an authorized local English audio or video file into a faithful, naturally paced Mandarin MP3. The first production profile uses Alibaba Cloud in Beijing, one steady male narrator voice (`Andre`), resumable ordered stages, bounded cloud concurrency, and a privacy-safe production report.

The path is audio-only. It does not download remote media, align speech to the source-video timeline, clone voices, or integrate with the existing GUI.

## CLI

Create a production run:

```text
uv run --no-sync cli.py \
  --task podcast \
  --name /absolute/path/to/english-input.m4a \
  --output-dir /absolute/path/to/run-directory \
  --podcast-profile alibaba-podcast-v1 \
  --report /absolute/path/to/report.json
```

Resume an existing run:

```text
uv run --no-sync cli.py \
  --task podcast \
  --resume /absolute/path/to/run-directory
```

Use `--task benchmark` instead of `--task podcast` with the same `--resume` and `--review` forms when operating on a benchmark run. Benchmark review also refreshes `benchmark-summary.json`.

Record the listener-owned quality decision without constructing a cloud client:

```text
uv run --no-sync cli.py \
  --task podcast \
  --resume /absolute/path/to/run-directory \
  --review accepted \
  --review-note "steady voice and clear translation"
```

Use `--review rejected` when the listening edition is not acceptable. A review is valid only after finalization has moved the run to `awaiting_review`. Repeating the same terminal decision is state-idempotent and repairs its report if the prior report replacement was interrupted; it also replaces `decided_at` and replaces the prior note with the newly supplied note (including clearing it when omitted). Attempting to change `accepted` to `rejected` or vice versa is rejected.

Run the fixed benchmark through the same coordinator:

```text
uv run --no-sync cli.py \
  --task benchmark \
  --name output/benchmarks/english-podcast-5m.m4a \
  --output-dir output/benchmarks/runs/<run-id> \
  --podcast-profile alibaba-podcast-v1 \
  --cache-mode cold \
  --report output/benchmarks/reports/<run-id>.json
```

`--task podcast` and `--task benchmark` share implementation. Benchmark mode adds fixed-profile validation and target evaluation; it does not contain a second media pipeline.

When `--output-dir` is omitted, the CLI creates a collision-safe sibling directory named `<source-stem>-zh-podcast-<YYYYMMDD-HHMMSS>`. A supplied output directory must be empty. If it already contains a manifest, the CLI requires `--resume`; if it contains any other file but no manifest, creation is rejected without overwriting it.

The first fixed benchmark sample is `<repository-root>/output/benchmarks/english-podcast-5m.m4a`: duration 300.010 seconds, SHA-256 `fba898bc3ff430ef7a2a78516600e6db7d00c33afa4f8b24fda20a1c1387501f`. This ignored, authorized local fixture is supplied directly and privately by the repository owner; there is intentionally no public download or alternate acquisition channel. A new checkout must request that owner-only handoff, place the file at the exact repository-relative path, and let the CLI verify SHA-256 plus duration before any cloud client is constructed. Duration is the FFprobe container-format duration rounded to the nearest millisecond and accepted within ±20 ms. Without this exact fixture, benchmark execution is intentionally unavailable.

## Production profile

`alibaba-podcast-v1` is explicit and fingerprinted:

| Concern | Value |
| --- | --- |
| Region | China (Beijing) |
| Source / target | English / Simplified Mandarin |
| ASR | `qwen-audio-3.0-asr-flash-filetrans` asynchronous whole-file task |
| Translation | `qwen-mt-flash` |
| TTS | `qwen3-tts-flash-2025-11-27`, non-realtime streaming |
| Voice | `Andre` for every chunk |
| Translation concurrency | 4 |
| Translation start rate | 60 RPM, smooth start |
| TTS concurrency | 4 |
| TTS start rate | 150 RPM, smooth start, empty initial bucket |
| Translation chunk | speaker-bounded, deterministic 600-unit packing target / 1,000-unit hard limit |
| TTS chunk | contiguous turn text, soft 500-character ceiling, hard 600-character rejection |
| Output | 48 kHz, mono, 64 kbps MP3 |

`DASHSCOPE_API_KEY`, when present in the process environment, is shared by all three providers. Otherwise the provider factory reads the normal pyVideoTrans application settings already loaded as `videotrans.configure.config.params`: ASR and translation use `qwenmt_key`, falling back to `qwentts_key`; TTS uses `qwentts_key`, falling back to `qwenmt_key`. A Beijing workspace may be selected in the order process environment `DASHSCOPE_WORKSPACE_ID`, application setting `qwenmt_spaceid`, application setting `qwentts_spaceid`; otherwise the public Beijing endpoint is used. Credentials are never copied into a manifest, cache key, report, exception, debug line, or real-value test fixture. Credential validity and authorization to incur paid usage are operator-owned preconditions, not CLI state.

## Ordered coordinator

One foreground coordinator owns five stages:

```text
prepare -> asr -> translate -> tts -> finalize -> listening quality gate
```

Provider adapters return data and numeric metadata; they never change run state. The coordinator holds a run-directory lock and performs every manifest transition atomically.

### Prepare

- Fingerprint the source without storing its absolute path in the report.
- Convert the source once to 16 kHz mono 48 kbps AAC before upload. Mono is required when speaker diarization is enabled, and the lower-rate canonical artifact reduces upload size.
- Persist input duration and artifact fingerprint.
- Store the absolute source location only in private `run.private.json`, so a crash during prepare/upload can resume. The public manifest and report contain only its fingerprint.

### ASR

- Submit one asynchronous whole-file task.
- Persist the Alibaba task ID before the first poll.
- Persist a local `in_flight` submission marker immediately before the submit call. If the process stops after that marker but before the task ID is durably saved, recovery stops with `asr_submission_uncertain`; it never submits a second paid task automatically. This release cannot attach a discovered task ID, so the operator checks Alibaba's task history for cost/accounting and starts a new run only after explicitly accepting another submission.
- Poll every 2–5 seconds with bounded backoff.
- On resume, poll the saved task ID rather than submit again.
- Persist timestamped English segments and numeric usage as a private stage artifact; the public report stores only its fingerprint and counts.
- Upload local input to Alibaba's 48-hour temporary OSS and enable OSS-resource resolution on ASR submission. A saved ASR task is never resubmitted merely because the temporary upload URL is no longer available.

### Translate

- Build deterministic semantic chunks while preserving source order and speaker boundaries where available.
- Use at most four requests in flight.
- Persist each accepted translated chunk before updating its manifest state.
- Reassemble translated rows deterministically.

### TTS

- Coalesce contiguous text without exceeding the 500-character soft ceiling; split at sentence punctuation.
- Use `Andre` for every chunk. Speaker boundaries remain in the chunk/order model but do not select different voices.
- Gate request starts with both a four-request semaphore and a smooth 150-RPM token bucket.
- Persist each lossless audio chunk atomically with returned billable-character usage.

### Finalize

- Concatenate committed chunks by sequence number.
- Encode once to a 48 kHz mono 64 kbps MP3.
- Do not insert the original subtitle/video timeline gaps.
- Fingerprint and probe the final file before moving the run to `awaiting_review`.

## Manifest and resume invariants

The run directory contains a versioned `manifest.json`, private stage artifacts, scratch files, and the final MP3. It also contains the public production report by default; when `--report` selects an external path, that external file replaces the default report rather than duplicating it.

- Writes use temporary file, flush, `fsync`, and atomic rename.
- A stage or chunk is resumable only after its artifact is validated, fingerprinted, and recorded.
- On startup, `in_flight` chunks become `pending`; `committed` chunks remain committed.
- A saved ASR task ID survives restart and remains the identity of that paid submission.
- Resume begins at the first incomplete stage or chunk.
- One lock prevents two processes from resuming the same run concurrently.
- The current personal-tool release retains private checkpoints after acceptance; cleanup is manual so useful recovery evidence is not deleted unexpectedly.

Run statuses are `pending`, `running`, `interrupted`, `needs_attention`, `awaiting_review`, `accepted`, and `rejected`. A new run goes `pending -> running`; a caught stage/chunk failure goes `running -> needs_attention`; loading any unfinished run for recovery first records `pending|running|needs_attention -> interrupted`, and starting its next incomplete operation returns it to `running`. A failed stage or chunk remains `failed` until explicit `--resume`, then its next start changes it to `in_flight`; credentials/quota/network/provider faults must be corrected first. Finalization commits `running -> awaiting_review`; listener review permits only `awaiting_review -> accepted|rejected`. Repeating the same terminal review is idempotent, the opposite terminal review is invalid, and ordinary resume of `accepted|rejected` only validates and returns the existing result. `awaiting_review` may be resumed to revalidate artifacts and regenerate its report without repeating paid work. Work stages use `pending|in_flight|completed|failed`, and chunk work uses `pending|in_flight|committed|failed`; recovery changes local `in_flight` chunks to `pending`, but keeps ASR `in_flight` when its paid task ID exists. The exact validated schema and transition implementation live in `videotrans/podcast/manifest.py`; unsupported versions, profile-fingerprint mismatches, and `artifact_invalid` require a new run or documented manual restoration rather than implicit migration. Artifact validation failure never changes the existing run or review status. On resume, every reused artifact is checked against its recorded relative path, size, and SHA-256. Missing or mismatched committed artifacts are not silently replaced or billed again. An expired or missing remote ASR task stops with the provider's safe error code and is never automatically resubmitted.

## Cache identity

All keys are SHA-256 over UTF-8 JSON with sorted object keys, no insignificant whitespace, finite values only, and an explicit namespace prefix. Text normalization collapses whitespace. The deterministic translation estimator counts each CJK character and punctuation mark as one unit and each Latin/digit run as `ceil(length/4)` units. Its 1,000-unit limit is hard; 600 units is a packing target within one speaker turn, not a minimum request size. Translation rows keep source order, never cross a known speaker boundary, split over-budget rows at sentence then readable punctuation/space boundaries, and use exact Unicode character boundaries only as fallback. TTS groups contiguous same-speaker rows and applies the same sentence-first split under 500 characters. Chunk identity includes sequence number, normalized-text hash, and chunk-policy version. TTS adds voice and output audio specification.

Changing model, region, voice, text normalization, stage schema, or chunk policy is a cache miss. Artifacts from different identities never mix within a run.

## Retry and error policy

Translation, TTS, and ASR polling retry only throttling, transient 5xx responses, connection resets, and timeouts. They use randomized exponential backoff, at most five retries after the initial attempt, and reacquire the rate token for every rate-limited provider retry. ASR submit is the deliberate exception: after its durable `in_flight` marker, any raised exception or missing/malformed task ID is treated as `asr_submission_uncertain` and is never retried automatically in the current process or after restart. This release has no supported task-ID attachment command; inspect Alibaba task history for cost/accounting, then explicitly start a new run only if another submission is acceptable.

Authentication, invalid model/voice/input, and exhausted quota are permanent failures. Stop with an actionable error code; never switch provider, model, region, or voice automatically. A timeout increments attempts/retries, but without returned numeric usage the tool does not invent a monetary amount.

## Production report

The JSON report follows the accepted benchmark contract:

- schema version and run/cache status;
- input duration, language, and fingerprint;
- profile fingerprint and non-secret provider configuration;
- ordered stage timing, attempts, retries, cache hits, confirmed cost, uncertain-cost attempts, artifact fingerprint, and safe error code;
- output format, duration, bitrate, channels, and fingerprint;
- listener-owned quality status;
- explicit privacy flags.

It must not contain credentials, transcript/translation text, absolute source path, provider response bodies, or arbitrary exception strings.

Cost fields use numeric usage returned by successful adapter responses and Beijing public list prices last checked on 2026-09-15: ASR CNY 0.00022 per second, Qwen MT CNY 0.7/1.95 per million input/output tokens, and Qwen3 TTS Flash CNY 0.8 per 10,000 characters. This personal-tool release has no billing reconciliation step, so calculated spend remains `cost_unconfirmed_cny` and `cost_confirmed_cny` remains zero. Translation's combined-token fallback prices every token at the higher output-token rate. Any provider call that raises is treated as having no usable usage—even if its error body contains a number—and contributes zero cost while remaining visible in attempts/retries. Each stage estimate is rounded to 8 decimal CNY; totals are the exact decimal sum of those stored stage values, serialized as JSON numbers. The total and `total / (input_duration_ms / 3_600_000)` per-source-hour figure are estimates, not invoices.

The authoritative strict report schema is validated in `videotrans/podcast/report.py`. Without `--report`, the only report is `<run-directory>/production-report.json`; with `--report`, the only report is the supplied path. At new-run creation, that path is expanded and resolved (including symlinks) against the current working directory, checked as the normalized absolute target, and stored as an absolute path in private `run.private.json`; changing working directory later cannot redirect it. A custom report target must not exist and cannot be the source, manifest, manifest lock, private state, final MP3, benchmark summary, or anything below the private-artifact directory. Resume/review may only use the saved destination; they cannot rebind or overwrite a different report. The manifest remains authoritative for run/review state. Review writes it first, then atomically replaces the existing derivative report; rerunning the same review repairs an interruption when that report still exists in its pre-review form. Loss of `run.private.json`, or loss of an external report after terminal review, requires manual restoration from backup; there is no supported hand-authored recovery format. `--review-note` is optional public free text. The CLI validates report structure and rejects obvious unsafe keys/absolute paths, but cannot reliably classify arbitrary prose, so the listener is responsible for excluding credentials, source paths, transcript excerpts, or other private text.

## Runtime and cost acceptance

The current cold five-minute baseline is 229.913 seconds and approximately CNY 0.2959.

- Engineering target: fixed five-minute sample no more than 75 seconds; representative two-hour source no more than 30 minutes.
- First-release acceptance: fixed sample no more than 115 seconds; representative two-hour source no more than 45 minutes.
- Cost is measured and optimized toward CNY 2 per source hour, but is not a runtime release blocker.

A performance claim requires one uninterrupted cold candidate, listener acceptance of Andre and the audio, one uninterrupted confirmation, a separate TTS interruption/resume drill, and one representative two-hour run. “Unchanged” means the same input fingerprint, profile fingerprint, and Git commit. Keep this benchmark deliberately simple: the CLI writes independent per-run summaries and does not claim or enforce their relationship. The operator records `candidate=<run-id> commit=<sha>` in the candidate review note and `confirms=<candidate-run-id> commit=<same-sha>` in the confirmation note, then manually checks `abs(confirm_ms - candidate_ms) / candidate_ms <= 0.15`. A resumed run is diagnostic evidence only and cannot serve as the candidate or confirmation.

The minimum listening gate is three windows computed in integer milliseconds: `[0, min(60000, duration)]`; a 60-second window centered on `floor(duration / 2)` and shifted inside the available duration; and `[max(0, duration - 60000), duration]`. If output is shorter than 60 seconds, all three collapse to the whole output; overlap is allowed. For a performance claim, record the exact three ranges and any defect summary in `--review-note`; the generic CLI keeps the note optional and does not parse this human evidence. `accepted` means no material translation-clarity, speaker-turn, voice-naturalness, or rhythm defect was heard in those windows.

Benchmark mode currently supports cold new runs only: each invocation without `--resume` creates a new run directory and re-executes prepare, upload, ASR, translation, TTS, and finalization with no cross-run artifact reuse. `--resume` is recovery within that same cold run and reuses its paid ASR task and committed chunks after validation; `--review` changes only listener-owned state. Each stage timer accumulates every attempted execution interval through its final durable write, including failed attempts, while process downtime, CLI startup, and human review are excluded. Because downtime is excluded, any resumed run is ineligible as candidate/confirmation evidence. The atomically written summary is `<run-directory>/benchmark-summary.json`; benchmark resume/review regenerates it from the report if it is missing or stale. It contains elapsed time, both five-minute thresholds, Boolean pass fields, fixed-sample validation, and listening status. A completed pipeline exits zero even when a performance target is missed; target failure is represented by those machine-readable Booleans. Provider/pipeline failure exits nonzero. Warm-cache comparison is deliberately outside this minimal benchmark release.

## Implementation slices

1. Durable manifest, cache identities, run lock, and resume normalization.
2. Alibaba whole-file asynchronous ASR adapter.
3. Deterministic translation/TTS chunking, rate limiting, concurrency, and retry scheduler.
4. Alibaba Qwen-MT and streaming Qwen3-TTS adapters with numeric usage capture.
5. Ordered MP3 finalizer and media probing.
6. Privacy-safe report builder.
7. Coordinator and CLI integration for `podcast`, `benchmark`, and `--resume`.
8. Offline unit/integration tests, interrupted-run test, CLI tests, and reader review of this specification.
9. After explicit operator authorization for paid usage: one paid candidate, one confirmation, then one representative full-programme validation.

## Compatibility

Existing `stt`, `tts`, `sts`, and `vtv` tasks retain their behavior. New code lives under `videotrans.podcast` and is called only by the two new task values. The first implementation may use adapter injection for tests but must provide real Alibaba factories for the CLI.
