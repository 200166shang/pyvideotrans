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
| TTS concurrency | 4 |
| TTS start rate | 150 RPM, smooth start, empty initial bucket |
| Translation chunk | semantic/speaker-aware, approximately 600–1000 tokens |
| TTS chunk | contiguous turn text, soft 500-character ceiling, hard 600-character rejection |
| Output | 48 kHz, mono, 64 kbps MP3 |

Credentials come from the existing local pyVideoTrans configuration or environment. They are never copied into a manifest, cache key, report, exception, debug line, or test fixture.

## Ordered coordinator

One foreground coordinator owns five stages:

```text
prepare -> asr -> translate -> tts -> finalize -> listening quality gate
```

Provider adapters return data and numeric metadata; they never change run state. The coordinator holds a run-directory lock and performs every manifest transition atomically.

### Prepare

- Fingerprint the source without storing its absolute path in the report.
- Convert the source once to the audio specification required by whole-file ASR when needed.
- Persist input duration and artifact fingerprint.

### ASR

- Submit one asynchronous whole-file task.
- Persist the Alibaba task ID before the first poll.
- Poll every 2–5 seconds with bounded backoff.
- On resume, poll the saved task ID rather than submit again.
- Persist timestamped English segments and numeric usage as a private stage artifact; the public report stores only its fingerprint and counts.

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

The run directory contains a versioned `manifest.json`, private stage artifacts, scratch files, the final MP3, and the public production report.

- Writes use temporary file, flush, `fsync`, and atomic rename.
- A stage or chunk is resumable only after its artifact is validated, fingerprinted, and recorded.
- On startup, `in_flight` chunks become `pending`; `completed` chunks remain completed.
- A saved ASR task ID survives restart and remains the identity of that paid submission.
- Resume begins at the first incomplete stage or chunk.
- One lock prevents two processes from resuming the same run concurrently.
- Cleanup removes reproducible scratch only after listener acceptance. Manifests, usage metadata, reports, and final MP3 remain.

## Cache identity

All keys are SHA-256 over canonical JSON. Stage identity includes input fingerprint, profile fingerprint, stage schema version, provider, pinned model, region, source/target languages, and processing options. Chunk identity additionally includes sequence number, normalized-text hash, and chunk-policy version. TTS adds voice and output audio specification.

Changing model, region, voice, text normalization, stage schema, or chunk policy is a cache miss. Artifacts from different identities never mix within a run.

## Retry and error policy

Retry only throttling, transient 5xx responses, connection resets, and timeouts. Use randomized exponential backoff, at most five retries after the initial attempt, and reacquire the rate token for every retry.

Authentication, invalid model/voice/input, and exhausted quota are permanent failures. Stop with an actionable error code; never switch provider, model, region, or voice automatically. If a timeout leaves billing uncertain, record `cost_unconfirmed`.

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

## Runtime and cost acceptance

The current cold five-minute baseline is 229.913 seconds and approximately CNY 0.2959.

- Engineering target: fixed five-minute sample no more than 75 seconds; representative two-hour source no more than 30 minutes.
- First-release acceptance: fixed sample no more than 115 seconds; representative two-hour source no more than 45 minutes.
- Cost is measured and optimized toward CNY 2 per source hour, but is not a runtime release blocker.

A performance claim requires one cold candidate, listener acceptance of Andre and the audio, one unchanged confirmation within 15%, a TTS interruption/resume drill, and one representative two-hour run. Paid validation is deferred until the previously exposed Alibaba API key is rotated.

## Implementation slices

1. Durable manifest, cache identities, run lock, and resume normalization.
2. Alibaba whole-file asynchronous ASR adapter.
3. Deterministic translation/TTS chunking, rate limiting, concurrency, and retry scheduler.
4. Alibaba Qwen-MT and streaming Qwen3-TTS adapters with numeric usage capture.
5. Ordered MP3 finalizer and media probing.
6. Privacy-safe report builder.
7. Coordinator and CLI integration for `podcast`, `benchmark`, and `--resume`.
8. Offline unit/integration tests, interrupted-run test, CLI tests, and reader review of this specification.
9. After key rotation: one paid candidate, one confirmation, then one representative full-programme validation.

## Compatibility

Existing `stt`, `tts`, `sts`, and `vtv` tasks retain their behavior. New code lives under `videotrans.podcast` and is called only by the two new task values. The first implementation may use adapter injection for tests but must provide real Alibaba factories for the CLI.
