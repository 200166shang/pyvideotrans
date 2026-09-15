# Provider research: retained Beijing podcast profile — 2026-09-15

## Conclusion

This research retains the authoritative profile in `videotrans/podcast/profiles.py`:
Beijing `qwen-audio-3.0-asr-flash-filetrans`, `qwen-mt-flash`, and
`qwen3-tts-flash-2025-11-27` with `Andre`. No cloud call, model change, or
configuration change was made.

Do not prioritize segmented ASR. The existing whole-file model already supports
the required diarization and timestamp output. Segmenting is technically
possible, but Alibaba does not document ASR concurrent-task headroom, latency
benefit, or stable speaker identities across separate files. A `speaker_id` is
only documented as an integer index in a recognition result, so IDs from two
requests cannot safely be treated as the same people.

Private ASR task diagnostics are feasible and should precede a segmentation
decision. Query results expose provider task timestamps and `task_metrics`.
The user-approved gate remains a five-minute screening followed by a
long-programme validation after the plan and budget are confirmed.

## Documented limits and response fields

| Concern | Official fact | Pipeline consequence |
| --- | --- | --- |
| ASR flow | The retained model is asynchronous: submit returns `task_id`; query `GET /api/v1/tasks/{task_id}` reports completion. [ASR HTTP API](https://help.aliyun.com/en/model-studio/fun-asr-recorded-speech-recognition-http-api) | Retain submit-once/task-ID resume handling. |
| ASR capacity | One request accepts one URL; max file is 12 hours / 2 GB. With diarization, audio should stay within two hours or recognition can fail/time out. [ASR models](https://help.aliyun.com/en/model-studio/asr-model/) | A <=2-hour programme does not need segmentation for provider input limits. |
| Diarization | Diarization is mono-only. Sentences receive `speaker_id` when enabled; it is an integer index starting at zero. [ASR HTTP API](https://help.aliyun.com/en/model-studio/fun-asr-recorded-speech-recognition-http-api) | Existing mono preparation fits. **Unknown:** no official cross-file speaker-ID consistency guarantee was found. A segmented design must namespace IDs or evaluate reconciliation. |
| Transcript time | Sentence `begin_time` and `end_time` are in-audio milliseconds. [ASR HTTP API](https://help.aliyun.com/en/model-studio/fun-asr-recorded-speech-recognition-http-api) | Preserve source timestamps and add segment offset only after a defined boundary policy. |
| Query timing/metrics | Query output includes `submit_time`, `scheduled_time`, `end_time`, result URLs, `task_metrics` (`TOTAL`, `SUCCEEDED`, `FAILED`), and `usage.duration`. Result URLs last 24 hours. [ASR HTTP API](https://help.aliyun.com/en/model-studio/fun-asr-recorded-speech-recognition-http-api) | Provider queue is `scheduled-submit`; provider execution is `end-scheduled`; record those redacted values plus local operation timings. `task_metrics` is outcome counting, not latency. |
| Polling/callback | Task query defaults to 20 QPS and can scale to 100. Callbacks avoid final polling in high-concurrency work, but documented callback delivery is typically 1–90 seconds after completion and can duplicate. [Non-real-time ASR guide](https://help.aliyun.com/en/model-studio/non-realtime-speech-recognition-user-guide) | Current one-task 2–5 s polling is reasonable to diagnose. Callback is not a proven single-task speed improvement. |
| Translation length | Qwen-MT accepts at most 8,192 input tokens; `qwen-mt-flash` context is 16k. [Qwen-MT guide](https://help.aliyun.com/en/model-studio/machine-translation), [model table](https://help.aliyun.com/en/model-studio/text-generation-model/) | The current 1,000-unit hard chunk limit is conservatively below the provider cap, although it is not the provider tokenizer. |
| Translation quota | Beijing `qwen-mt-flash`: 60 RPM and 35,000 TPM including input and output. Limits apply at Alibaba root-account scope across RAM users, workspaces, and API keys; RPS/TPS may also be enforced. [Rate limiting](https://help.aliyun.com/en/model-studio/rate-limit) | Current `translation_rpm=60` is already the public RPM ceiling. 120 RPM needs an explicit provider quota increase; concurrency alone cannot remove one-second start spacing. |
| TTS input/metering | Qwen3-TTS has a 600-character request maximum and returns `usage.characters`. [Qwen-TTS API](https://help.aliyun.com/en/model-studio/qwen-tts-api) | The current 250-character profile is valid; smaller chunks raise request count and need measured latency evidence. |

## Diagnostics to add before any ASR change

Keep all fields private and content-free: task-ID/request-ID hashes, task status,
parsed `submit_time`/`scheduled_time`/`end_time`, `task_metrics`, and
`usage.duration`. Measure local monotonic elapsed time and count for upload,
submit HTTP, every poll HTTP, intentional poll sleep, retry sleep, result
download, parsing, and durable artifact commit. Do not call the residual
“inference”: local poll time also includes network, result download, parsing,
and persistence.

Alibaba documents timestamp strings in examples but this research found no
explicit time-zone/format contract. Record the parser assumption. With the
current one-URL request, successful metrics should normally be `1/1/0`, but
persist actual values because task success is not a substitute for inspecting
individual subtask status. [ASR HTTP API](https://help.aliyun.com/en/model-studio/fun-asr-recorded-speech-recognition-http-api)

## Segmented-ASR feasibility gate

Only investigate after a diagnostic whole-file run shows a material provider-task
component that polling or the existing translation/TTS work cannot address.

| Requirement | Status | Evidence required before paid screening |
| --- | --- | --- |
| Input validity | Supported within documented per-file limits. | Validate duration, overlap, source-order merge, timestamp offsets, and no text duplication/loss. |
| Parallel task speed | **Unknown.** No published retained-model concurrent-task quota or service-time guarantee found. | Account-approved submission cap and offline recovery/retry proof. |
| Speaker continuity | **Unknown/not documented.** `speaker_id` is result-local. | Segment namespace or evaluated reconciliation; listening review must approve turn integrity. |
| Cost | ASR uses successful metered audio duration; detected speech content is billed. | Quantify overlap/reprocessing, which can increase billed seconds. |
| Recovery | Result URL lasts 24h; callback delivery can duplicate. | Durable task identity per segment before polling; no automatic re-submit after uncertain submission. |

## Cost formula and two-five-minute budget

Current Beijing public prices: retained ASR CNY 0.00022 per metered second;
`qwen-mt-flash` CNY 0.70/M input tokens and CNY 1.95/M output tokens; retained
Qwen3 TTS Flash CNY 0.80/10,000 input characters. [Model
pricing](https://help.aliyun.com/en/model-studio/model-pricing)

For metered ASR seconds `D`, MT input/output tokens `I`/`O`, and TTS characters
`C`:

```text
estimated_CNY = 0.00022 × D + 0.70 × I / 1,000,000
              + 1.95 × O / 1,000,000 + 0.80 × C / 10,000
```

For the fixed 300.010-second screening file, ASR alone is CNY `0.0660022` per
run and CNY `0.1320044` for baseline plus candidate. The total two-run formula
is:

```text
2 × (0.0660022 + 0.70 × I5 / 1,000,000
                 + 1.95 × O5 / 1,000,000 + 0.80 × C5 / 10,000)
```

`I5`, `O5`, and `C5` must come from observed usage or a stated conservative
estimate. Existing reported five-minute cost, CNY 0.302, is a useful planning
reference, so two such runs are about CNY 0.604; it is not a provider invoice or
a replacement for usage-derived approval. Free-quota eligibility, billing
rounding, account balance, and source-specific usage are **unknown** without
account access. Chunk pacing should not materially change content billing;
segmentation can increase ASR cost through overlap or duplicate work.

## Recommended approval boundary

1. Implement private timing/metric diagnostics and an offline launch-pacing
   replay only; retain models, Andre, whole-file ASR, and public-report privacy.
2. Do not raise translation RPM above 60 without confirmed account quota.
3. Confirm the baseline/candidate budget from the formula, then run the
   authorized five-minute screen. If its timing and listening gates pass,
   approve the separate 30–60-minute long-programme validation.
4. Treat segmented ASR as a separate plan/budget requiring durability, ordering,
   overlap-billing, timestamp, and speaker-identity evidence.

