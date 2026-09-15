# Alibaba Cloud ASR strategy for long English programmes

Research date: 2026-09-15  
Scope: official Alibaba Cloud documentation and repository source at commit [`3eb3b2f`](https://github.com/200166shang/pyvideotrans/tree/3eb3b2f3ab11013632d5d35329620c861ce8df7c). No paid API was called.

## Recommendation

Use Alibaba Cloud Model Studio's **Qwen-Audio-3.0-ASR-Flash-Filetrans** model (`qwen-audio-3.0-asr-flash-filetrans`) through the **DashScope asynchronous file-transcription HTTP API**, submitting one complete one- to two-hour English source programme per task.

For the first implementation:

1. Upload the prepared mono audio to a URL that Model Studio can read. For this personal, low-concurrency tool, DashScope temporary upload is an acceptable first integration; retain the local source so an expired upload can be recreated. Prefer durable OSS storage if the workflow later becomes production-like.
2. Submit one asynchronous task with `language_hints: ["en"]`, `channel_id: [0]`, and `diarization_enabled: true`. Supply `speaker_count` only when the user knows it; otherwise let the service infer it. Optional programme names and technical vocabulary can be sent as context or hotwords.
3. Persist the input fingerprint, model and parameters, source URL expiry, `task_id`, request ID, and submission time immediately. On restart, query the saved task rather than submitting again.
4. Poll every 2–5 seconds with bounded exponential backoff. For this single-user workflow, EventBridge adds unnecessary infrastructure; it is intended to avoid query-rate pressure in high-concurrency systems.
5. On success, download the result JSON immediately, because the result URL and task queryability expire after 24 hours. Persist the raw JSON before converting sentence timestamps and `speaker_id` values to the repository's subtitle and speaker artifacts.
6. Do not pre-split the programme into the repository's current short VAD clips. Whole-file submission preserves long context and stable speaker labels and removes hundreds or thousands of sequential HTTP round trips. Consider coarse parallel chunks only if a measured full-programme benchmark later proves that server processing time, rather than client request overhead, is the remaining bottleneck.

This is a recommendation to benchmark, not a measured speed claim. Alibaba Cloud publishes request-rate limits but no file-transcription latency SLA, real-time factor, or active-task concurrency for this model. The first production run must record upload, queue, service, result-download, and parse times separately.

## Why this API and model

Alibaba Cloud currently identifies `qwen-audio-3.0-asr-flash-filetrans` as its recommended non-real-time model. It is designed for complete audio/video files, supports context and hotwords, and supports speaker diarization. Its documented input ceiling is 2 GB and 12 hours; with diarization enabled, Alibaba recommends no more than two hours. English is explicitly supported. These properties directly match a one- to two-hour English source programme and the requirement to preserve speaker turns where possible. [Model selection and audio specifications](https://help.aliyun.com/zh/model-studio/asr-model) · [Model card](https://help.aliyun.com/zh/model-studio/qwen-audio-3-0-asr-flash-filetrans)

The API is asynchronous by design: a submission returns a `task_id` in `PENDING`, and a separate endpoint reports `PENDING`, `RUNNING`, `SUCCEEDED`, or `FAILED`. This avoids holding one HTTP request open for a long transcription and gives the local production run a durable resume point. The completed response includes submit, scheduled, and end times, plus a `usage.duration` value suitable for the production report. [HTTP API reference](https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-http-api) · [Asynchronous task states](https://help.aliyun.com/zh/model-studio/manage-asynchronous-tasks)

The result contains paragraph text, sentence and word start/end timestamps in milliseconds, and `speaker_id` on each sentence when diarization is enabled. Sentence timestamps are sufficient for the translation/TTS pipeline; word timestamps should be retained in the raw result but need not enter `SrtItem` initially. [Result schema](https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-http-api#section-fbe-3v8-t1c) · [Timestamp and diarization behavior](https://help.aliyun.com/zh/model-studio/non-realtime-speech-recognition-user-guide)

### Alternatives within Alibaba Cloud

| Option | Long-file fit | Speaker/timestamps | Beijing list price | Decision |
| --- | --- | --- | ---: | --- |
| `qwen-audio-3.0-asr-flash-filetrans` | Async HTTP; 1 URL/task; 12 h, 2 GB | Sentence/word timestamps; diarization for mono audio, recommended at ≤2 h | ¥0.00022/s = **¥0.792/source-hour** | **Default**: current recommended model and strongest feature fit. |
| `fun-asr` | Same async API and 12 h/2 GB envelope | Sentence/word timestamps and diarization | ¥0.00022/s = ¥0.792/source-hour | Valid fallback, but no advantage for this English-only use case is documented. |
| `qwen3-asr-flash-filetrans` | Async HTTP; 12 h, 2 GB | Sentence timestamps by default, optional word timestamps; **no diarization** | ¥0.00022/s = ¥0.792/source-hour | Reject for the initial route because speaker turns matter. |
| `paraformer-v2` | Async HTTP; 12 h, 2 GB | Sentence/word timestamps, optional timestamp alignment and diarization | ¥0.00008/s = **¥0.288/source-hour** | Keep as the cost-control candidate if listening quality passes; Alibaba describes Paraformer as an earlier generation and recommends migration to Fun-ASR or Qwen-ASR where possible. |
| Real-time WebSocket ASR | Unlimited-duration stream | Current recommended Qwen streaming model has no diarization | Qwen Audio streaming: ¥0.00033/s = ¥1.188/source-hour | Reject: the offline file API is purpose-built for this source and is cheaper, resumable by task ID, and speaker-aware. |

Prices are computed from Alibaba Cloud's current per-second Beijing prices; model output is not charged. The Qwen/Fun figure is ¥1.584 for a two-hour source programme, while Paraformer is ¥0.576. These are ASR-only estimates and exclude storage/network costs. The production report should use the returned billed duration and actual bill rather than assuming container duration equals billed duration. [Model Studio pricing](https://help.aliyun.com/zh/model-studio/model-pricing) · [Paraformer metering](https://help.aliyun.com/zh/isi/developer-reference/metering-and-billing)

The default leaves about ¥1.208 of the preferred ¥2/source-hour production cost for translation and TTS. It therefore fits the preference but makes downstream cost measurement important; Paraformer provides a cheaper fallback if the complete production run exceeds that preference.

## Limits, quotas, and request behavior

- A request accepts one publicly accessible HTTP/HTTPS URL. The REST API can also resolve an `oss://` temporary URL, while direct local-file bytes are not the long-file input contract. The model accepts common audio/video containers, arbitrary sample rates, up to 2 GB and 12 hours. [Audio specifications](https://help.aliyun.com/zh/model-studio/asr-model) · [HTTP request schema](https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-http-api)
- The submission must include `X-DashScope-Async: enable`. Alibaba recommends workspace-specific regional domains for performance and stability. API keys and endpoints are region-specific. [HTTP API endpoints and headers](https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-http-api)
- `qwen-audio-3.0-asr-flash-filetrans` and `fun-asr` are limited to 600 submissions per minute; `qwen3-asr-flash-filetrans` to 100 RPM; and `paraformer-v2` to 1,200 RPM. These are request-rate limits, not proof of transcription throughput. The published table does not state an active file-job concurrency limit for these models. [Model Studio rate limits](https://help.aliyun.com/zh/model-studio/rate-limit)
- Task polling is limited to 20 QPS by default; the production guide says it can be expanded to 100 QPS and recommends 2–5 second polling or EventBridge for high concurrency. A single personal production run is far below that ceiling. [Production guidance](https://help.aliyun.com/zh/model-studio/non-realtime-speech-recognition-user-guide#section-y0z-lck-8m2) · [Async task management](https://help.aliyun.com/zh/model-studio/manage-asynchronous-tasks)
- Temporary upload URLs last 48 hours. The upload-policy endpoint is limited to 100 QPS and returns a model-dependent maximum upload size, which the client must check before uploading. Alibaba says temporary upload is not for production/high-concurrency use and recommends OSS there. [Temporary-file upload](https://help.aliyun.com/zh/model-studio/get-temporary-file-url/)
- A successful transcription URL is valid for 24 hours; after expiry, the task can no longer be queried and the old result URL cannot be downloaded. Download-and-persist is therefore part of successful completion, not optional cleanup. [Task result lifetime](https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-http-api#section-aqe-8ur-1ze)
- Diarization works only for mono audio. `speaker_count` is a hint from 2 to 100, not a guarantee. At one to two hours, the source is inside Alibaba's recommended diarization envelope, but two hours is the upper edge and failures/timeouts remain possible. [Diarization parameters](https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-http-api#section-5qx-3ru-od8)

## Fit with the current repository

The existing Alibaba adapter already reuses the desired API key and workspace settings, but its execution strategy is the wrong shape for long files:

- `Qwen3ASRRecogn.__post_init__` always invokes `cut_audio()`. The common cutter runs VAD and emits clips capped at 25 seconds; the repository default is 5 seconds. [`_qwen3asr.py`](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/recognition/_qwen3asr.py#L18-L29) · [`_base.py`](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/recognition/_base.py#L150-L210) · [`_app_settings.py`](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/configure/_app_settings.py#L160-L162)
- Both current Alibaba paths then process those clips sequentially. The newer short-file path Base64-encodes each WAV clip and performs one synchronous POST per clip. Optional `asr_wait` adds a delay after every successful clip. [`_qwen3asr.py`](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/recognition/_qwen3asr.py#L31-L79) · [`_qwen3asr.py`](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/recognition/_qwen3asr.py#L82-L160)
- The selectable model list currently contains only short-file models, not any `*-filetrans` model. [`contants.py`](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/configure/contants.py#L127)
- The generic recognition stage does reuse an already completed source SRT, but it has no persisted cloud task state between submission and SRT creation. [`_stage_recogn.py`](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/task/_stage_recogn.py#L22-L35)

The implementation should therefore add a dedicated long-file execution path (preferably a separate adapter class/module registered under the existing Alibaba provider) rather than adding another condition to the synchronous clip loop. It can reuse `qwenmt_key`, the workspace/base URL configuration, `BaseRecogn.run()`, and the normal `List[SrtItem]` return contract. Convert every returned sentence to an `SrtItem`, and write a same-length `speaker.json` list such as `spk0`, `spk1`, matching existing diarizing adapters so the later diarization stage sees the file and skips local recomputation. The repository already uses this integration pattern for cloud ASR providers. [`SrtItem`](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/task/taskcfg.py#L68-L94) · [OpenAI diarization adapter](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/recognition/_openairecognapi.py#L114-L160) · [Diarization short-circuit](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/task/_stage_diariz.py#L5-L12)

The long-file adapter also needs a small persisted state record in `cache_folder`, separate from the final SRT, with atomic state transitions such as `uploaded`, `submitted`, and `result_saved`. A network error while polling must never trigger a new paid submission while a saved nonterminal `task_id` exists. Because Alibaba documents no idempotency key for task submission, duplicate prevention is the client's responsibility; this is an inference from the API contract, not an Alibaba guarantee.

## Minimal benchmark and acceptance

No wall-clock or transcription-quality result can be claimed from documentation alone, and this research deliberately did not call paid APIs. After implementation, run the existing fixed five-minute benchmark sample once with the current short-clip Alibaba adapter and once with the file-transcription path. If the new path passes the listening quality gate, confirm once on a representative full programme before making it the default.

Record:

- local preprocessing and upload seconds;
- queue seconds (`scheduled_time - submit_time`);
- service seconds (`end_time - scheduled_time`);
- result download/parse seconds and end-to-end ASR seconds;
- real-time factor (`end-to-end ASR seconds / source seconds`);
- request count, retries, resumed-task count, and duplicate submissions;
- returned `usage.duration`, calculated model cost, and actual billed cost;
- sentence count, empty/gap/overlap checks, observed speakers, and the listener's acceptance.

Do not claim that one whole-file task is faster until this comparison passes. The defensible expectation is narrower: it eliminates the current client's per-clip VAD export, Base64 encoding, sequential request/response latency, and optional per-clip sleep. Alibaba's server-side queue and model processing time remain empirical.

## Decision boundary

Adopt `qwen-audio-3.0-asr-flash-filetrans` as the implementation target. Keep `paraformer-v2` as a later, same-account cost-control experiment only if total production cost cannot stay near the preferred ¥2/source-hour or if the recommended model's measured wall-clock performance is unacceptable. Do not add a second cloud provider, do not use real-time ASR for offline programmes, and do not parallelize fine-grained clips before the whole-file baseline is measured.
