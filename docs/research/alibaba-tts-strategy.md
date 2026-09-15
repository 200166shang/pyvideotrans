# Alibaba TTS throughput and multi-speaker strategy

Research date: 2026-09-15

## Decision

Use Alibaba Model Studio's **`qwen3-tts-flash-2025-11-27` in China (Beijing), through the non-realtime HTTP API with SSE streaming enabled**. Treat the stable alias `qwen3-tts-flash` as equivalent today, but pin the dated snapshot for benchmarks and production runs until a newer version passes the listening quality gate. Alibaba currently documents the stable alias as equivalent to this snapshot; both cost CNY 0.8 per 10,000 billable characters and have a 180 RPM limit. The realtime counterpart costs CNY 1.0 per 10,000 characters and has the same 180 RPM limit, so WebSocket realtime does not buy more documented request quota for this offline workload. [Alibaba pricing](https://help.aliyun.com/zh/model-studio/model-pricing) · [Alibaba rate limits](https://help.aliyun.com/zh/model-studio/rate-limit)

Keep the system voices already exposed by pyvideotrans. Do not use voice cloning, voice design, or the instruct model in the first production configuration. `qwen3-tts-flash` supplies Mandarin system voices, while the repository already maps those names into its Qwen adapter. [Alibaba model/feature matrix](https://help.aliyun.com/zh/model-studio/tts-model) · [repository voice map](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/voicejson/qwen3tts.json#L1-L50)

This decision is based on documentation and source inspection only. No paid API was called. The initial concurrency and chunk-size values below are safe starting points to validate on the fixed benchmark sample, not measured throughput results.

## Why this model and mode

| Candidate | Documented price in Beijing | Documented limit | Fit |
| --- | ---: | ---: | --- |
| `qwen3-tts-flash-2025-11-27` HTTP | CNY 0.8 / 10k billable characters | 180 RPM | **Selected:** system voices, lowest relevant price, reproducible snapshot |
| `qwen3-tts-flash-realtime-2025-11-27` WebSocket | CNY 1.0 / 10k billable characters | 180 RPM | Same request quota at 25% higher list price; interactive latency is not the goal |
| `qwen-audio-3.0-tts-flash` | CNY 1.0 / 10k billable characters | 3 RPS | Fast first packet and system voices, but no documented quota or cost advantage for this batch |
| `cosyvoice-v3.5-flash` | CNY 0.8 / 10k billable characters | 3 RPS | No system voices; it requires prior voice cloning or design |
| `cosyvoice-v3-flash` | CNY 1.0 / 10k billable characters | 3 RPS | Has Chinese system voices, but costs more than selected model |

The prices and limits are from Alibaba's current [pricing](https://help.aliyun.com/zh/model-studio/model-pricing) and [rate-limit](https://help.aliyun.com/zh/model-studio/rate-limit) tables. Alibaba describes realtime TTS as a low-first-packet-latency interface and explicitly points audiobook/courseware batch scenarios to non-realtime synthesis. [Realtime TTS guide](https://help.aliyun.com/zh/model-studio/realtime-tts-user-guide)

Use `stream=True` on the non-realtime `MultiModalConversation` call. Alibaba's Qwen-TTS API streams Base64 audio in intermediate `audio.data` chunks and supplies the complete file URL only on the final chunk. Consuming `audio.data` directly removes the adapter's second HTTP download from the critical path; it is still the non-realtime model, not the `-realtime` WebSocket model. [Qwen-TTS API](https://help.aliyun.com/zh/model-studio/qwen-tts-api)

## Request granularity

One API request should represent **one contiguous speaker turn**, not one subtitle row:

1. Coalesce adjacent translated rows only while their reliable `speaker_id` is the same.
2. Preserve sentence punctuation and insert a natural sentence separator when joining rows.
3. Use a soft ceiling of **500 Unicode characters after final text normalization**. Split at sentence punctuation before the API's hard maximum of 600 characters. Never split in the middle of a number, Latin identifier, or sentence unless no legal boundary exists. Alibaba documents a 600-character maximum for Qwen3-TTS input and recommends specifying `language_type="Chinese"` for known single-language text. [Qwen-TTS API request fields](https://help.aliyun.com/zh/model-studio/qwen-tts-api)
4. Never merge across a speaker boundary. Give every chunk a monotonically increasing sequence number so concurrent completions can be assembled in dialogue order.

This removes repeated request setup and the repository's per-row delay without creating giant retry units. The current production path creates one queue entry and WAV target per subtitle row, and the Qwen adapter makes one non-streaming API call followed by an OSS download and conversion for each entry. [queue construction](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/task/_stage_dubbing.py#L54-L91) · [current Qwen adapter](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/tts/_qwentts.py#L39-L69)

## Async scheduling and bounded concurrency

Use one asynchronous FIFO work queue for the production run:

- **Start with a semaphore of 4 in-flight requests.** This is deliberately bounded and should be the only Qwen-specific concurrency control; do not submit the entire programme to an unbounded executor.
- Put a token bucket before the semaphore, in that order, with a working limit of **150 RPM / 2.5 request starts per second** and `initial_tokens=0`. The published ceiling is 180 RPM and Alibaba warns that it may be enforced as 3 RPS and that sudden bursts can be throttled. The margin also allows account-shared traffic outside this process.
- Every retry must reacquire a rate token. Warm from zero and pace starts evenly; do not launch four requests simultaneously at startup.
- Make `4`, `150 RPM`, and the 500-character soft ceiling visible in the production report. Benchmark only this starting configuration first. Tune concurrency upward only if TTS remains the bottleneck, there are no 429s/timeouts, and the listening output is accepted.

Alibaba's rate-limit guidance prescribes an RPM token bucket followed by a concurrency semaphore, smooth startup, and exponential backoff with jitter. Limits are aggregated at the root-account level across RAM users, workspaces, and API keys. [Alibaba rate-limiting best practices](https://help.aliyun.com/en/model-studio/rate-limiting-best-practices) · [Alibaba rate-limit rules](https://help.aliyun.com/zh/model-studio/rate-limit)

The repository already has a thread-pool path, but its defaults are `dubbing_thread=1`, `dubbing_wait=1`, and only one retry. In serial mode it sleeps after every subtitle entry. Raising the global thread count alone is insufficient because that executor has no provider-specific start-rate limiter and submits all rows immediately. [current defaults](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/configure/_app_settings.py#L168-L205) · [current TTS executor](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/tts/_base.py#L143-L200)

## Rate-limit and retry behavior

- Retry only HTTP 429, transient 5xx responses, connection resets, and timeouts.
- Use random exponential backoff, minimum 1 second, maximum 60 seconds, at most five retries after the first attempt.
- On any 429, stop new request starts for the whole production run for 60 seconds, reduce the in-flight limit by half (minimum 1), then resume with an empty token bucket. Alibaba says throttling usually clears within one minute and recommends smoothing plus exponential backoff rather than fixed waits. [Alibaba rate-limit FAQ](https://help.aliyun.com/zh/model-studio/rate-limit) · [Alibaba error codes](https://help.aliyun.com/zh/model-studio/error-code)
- Do not retry authentication, invalid model/voice, input-length, or exhausted-quota errors. Surface these as actionable failures.
- Do not switch models automatically. A model change can alter voice and listening quality, invalidating a partially synthesized programme. Persist completed chunks and resume the failed chunk with the same model and voice.
- Record every attempt's request ID, start/end/first-audio timing, status, retry reason, and whether usage was returned. A timeout after server acceptance may have unknown billing; mark it `cost_unconfirmed` rather than pretending it was free or charging it twice locally.

The existing adapter uses a fixed two-second retry and the global retry count, so it does not smooth concurrent retries and does not distinguish throttling from permanent request errors. [current retry decorator](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/tts/_qwentts.py#L39-L40)

## Stable multi-speaker voice assignment

Speaker preservation is a deterministic mapping problem, not voice cloning:

- If diarization is accepted as reliable, assign voices by `speaker_id` and persist the mapping in the production-run manifest. Reuse it for every turn, retry, cache lookup, and resume.
- If reliable gender metadata exists, select from the corresponding ordered pool. Suggested Mandarin pools are female `[Cherry, Maia, Serena, Elias]` and male `[Ethan, Neil, Moon, Kai]`.
- If gender is unknown, assign by first appearance from `[Ethan, Cherry, Neil, Maia, Moon, Serena, Kai, Elias]`. The purpose is audible separation; it is not an assertion about the original speaker's gender.
- If the whole diarization result fails the upstream reliability gate, collapse the complete programme to the single fallback voice **`Cherry`**. Do not alternate voices based on subtitle row number.
- If one persisted voice becomes invalid, fail validation before paid synthesis and ask for/remap that voice once. During a production run, never silently change a speaker's voice.

Alibaba documents these as Mandarin system voices and describes `Cherry`, `Maia`, `Serena`, and `Elias` as female voices and `Ethan`, `Neil`, `Moon`, and `Kai` as male voices. [Qwen-TTS voice list](https://help.aliyun.com/zh/model-studio/qwen-tts-voice-list) The repository already supports per-line voice selection and includes the same system voice IDs. [per-line roles](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/task/_stage_dubbing.py#L54-L82) · [repository voice map](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/voicejson/qwen3tts.json#L1-L50)

## MP3 assembly

Keep streamed chunk audio in one lossless intermediate specification (24 kHz, mono, 16-bit PCM/WAV), then assemble chunks by sequence number and encode **once** to the agreed mono 64 kbps MP3. Concurrent requests may finish out of order, so each completed chunk must be atomically finalized before it becomes resumable. Add only the natural pause encoded by punctuation, plus a small explicit turn pause if the listening quality gate requires it; do not restore the original video's full subtitle gaps.

This matches the repository's existing WAV-segment architecture and final ordered FFmpeg concatenation. The repository currently creates WAV cache entries, concatenates matching streams into a final WAV, and only then converts the final artifact to its requested output extension. [WAV cache entries](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/task/_stage_dubbing.py#L63-L84) · [ordered audio concat](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/task/_rate.py#L620-L672) · [final output conversion](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/task/dubbing.py#L272-L285)

Implementation must add model, model snapshot, region, language, voice, normalized-text hash, and audio specification to the cache key. The current key includes provider type and voice but not the selected Qwen model or region; reusing that cache across model changes would invalidate both listening comparisons and benchmark timings. [current cache key](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/task/_stage_dubbing.py#L54-L87)

## Cost accounting

Use Alibaba's returned `usage.characters`, not Python `len(text)`, as the authoritative billable unit. Alibaba counts each Han character as two characters and punctuation, spaces, digits, and Latin letters as one. The Qwen3 response reports `usage.characters`. [Alibaba character-count rules](https://help.aliyun.com/zh/model-studio/model-pricing) · [Qwen-TTS response schema](https://help.aliyun.com/zh/model-studio/qwen-tts-api)

For the selected Beijing snapshot:

```text
confirmed_tts_cost_cny = sum(successful_attempt.usage.characters) * 0.8 / 10_000
tts_cost_per_source_hour_cny = confirmed_tts_cost_cny * 3_600 / source_duration_seconds
```

The production report should contain:

- model ID/snapshot, region, unit price, and source duration;
- submitted Unicode length and returned billable characters per chunk;
- confirmed TTS cost, unconfirmed-cost attempt count, cache-hit characters, and retry count;
- TTS wall-clock time, queue wait, API time to first audio, stream completion time, assembly time, and real-time factor;
- the persisted `speaker_id -> voice` mapping and whether diarization used reliable multi-speaker mode or the single-voice fallback.

Cached chunks have zero new API cost for that production run but must retain their original usage metadata for auditability. Do not use temporary free quota to report the algorithmic unit cost; report list-price cost and, separately, the amount actually billed if available.

## Benchmark checks before implementation is accepted

Run the fixed five-minute sample once with the current adapter and once with this configuration. The candidate is acceptable only if:

1. every source turn is present and ordered, with no truncated or duplicated streamed audio;
2. the listener accepts voice naturalness and speaker-turn integrity;
3. the report proves which improvement came from turn coalescing, zero per-row delay, bounded concurrency, removal of the second download, and single final MP3 encode;
4. no 429 is hidden by retries, and all retry/cost uncertainty is visible;
5. a resume run skips already finalized chunks and preserves the same voice mapping.

Do not claim a synthesis speedup from this document alone. The source inspection identifies likely avoidable latency, but only the benchmark sample can quantify it.
