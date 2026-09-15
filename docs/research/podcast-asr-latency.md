# ASR latency investigation — 2026-09-15

## Outcome

Retain the existing ASR model and polling policy. Add content-free private timing diagnostics for the next normal production run, rather than infer provider latency from the historical 17.627-second ASR aggregate. No new cloud run is part of this investigation.

## What the current evidence establishes

`PodcastCoordinator._asr` times a serial chain:

1. Mark stage active and save the manifest.
2. Upload prepared audio, unless a task ID is already saved.
3. Persist the submission-uncertain boundary, submit once, persist the returned task ID.
4. Poll for completion, sleeping between incomplete responses or transient errors.
5. On success, `AlibabaWholeFileAsrClient.poll` also downloads result documents and parses segments before returning.
6. Write the ASR artifact and commit its receipt in the manifest.

The accepted run's 17.627 seconds covers this chain, not inference alone. Existing report/trace fields contain only the aggregate; neither upload duration, poll count nor poll-response timing was saved. The split cannot be recovered faithfully from that report. The synchronous poll API also combines status-query time with download/parse time.

## Offline polling experiment

The script uses the actual coordinator `_wait_for_asr` with a virtual clock and fake status responses. It compares the current 2, 3, 4.5, 5, 5… second sleeps with constant 2-second sleeps. It runs 3,000 hypothetical readiness times from 0.01 to 30 seconds, each under three fixed request-latency assumptions. Status is sampled at request start; response arrives after that assumed latency. There are no uploads, submissions, downloads, retries or network calls.

| Assumed query latency | Mean detection saving with fixed 2 s | Range of saving | Cases faster / slower | Mean polls, current → fixed |
| --- | ---: | ---: | ---: | ---: |
| 0 ms | 1.300 s | −1.500 to 4.500 s | 2,150 / 650 | 5.517 → 9.000 |
| 100 ms | 1.255 s | −1.900 to 5.000 s | 2,160 / 630 | 5.430 → 8.650 |
| 500 ms | 1.342 s | −1.500 to 5.000 s | 1,950 / 550 | 5.150 → 7.500 |

These means describe an evenly spaced scenario grid, not an observed completion-time distribution or predicted user speedup. A shorter sleep interval changes query phase and therefore does not guarantee earlier detection. For example, with zero query latency, a task ready at 4.99 seconds is detected at 5 seconds by the current policy but at 6 seconds by fixed 2-second polling; a task ready at 9.51 seconds is detected at 14.5 versus 10 seconds.

The official [non-real-time ASR guide](https://help.aliyun.com/zh/model-studio/non-realtime-speech-recognition-user-guide), checked 2026-09-15, recommends polling intervals such as 2–5 seconds. It also documents completion callbacks, but reports delivery latency around 1–90 seconds. Neither guidance establishes a speed advantage for changing this personal pipeline's policy. Callback infrastructure is outside this slice.

## Diagnostic implementation contract

- Add only numeric ASR phase durations and invocation counts in private run state; record durations even when an operation raises.
- Separate upload, submit, inclusive poll/result processing, normal poll sleep and retry sleep. The inclusive poll measurement does **not** distinguish provider status HTTP time from result download/parse; it must never be labeled inference time.
- Use the coordinator's injected monotonic clock. Accumulate in memory and reuse existing persistence boundaries, avoiding a new synchronous disk write per measurement.
- Retain upload → uncertain marker → submit → saved task ID ordering. Resume from a task ID polls it without uploading or submitting again. Completed ASR stays on its existing artifact reuse path.
- Preserve the strict public trace and report schemas and profile fingerprint. Do not add nested ASR events to the trace: existing replay consumers expect one outer ASR start/commit pair.
- Diagnostics are best effort across abrupt process termination: since timings are not separately checkpointed after each operation, some work since the last existing save can be absent. They are active execution measurements, not billing evidence or a complete wall-clock decomposition across crashes.

New runs store `asr_timing` in `run.private.json`, with paired `_elapsed_ms` and `_count` fields for `upload`, `submit`, `poll_inclusive`, `poll_sleep`, and `poll_retry_sleep`. An old resumed run receives these fields lazily when an ASR operation executes; a completed old ASR stage does not acquire fabricated measurements. Local artifact and manifest work remains in the outer stage total rather than a separately measured phase. Per-operation millisecond rounding means subtracting phase sums from that total is only an approximate local/uninstrumented remainder.

To inspect only the diagnostic fields, without printing the private source path or other run content:

```sh
python - /path/to/run/run.private.json <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    state = json.load(source)
print(json.dumps(state.get("asr_timing", {}), indent=2))
PY
```

## Next decision from a measured run

First compare upload and submit with the inclusive polling and actual sleep totals. Large upload time points toward file transfer; large sleep time justifies examining query timing, but total sleep includes time while the provider is genuinely busy and is not automatically removable overhead. Large inclusive poll time needs finer transport/download instrumentation before changing the model. Only then evaluate segmented ASR, which would additionally need stable speaker identity, source ordering and recovery semantics across segments.

No new paid experiment is authorized or run here. Existing completed runs cannot be retroactively populated with these measurements.

## Reproduction

```sh
PYTHONPATH=. python scripts/benchmarks/asr_polling.py \
  --output /tmp/podcast-asr-polling-results.json
```

The script asserts known cadence boundaries and executes 18,000 simulated polling loops (3,000 readiness times × 3 latency assumptions × 2 policies). Content-free results: [podcast-asr-polling-results.json](podcast-asr-polling-results.json).

## Verification and review

### Completed diagnostic implementation (issue #17)

The approved follow-up now adds `status_query`, `result_download`, and
`result_parse` count/duration fields inside the existing inclusive poll time.
Optional `provider_queue_elapsed_ms` and `provider_task_elapsed_ms` values are
derived only from ordered compatible server timestamps; unusable metadata stays
null. Those server durations are not additive local phases or pure inference
measurements. The full pipeline test verifies actual client diagnostics reach
private state without entering the public report.

All **219 podcast tests passed** in 7.12 seconds. Compilation/diff checks passed;
the 8 baseline Ruff findings are unchanged. Luna Standards review and Sol Spec/
compatibility review both found zero blocking issues against `3667370`. Sol
replaced the Terra implementation agent for the compatibility review after Terra
hit its usage limit; root completed the remaining tests and integration. The
single cloud baseline belongs to dependent issue #18, not this offline evidence.

### Earlier coarse measurement verification

- All **208 podcast tests passed** (7.49 seconds), including new fake-clock coverage for successful phase accumulation, failed submit persistence, retry sleep, task-ID reuse and byte-identical completed-ASR private state.
- Repeating the 18,000-case polling simulation after instrumentation produced byte-identical results, confirming the simulated policy behavior is unchanged.
- Compilation and diff checks passed. Ruff findings were compared with fixed revision `3667370`: all 8 existing findings in the touched production/test files are unchanged, with no new finding. The new benchmark script passes Ruff.
- Standards review found no production failure/recovery defect. An intermediate observation about unused test helpers was checked against the final tests, which use those helpers. Root Spec review found the diagnostic contract satisfied; inclusive poll granularity and interrupted-run timing limits are explicitly retained above.
- Work remains in the isolated `codex/podcast-next-performance` worktree; no cloud run, profile change or merge was performed.
