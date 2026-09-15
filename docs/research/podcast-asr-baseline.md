# Instrumented five-minute cloud baseline

Measured 2026-09-15 for approved spec #16 and ticket #18, using implementation
commit `56fd5b2`. Exactly one fresh cold run was executed; no replacement or
confirmation run was made. Production succeeded; listening acceptance is pending.

## Identity and result

- Input: 300,010 ms; SHA-256 `fba898bc3ff430ef7a2a78516600e6db7d00c33afa4f8b24fda20a1c1387501f`.
- Profile: `alibaba-podcast-tts-throughput`; fingerprint `f3cccbfc222a22e695decb8104d6b77662009074380a48bdeb9582557832a247`.
- Retained Beijing ASR `qwen-audio-3.0-asr-flash-filetrans`, MT `qwen-mt-flash`, TTS `qwen3-tts-flash-2025-11-27`, Andre; unchanged scheduling/polling settings.
- Production wall time: **38.581 s**; launcher elapsed: 38.863 s (different boundary).
- Estimated, unconfirmed cost: **CNY 0.305339**; retries 0; cache hits 0.
- 12 translation and 14 TTS chunks committed in source order, one attempt each.

| Stage | Elapsed seconds |
| --- | ---: |
| Prepare | 0.898 |
| ASR | 16.186 |
| Translation | 11.452 |
| TTS | 18.483 |
| Finalize | 1.324 |

Translation and TTS overlap; stage totals must not be added as sequential work.
The previous accepted run was 38.471 s. The new baseline is 0.110 s slower
(about 0.29%). These two individual cloud observations do not establish a
performance regression or improvement attributable to diagnostics.

## ASR diagnostics

| Local operation | Seconds | Count |
| --- | ---: | ---: |
| Upload | 3.539 | 1 |
| Submit | 0.269 | 1 |
| Poll inclusive | 2.854 | 4 |
| Normal poll sleep | 9.515 | 3 |
| Retry sleep | 0 | 0 |
| Status query, within poll | 0.830 | 4 |
| Result download, within poll | 2.023 | 1 |
| Result parsing, within poll | 0.000 | 1 |

Parsing rounded below millisecond resolution; it does not imply zero work.
Top-level measured local operations total 16.177 s, leaving 0.009 s relative
to the ASR stage boundary for other work and rounding.

Optional provider timestamps yielded queue duration **0.073 s** and task duration
**7.472 s**. These provider durations overlap local polling/waiting and must not
be added to local totals. Task duration is not a measurement of pure inference.
The 9.515 s sleep total includes waiting while the provider works, so removing
that entire time is not an achievable speedup estimate.

## Verification and decision

Implementation verification: 219 podcast tests passed, compilation and diff
checks passed, and no new Ruff findings relative to the fixed base (8 existing
findings). Independent Standards and Spec reviews passed; Sol completed Spec
review after Terra encountered a quota limit.

Cloud validation checked cold input/profile identity, ordered committed artifacts
and receipts, fingerprints, and zero retries. The output is a 352.080 s MP3,
48 kHz mono, 64 kbps, 2,817,069 bytes. Complete FFmpeg decode passed. Local
beginning/middle/end 60-second listening samples were generated. Automated
checks do not establish translation fidelity or listening quality; the human
listening gate remains pending.

**Decision: retain the current ASR model, whole-file strategy and polling policy.**
This run establishes a usable decomposition, not a new speed optimization.
Upload and result transfer are measurable investigation targets, and polling
readiness could be studied further, but a single observation does not justify
a 10% speed claim or a segmentation change. Prior offline polling simulations
also did not establish that gain. A future optimization proposal should quantify
recoverable critical-path time before changing production behavior. No further
paid run or long-programme validation was performed.

## Private evidence

Local run directory: `output/experiments/issue18-baseline-20260915T095756Z`
in the user's main checkout. Numeric evidence comes from `production-report.json`,
`benchmark-summary.json`, and the `asr_timing` object in `run.private.json`.
The exclusive slot marker is `output/experiments/asr-baseline-issue18-slot.json`.
Private logs, transcripts, credentials and signed URLs are not published here.
