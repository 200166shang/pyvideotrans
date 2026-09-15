# Measure whole-file ASR latency with one cloud baseline

## Problem Statement

The listener wants faster Chinese listening editions without losing faithful
translation, speaker turns, the standard narrator voice, or recovery safety.
The accepted five-minute production run took 38.471 seconds, including an ASR
aggregate of 17.627 seconds. That aggregate cannot distinguish upload, provider
requests, result processing, intentional waiting, or server task time. Existing
evidence does not justify an implementation claiming another 10% improvement.

## Solution

Add private, content-free ASR diagnostics and perform exactly one instrumented
five-minute production run using the retained default profile. Produce a current
baseline and an evidence-based next-step decision. Diagnostics are not a speed
optimization; the broader 10% candidate goal remains unproven.

The listener approved this scope, the two-ticket split, test seams, and cloud
allowance together on 2026-09-15. No further routine confirmation is needed to
publish, implement, review, commit, or execute this specific baseline.

## User Stories

1. As a listener, I want total wall time measured so that I can assess actual production speed.
2. As a listener, I want audio upload distinguished from ASR work so that transfer time is not called inference.
3. As a listener, I want submission time and attempt counts recorded so that slow submissions are visible.
4. As a listener, I want status-query time separated from result download and parsing so that polling is interpretable.
5. As a listener, I want ordinary and retry sleeps distinguished so that waiting is not mistaken for provider execution.
6. As a listener, I want optional provider task-time differences recorded so that server queue and task execution can be examined separately.
7. As a listener, I want missing or malformed timing metadata to remain unavailable so that diagnostics do not invent zeros or fail a valid transcription.
8. As a listener, I want successful and failed operation durations counted so that error paths do not disappear from the evidence.
9. As a listener, I want a saved ASR task resumed without another submission so that diagnostics never duplicate potentially paid work.
10. As a listener, I want old runs and their profiles preserved so that adding measurements does not invalidate recovery.
11. As a listener, I want private text, credentials and signed URLs excluded from published evidence.
12. As a listener, I want one scoped cold cloud run with measured estimated cost, retries and integrity checks.
13. As a listener, I want the listening quality gate left to me so that automated checks do not claim subjective acceptance.
14. As a listener, I want a failed or inconclusive measurement reported honestly without an automatically repeated paid run.

## Implementation Decisions

- Retain Beijing ASR qwen-audio-3.0-asr-flash-filetrans, translation qwen-mt-flash, TTS qwen3-tts-flash-2025-11-27 and Andre. Preserve all current profile values/fingerprints, whole-file input, output format and polling policy.
- Extend only private diagnostic state. Keep manifest v2 and strict public trace/report schemas unchanged. Do not add ASR subevents to the existing performance trace.
- Reuse the prepared private counters for upload, submit, inclusive poll and ordinary/retry sleep. Add measured status-query, result-download and parsing subphases at the ASR client boundary; make the relationship to inclusive poll explicit so nested times are not summed.
- Measure operations using injectable monotonic clocks, including calls that raise. Accumulate in memory and reuse existing persistence boundaries, avoiding synchronous disk writes for every timing observation.
- Parse optional provider submission/scheduled/end metadata defensively. Only valid ordered timestamps from a compatible single provider time domain may yield queue/task-duration differences. Missing/malformed/out-of-order/mixed time-domain data yields unavailable diagnostics, never a recognition failure. Do not subtract provider timestamps from the local monotonic clock or claim provider task time is pure model inference.
- Diagnostic fields contain numeric counts/durations or explicit unavailable values only. Do not persist signed result URLs, transcript excerpts or credentials as diagnostics. Existing private transcription artifacts keep their established role.
- Preserve upload, durable uncertain marker, single submission, durable task ID, polling, artifact and manifest order. Completed ASR reuse must not fabricate timing data. Old partial runs receive new diagnostics only for newly observed work; document partial coverage across resume/crash.
- Execute exactly one new fixed five-minute cold production run after checks and review, in a fresh private directory. The slot is consumed by its first cloud submission, even if it fails. No replacement, confirmation, candidate or long-programme run is authorized by this spec.
- Estimated planning cost is approximately CNY 0.30 with a CNY 1 allowance. This is not a provider-enforced hard cap. Preserve existing safe no-success retries and never automatically resend an uncertain submission.

## Testing Decisions

Use the existing coordinator and ASR-client seams with fake transports and
injected clocks. Exercise observable persisted diagnostics and output/recovery
behavior rather than helper implementation details. Cover success, failed
operations, ordinary/retry sleeps, status versus result processing, missing and
invalid server metadata, old-state resume, uncertain submission, and unchanged
completed-ASR reuse. Preserve existing profile, public trace/report, sealed
chunking, fail/drain and artifact integrity tests. Run targeted tests during
implementation, the full podcast suite at completion, compilation/diff checks,
and compare touched-file Ruff findings to the fixed base revision.

Independent Standards and Spec reviews use fixed revision
3667370852059f8bf95174a2e02d6114aa744d94, including the prepared uncommitted
diagnostic work. Fix and reverify actionable findings before the cloud run.
After cloud execution, verify cold identity/profile, retries, ordered artifacts,
MP3 format and complete decode; produce beginning/middle/end review audio if
the existing workflow supports it. Listening acceptance remains pending until
the listener explicitly supplies it.

## Out of Scope

Segmented ASR, model/voice changes, quota increases, faster polling, performance
profile promotion, a new scheduler, manifest migration, extra paid runs, and
representative long-programme validation. Publishing this spec does not
authorize a greater-than-60-RPM translation setting.

## Further Notes

Work in the isolated performance worktree; preserve the user's older dirty main
checkout and private outputs. The fork's GitHub Issues are authoritative.
Root Astra retains planning context; each approved implementation ticket starts
in a fresh agent. Terra owns routine implementation; Sol handles any evidenced
state/compatibility difficulty. Luna and Terra review Standards and Spec
independently. The measured result determines the next proposal; no 10% speed
claim follows from this diagnostic baseline alone.
