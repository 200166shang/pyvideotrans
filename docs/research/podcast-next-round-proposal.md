# Next performance round — approved diagnostic baseline

The listener approved this proposal on 2026-09-15. Authoritative specification:
[issue #16](https://github.com/200166shang/pyvideotrans/issues/16). The approved
implementation tickets are [#17](https://github.com/200166shang/pyvideotrans/issues/17)
and [#18](https://github.com/200166shang/pyvideotrans/issues/18), with #18 blocked
by #17. The cloud allowance below applies to exactly one new five-minute run.

## Confirmed intent

The listener confirmed five-minute screening, a candidate goal of at least 10%
lower production wall time, retained Beijing models and Andre, faithful text and
speaker-turn quality, and eventual representative long-programme validation.
Research may examine segmented ASR. The listener requested one combined plan
confirmation before specification, tickets and fresh-agent implementation.

## Research decision

Do not yet commit to a performance candidate. The exact retained translation
model is already at its public 60 RPM ceiling. An exploratory coupled replay
using real scheduler/virtual-clock code reproduced the latest run within 4.6 ms,
and predicted 35.978 seconds at 120 RPM versus 38.471 observed (6.48% lower), but
that setting requires a provider-approved quota increase. It is conditional
headroom evidence, not an available production setting or a performance claim.

The replay preserved observed request service durations, TTS finalization tail,
and the highest contiguous translated prefix present at each observed TTS queue
event (which includes the lookahead sometimes needed to seal a TTS chunk). Those
frozen observations are not causal predictions. The scratch experiment is not
the earlier concurrency-4-only replay and does not establish a new calibration
contract for that tool.

Fixed 2-second ASR polling remains a possible experiment, not a proven 10%
improvement: the offline readiness grid averages only about 1.3 seconds and some
readiness phases regress. Segmentation changes boundary context, speaker-label
scope and recovery state; current evidence is insufficient to approve it as an
equivalent replacement. Cross-file speaker labels cannot simply be equated.

See [provider research](podcast-asr-options-provider.md), [ASR polling
investigation](podcast-asr-latency.md), and [local commit
measurement](podcast-local-commit.md).

## Recommended next deliverable

Make one instrumented, otherwise unchanged five-minute cloud production run.
Its purpose is to resolve the missing latency split and provide a current
baseline, not to claim that diagnostics themselves speed up production.

### Proposed tickets

1. **Complete ASR latency diagnostics** — no blocker. Terra implements local
   upload/submit/status-query/download/parse/sleep timing and safe optional
   provider task-time differences. Reuse the existing private counters; keep
   content and signed URLs private. Missing or malformed provider timestamps
   do not fail recognition or become zero-time evidence. Server-time differences
   remain separate from local monotonic time; no unsupported clock subtraction.
   Existing profile fingerprints, public trace/report schemas and submission
   recovery behavior remain unchanged. Tests cover errors, missing timestamps,
   old-run resume, and durable task-ID reuse.
2. **Verify and measure the retained profile** — blocked by ticket 1. Review at
   the public coordinator/provider seams with fake transports and clocks; run
   the full podcast suite and separate Standards/Spec reviews. Commit the
   diagnostic slice, then run the fixed five-minute source once in a fresh
   private directory with zero cache hits and current default settings. Report
   complete wall time, phase timing, retries, estimated cost and output integrity.

### Cloud scope for this deliverable

- Exactly one new five-minute production run, with existing models, narrator,
  rate limits, chunk sizes and polling policy.
- Historical usage suggests roughly CNY 0.30; propose a **CNY 1 estimated-spend
  planning allowance**. Actual charges depend on returned usage and provider
  billing; this is not a cloud-enforced hard cap.
- The slot is consumed by the first cloud submission. No automatic replacement
  run after failure. Keep existing safe no-success retry handling; never resend
  an uncertain paid submission.
- At completion, decide from measured evidence whether a >=10% candidate is
  credible. If not, report that result rather than manufacture a speed claim.
- No candidate or long-programme run is included in this immediate allowance.
  Those remain the confirmed overall goal but require a candidate and a source
  duration to make their combined plan and budget concrete.

## Missing long-programme input

Existing local production metadata points to the fixed five-minute sample and
an original source lasting 344.688 seconds. Neither is a representative
30–60-minute programme. The listener has been asked for the authorized long
source path, or to explicitly scope this round to the five-minute sample.

## Agent and stage contract

Root Astra retains grill-with-docs through to-spec/to-tickets context and owns
design decisions and final judgment. Terra handles ordinary implementation in
a fresh context per approved ticket. Sol takes any concurrency/recovery design
that proves necessary. Luna and Terra perform independent Standards and Spec
reviews, respectively. No overlapping write ownership. Use low effort for
retrieval/review and medium for implementation or difficult state reasoning.

After listener confirmation, publish the spec and approved tickets in the fork's
GitHub tracker, execute their dependency order, commit reviewed work, and perform
the one scoped cloud validation without another routine permission checkpoint.
