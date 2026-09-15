# TTS throughput experiment for Chinese listening editions

## Approved outcome and scope

Follow-up to #12 and merged PR #13. The listener approved this round on 2026-09-15: prioritize total completion time; retain Alibaba Beijing, existing ASR/translation/TTS models, Andre, faithful full translation, source order, speaker turns and MP3 format. Compare TTS concurrency 4/6/8 offline first with unchanged 150 RPM and 500-character chunks. Only if insufficient, investigate sentence-based 350/250-character chunks with concurrency 4/6/8, then segmented ASR as a later option. Existing incremental and independent batch chunking must remain equivalent under the chosen ceiling.

At least 10% end-to-end improvement against the historical uninterrupted cold five-minute 52.009-second baseline is the candidate timing gate: <=46.8081 seconds. 45 seconds is aspirational. The old baseline is reused because no new baseline slot was authorized; one comparison cannot establish causation or stable performance.

## Single vertical slice

1. Add a reusable content-free offline replay using real AsyncScheduler and observed per-chunk release/service timings from the accepted v2 trace. Compare 4/6/8 with the same smooth rate. Require replay-4 calibration within 0.5 seconds of measured total. Report latency sensitivity and frozen-translation-release assumptions; do not claim predictions are measurements. Reject incomplete, resumed or retry traces rather than infer missing observations.
2. Select the best credible concurrency candidate if it clears 10%; investigate chunking only if none does. Add a separately named fingerprinted experimental profile with only ID, TTS concurrency and (if the fallback is selected) the TTS soft character ceiling changed. Keep v2 available and default; do not change models, text content/order, chunk identity algorithm, rate limits or recovery behavior. Smaller chunks can change sentence spacing and synthesis boundaries under the existing canonical algorithm, so listener review remains required.
3. Before cloud use, run relevant offline scheduling, profile, recovery, integrity and CLI tests plus static checks and separate Standards/Spec reviews against ba4076d.
4. Execute at most ONE new five-minute cold candidate, with the fixed authorized source fingerprint, fresh private directory and content-free report/trace. No new baseline, confirmation, long-programme run, automatic replacement candidate or extra paid round. The slot is consumed when the first cloud request is made even if the run fails. Existing safe no-success request retry policy remains; uncertain submissions must never be automatically resent.
5. Compare historical and candidate totals, input/profile, cost, retries, ordered artifact integrity, format and full decode. Preserve listener-owned first/middle/last 60-second review as pending until the listener accepts it. Do not promote the candidate to default based on timing alone.

## Verification and boundaries

Replay tests cover calibration, queueing headroom/no-headroom, latency sensitivity, strict trace rejection, ordering and rate pacing. Existing pipeline tests protect sealed-chunk equivalence, fail/drain, uncertainty and resume. No production scheduler rewrite, model substitution, new provider, GUI integration or long-programme claim is included. New product decisions are unnecessary for this bounded experiment; a broader redesign returns to planning. Production cost is reported with the existing CNY3.85/source-hour informational watchline.

## Selected experimental profile

Offline unchanged-chunk concurrency 6/8 yields only 9.8847%, below the gate. The authorized fallback selects `alibaba-podcast-tts-throughput`: TTS concurrency 8 and sentence-first soft ceiling 250, keeping 150 RPM and all other v2 provider/output/recovery settings. It is explicit opt-in; CLI default remains `alibaba-podcast-v2`.

The chunking sweep preserves exact ordered non-whitespace characters and speaker ownership, and requires incremental/batch oracle equality. It allocates each original observed TTS service interval across the corresponding characters, with 0.5/1/2-second fixed request overhead and 1.0/1.1/1.2 service-latency multipliers. New per-chunk latencies are not observed evidence; the 38.500–41.562-second prediction is a scenario range, not a confidence interval or promise. Only the single cold candidate can test this hypothesis.

## Listener acceptance and release completion

On 2026-09-15 the listener explicitly accepted the listening result and requested completion. This supersedes the experimental default/pending-review statements above: the accepted throughput profile becomes the default for **new** podcast and benchmark runs. Its ID and fingerprint remain unchanged from the measured candidate. Explicit `--podcast-profile alibaba-podcast-v2` remains available; when `--resume` or review omits the profile, the CLI selects the profile saved in `run.private.json`. Missing/unknown saved profiles fail rather than silently selecting the new default, and the existing fingerprint checks remain authoritative.

Record accepted review in the manifest, production report and benchmark summary without constructing cloud clients; update the PR for the final release behavior, merge after verification, and close #14. The one-run evidence limit remains: no new paid, confirmation or long-programme run is requested or required to finish this accepted release.
