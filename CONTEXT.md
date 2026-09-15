# Chinese Podcast Production

This context turns a source programme supplied by the listener into a Chinese listening edition whose speed, cost, and listening quality can be evaluated consistently.

## Language

**Source programme**:
An authorised local English-language audio or video file supplied as the input to production.
_Avoid_: YouTube download, original video

**Chinese listening edition**:
The faithful, naturally paced Mandarin MP3 produced for personal listening. It may differ in duration from the source programme and preserves speaker turns when they can be identified reliably, without requiring imitation of the original voices.
_Avoid_: translated video, dubbed video, Chinese copy

**Standard narrator voice**:
The single, consistent Mandarin system voice used across Chinese listening editions unless the listener explicitly revises the preference. The current standard narrator voice is Alibaba `Andre`. Speaker turns may remain structurally distinct without changing narrator voice.
_Avoid_: cloned voice, original-speaker voice, per-speaker voice

**Production run**:
One end-to-end attempt to turn a source programme into a Chinese listening edition.
_Avoid_: job, conversion, translation

**Benchmark sample**:
A fixed five-minute representative excerpt used for one baseline run, one run per credible candidate, and one confirmation run of the leading candidate before full-programme validation.
_Avoid_: test video, demo clip

**Listening quality gate**:
The listener's acceptance of translation clarity, speaker-turn integrity, voice naturalness, and rhythm before a speed improvement counts.
_Avoid_: quality feels okay, usable audio

**Output equivalence**:
The requirement that an optimized production preserves the complete translated text, source order, speaker-turn ownership, standard narrator voice, and MP3 specification. Internal speech-synthesis chunk boundaries may change when the listening quality gate still passes.
_Avoid_: byte-identical audio, identical timestamps, identical chunk files

**Production cost**:
The measured cloud-service spend for one hour of source programme; two yuan per source hour is a preference to optimise toward, not a stopping condition.
_Avoid_: budget cap, fixed price

**Cost watchline**:
A comparison threshold that makes a production-cost increase visible for review without blocking an otherwise faster, acceptable listening edition. The second optimization round watches CNY 3.85 per source hour.
_Avoid_: cost cap, hard budget, release blocker

**Runtime target**:
A provisional cold-run wall-clock objective relative to source programme duration. It becomes a validated performance claim only after representative full-programme evidence passes the listening quality gate.
_Avoid_: SLA, guaranteed speed, warm-cache time

**Production report**:
The comparison record for a production run, containing end-to-end and stage timings, cloud cost, retries, and the listener's acceptance decision.
_Avoid_: benchmark log, speed result

**Performance trace**:
A local, content-free event record containing relative timing, chunk identity, dependency, queue, start, completion, and commit events for offline replay. It contains no transcript, translation, media content, credential, or provider response body.
_Avoid_: debug dump, request log, transcript log

**Stage overlap**:
The production arrangement in which a committed translated chunk may enter speech synthesis before translation of the whole source programme has finished, while final assembly still preserves source order.
_Avoid_: parallel pipeline, streaming output, out-of-order podcast

**Sealed synthesis chunk**:
An append-only speech-synthesis work item whose text, source order, speaker ownership, and identity can no longer change and may therefore be submitted and recovered independently.
_Avoid_: partial chunk, mutable TTS request, streaming token

**Uncertain submission**:
A provider request that may already have incurred a charge but has no verified local artifact. A production run stops for review at this state instead of automatically submitting the request again.
_Avoid_: retryable failure, pending chunk, free retry

**Confirmation run**:
The single paid rerun of an accepted candidate used to verify runtime stability. It repeats automated integrity checks and requires additional listening only when those checks find an anomaly.
_Avoid_: third benchmark, second listening review, warm rerun
