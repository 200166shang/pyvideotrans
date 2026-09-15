# Alibaba translation strategy for natural Mandarin podcasts

- Research date: 2026-09-15
- Repository snapshot: [`3eb3b2f3ab11013632d5d35329620c861ce8df7c`](https://github.com/200166shang/pyvideotrans/tree/3eb3b2f3ab11013632d5d35329620c861ce8df7c)
- Decision ticket: [Choose Alibaba translation strategy for natural Mandarin](https://github.com/200166shang/pyvideotrans/issues/7)

## Decision

Use **Alibaba Cloud Model Studio Qwen-MT through its OpenAI-compatible API, with `qwen-mt-flash` in the Beijing region**, for the first implementation and benchmark.

This is the best fit for a faithful Chinese listening edition with low wall-clock time because Alibaba recommends Flash for general translation as the balance of quality, speed, and cost. Alibaba positions Plus for the highest quality in professional domains, Lite for simple latency-sensitive text, and says Turbo will no longer be updated and should be replaced by Flash. Qwen-MT also exposes terminology, translation-memory, and domain controls that the older `TranslateGeneral` path lacks. These are documented product distinctions, not measured quality claims; the listener's acceptance remains the quality gate. ([Qwen-MT model selection](https://help.aliyun.com/en/model-studio/machine-translation#model-selection))

Use the OpenAI-compatible form of the dedicated Qwen-MT endpoint, not a general Qwen model with a translation prompt. It supports `translation_options` via `extra_body`, asynchronous Python clients are realistically integrable with the repository's existing `openai` dependency, and each response exposes `prompt_tokens`, `completion_tokens`, and `total_tokens` for cost accounting. ([Qwen-MT API reference](https://help.aliyun.com/en/model-studio/qwen-mt-api))

The production policy is:

- translate semantic, speaker-aware chunks through real-time Qwen-MT requests;
- run up to four requests concurrently behind account-wide request and token rate limiters;
- preserve programme-wide terminology and style with a compact `terms` list, an English `domains` description, and a small fixed `tm_list`;
- validate every chunk before accepting it, retry only transient failures, and resume from per-chunk cached results;
- compute translation spend from response token usage and the recorded regional price, then report CNY per source-programme hour.

Do **not** design around Model Studio Batch Inference: the current Qwen-MT-Flash capability page explicitly marks Batch Inference unsupported. "Batching" in this strategy therefore means application-side semantic chunking plus bounded concurrent real-time calls, not Alibaba's asynchronous Batch API. ([Qwen-MT-Flash model information](https://help.aliyun.com/zh/model-studio/qwen-mt-flash))

## What the repository does today

The repository already has two Alibaba translation paths:

1. [`QwenMT`](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/translator/_qwenmt.py#L13-L73) calls DashScope `Generation.call`, selects a configured `qwen-mt-*` model, passes `terms` from `glossary.txt`, and passes an optional `domains` string. The configured default is [`qwen-mt-turbo`](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/configure/_app_params.py#L155-L158).
2. [`Ali`](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/translator/_ali.py#L17-L49) calls the older Machine Translation `TranslateGeneral` API at the Hangzhou endpoint with `scene='general'` and a fixed two-second retry wait.

The common translator does not currently execute requests concurrently. `trans_thread` is a misleading name for the **number of subtitle rows in one request**: `_run_text` and `_run_srt` iterate over those batches in ordinary `for` loops and sleep after each call. The defaults are ten plain-text rows or fifty AI/SRT rows, not ten or fifty workers. ([batch construction and sequential loops](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/translator/_base.py#L46-L71), [default row counts](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/configure/_app_settings.py#L168-L172))

There are four material gaps in the Qwen-MT path:

- it has no transient-error retry policy or request/token limiter;
- it does not capture response usage or request IDs;
- it never passes Qwen-MT's `tm_list` translation-memory option;
- its cache identity is unsafe for benchmarks: `BaseTrans` includes `self.model_name` in the key, but `QwenMT` keeps the configured model in a local `model_name` variable and never assigns `self.model_name`. The key also omits glossary and domain configuration. Different model/context configurations can therefore reuse the same cached result. ([local model selection](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/translator/_qwenmt.py#L32-L39), [cache-key construction](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/translator/_base.py#L171-L187))

The existing "global reference context" does not solve Qwen-MT continuity. `_set_context` builds a prompt for AI translators, but the dedicated Qwen-MT branch sends only the current text and `translation_options`; Qwen-MT itself is single-turn and supports only one user message, not a multi-turn conversation or system message. ([repository path](https://github.com/200166shang/pyvideotrans/blob/3eb3b2f3ab11013632d5d35329620c861ce8df7c/videotrans/translator/_qwenmt.py#L22-L67), [Qwen-MT limitations](https://help.aliyun.com/en/model-studio/machine-translation#limitations))

## API and model comparison

| Choice | Current status here | Official fit | Decision |
| --- | --- | --- | --- |
| `qwen-mt-flash` | Already selectable through `QwenMT` | Alibaba's general-use recommendation; fast, low-cost, 92 languages, incremental streaming | **Primary** |
| `qwen-mt-plus` | Already selectable through `QwenMT` | Highest translation quality for professional/formal/technical material, but slower, costlier, and lower Beijing TPM | Conditional fallback if Flash fails the listening quality gate |
| `qwen-mt-lite` | Already selectable through `QwenMT` | Fastest and cheapest, but explicitly basic and intended for simple real-time text | Do not benchmark initially |
| `qwen-mt-turbo` | Current repository default | Fair quality, same Beijing price/rate limit as Flash, but Alibaba says it will not be updated | One existing-path baseline only; migrate away |
| Machine Translation `TranslateGeneral` | Existing `Ali` adapter | 50 QPS, under 5,000 characters/request; general edition costs CNY 50/million characters after free quota | Operational fallback, not first benchmark candidate |
| General Qwen text model through `_openai` | Realistically usable now | More prompt freedom, but not the purpose-built translation surface and lacks Qwen-MT controls as a typed contract | Out of the first implementation |

The Qwen-MT model positioning comes from Alibaba's [Qwen-MT guide](https://help.aliyun.com/en/model-studio/machine-translation#model-selection). The legacy API limits are [50 QPS and fewer than 5,000 characters per `TranslateGeneral` request](https://help.aliyun.com/zh/machine-translation/developer-reference/limits), and its general-edition list price is [CNY 50 per million characters](https://help.aliyun.com/zh/machine-translation/product-overview/pricing-of-machine-translation). The Qwen-MT pricing page lists Beijing real-time prices of CNY 0.70/million input tokens and CNY 1.95/million output tokens for Flash and Turbo, versus CNY 1.80 and CNY 5.40 for Plus. ([Model Studio pricing](https://help.aliyun.com/zh/model-studio/model-pricing#qwen-translation))

## Segmentation and request batching

Alibaba caps Qwen-MT input at 8,192 tokens and recommends splitting long text at paragraph or complete-sentence boundaries rather than by character count. Terms, translation-memory entries, and domain prompts also consume input tokens. ([going-live guidance](https://help.aliyun.com/en/model-studio/machine-translation#going-live))

Use this deterministic policy for the first implementation:

1. Normalize ASR fragments into complete sentences without merging across a known speaker change.
2. Preserve stable source segment IDs, source times, and speaker IDs outside the translated text.
3. Build each request at a sentence or speaker-turn boundary, targeting **600–1,000 source tokens** and hard-capping the complete request—including IDs and all translation options—at **6,000 estimated input tokens**. The lower target produces several independent chunks even for the five-minute benchmark sample; the 6,000-token cap leaves headroom below Alibaba's 8,192-token maximum.
4. Add at most the immediately preceding and following complete source sentence as reference-only overlap when a boundary would otherwise break a pronoun, idiom, or short response. Discard duplicate translated overlap during assembly.
5. Require a one-to-one set of stable IDs in the response. If IDs/count/order fail validation, split the chunk at the nearest sentence boundary and retry the two smaller chunks; never silently pad missing translations, as the current plain-text runner does.

The exact 600–1,000-token target and 6,000-token cap are engineering starting points, not Alibaba-published optima. Record actual chunk size and latency so the later benchmark can adjust them.

Use non-streaming responses initially. Flash's incremental streaming can reduce perceived latency, but a production run needs each validated chunk before assembly and cost recording; streaming by itself does not demonstrate lower time to a complete Chinese listening edition. This is an inference from the product's batch workflow. Alibaba documents the streaming behavior but makes no claim that it reduces total generation time. ([streaming output](https://help.aliyun.com/en/model-studio/machine-translation#streaming-output))

## Context preservation

Set `source_lang="English"` and `target_lang="Chinese"` explicitly; Alibaba says explicit source language improves accuracy. Do not use automatic detection because the input contract guarantees English. ([Qwen-MT operation](https://help.aliyun.com/en/model-studio/machine-translation#how-it-works))

Use all three native controls, kept compact and immutable during one production run:

- `terms`: proper names, programme names, recurring technical terms, acronyms, and names whose Chinese rendering must remain stable. Reuse the existing `glossary.txt` parser.
- `domains`: one short English description such as "Conversational long-form technology podcast; produce faithful, idiomatic, naturally spoken Simplified Chinese; do not summarize or omit content." Alibaba requires domain prompts to be in English.
- `tm_list`: a small set of listener-approved English/Chinese sentence pairs that demonstrate the desired conversational style. Start empty; populate it only from accepted benchmark translations, not unreviewed model output.

Alibaba documents these fields and notes that translation-memory pairs teach sentence pattern/style, while all reference material consumes the input budget. ([terms, translation memory, and domain prompting](https://help.aliyun.com/en/model-studio/machine-translation#translation-enhancement))

Do not pass the whole transcript as context and do not serialize chunks on previous model output. The fixed glossary, domain prompt, translation memory, and source-sentence overlap give each request enough shared context to remain independently schedulable.

## Concurrency and rate control

For Beijing, Alibaba currently publishes Qwen-MT-Flash limits of **60 RPM and 35,000 input-plus-output TPM**. Limits are account-wide across RAM users, workspaces, and API keys; Alibaba can also enforce derived RPS/TPS and burst protection. ([Model Studio rate limits](https://help.aliyun.com/zh/model-studio/rate-limit#qwen-translation))

Start with:

- four in-flight requests;
- smooth dispatch through token buckets capped at 80% of the account's live allowance: initially 48 RPM and 28,000 TPM for the published Beijing default;
- reserve tokens from the request estimate before dispatch, then reconcile against response usage;
- halve concurrency after a 429 or a sustained latency rise, and add one worker only after a stable window, never exceeding the configured maximum.

Four workers are a benchmarkable starting point, not a claim that four is universally optimal. The limiter, rather than worker count alone, is authoritative. Account-specific limits should be read before a production run and stored in the production report.

## Retry and resume policy

Retry only rate limits, HTTP 408/5xx, connection failures, and timeouts. Honor `Retry-After` when present; otherwise use exponential backoff with full jitter (1-second base, 30-second cap), for at most four attempts. Smooth dispatch and exponential backoff follow Alibaba's rate-limit guidance. ([rate-limit handling](https://help.aliyun.com/zh/model-studio/rate-limit#how-to-avoid-rate-limiting))

Fail fast for authentication/authorization errors and other non-transient 4xx responses. Treat malformed or structurally incomplete output as a validation failure: change the request by splitting the chunk before retrying rather than repeating an identical request.

Persist each accepted chunk atomically. Its identity must hash the source segment IDs/text plus region, endpoint, model ID, source/target languages, segmentation-policy version, domain prompt, terms, translation memory, and output-format version. This both enables restart and prevents a cached translation from contaminating model or context comparisons.

## Cost accounting

For every attempt, record model ID, region/endpoint, request ID, chunk ID, start/end time, status/error class, retry ordinal, and reserved tokens. For every successful response, record reported input and output tokens. The OpenAI-compatible response schema exposes both token counts. ([response usage schema](https://help.aliyun.com/en/model-studio/qwen-mt-api#chat-response-object-non-streaming-output))

Snapshot the effective regional prices into the production report. At the current Beijing Flash list price:

```text
translation_cost_cny = input_tokens / 1_000_000 * 0.70
                     + output_tokens / 1_000_000 * 1.95

translation_cny_per_source_hour = translation_cost_cny
                                / source_duration_hours
```

For scale only, 12,000 input tokens plus 12,000 output tokens would calculate to CNY 0.0318 at those list prices. This is an illustration, not an estimate of a particular podcast and not a measured production cost. Keep uncertain attempts separate and reconcile the request ledger with Alibaba's billing details before finalizing the production report.

## Minimal benchmark plan

No paid API was called for this research. When implementation work later enables the benchmark, minimize time and spend:

1. Run the fixed five-minute benchmark sample once through the current `qwen-mt-turbo` path as the baseline, with cache reads and writes isolated from all other runs.
2. Run the same sample once with the decided `qwen-mt-flash` configuration.
3. If the listener accepts Flash, run Flash once more for confirmation and stop comparing models.
4. Only if Flash fails the listening quality gate, run one `qwen-mt-plus` candidate and confirm it once if accepted.

Keep transcript, semantic segmentation, context controls, concurrency cap, and output assembly fixed when comparing models. Record translation wall-clock time, tokens, computed spend, retries, chunk validation failures, and the listener's acceptance. Do not run Lite, `TranslateGeneral`, or a general Qwen model unless both Flash and Plus fail or an implementation constraint makes Qwen-MT unavailable.

## Unknowns to resolve during implementation

- Actual latency and listening quality cannot be inferred from documentation; the benchmark decides them.
- The public limits are account defaults, not proof of this account's effective quota.
- The moving `qwen-mt-flash` alias can change underneath a later benchmark. Record the request/response model identity and research date; if Alibaba announces an alias update, establish a new baseline.
- ID/format preservation and the best semantic chunk size require validation on the fixed transcript because Qwen-MT does not offer structured output on its model capability page.

This ticket makes a translation-strategy decision only. It adds no production implementation.
