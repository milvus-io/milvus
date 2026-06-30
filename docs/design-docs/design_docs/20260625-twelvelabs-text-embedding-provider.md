# MEP: Add TwelveLabs Text Embedding Provider (`twelvelabs`)

- **Created:** 2026-06-25
- **Author(s):** @mohit-twelvelabs
- **Status:** Draft
- **Component:** Proxy | QueryNode | DataNode | Function
- **Related Issues:** TBD
- **Released:** [TBD]

## Summary

This proposal introduces a new text embedding provider `twelvelabs` for the Milvus `TextEmbedding` function.
The provider integrates with the [TwelveLabs](https://twelvelabs.io) Embed API (Marengo multimodal embedding model) and lets users generate text embeddings during insert/search pipelines in the same way as the existing providers (`openai`, `cohere`, `tei`, etc.).

The feature includes:

1. New provider implementation in `internal/util/function/embedding`.
2. Provider selection integration in `TextEmbeddingFunction`.
3. Provider config and credentials support in `paramtable`/`milvus.yaml`.
4. Unit tests following existing function test patterns.

## Motivation

TwelveLabs Marengo is a multimodal embedding model that embeds text, image, audio, and video into a single shared 512-dimensional vector space. A common workflow is **video search**: video embeddings are generated out-of-band (TwelveLabs SDK / API) and stored in Milvus, then a user issues a natural-language query that must be embedded with the *same* model so that text and video vectors are comparable.

Today, Milvus users on TwelveLabs must run an external embedding job to embed the query text before searching, which creates:

- additional latency and operational complexity,
- duplicated auth/retry/error handling logic,
- weaker parity with first-class Milvus function providers.

Adding `twelvelabs` lets the query side be embedded in-pipeline (via the `TextEmbedding` function on the search path), keeping cross-modal text→video search consistent with how every other provider is used.

## Public Interfaces

### Function schema parameters

No new function type is introduced. The existing `FunctionType_TextEmbedding` is reused with:

- `provider=twelvelabs`
- `model_name=<TwelveLabs embedding model, e.g. marengo3.0>`
- `dim=<optional, must match output field dim — Marengo is 512>`
- `credential=<optional, preferred>`

### Config interfaces

New config group keys under:

- `function.textEmbedding.providers.twelvelabs.enable`
- `function.textEmbedding.providers.twelvelabs.credential`
- `function.textEmbedding.providers.twelvelabs.url`

New environment variable:

- `MILVUS_TWELVELABS_API_KEY`

## Design Details

### Architecture placement

The provider implements the existing `textEmbeddingProvider` interface:

- `MaxBatch() int`
- `FieldDim() int64`
- `CallEmbedding(ctx, texts, mode) (any, error)`

The `twelvelabs` provider is selected in `NewTextEmbeddingFunction(...)` by `provider=twelvelabs`.

### Request/Response mapping

The TwelveLabs Embed endpoint (`POST /v1.3/embed`) accepts `multipart/form-data` and the `x-api-key` header. Milvus provider parameters map to API fields:

- `model_name` -> form field `model_name`
- a single input text -> form field `text`
- API key -> `x-api-key` header

The response shape for a text input is:

```json
{"model_name":"marengo3.0","text_embedding":{"segments":[{"float":[ ... 512 floats ... ]}]}}
```

The provider reads `text_embedding.segments[0].float`.

Provider output type:

- `[][]float32` only (Marengo returns float embeddings).

Validation rules:

1. Returned embedding dimension must equal the output field dimension.
2. If `dim` param is provided, it must match the output field dimension (existing Milvus rule).
3. Output field must be a `FloatVector`.

### Batching and timeout

The TwelveLabs text-embed endpoint embeds **one text per request**, so the provider issues one HTTP request per input row. The external cap by `extraInfo.BatchFactor` still applies via `MaxBatch()`.

Default values:

- `maxBatch = 64`
- request timeout follows the global `function.model.requestTimeout` (default `30s`), overridable per-function via `timeout_ms`.

### Credential resolution order

Credential parsing uses the existing utility `models.ParseAKAndURL(...)` with standard precedence:

1. Function param (`credential`)
2. `milvus.yaml` provider config
3. Environment variable (`MILVUS_TWELVELABS_API_KEY`)

This keeps behavior consistent with other providers.

### Error handling

The provider classifies HTTP failures the same way as `models.send(...)`: `429` and `5xx` are mapped to retryable service-unavailable errors, other `4xx` to permanent function-failed errors. Provider-level errors are normalized to existing embedding-provider style: missing credential, empty response, embedding dim mismatch.

## Compatibility, Deprecation, and Migration Plan

### Compatibility

- Fully backward compatible for existing users.
- No behavior change for existing providers.
- No schema migration required.
- Opt-in: the provider is only used when `provider=twelvelabs` is set on a function.

### Deprecation / migration

- No deprecations in this MEP.
- Existing function definitions continue to work unchanged.

## Security Considerations

1. API keys must be configured via credential config/env; avoid hard-coding in function params.
2. Credentials are redacted in logs via the existing Milvus credential handling path.
3. Requests must use HTTPS endpoints.

## Observability

Initial version relies on existing error surfaces from the function execution path. Follow-up (optional): provider-specific request latency metrics and response-code counters.

## Test Plan

### Unit tests (`twelvelabs_embedding_provider_test.go`)

1. Happy path with 1 text.
2. Multiple texts, order preserved (one request per text).
3. Embedding dim mismatch should return error.
4. Empty response (no segments) should return error.
5. HTTP error should return error.
6. Missing credential / missing model name should return error.
7. Unsupported (non-FloatVector) output type should return error.
8. Custom URL and default URL behavior; basic getters.

### Regression checks

Run the existing embedding package test suites with the required Milvus flags:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/embedding/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./pkg/util/paramtable/...
```

## Rejected Alternatives

### 1) Implement the provider outside the TextEmbedding framework

Rejected because it duplicates runtime logic and creates inconsistent UX.

### 2) Add a TwelveLabs-specific function type

Rejected because provider extension is sufficient and aligns with existing architecture.

### 3) Add a new HTTP client dependency

Rejected because the standard library `net/http` + `mime/multipart` covers the single multipart call; no new dependency is needed.

## Open Questions

1. Should image/audio query embedding (also supported by Marengo) be surfaced through a future multimodal function type? Out of scope for this MEP.

## User Documentation Draft (milvus.io style)

### Title and scope

- Page title: `TwelveLabs`
- Feature scope: `TextEmbedding` provider `twelvelabs`
- Audience: users building cross-modal (text → video) search in Milvus.

### Prerequisites

1. Milvus instance with the function feature enabled.
2. A TwelveLabs account and API key (free tier available at https://twelvelabs.io).
3. A `FloatVector` output field whose dim matches the model (Marengo: 512).

### Configuration example

```yaml
function:
  textEmbedding:
    providers:
      twelvelabs:
        credential: twelvelabs_cred
        enable: true
        url: https://api.twelvelabs.io/v1.3/embed
```

```yaml
credential:
  twelvelabs_cred:
    apikey: <YOUR_TWELVELABS_API_KEY>
```

### Function parameter table

- `provider` (required): must be `twelvelabs`
- `model_name` (required): TwelveLabs embedding model, e.g. `marengo3.0`
- `dim` (optional): must match output field dimension if specified (512 for Marengo)
- `credential` (recommended): credential name from Milvus credential config

### End-to-end usage

1. Generate video embeddings with the TwelveLabs API/SDK and insert them into a 512-dim `FloatVector` field.
2. On a separate collection (or the same field) add a `TextEmbedding` function with `provider=twelvelabs` over a `VARCHAR` query field.
3. Run a text search — Milvus embeds the query text with Marengo and searches it against the stored video vectors.

### Troubleshooting section

- 401/403: invalid or missing API key.
- 429: request rate exceeded; retry with backoff.
- Dim mismatch: output field dim is not equal to the model output dim (512).
- Provider disabled: `function.textEmbedding.providers.twelvelabs.enable` is false.

## References

- Milvus embedding provider architecture: `internal/util/function/embedding`
- Milvus provider config path: `pkg/util/paramtable/function_param.go`
- TwelveLabs Embed API docs: https://docs.twelvelabs.io/v1.3/api-reference/embeddings/create-text-image-audio-embeddings
