# MEP: Add Yandex Cloud Text Embedding Provider (`yc`)

- **Created:** 2026-02-27
- **Author(s):** @edddoubled
- **Status:** Draft
- **Component:** Proxy | QueryNode | DataNode | Function
- **Related Issues:** TBD
- **Released:** [TBD]

## Summary

This proposal introduces a new text embedding provider `yc` for Milvus `TextEmbedding` function.  
The provider integrates with Yandex Cloud AI Studio text embedding API and enables users to generate embeddings during insert/search pipelines in the same way as existing providers (`openai`, `cohere`, `tei`, etc.).

The feature includes:

1. New provider implementation in `internal/util/function/embedding`.
2. Provider selection integration in `TextEmbeddingFunction`.
3. Provider config and credentials support in `paramtable`/`milvus.yaml`.
4. Unit and integration tests with existing function test patterns.

## Motivation

Milvus currently supports multiple external embedding providers but does not provide a built-in Yandex Cloud provider.
Users on Yandex Cloud currently need custom middleware or external embedding jobs, which creates:

- additional latency and operational complexity,
- duplicated auth/retry/error handling logic,
- weaker parity with first-class Milvus function providers.

Adding `yc` keeps user experience consistent across cloud providers and reduces integration friction.

## Public Interfaces

### Function schema parameters

No new function type is introduced. Existing `FunctionType_TextEmbedding` is reused with:

- `provider=yc`
- `model_name=<yandex modelUri>`
- `dim=<optional, must match output field dim>`
- `credential=<optional, preferred>`

### Config interfaces

New config group keys under:

- `function.textEmbedding.providers.yc.enable`
- `function.textEmbedding.providers.yc.credential`
- `function.textEmbedding.providers.yc.url`

New environment variable:

- `MILVUS_YC_API_KEY`

## Design Details

### Architecture placement

The provider follows existing `textEmbeddingProvider` interface:

- `MaxBatch() int`
- `FieldDim() int64`
- `CallEmbedding(ctx, texts, mode) (any, error)`

The `yc` provider is selected in `NewTextEmbeddingFunction(...)` switch by `provider=yc`.

### Request/Response mapping

Milvus provider parameters map to Yandex API fields:

- `model_name` -> `modelUri`
- input text(s) -> request text payload
- API key -> `Authorization` header

Provider output type:

- `[][]float32` only

Validation rules:

1. Returned embedding count must equal input text count.
2. Returned embedding dimension must equal output field dimension.
3. If `dim` param is provided, it must match output field dimension (existing Milvus rule).

### Batching and timeout

Batch behavior follows existing providers:

- internal chunking by `maxBatch`
- external cap by `extraInfo.BatchFactor`

Default values:

- `maxBatch = 128`
- `timeoutSec = 30`

These defaults align with existing provider implementations and can be tuned later by follow-up changes if needed.

### Credential resolution order

Credential parsing uses existing utility `models.ParseAKAndURL(...)` with standard precedence:

1. Function param (`credential`)
2. `milvus.yaml` provider config
3. Environment variable (`MILVUS_YC_API_KEY`)

This keeps behavior consistent with other providers and avoids introducing a provider-specific credential flow.

### Error handling

Provider reuses existing HTTP utility `models.PostRequest(...)` for:

- HTTP error propagation (status/body),
- timeout handling,
- retry with exponential backoff and jitter.

Provider-level errors are normalized to existing embedding provider style:

- missing credential,
- embedding count mismatch,
- embedding dim mismatch.

### API compatibility strategy

Yandex documentation may evolve request/response schema over time.
To reduce tight coupling risk, the provider supports response adaptation for both:

- single-embedding response shape,
- batched embeddings response shape.

If API contract changes in future, the adaptation layer can be extended without changing function runtime interfaces.

## Compatibility, Deprecation, and Migration Plan

### Compatibility

- Fully backward compatible for existing users.
- No behavior change for existing providers.
- No schema migration required.

### Deprecation / migration

- No deprecations in this MEP.
- Existing function definitions continue to work unchanged.

## Security Considerations

1. API keys must be configured via credential config/env; avoid hard-coding in function params.
2. Credentials should be redacted in logs (existing Milvus credential handling path).
3. Requests must use HTTPS endpoints.
4. Future IAM-token support should follow same secure storage guidance.

## Observability

Initial version relies on existing error surfaces from function execution path.
Follow-up (optional) improvements:

- provider-specific request latency metrics,
- response code counters by provider.

## Test Plan

### Unit tests (`yc_embedding_provider_test.go`)

1. Happy path with 1 text.
2. Batch path with multiple texts, order preserved.
3. Embedding count mismatch should return error.
4. Embedding dim mismatch should return error.
5. Missing credential should return error.
6. Custom URL and default URL behavior.

### Integration tests (`text_embedding_function_test.go`)

1. `provider=yc` function creation and insert path.
2. Provider disabled path (`yc.enable=false`).
3. Unsupported provider behavior remains unchanged.

### Regression checks

Run existing embedding package test suites with required Milvus flags:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/embedding/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./pkg/util/paramtable/...
```

## Rejected Alternatives

### 1) Implement provider outside TextEmbedding framework

Rejected because it duplicates runtime logic and creates inconsistent UX.

### 2) Add Yandex-specific function type

Rejected because provider extension is sufficient and aligns with existing architecture.

### 3) Introduce new HTTP client dependency

Rejected because existing `models.PostRequest` already provides retries, timeout, and standardized behavior.

## Open Questions

1. Should first release support IAM token in addition to API key, or API key-only with IAM in follow-up?
2. What is the final supported request schema for batch mode in Yandex endpoint used by Milvus deployment target?
3. Are there provider-specific token/input limits that should be surfaced in user-facing docs?

## User Documentation Draft (milvus.io style)

This section is a draft outline for the user-facing documentation page (similar in structure to existing provider pages such as OpenAI).

### Title and scope

- Page title: `Yandex Cloud`
- Feature scope: `TextEmbedding` provider `yc`
- Audience: users configuring function-based embedding in Milvus

### Prerequisites

1. Milvus instance with function feature enabled.
2. Yandex Cloud account and AI Studio embeddings access.
3. Valid credential (API key in phase 1).
4. A valid `modelUri` compatible with Yandex text embedding API.

### Configuration example

```yaml
function:
  textEmbedding:
    providers:
      yc:
        credential: yandex_cred
        enable: true
        url: https://llm.api.cloud.yandex.net/foundationModels/v1/textEmbedding
```

```yaml
credential:
  yandex_cred:
    apikey: <YOUR_YC_API_KEY>
```

### Function parameter table

- `provider` (required): must be `yc`
- `model_name` (required): mapped to Yandex `modelUri`
- `dim` (optional): must match output field dimension if specified
- `credential` (recommended): credential name from Milvus credential config

### End-to-end usage

1. Create collection with source text field and float vector output field.
2. Add `TextEmbedding` function with `provider=yc`.
3. Insert plain text data and verify vector output generated automatically.
4. Run text query path and verify embedding + search pipeline.

### Troubleshooting section

- 401/403: invalid or missing API key, insufficient Yandex IAM permission.
- 429: request rate exceeded; reduce batch size and retry with backoff.
- Dim mismatch: output field dim is not equal to model output dim.
- Provider disabled: `function.textEmbedding.providers.yc.enable` is false.

### Notes and limitations

- Initial release supports float embeddings only.
- Batch mode request/response shape must be confirmed against final API contract.
- IAM token auth is planned as a follow-up if not included in phase 1.

## References

- Milvus embedding provider architecture: `internal/util/function/embedding`
- Milvus provider config path: `pkg/util/paramtable/function_param.go`
- Yandex text embedding API docs: https://yandex.cloud/ru/docs/ai-studio/embeddings/api-ref/Embeddings/textEmbedding
