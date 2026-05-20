# MEP: Add Qianfan Text Embedding Provider (`qianfan`)

- **Created:** 2026-05-20
- **Author(s):** @jimmyzhuu
- **Status:** Draft
- **Component:** Proxy | QueryNode | DataNode | Function
- **Related Issues:** #49966
- **Released:** [TBD]

## Summary

This proposal introduces a new text embedding provider `qianfan` for the Milvus `TextEmbedding` function.
The provider integrates with the Baidu Qianfan embeddings API and enables users to generate embeddings during insert/search pipelines in the same way as existing providers such as `openai`, `cohere`, `tei`, and `yc`.

The feature includes:

1. New Qianfan embedding client and provider implementation.
2. Provider selection integration in `TextEmbeddingFunction`.
3. Provider config and credentials support in `paramtable` and `milvus.yaml`.
4. Unit and function integration tests with the existing mock service patterns.

## Motivation

Milvus supports multiple external embedding providers, but users of Baidu Qianfan currently need custom middleware or external embedding jobs before writing vectors into Milvus.
Adding `qianfan` reduces integration friction for Qianfan users and keeps the Milvus function experience consistent across embedding providers.

## Public Interfaces

### Function schema parameters

No new function type is introduced. Existing `FunctionType_TextEmbedding` is reused with:

- `provider=qianfan`
- `model_name=<qianfan embedding model>`
- `dim=<optional, must match output field dim>`
- `credential=<optional, preferred>`

### Config interfaces

New config group keys under:

- `function.textEmbedding.providers.qianfan.enable`
- `function.textEmbedding.providers.qianfan.credential`
- `function.textEmbedding.providers.qianfan.url`

New environment variable:

- `MILVUS_QIANFAN_API_KEY`

## Design Details

### Architecture placement

The provider follows the existing `textEmbeddingProvider` interface:

- `MaxBatch() int`
- `FieldDim() int64`
- `CallEmbedding(ctx, texts, mode) (any, error)`

The `qianfan` provider is selected in `NewTextEmbeddingFunction(...)` by `provider=qianfan`.

### Request and response mapping

Milvus provider parameters map to Qianfan API fields:

- `model_name` -> `model`
- input text list -> `input`
- API key -> `Authorization: Bearer <key>`

Provider output type:

- `[][]float32`

Validation rules:

1. Returned embedding count must equal input text count.
2. Returned embedding dimension must equal output field dimension.
3. If `dim` param is provided, it must match output field dimension.

The initial version does not send `dim` to Qianfan. The current Qianfan embeddings API documents `model` and `input` for text embeddings, so Milvus keeps `dim` as local schema validation only.

### Batching and timeout

Batch behavior follows existing providers:

- internal chunking by `maxBatch`
- external cap by `extraInfo.BatchFactor`

Default values:

- `maxBatch = 128`
- `timeoutSec = 30`
- default URL: `https://qianfan.baidubce.com/v2/embeddings`

### Credential resolution order

Credential parsing uses existing utility `models.ParseAKAndURL(...)` with standard precedence:

1. Function param (`credential`)
2. `milvus.yaml` provider config
3. Environment variable (`MILVUS_QIANFAN_API_KEY`)

This keeps behavior consistent with other providers and avoids introducing a provider-specific credential flow.

### Error handling

Provider reuses existing HTTP utility `models.PostRequest(...)` for:

- HTTP error propagation,
- timeout handling,
- retry with exponential backoff and jitter.

Provider-level errors follow existing embedding provider style:

- missing credential,
- missing model name,
- embedding count mismatch,
- embedding dim mismatch.

## Compatibility, Deprecation, and Migration Plan

### Compatibility

- Fully backward compatible for existing users.
- No behavior change for existing providers.
- No schema migration required.

### Deprecation / migration

- No deprecations in this MEP.
- Existing function definitions continue to work unchanged.

## Security Considerations

1. API keys should be configured via Milvus credentials or environment variables instead of being hard-coded in function definitions.
2. Credentials should remain redacted by existing Milvus credential handling paths.
3. The default endpoint uses HTTPS.

## Observability

Initial version relies on existing error surfaces from the function execution path.
Follow-up improvements can add provider-specific request latency metrics or response code counters if the function framework standardizes those metrics.

## Test Plan

### Unit tests

1. Qianfan client check validates API key and URL.
2. Qianfan client sends expected request headers/body and sorts response items by `index`.
3. Qianfan provider handles single and batched embeddings.
4. Provider returns errors for count mismatch, dim mismatch, missing credential, missing model name, invalid dim, empty response, and HTTP failure.
5. Provider supports custom URL and default URL.

### Function tests

1. `provider=qianfan` function creation and insert path.
2. Provider disabled path (`qianfan.enable=false`).

### Regression checks

Run the existing embedding and paramtable test suites with required Milvus flags:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/models/qianfan/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/embedding/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./pkg/util/paramtable/...
```

## Rejected Alternatives

### 1) Reuse the OpenAI client package directly

Rejected because a Qianfan-specific client keeps provider ownership clear and leaves room for Qianfan-specific response or request changes without overloading OpenAI types.

### 2) Add a new function type

Rejected because provider extension is sufficient and aligns with existing Milvus `TextEmbedding` architecture.

### 3) Add rerank support in the same change

Rejected to keep the first PR small and reviewable. Qianfan rerank can be handled in a follow-up PR.

## References

- Baidu Qianfan embeddings API: https://cloud.baidu.com/doc/qianfan-docs/s/Um8r1tpwy
