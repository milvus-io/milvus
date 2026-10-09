# MEP: SGLang Reranker Integration

- **Created:** 2026-10-09
- **Author(s):** Rudra Prasad Bhuyan
- **Status:** Draft
- **Component:** Proxy
- **Related Issues:** #53843
- **Released:** TBD

## Summary

Add `provider: sglang` to model reranking. The provider calls an SGLang
server's `/v1/rerank` endpoint and returns one score for every candidate in
Milvus request order.

## Motivation

Milvus model reranking supports several remote provider protocols but has no
adapter for SGLang's ready reranking endpoint. Users must currently implement
the reranking call and score alignment outside Milvus.

## Public Interfaces

The model reranker accepts the existing provider parameters with
`provider: sglang`:

```text
provider: sglang
endpoint: http://sglang:30000
model_name: BAAI/bge-reranker-v2-m3
credential: optional-credential-name
timeout_ms: optional-request-timeout
max_client_batch_size: optional-batch-size
```

The endpoint is the SGLang server base URL. Credentials follow existing model
provider resolution and are sent as a Bearer token when non-empty. The provider
can be enabled or disabled through `function.rerank.model.providers.sglang`.
No proto, SDK, persisted metadata, or metric interfaces change.

## Design Details

The existing `RerankModelExpr` batches candidates and invokes the existing
`rerank.ModelProvider` interface. The SGLang provider implements that
interface, so it reuses batching, configuration lookup, credentials, timeout
resolution, and downstream score handling.

For each batch, it posts to `/v1/rerank`:

```json
{
  "model": "BAAI/bge-reranker-v2-m3",
  "query": "query text",
  "documents": ["candidate 0", "candidate 1"]
}
```

SGLang returns an array of objects containing at least `index` and `score`.
The response order is not treated as candidate order. Each score is placed at
the response index, which is the original position in the request batch.

Every candidate must occur exactly once. Missing results, duplicate indexes,
negative indexes, and indexes outside the candidate batch are errors. JSON
uses pointer fields for `index` and `score`, so an omitted field is rejected
instead of being mistaken for the numeric zero value. Malformed JSON and HTTP
errors are returned through the existing model-service request path.

The SGLang client uses a context-aware form of the shared JSON request helper.
Its request context is derived from the rerank execution context and bounded by
the configured timeout. Cancellation and timeout terminate an in-flight HTTP
request and prevent retries after the context has ended.

## Compatibility, Deprecation, and Migration Plan

This is an additive provider. Existing providers and their HTTP request
behavior are unchanged. Existing collections do not require migration. Users
select SGLang explicitly in a model reranker function and need a reachable,
compatible SGLang reranker service.

## Test Plan

Focused HTTP-server tests verify the request path, payload, authorization, and
mapping of out-of-order response indexes to original candidates. They cover
duplicate, missing, negative, and out-of-range indexes; absent index or score
fields; malformed JSON; HTTP API errors; request timeout; and caller context
cancellation.

The tests use `httptest` and do not require a GPU, SGLang binary, model
download, Docker stack, or a live remote service.

## Rejected Alternatives

- Reuse the TEI or vLLM adapter: their endpoint paths and response schemas do
  not match SGLang's `/v1/rerank` protocol.
- Add a new reranking pipeline: the existing provider interface already owns
  batching and candidate-score alignment.
- Accept partial or positional results: this would attach a score to the wrong
  candidate or silently leave candidates unscored.

## References

- [Milvus issue #53843](https://github.com/milvus-io/milvus/issues/53843)
- [SGLang rerank API example](https://docs.sglang.ai/basic_usage/native_api.html)
