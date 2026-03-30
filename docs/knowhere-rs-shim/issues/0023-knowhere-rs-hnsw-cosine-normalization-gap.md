# 0023: knowhere-rs HNSW cosine path returns inner-product-like scores on non-unit vectors

## Summary

`knowhere-rs` HNSW returns large inner-product-like scores for `metric_type=COSINE` when vectors are not unit-normalized. In Milvus standalone this shows up on loaded `floatVec` HNSW searches as scores around `40.x`, while the same collection's `fp16Vec` cosine path stays in the expected `0.x` range.

This is a `knowhere-rs` core issue. The Milvus shim can work around it by normalizing float vectors for cosine before calling the Rust HNSW FFI, but the underlying behavior remains incorrect.

## Evidence

Remote authoritative run on `189.1.218.159`:

- standalone diagnostic test: `TestDebugHybridRangeDiag`
- collection: `Int64MultiVec`
- float index params: `HNSW`, `metric_type=COSINE`

Observed responses:

- `floatVec-plain scores=[0.21878535 40.657543 40.6846 ...]`
- `floatVec scores=[40.657543 40.6846 40.702637 ...]`
- `fp16Vec scores=[0.8524915 0.8510583 0.8449741 ...]`
- `weightedHybrid scores=[32.71957 32.716667 32.706177 ...]`

Authoritative standalone log from `/data/work/milvus-rs-integ/milvus-var/logs/standalone-stage1.log` proves the shim passed cosine through:

- `[knowhere-rs-shim][hnsw-ensure] config_metric=COSINE resolved_metric=COSINE dim=128 existing_handle=0`
- `[knowhere-rs-shim][hnsw-search] config_metric=COSINE node_metric=COSINE topk=10 nq=1 dim=128 raw_rows=9000 results=10 bitset=9000`

So this is not a build-config mismatch on the Milvus side. The HNSW handle is created and searched as `COSINE`, yet returned scores still look like unnormalized inner product.

## Impact

- standalone float32 HNSW search scores are semantically wrong for cosine
- hybrid search reranking is polluted because one branch contributes `40.x`-scale scores instead of cosine-scale scores
- existing local unit-vector smoke cases can miss the bug because unit vectors make cosine and inner product coincide

## Minimal Reproduction Shape

Use non-unit vectors where cosine ranking differs from inner-product ranking, for example:

- query: `[1, 1, 0, 0]`
- base A: `[100, 0, 0, 0]`
- base B: `[1, 1, 0, 0]`

Expected:

- cosine prefers `B`
- inner product prefers `A`

If HNSW returns `A` under `metric_type=COSINE`, the path is still effectively using unnormalized IP semantics.

## Current Mitigation

- keep the issue open against `knowhere-rs`
- apply normalization in the Milvus shim for `float + HNSW + COSINE` before `train/add/search`

