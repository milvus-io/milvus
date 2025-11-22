# Text search: `text_match` and `minimum_should_match` User Guide

## Introduction

This guide explains how to use the lexical `text_match` filter and the optional `minimum_should_match` parameter to control how many terms in a text query must match for a document to be considered a hit.

## Feature overview

**1. `text_match` filter**

`text_match(field, "query")` performs lexical matching (BM25 / inverted-index) against a `varchar`/text field and can be used inside boolean filter expressions.

**2. `minimum_should_match` parameter**

An optional parameter for `text_match` that requires at least a given number of query terms to match.

- Integer (e.g. `2`) — at least two terms must match. Only integer counts are supported. The number is capped at 1000. 
- Scope: applicable only to lexical/BM25-style `text_match` (indexed text). It does not apply to vector/ANN scoring.

**3. Hybrid usage**

`minimum_should_match` is commonly used in hybrid searches (vector + lexical) to tighten the lexical gating before ANN/reranking stages.

## Get started

Below are concise Go examples using the same client primitives 

### Example 1 — Single BM25 lexical search with a minimum

```go
query := "artificial intelligence machine learning"
minShouldMatch := "1" // at least one term must match
filter := fmt.Sprintf(`text_match(%s, "%s", minimum_should_match=%s)`, "document_text", query, minShouldMatch)

res, err := client.Search(ctx, milvusclient.NewSearchOption(
    "my_collection",
    5,
    []entity.Vector{entity.Text(query)},
).WithANNSField("text_sparse_vector").
    WithFilter(filter).
    WithOutputFields("id", "title", "document_text"))

// handle res / err...
```

### Example 2 — Hybrid weighted fusion (title + text)

```go
titleReq := milvusclient.NewAnnRequest("title_sparse_vector", 20, entity.Text(query)).
    WithFilter(fmt.Sprintf(`text_match(%s, "%s", minimum_should_match=1)`, "title", query))

textReq := milvusclient.NewAnnRequest("text_sparse_vector", 20, entity.Text(query)).
    WithFilter(fmt.Sprintf(`text_match(%s, "%s", minimum_should_match=2)`, "document_text", query))

hybridRes, err := client.HybridSearch(ctx,
    milvusclient.NewHybridSearchOption("my_collection", 10, titleReq, textReq).
        WithReranker(milvusclient.NewWeightedReranker([]float64{0.6, 0.4})).
        WithOutputFields("id", "title", "document_text"),
)

// handle hybridRes / err...
```

## Best practices

- **Start with conservative minimums.** For short queries prefer small integer values (1 or 2);
- **Measure recall vs precision.** Increasing `minimum_should_match` raises precision but lowers recall.
- **Use in hybrid searches to reduce reranker load.** Applying lexical gating early reduces the number of candidates passed to expensive rerankers.

## Notes

- `minimum_should_match` is evaluated by the lexical/text-match component before reranking.
- If omitted, `text_match` uses default behavior (no explicit minimum).
