# Struct Element-Level Hybrid Search

This document describes the intended end state for hybrid search when a vector
sub-field inside a struct array field is searched at element level.

This document does not change embedding-list search semantics. Embedding-list
search on a struct-array vector sub-field is treated like normal row-level
vector search.

## Concepts

A struct array field stores multiple struct elements per row. A vector sub-field
inside that struct array can be searched in two forms:

```text
element-level search  One query vector is matched against individual struct elements.
embedding-list search A list of query vectors is matched as one row-level request.
```

Only element-level search produces element-level candidates.

For example:

```text
structA: array<struct{
    image_vec: float_vector,
    text_vec: float_vector,
    tag: varchar
}>
normal_vector: float_vector
```

Element-level search on `structA[image_vec]` produces hits identified by:

```text
(primary_key, parent_struct_field, element_index)
```

Embedding-list search on `structA[image_vec]` and normal vector search on
`normal_vector` both produce row-level hits identified by:

```text
(primary_key)
```

Hybrid search must decide whether element-level hits from element-level
struct-array search remain element-level for rerank, or whether they are
collapsed to row-level candidates before rerank.

## Request Model

Row-level collapse behavior is configured per sub-search request, not on the
top-level hybrid search request.

This is required because each sub-search has its own `anns_field`, metric,
filter, limit, and collapse behavior. A single hybrid request can search
multiple struct sub-fields with different row-level collapse strategies.

User-facing row-collapse API example:

```python
AnnSearchRequest(
    data=[query_image],
    anns_field="structA[image_vec]",
    param={
        "metric_type": "COSINE",
        "params": {
            "ef": 100,
            "element_scope": {
                "collapse": {
                    "strategy": "topk_sum",
                    "topk": 3,
                },
            },
        },
    },
    limit=100,
)
```

Equivalent SDKs may expose typed options, but they should still serialize to the
sub-search request:

```go
annReq := client.NewAnnRequest("structA[image_vec]", limit, vectors).
    WithElementCollapse(client.ElementCollapseTopKSum, client.WithTopK(3))
```

The top-level hybrid request still owns only hybrid-level options such as final
`limit`, `offset`, output fields, consistency, and reranker configuration.

Embedding-list search on `structA[image_vec]` must not use `element_scope`; it is
already row-level and follows the same hybrid behavior as `normal_vector`.

If `element_scope` is missing, the row-level collapse strategy defaults to `max`
whenever row-level collapse is needed.

## Candidate Scope

Hybrid search infers final candidate scope from the sub-search types.

```text
all sub-searches are element-level and use the same parent struct array
  -> element-level hybrid, no collapse

otherwise
  -> row-level hybrid
  -> every element-level sub-search is collapsed to row candidates
  -> collapse strategy defaults to max unless element_scope.collapse overrides it
```

Element-level hybrid example:

```python
image_req = AnnSearchRequest(
    data=[query_image],
    anns_field="structA[image_vec]",
    param={
        "metric_type": "COSINE",
        "params": {"ef": 100},
    },
    limit=100,
)

text_req = AnnSearchRequest(
    data=[query_text],
    anns_field="structA[text_vec]",
    param={
        "metric_type": "COSINE",
        "params": {"ef": 100},
    },
    limit=100,
)

client.hybrid_search(
    collection_name,
    [image_req, text_req],
    ranker=RRFRanker(),
    limit=20,
)
```

Both sub-searches are element-level and use sub-fields of `structA`, so final
results are element-level.

## Compatibility Matrix

Hybrid search can combine row-level and element-level sub-searches only when the
candidate identity is well-defined.

Sub-search types:

```text
normal vector       A top-level vector field, such as normal_vector.
struct emb-list     Embedding-list search on a struct-array vector sub-field.
struct element      Element-level search on a struct-array vector sub-field.
```

Compatibility:

```text
left \ right      normal vector   struct emb-list   struct element
normal vector     row-level       row-level         row-level
struct emb-list   row-level       row-level         row-level
struct element    row-level       row-level         element-level if same parent, else row-level
```

Behavior:

```text
row-level
  Final candidates are keyed by primary key.
  Element-level sub-searches are collapsed before rerank.

element-level if same parent
  Allowed only when all element-level sub-searches use sub-fields of the same
  parent struct array. Final candidates are keyed by
  (primary_key, parent_struct_field, element_index).
```

For two `struct element` sub-searches with different parent struct arrays,
element offsets do not share identity. The request is still valid, but the final
candidate scope is row-level and both element-level sub-searches are collapsed.

## Row-Level Collapse

When inferred candidate scope is row-level, all element hits from the same row
are aggregated into one row-level candidate before hybrid rerank.

The collapse strategy is provided in that same sub-search request:

```json
{
  "element_scope": {
    "collapse": {
      "strategy": "max"
    }
  }
}
```

Supported initial strategies:

```text
max
sum
avg
topk_sum
topk_avg
```

Strategy behavior:

```text
max       Keep the best element score for the row.
sum       Sum all returned element scores for the row.
avg       Average all returned element scores for the row.
topk_sum  Sum the best K returned element scores for the row.
topk_avg  Average the best K returned element scores for the row.
```

`topk` is required for `topk_sum` and `topk_avg`, and invalid for strategies that
do not use it.

Collapse operates on the returned element hits from that sub-search. It does not
scan every element in a row after ANN search. Therefore, the sub-search `limit`
controls both recall and the number of elements available for aggregation.

Metric direction must be respected:

```text
positively related metrics: larger score is better
negatively related metrics: smaller score is better
```

## Element-Level Hybrid Rerank

Element-level hybrid rerank is used only when every sub-search is element-level
and all sub-searches refer to vector sub-fields under the same parent struct
array.

Valid:

```text
structA[image_vec] + structA[text_vec]
```

These two sub-fields share the same element identity:

```text
(primary_key, "structA", element_index)
```

The hybrid reranker should rank element candidates using that key. Final results
may remain element-level and expose the matched `element_index`.

Row-level fallback:

```text
structA[image_vec] + structB[text_vec]
```

Even if both hits have `element_index = 3`, those offsets refer to different
arrays. They must not be treated as the same element. The hybrid search falls
back to row-level scope and collapses both element-level sub-searches before
rerank.

## Validation Rules

1. `element_scope.collapse` is valid only on element-level search over
   struct-array vector sub-fields when the inferred candidate scope is row-level.
2. Normal vector fields are always row-level.
3. Embedding-list search on struct-array vector sub-fields is always row-level.
4. Normal vector sub-searches and embedding-list sub-searches must reject
   non-default element collapse settings.
5. If row-level scope requires collapsing element-level hits and collapse config
   is omitted, use `max`.
6. If inferred candidate scope is element-level, reject `element_scope.collapse`
   because no row-level collapse is performed.
7. Hybrid search supports only plain top-K for struct-array vector sub-searches.
   Element-level and embedding-list sub-searches reject group-by, range search,
   and search iterator.
8. `sum` and `topk_sum` collapse strategies are valid only for positively
   related metrics such as `IP` and `COSINE`. Negative distance metrics such as
   `L2` must use `max`, `avg`, or `topk_avg`.

## Result Semantics

For row-level hybrid search:

```text
result key: primary_key
duplicates: no duplicate primary keys in final results
element_index: not returned
```

For element-level hybrid search:

```text
result key: (primary_key, parent_struct_field, element_index)
duplicates: no duplicate element keys in final results
element_index: returned
```

## Execution Order

The intended pipeline is:

```text
1. Execute each sub-search.
2. Reduce each sub-search result.
3. Infer final candidate scope from all sub-searches.
4. If scope is row-level, collapse every element-level sub-search to row
   candidates using that sub-search's collapse strategy.
   Normal vector sub-searches and embedding-list sub-searches are already
   row-level.
   If scope is element-level, keep element candidates.
5. Apply hybrid rerank.
6. Assemble output fields according to the final result level.
```

This keeps collapse local to the sub-search that produced element-level hits,
while keeping the hybrid reranker responsible only for combining already
normalized candidate lists.
