# HNSW_SQ Support in the Go Client

update: 6.25.2026, by [Kushal Shetter S](https://github.com/Kushals2004)

related issue: [#44635](https://github.com/milvus-io/milvus/issues/44635)

## 1. Background

`HNSW_SQ` is the HNSW graph index combined with scalar quantization (SQ). The raw
float vectors stored in the graph are compressed with a scalar quantizer, which
substantially reduces the memory footprint of the index at the cost of a small
amount of recall. An optional refinement step re-ranks the top candidates using a
higher-precision representation to recover that recall.

The index type is already supported by Milvus on the server side and is exposed by
the Python SDK. The Go client (`client/index`), however, provides constructors for
`HNSW`, `IVF_SQ8`, `IVF_RABITQ`, `SCANN`, etc., but has no constructor for
`HNSW_SQ`. Users following the Go SDK have no first-class way to build the index or
to pass its search-time parameters. This document describes closing that
feature-parity gap.

## 2. Goals

- Add `HNSW_SQ` to the Go client's `IndexType` enumeration.
- Provide an index constructor that emits the build parameters Milvus expects:
  `M`, `efConstruction`, `sq_type`, and the optional `refine` / `refine_type`.
- Provide a search-parameter (`AnnParam`) type carrying `ef` and the optional
  `refine_k`.
- Keep the public API consistent with the existing index constructors so the change
  is small and idiomatic.

### Non-goals

- `HNSW_PQ` and `HNSW_PRQ` (which have the same gap) are out of scope here and can
  be handled as follow-ups using the same pattern.
- No server-side / knowhere changes — this is a pure client-side addition.

## 3. Parameters

### Build parameters

| Param            | Meaning                                                             | Notes                                  |
| ---------------- | ------------------------------------------------------------------- | -------------------------------------- |
| `M`              | Max neighbors per node in the HNSW graph                            | Same as plain `HNSW`                   |
| `efConstruction` | Candidate neighbors considered while building                       | Same as plain `HNSW`                   |
| `sq_type`        | Scalar quantizer type: `SQ6`, `SQ8`, `BF16`, `FP16`                 | Required                               |
| `refine`         | Whether the refinement step is enabled                              | Optional; only emitted when enabled    |
| `refine_type`    | Precision of the data used for refinement (must exceed `sq_type`)   | Required when `refine` is enabled      |

### Search parameters

| Param      | Meaning                                                          | Notes                  |
| ---------- | ---------------------------------------------------------------- | ---------------------- |
| `ef`       | Search-time exploration factor                                   | Same as plain `HNSW`   |
| `refine_k` | Refinement magnification factor; top `refine_k * limit` re-ranked| Optional               |

The client passes `sq_type` / `refine_type` through as strings; validation of the
allowed values stays on the server, matching how the Go client already treats the
refine type of `IVF_RABITQ`.

## 4. API Design

The implementation mirrors the existing `hnswIndex` and the optional-refinement
pattern already used by `ivfRabitQIndex`.

```go
// Build: refinement is opt-in via a chained builder.
idx := index.NewHNSWSQIndex(entity.L2, 16, 200, "SQ8")
idx = idx.WithRefineType("FP32") // optional

// Search:
ap := index.NewHNSWSQAnnParam(64)
ap.WithRefineK(2.0) // optional
```

- `NewHNSWSQIndex(metricType, m, efConstruction, sqType)` returns `*hnswSQIndex`.
- `(*hnswSQIndex).WithRefineType(refineType)` enables refinement and sets the type.
  When refinement is not enabled, `refine` / `refine_type` are omitted from the
  emitted params entirely.
- `NewHNSWSQAnnParam(ef)` returns `*hnswSQAnnParam`, embedding the existing HNSW ann
  param so it already carries `ef`.
- `(*hnswSQAnnParam).WithRefineK(refineK float64)` adds `refine_k`. It is a
  `float64` because the magnification factor may be fractional.

## 5. Affected Files

- `client/index/common.go` — add the `HNSWSQ IndexType = "HNSW_SQ"` constant.
- `client/index/hnsw.go` — add `hnswSQIndex`, its constructor and `WithRefineType`,
  and `hnswSQAnnParam` with `WithRefineK`.
- `client/index/hnsw_test.go` — unit tests for the emitted build / search params
  (with and without refinement).

## 6. Test Plan

Unit tests assert the emitted parameter maps:
- build params with refinement disabled (no `refine` / `refine_type` keys present);
- build params with refinement enabled (`refine=true`, `refine_type` set);
- search params carry `ef` and `refine_k`.

`go test ./index/` passes and `gofmt` is clean. End-to-end index creation against a
running Milvus instance is covered by existing SDK integration tests once the index
type is recognized by the client.

## 7. Future Work

Apply the same pattern to add `HNSW_PQ` and `HNSW_PRQ` to the Go client.
