# MEP: Optional e-means IVF centroid training

- **Created:** 2026-11-19
- **Author(s):** @jja725
- **Status:** Under Review
- **Component:** Index
- **Related Issues:** #55762, zilliztech/knowhere#1983

## Summary

Add `e_means_epochs` as an optional build parameter for CPU IVF indexes. A
positive value selects native e-means in Knowhere for coarse centroid training.
Omitting it, or setting it to zero, preserves the current trainer.

## Motivation

IVF index building spends substantial work training its coarse quantizer.
E-means combines mini-batches with exponential moving sums and counts. The
paper reports fewer training passes than Lloyd k-means; these results do not
establish a Milvus speedup. This change makes the algorithm selectable so its
build time and recall can be evaluated on users' data.

## Public Interfaces

Use the existing index-parameter map; no SDK or protobuf changes are needed:

```python
index_params.add_index(
    field_name="vector",
    index_type="IVF_FLAT",
    metric_type="L2",
    params={"nlist": 1024, "e_means_epochs": 2.5},
)
```

The epoch budget may be fractional. Supported metrics are L2 and COSINE.
CPU IVF_FLAT, IVF_PQ, IVF_SQ/IVF_SQ8, IVF_FLAT_CC, IVF_SQ_CC, SCANN,
IVF_RABITQ and IVF_RABITQ_FASTSCAN use the same coarse-training implementation.
GPU indexes, binary IVF, HNSW and IP clustering are not supported by this option.
The ordinary `knowhere.<index>.build` defaults mechanism can supply the parameter.

## Design Details

Milvus validates the algorithm-specific parameter and passes it through the
existing Knowhere configuration path. Knowhere resolves the epoch budget in
each numeric CPU IVF implementation and forwards it to its vendored FAISS
level-1 quantizer. A positive budget takes precedence over `use_super_kmeans`.
PQ codebook training is unchanged.

The native CPU trainer implements Algorithm 1 from the paper, using the batch
and step scaling laws shipped with FAISS 1.16.0. It uses a cosine step schedule,
revives low-mass clusters every five batches, and computes closing centroids
from the moving assignments of the final pass. Low-dimensional defaults match
the reference: quality batches at dimension <= 16 and no revival at dimension
<= 2. A remainder batch is retained. For budgets >= 1, the final pass is complete;
any fractional pass precedes it. Budgets below 1 use a partial final pass.

Training coordinates are FP32, while sums and moving counts are FP64. Seeded
permutations use the native RNG and are not bit-identical to NumPy. The existing
training sample cap and redo count are retained. Spherical centroids are used
for the existing cosine path. Final objective evaluation is an additional
assignment pass for selecting the best redo. Training checks interruption at
each logical batch. The code has no Python or GPU runtime dependency.

Invalid, non-finite and unsupported parameter combinations fail validation.
A positive budget that rounds to zero training rows fails at training time.
The native trainer rejects non-finite vectors and distance overflow.

## Compatibility, Deprecation, and Migration Plan

This requires [Knowhere #1984](https://github.com/zilliztech/knowhere/pull/1984).
Land that dependency and refresh the pin to its merged commit before merging
this Milvus change. Existing indexes and index formats are
unchanged; trained centroids are serialized by the existing IVF writers.
Readers do not need e-means to search those centroids. Ordinary index metadata
retains the requested build parameter. During rolling upgrades, enable the
option only after all index-building nodes use the updated Knowhere version;
older builders may ignore an unknown parameter. No default is changed.

## Test Plan

- Native trainer: last-pass means, fractional budgets, deterministic training,
  invalid inputs, spherical normalization and wide accumulation.
- Knowhere IVF: L2/cosine build, exact-baseline recall, serialization and reload.
- Milvus: epoch parsing, invalid values, unsupported metrics and index families.
- Before recommending a default, benchmark wall-clock training time and query
  recall on representative datasets against the existing trainer.

## Rejected Alternatives

Embedding `faiss.contrib.e_means` would require Python in the index-building
process. Automatically replacing the current trainer would make an unmeasured
performance/recall tradeoff for existing deployments.

## References

- [e-means paper](https://arxiv.org/abs/2611.09501)
- [FAISS 1.16.0 reference implementation](https://github.com/facebookresearch/faiss/tree/v1.16.0/contrib/e_means)
