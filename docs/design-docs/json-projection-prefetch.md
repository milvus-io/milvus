# JSON stats prefetch and filter evaluation

Related issue: https://github.com/milvus-io/milvus/issues/50583

A sealed-segment predicate that selects `JsonStats` on the existing driver
prefetch pool warms the projection for its JSON path and type. Numeric predicates
request both integer and floating-point projections. CONTAINS uses the array
projection and EXISTS uses all matching path components. When evaluation needs
shared BSON data, prefetch warms the posting index used to find its rows.
Each selected column submits its cell IDs together through `PrefetchChunks`.

The filter starts evaluating inputs in the existing order. Each leaf joins its
own prefetch before using its reader. For example, in `A AND B`, A can run while
B's projection is loading. Selecting a batch size, grouping LIKE predicates, or
advancing a skipped input's cursor can still require the execution path of B.
Path selection is shared between the prefetch worker and the execution thread
through `EnsureExecPathDetermined()`.

A speculative JSON read can fail for a predicate that is subsequently skipped.
If evaluation reaches that predicate, the normal demand read reports any
persistent error. Driver close joins outstanding work before releasing the
request context.
