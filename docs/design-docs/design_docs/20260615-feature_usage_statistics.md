# MEP: Feature Usage Statistics

- **Created:** 2026-06-15
- **Author(s):** TBD
- **Status:** Draft
- **Component:** Metrics | Proxy | QueryNode | DataNode | C++
- **Related Issues:** TBD
- **Released:** TBD

## Summary

This design introduces a lightweight feature usage statistics mechanism for recording when selected Milvus feature code paths are exercised. Milvus exposes low-cardinality Prometheus counters from both Go and C++ code paths. External monitoring systems can aggregate these metrics over a time window when feature-level usage visibility is needed.

The first version is intentionally approximate. It is not exact request counting or persistent feature-state tracking.

## Goals

- Track recent exercise of selected Milvus feature code paths with minimal runtime overhead.
- Support feature recording from both Go and C++ without adding cgo calls on hot paths.
- Avoid duplicate Prometheus metric-family conflicts when Go and C++ report related features.
- Keep metric labels bounded and safe for external aggregation.

## Non-Goals

- Exact per-request feature usage counts.
- Durable feature usage state across process restarts.
- Tenant, collection, field, query, or user-level analytics inside Milvus.
- Outbound telemetry from Milvus OSS.

## Metric

Milvus exposes one shared metric shape:

```text
milvus_feature_report_total{feature="<feature>", source="<go|cpp>"}
```

The counter counts throttled reports, not raw feature invocations. It is intended to be queried as a recent-presence signal over a time window.

`feature` is a stable enum, for example:

```text
hybrid_search
partition_key
dynamic_field
bm25_function
resource_group
bulk_import
```

`source` identifies the language/runtime that emitted the observation. This prevents Go and C++ metric ownership conflicts while allowing aggregation to sum across both sources.

Forbidden labels include collection name, field name, username, tenant id, query text, expression text, request id, and any unbounded identifier.

## Design

Milvus provides two lightweight recorders:

- Go recorder under the Go metrics package.
- C++ recorder under the common C++ library.

Both recorders use the same feature-name spec. The spec can initially be maintained as a small shared documentation table, and later generated into Go and C++ constants if the feature list grows.

Each recorder owns only its local runtime's metric emission:

```go
metrics.FeatureHybridSearch.Record()
```

```cpp
feature_usage::Record(feature_usage::HybridSearch);
```

The Go recorder emits `source="go"`. The C++ recorder emits `source="cpp"`. Go code does not call into C++ for feature recording, and C++ code does not call into Go.

## Throttling

Feature usage statistics are approximate signals, so each process reports at most once per feature per interval, for example once per hour.

Each recorder maintains in-memory throttle state:

```text
feature -> next_allowed_report_time
```

On `Record(feature)`:

1. Read the next allowed report time.
2. Return immediately if the current time is before it.
3. Use an atomic compare-and-swap to move the next allowed time forward.
4. Only the caller that wins the CAS increments the Prometheus counter.

This keeps hot-path overhead low. Most calls perform only an atomic read, and actual counter increments are rare. Process restarts reset throttle state, which is acceptable for approximate usage visibility.

## Aggregation

Deployment identity is attached outside Milvus during scraping or downstream ingestion. Milvus itself does not expose deployment-specific identity in this metric.

Example PromQL for deployments that reported each feature in the last 30 days:

```promql
count by (feature) (
  sum by (cluster, feature) (
    increase(milvus_feature_report_total[30d])
  ) > 0
)
```

Because the aggregation sums by `cluster, feature`, the `source` label is intentionally removed. A feature reported from either Go or C++ counts as usage for that deployment.

## Tradeoffs

The design can miss rare usage if a process records a feature and exits before Prometheus scrapes it. It can also report a feature again after process restart because throttle state is in memory. Both behaviors are acceptable because the output is directional usage visibility, not exact accounting.

Adding the `source` label slightly increases cardinality, but the label has only two values and avoids duplicate metric-family conflicts between Go and C++ exporters.
