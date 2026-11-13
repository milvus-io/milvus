# MEP: Resource Group Compliance Metrics

- **Created:** 2026-11-03
- **Author(s):** @brianzilliz
- **Status:** Under Review
- **Component:** Coordinator
- **Related Issues:** None

## Summary

Export resource group load compliance as native Prometheus metrics, using the
same evaluator as `GET /management/replica/loadconfig/compliance`. Evaluate on
each scrape of the active coordinator and publish independent resource group
verdicts without retaining an earlier successful result.

## Motivation

Resource group membership, replica counts, and collection checkpoints do not
establish whether a resource group can serve its expected workload. Consumers
that calculate usable capacity need the full compliance verdict, including live
shard serviceability, query visibility, resource cleanup, and WAL placement.
A cluster-wide verdict would also exclude healthy groups when another group is
still loading or releasing resources.

## Public Interfaces

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `milvus_querycoord_resource_group_compliance` | Gauge | `rg` | `1` when the group satisfies its compliance obligations, otherwise `0`. |
| `milvus_querycoord_resource_group_compliance_check_success` | Gauge | None | `1` when the current evaluation completes, otherwise `0`. |

An evaluation failure emits only check success `0`; no group verdicts are
published for that scrape. Standby coordinators and stopped coordinators emit
neither metric. An omitted group is not an explicit Ready verdict.

The HTTP endpoint retains its response fields, status codes, summary behavior,
and per-resource-group output, including the existing empty array behavior.

## Design Details

Extract the HTTP handler's evaluation into a shared function returning both the
HTTP response and a map of resource group verdicts. Preserve checks for expected
replica placement and counts, live shard leaders, query visibility, leaked
resources, and incoming and outgoing WAL placement. A failure attributed to one
group does not overwrite the verdict of another group.

For an instance with no loaded collections, seed the metric map with current
resource groups, configured load targets, and the default resource group. Keep
any existing WAL or configuration violation; do not create a Ready verdict when
an unattributed violation prevents establishing compliance. These extra entries
are specific to metrics and do not alter the HTTP response.

Register a custom collector with the QueryCoord metrics registry. The active
coordinator installs an evaluator callback after startup. Each collection uses a
fresh evaluation with a five-second context deadline and creates constant gauge
samples. Deleted groups disappear on subsequent scrapes. There is no background
cache, persistent state, or metadata migration.

The collector holds a read lock through evaluation and sample publication.
Clearing the callback takes the write lock, so coordinator shutdown waits for
in-flight collection before stopping the evaluator's dependencies. A standby
coordinator does not clear the active coordinator's callback.

## Compatibility, Deprecation, and Migration Plan

The metrics are additive and require no configuration change. Older versions do
not export either signal. Consumers can distinguish unsupported versions from
an evaluation failure by the presence of the check-success metric and must
require explicit group readiness when compliance is available. Deploy the signal
before enabling consumers that depend on it. Rollback removes the new signals
without changing stored data or the HTTP API.

## Test Plan

Extend the existing compliance cases to compare HTTP and metric verdicts for
independent groups, placement, serviceability, cleanup, WAL failures, and empty
collections. Verify request errors and evaluation failures, and test coordinator
activation and shutdown. Collector tests cover deleted groups, recovery after
failure, absence on standby, and shutdown during an in-flight evaluation; run
these with the Go race detector. Coordinator tests require the normal Milvus
native build dependencies.

## Rejected Alternatives

- Inferring compliance from membership or checkpoint metrics omits obligations
  that the existing compliance evaluator already checks.
- Exporting only cluster readiness excludes healthy groups during changes to
  other groups.
- Caching the last Ready result can report stale capacity after an evaluation
  failure or group deletion.
- Polling the HTTP endpoint from a separate exporter duplicates lifecycle and
  error handling and introduces an additional deployment dependency.

## References

- [Compliance evaluator and HTTP handler](../../../internal/coordinator/restful_replica.go)
- [Native compliance collector](../../../pkg/metrics/querycoord_compliance.go)
