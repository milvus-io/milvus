# Per-segment Load Resource Estimates Based on LoadDiff

Status: Design and implementation draft. Date: 2026-11-26.

## Scope

C++ uses the LoadDiff that will actually execute to estimate the additional peak resources needed to load a sealed segment and its final resource footprint. Go continues to own load scheduling, resource reservations, admission, GPU memory checks, and the final serving state.

This phase does not introduce a C++ pacer, AcquireLoadPermit, a shared reservation ledger across segments, or dynamic deductions based on other loads in progress. It also leaves the committed and committed + RSS admission constraints unchanged.

## Flow

```text
Go creates an unpublished segment / pins an existing segment
  → C++ PrepareLoadPlan
      Capture metadata and state identity
      Read planning metadata
      Build LoadDiff
      Estimate loading and loaded resources from the load operations
  ← Plan handle + memory/disk estimates
Go adds Go-owned overhead and GPU requirements, then reserves using existing policy
  Rejected → Release the plan and any unpublished segment
  Accepted → C++ ExecuteLoadPlan
               Validate state → Execute the same LoadDiff → Finalize → Publish
             Go completes the existing follow-up steps
             Release the reservation after load subtasks finish and cleanup completes
```

Preparation and estimation form one logical operation. The Go wrapper may implement this with asynchronous preparation followed by a computation-only C API call. The internal estimation protobuf remains separate from persisted metadata.

## Resource Semantics

- `loading`: The additional peak resources that executing the plan may require beyond resources already present, including new resources and temporary buffers. When replacing an index, its old resources cannot be deducted before they are released.
- `loaded`: The estimated final footprint of the target segment. This is not a second reservation and must not simply be added to `loading`.
- These values are estimates derived from current metadata and runtime configuration, not measured RSS. They do not guarantee coverage of unknown allocations inside plugins.
- Go may apply an admission ratio using existing policy; this phase adds no configuration. A ratio below 1 represents overcommit and cannot be described as a hard upper bound.
- C++ estimates field/index concurrency, mmap usage, and streaming scratch budgets within a segment. Requirements across segments are added conservatively, without relying on a snapshot of other loads currently in progress.
- GPU memory checks remain in Go. The current check reads per-device usage and simulates placement; it is not a GPU reservation shared across requests. This phase adds no `gpu` / `gpu_count` arrays to the C ABI.

## Plan Ownership and Lifetime

```cpp
struct PreparedLoadPlan {
    Segment* owner;                 // Go protects its lifetime
    weak_ptr<const State> base;     // Does not retain old runtime resources
    SegmentLoadInfo target;
    FieldLayout fields;
    LoadDiff diff;
    bool consumed;                 // Single use, protected by the segment update lock
};
```

Preparation holds the update lock briefly to capture state. It does not hold that lock, a query read lease, or a cache-cell pin while reading the manifest or waiting for Go admission.

Execution checks state identity under the segment update lock. If the state has changed, the plan becomes invalid and returns a retriable error. It must not recompute a larger diff under the old reservation. The caller must release the old plan, prepare a new one, and repeat admission. Successfully published resources belong to the segment and are not destroyed by ReleasePlan.

Cancellation uses the existing CFuture/OpContext mechanism. The native operation must finish before releasing the plan, segment pin, or load reservation. Update locks are independent across segments.

## Planning Metadata and Payload Reads

Prepare may read a fixed-version manifest and the field membership directory of legacy V2 column groups. It does not read field values or index payloads, construct runtime readers or indexes, or perform warmup.

`LoadPlanningMetadata()` explicitly indicates that metadata I/O may occur. `GetColumnGroups()` only accesses the prepared cache. Query threads do not trigger implicit reads of published metadata.

Missing field sizes or encrypted-slice information retain conservative estimates. Unknown encryption status is not treated as unencrypted. This phase does not change persisted meta/loadinfo protobuf definitions.

## Go and C++ Responsibilities

| Work | Owner |
|---|---|
| Planning, execution, and internal publication of sealed fields/indexes | Worker segcore |
| Node admission, GPU checks, scheduling across segments, cancellation, and retries | Worker Go |
| Downloading, parsing, and assembling delta logs, then calling native deletion APIs | Worker Go + segcore, preserving the existing order |
| Sealed bloom / BM25Stats | Delegator Go PK/IDF oracle |
| Manager registration and distribution visibility | Go |

Workers and delegators may run on different nodes. Their bloom/BM25Stats costs must not all be folded into the worker's native resource estimate.

External collection reopen currently publishes the native view first, replays deletes in Go, and updates Go load info after replay succeeds. This phase preserves that behavior and does not provide an atomic commit across Go and C++. A failure after native publication must not be treated as if publication never happened.

## Stateless Callers

QueryCoord has no local segment instance. It continues to use the stateless estimation entry point and shares the underlying size formulas. Growing segments and other paths outside prepared sealed loading retain their existing protocol.

## Required Validation

1. Unchanged fields and indexes do not contribute to the additional loading peak; replacements do not deduct old resources prematurely.
2. Initial loads, empty diffs, default fields, index replacements, and field reloads.
3. V1/V2/V3, manifest metadata reads, and resolving legacy V2 field membership.
4. mmap, mixed eager/lazy projections, enabled/disabled budgets, and unknown encryption status.
5. Resource cleanup after admission rejection, cancellation, stale plans, and partial failures.
6. Preservation of Go GPU checks, delta execution order, and delegator stats lifetimes.
7. Continued availability of stateless QueryCoord estimation.

## References

- [LoadDiff-based loading design](20260204-loaddiff_based_segment_load.md)
- [Reopen publication and query leases](../20260720-segment-reopen-request-read-lease-drain-design.md)
- The estimator prototype was based on `34aa0be4eb`. This document describes the intended contract; it does not establish that validation has passed. See the PR for validation status.
