# Isolate search fanout from the middle-priority pool

## Motivation

Search currently shares the MIDDLE executor with batch field and index loading.
Multi-segment primary-key filling, global distance refinement, and ordered field
materialization can queue behind background load work.

## Design

Add SEARCH_FANOUT to the existing ThreadPools registry, with its own ThreadPool
instance, worker threads, queue, and `priority="search_fanout"` metrics. Reuse the
existing executor implementation, including idle worker reclamation and future
exception propagation.

Both MIDDLE and SEARCH_FANOUT use
`common.threadCoreCoefficient.middlePriority` and the per-pool
`common.threadCoreCoefficient.maxThreadsSize` cap. A resize of either identity
updates the shared coefficient and both existing pools. Missing pools remain
lazy and use the latest coefficient when first requested. Pool creation and
configuration publication share the registry lock; executor resizing happens
outside that lock so retiring workers can still resolve dependent pools.

Move only these fanout submission points:

- `ReduceHelper::FillPrimaryKey`
- `ReduceHelper::RefineDistances`
- `MaterializeOrderedFields`, used by ordered output and selected-field filling

Single-segment paths, cancellation tokens, result ordering, future draining,
and error propagation retain their current behavior. The inner data/index
loaders keep their existing execution pools and load admission budgets.
SEARCH_FANOUT is not included in `GetLoadExecutorWorkers()`.

Tasks running in SEARCH_FANOUT must not synchronously wait for child tasks
submitted to SEARCH_FANOUT. The current three fanout bodies call segment-level
operations, not another search fanout entry point.

## Compatibility and resource cost

Existing pool IDs, config keys, metric names and label keys remain unchanged.
The new metric label value is additive. Each pool has its own capacity: when
MIDDLE was capped at 16 workers, the two pools can together use 32 workers.
CPU, memory, and storage bandwidth are still shared. This change isolates
executor queues; it does not promise complete latency isolation for cold reads.

The same routing and configuration contract applies to master and 3.0.
Master records the future migration to global coroutines in a source TODO.

## Verification

Exercise configuration changes before first use and after either/both pools
exist; cap reduction and removal; independent task execution with MIDDLE
blocked; unchanged load worker accounting; and separate metric updates.
Verify task exceptions and cancellation still propagate through futures.
Inspect all three submission paths and their inner loader routing.

Integration validation should cover multi-segment PK filling, refinement,
ordered field materialization, cold data, and canceled searches. Mixed load and
search benchmarks are required before making latency or throughput claims.
