# MEP: PredicateDelete — Expression Delete as Durable Predicate Events

- **Created:** 2026-06-26
- **Author(s):** @MrPresent-Han
- **Status:** In Progress (Part 1 — Proxy/WAL Producer — implemented)
- **Component:** Proxy, StreamingNode, QueryNode (Delegator/Segcore), DataCoord/DataNode, milvus-storage
- **Related Issues:** #50433
- **Released:** TBD

## Summary

Today a complex `delete by expr` is executed by first querying the matched primary keys (PKs) and then writing ordinary `(pk, ts)` delete rows. This is reliable when the expression matches few rows, but when the expression matches a huge number of rows a single delete is amplified into a massive number of PK delete entries that propagate pressure into the WAL, StreamingNode, L0 compaction, and the QueryNode delegator.

**PredicateDelete** turns a large-scale expression delete from *"a huge set of PK rows"* into *"a small set of durable predicate events"*. The system first guarantees that online queries never miss a delete, then converges physically through L0 forwarding and segment rewrite. PredicateDelete runs as a **parallel lifecycle** alongside the existing PK-delete path; it does not replace it and preserves a full fallback.

This MEP describes the end-to-end design across five parts. **Part 1 (Proxy/WAL producer) is implemented by #50501**; Parts 2–5 are the forward plan and are summarized here for context.

## Motivation

The current expression-delete main path (`query matched PKs → write ordinary `(pk, ts)` deletes`) has these shortcomings when the match count is large:

- **WAL IO pressure**: the expression delete is expanded into a large PK payload; WAL write, replication, and recovery cost grow linearly with the matched row count.
- **StreamingNode buffer pressure**: SN must buffer, account, and sync ordinary delete rows; large PK/timestamp volumes amplify memory and flush pressure.
- **L0 compaction pressure**: many PK delete rows produce many L0 carriers and additional L0 compaction scheduling/execution pressure.
- **Delegator burden**: the QueryNode delegator must always load/replay large L0 segments, slowing recovery load and increasing CPU on the delegator node.

The goal is to make the durable footprint of a complex delete proportional to *the number of delete operations*, not *the number of matched rows*.

```text
delete expr
  -> proxy:         PredicateDelete WAL event
  -> streamingNode: predicate-delta L0 persistence
  -> queryNode:     replay / evaluate (online correctness)
  -> datanode:      L0 compaction forwards events
  -> datanode:      rewrite drops matched rows (physical convergence)
```

## Goals

- Keep the durable representation of a large expression delete `O(events)` instead of `O(matched rows)`.
- Preserve online correctness (no missed deletes) at all times, including during segment loading.
- Run as a parallel path that never breaks existing PK-delete semantics, with a config-gated fallback.

## Non-Goals

- Replacing the PK-delete path. PK deletes remain concrete `(pk, ts)` rows.
- Supporting arbitrary expressions in the first version. JSON, array, text-match, function, and complex cast nodes are unsupported and must fail fast / fall back.
- Reusing predicate event counts as deleted-row counts (see Guardrails).

## Overall Principles

- **Parallel paths**: PK delete is concrete `(pk, ts)` rows; PredicateDelete is an expression event and does not reuse PK deltalog semantics.
- **Independent counting**: a predicate event count is *not* a deleted-row count. It must not feed delete-ratio, `TotalRows`, or deleted-row metrics.
- **Execution source of truth**: `serialized_expr_plan` is the correctness source; `expr` is for debug/log only. Executors must never re-parse the expression string.
- **Schema binding**: each event carries `schema_version`; replay/compaction must interpret the expression using the delete-time schema, not the latest schema.
- **Convergence order**: QueryNode guarantees online correctness, L0 compaction forwards pending events, and the rewrite stage physically drops matched rows.

## Part 1 — Proxy / WAL Producer (implemented, #50501)

**Components:** Proxy delete task (`internal/proxy/task_delete.go`, `task_delete_streaming.go`), WAL append helper, config `common.enablePredicateDelete` / `common.predicateDeleteHitCountThreshold`, `msgpb.DeleteRequest`.

### Routing and fallback

Proxy reuses the existing `getPrimaryKeysFromPlan` split point:

- `pk == x` and `pk IN [...]` continue to use **Simple Delete** (direct `(pk, ts)` rows).
- Any other expression is a **non-simple delete**. When `common.enablePredicateDelete=false` (default), it keeps the existing query-PK fallback (`complexDelete`). When enabled, the producer additionally guards on match-count to avoid using a predicate event for tiny deletes.

```go
isSimple, pk, numRow := getPrimaryKeysFromPlan(schema, plan)
if isSimple {
    return simpleDelete(ctx, pk, numRow)
}
return nonSimpleDelete(ctx, plan) // predicate-delete decision + fallback
```

```go
// nonSimpleDelete (simplified)
if !enablePredicateDelete { return complexDelete(plan) }            // feature off
if !canUsePredicateDelete(plan, schema) { return complexDelete(plan) } // unsupported expr -> PK fallback
matched, err := countPredicateDeleteHits(plan)                     // count-only query
if err != nil { return err }
if matched <= predicateDeleteHitCountThreshold { return complexDelete(plan) } // small delete -> PK path
return predicateDelete(plan)                                       // emit predicate event
```

The threshold (`common.predicateDeleteHitCountThreshold`, default `1024`) ensures PredicateDelete is only used when it actually pays off; below the threshold the cheaper PK path is used.

### Supported expressions (first version)

`canUsePredicateDelete` conservatively allow-lists expressions that are safe to evaluate later by Segcore / storage:

- Comparison ops on a plain scalar field vs. a scalar literal: `==`, `!=`, `<`, `<=`, `>`, `>=`.
- `field IN [literal, ...]` (plain field membership, not container `IsInField`).
- `IS NULL`.
- Boolean composition: `AND`, `OR`, `NOT`.

Unsupported (fall back to PK delete): JSON path / array-element columns, dynamic fields, templated expressions, container-membership term, non-scalar literals, column-vs-column comparisons, `IS NOT NULL`, arithmetic, etc.

### All-vchannel append

A PredicateDelete event has no PK hash, so it cannot be routed to a single vchannel by PK. The proxy appends the **same** predicate event to **every** vchannel of the collection. `partition_ids` is the authoritative scope; PK fields / `NumRows` carry no authoritative meaning for a PredicateDelete.

### WAL contract

The WAL payload extends the existing `DeleteRequest`; no new streaming message type is introduced.

```text
PredicateDelete (carried on DeleteRequest):
  expr                  // diagnostic only
  serialized_expr_plan  // proto.Marshal(planpb.PlanNode) — correctness source
  delete_timestamp      // MVCC ts
  schema_version        // delete-time schema
  partition_ids         // authoritative scope
```

## Part 2 — StreamingNode / flushcommon: persist predicate delta (planned)

StreamingNode does not evaluate the predicate; it only persists it durably. PredicateDelete uses an independent buffer / writer / metadata and must not enter `storage.DeleteData`. One Arrow row represents one predicate delete event. Multi-partition events are not fanned out into multiple L0 segments; the carrier organizes at channel/WAL level and keeps the full partition scope in the event's `partition_ids`.

`SegmentInfo.PredicateDeltalogs` is a scheduling summary (event count / log size / timestamp range / segment-level `partition_ids` union) that DataCoord can read without opening predicate-delta files. In V3, the real file paths live in a separate manifest `predicate_delta_logs` section.

```text
metadata: milvus.predicate_delta.format_version = 1
columns:  source_msg_id, delete_ts, schema_version,
          partition_ids, expr, serialized_expr_plan
```

## Part 3 — QueryNode / Segcore: online correctness (planned)

The QueryNode pipeline recognizes PredicateDelete first and must not enter PK-delete alignment / empty-timestamp validation (a PredicateDelete has no PrimaryKeys/Timestamps). The delegator keeps an independent predicate buffer for online forwarding and loading-segment catch-up; raw events never go into `storage.DeleteData` or the PK delete buffer.

Online sealed/growing segments apply an event immediately after partition-scope selection (no bloom-filter prefiltering, since there is no PK). Loading a sealed segment is the no-gap critical window: replay persisted predicate L0, snapshot the predicate buffer, catch up new events, then join distribution. Segcore evaluates the expression to matched row offsets and inserts `DeletedRecord(delete_ts, row_offset)` into the existing MVCC delete model.

## Part 4 — DataCoord / DataNode: L0 compaction forwarding (planned)

L0 compaction does not evaluate the predicate; it forwards pending predicate events from L0 carriers to potentially affected target L1/L2 segments. DataCoord reads only the `PredicateDeltalogs` summary (partition union) for scheduling. Predicate L0 segments are organized separately from ordinary PK-delete L0 segments. Compaction results emit `Deltalogs` and `PredicateDeltalogs` in parallel; predicate events never go into ordinary `Deltalogs`, and event count is never treated as deleted rows.

## Part 5 — milvus-storage: delete-aware reader / physical rewrite (planned)

Physical rewrite computes a row-level alive mask from the manifest delete logs; the Milvus writer appends alive rows to a new segment. The first version returns an aligned `RecordBatch + keep_mask` (bit=1 means alive) rather than filtering inside storage. A `DeleteEvaluator` loads both PK delta (`pk -> max_delete_ts`) and predicate delta (`serialized_expr_plan` → storage IR → Arrow compute). Every delete type requires `row_ts <= delete_ts`; a null predicate result does not delete (only a definite-true match deletes).

```text
deleted   = (pk_match(row) AND row_ts <= pk_delete_ts)
          OR (predicate_true(row) AND row_ts <= predicate_delete_ts)
keep_mask = NOT deleted
```

## Public Interfaces

### Config (`common`)

| Key | Default | Refreshable | Meaning |
|-----|---------|-------------|---------|
| `common.enablePredicateDelete` | `false` | yes | Master switch for the PredicateDelete producer path. |
| `common.predicateDeleteHitCountThreshold` | `1024` | yes | Use predicate delete only when the matched row count is greater than this threshold; otherwise fall back to PK delete. |

### WAL payload

PredicateDelete reuses `msgpb.DeleteRequest` and carries `serialized_expr_plan`, `delete_timestamp`, `schema_version`, and `partition_ids` (authoritative scope). No new streaming message type is added.

## Risks & Guardrails

- **Rolling upgrade**: old QueryNode/DataNode must not silently ignore a PredicateDelete event; a feature/version gate is required.
- **No-gap correctness**: a loading segment must not become visible until replay + buffer catch-up completes.
- **Schema evolution**: executors must use the delete-time schema/version and never reinterpret the expression with the latest schema.
- **Event identity / idempotency**: the first version may use `source_msg_id` / WAL position; long term needs stable event identity or a checkpoint.
- **Metrics semantics**: a predicate event count means *number of events only* — never deleted-row count, delete ratio, or task `TotalRows`.

## Conclusion

PredicateDelete reduces a large-scale `delete by expr` from a huge set of PK delete rows to a small set of durable predicate events. It establishes a parallel lifecycle across Proxy, StreamingNode, QueryNode, DataCoord/DataNode, and milvus-storage without breaking existing PK-delete semantics or the fallback path: Proxy produces events, StreamingNode persists them, QueryNode guarantees online visibility, DataNode L0 compaction forwards pending events, and full/mix compaction or the milvus-storage delete-aware reader physically filters rows during rewrite. This lowers long-term pressure on WAL IO, StreamingNode buffers, L0 compaction, and delegator L0 replay.
