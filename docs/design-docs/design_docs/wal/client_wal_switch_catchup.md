# Client-side read-only WAL catch-up

Related issue: [#54763](https://github.com/milvus-io/milvus/issues/54763).
Tracking issue: [#52013](https://github.com/milvus-io/milvus/issues/52013).
Depends on [#54764](https://github.com/milvus-io/milvus/pull/54764) for synchronous
remote WALNameMismatch propagation.

## Scope

MQ switching is an infrequent maintenance operation. A standalone client-side
helper catches up an old position after WALNameMismatch, replacing the server-side
migration-chain approach in #52018. Support one A-to-B switch; the previous
backend's configuration, topic/log and history remain available throughout catch-up.
RocksMQ shares the already initialized embedded server in the same standalone
process. The helper adds no RocksMQ bootstrap or restart recovery mechanism.

## Recovery sequence

1. Local or remote consumer creation returns typed WALNameMismatch.
2. The handler invokes CatchUpWALAfterSwitch with the original position, filters and
   upper message handler. The helper opens the explicitly named backend read-only.
3. Assemble chunks and reuse existing ReOrderBuffer, TxnBuffer and PendingQueue
   utilities to filter, deduplicate, order and deliver the old prefix.
4. Treat AlterWAL as the final window boundary. Drain selected output before
   consuming this internal marker; return its TimeTick only after delivery.
5. Close the helper's scanner, WAL and opener. Retry live subscription with
   DeliverPolicyAll, preserving the original filters and adding TimeTickGT(cut).

StartAfter opens inclusively and excludes its matching logical output, so a
position on AlterWAL can still observe the marker. A failed helper never authorizes
All. Neither failed attempts nor cleanup close the upper handler.

## Boundary and retry rules

AlterWAL takes the PChannel write lock exclusively, waits for earlier appends and
complete chunk runs, persists its marker, and fences later writes before releasing
the lock. Ordinary TimeTick writes use that fenced path too. The helper therefore
closes its own reorder window at AlterWAL without requiring a trailing TimeTick.
RecoveryBarrier records from a later recovery add no business data to that prefix.
The helper relies on this existing writer contract and uses AlterWAL directly as
the boundary. The transaction buffer handles committed transactions and aborts
unfinished transactions at this cut. Existing chunk and ordering errors are preserved.

Before any output is accepted, errors return to the existing construction retry.
After output is accepted, the helper retries internally with the last successfully
handled LastConfirmed position and TimeTick filter, until completion or cancellation.
V0 uses inclusive TimeTick filtering because distinct legacy records can share a
tick; the existing MsgPackAdaptor retains its ID deduplication. Current-format
records use strict TimeTick filtering. Prefetch does not advance progress.
After catch-up completes, live-subscription creation retries also retain All and
the cut locally, avoiding a return to stale outer construction options. The upper
handler is preserved throughout these retries.

## Reuse and resource ownership

Normal SN scanner behavior and the outer Resuming consumer are unchanged. The
helper composes low-level buffers directly, without a shared scanner framework,
WAB, catalog, SN resource initialization or cached opener lifecycle. Only the
existing V0 decoder is extracted for reuse; SN retains a thin resolver wrapper.
Legacy VChannel lookup lazily creates the existing MixCoord-based temporary
lookup. Current-format records need no lookup.

Each call owns a fresh backend opener and its scanner/WAL attempts. Process-wide
limits allow four calls and pace physical records at 16 MiB/s with a 1 MiB burst.
Backend read-ahead is one record; broker prefetch remains backend-specific. Slot
admission, retry waits, pacing and delivery honor cancellation.

Two backend fixes remain: Woodpecker skips writer creation in RO mode and closes
its reader-owned handle; RocksMQ writer close preserves the historical topic.
Kafka, backend builders and the registry are unchanged. ID decoding and backend
selection reuse existing registration: each backend registers its builder and
message-ID decoder together.

## Validation and limits

Focused reader tests cover ordering, duplicates, filters, transactions, chunks,
StartAfter, legacy records, backend failure, delivery pressure,
cancellation and retry progress. Real RocksMQ tests exercise the public helper
after writer close and during concurrent insert/delete record appends. A gRPC
regression checks catch-up before the All handoff and retry after a failed live
subscription using an injected helper result.

Local handler package execution is blocked by stale native dependencies, including
missing milvus-storage/ffi_talon_c.h. The gRPC integration regression is therefore
included but locally unverified. Full QN insert/delete/query traffic through an
operational MQ switch and reconnect has not run; backend concurrency tests do not
establish that component/e2e behavior. This design adds no retention protocol,
multi-hop migration support or new outer-consumer terminal-error policy.
