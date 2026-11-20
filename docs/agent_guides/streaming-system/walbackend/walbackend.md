# WAL Backend

The WAL backend is the durable storage layer for WAL entries. Each PChannel maps 1:1 to a backend topic/partition. The backend is pluggable — implementations share a common `WALImpls` interface for append, read, and truncate operations.

## Supported Backends

- **Kafka**: Production-grade distributed log. Uses confluent-kafka-go. Message properties stored as Kafka headers.
- **Pulsar**: Alternative distributed messaging. Uses apache/pulsar-client-go. Async producer with exponential backoff initialization.
- **Woodpecker**: Milvus-native log storage (zilliztech/woodpecker). Supports writer fencing via `ErrFenced`.
- **RocksMQ (RMQ)**: In-process RocksDB-backed queue for standalone deployment. No network overhead but single-node only.

## WALImpls Interface

Normal backend changes are composed by `autowalimpls`; see
[AlterWAL](../../../design-docs/design_docs/wal/alter-wal.md) for the durable
link, synchronization boundary and startup chain-recovery protocol. A backend
switch preserves the outer WAL and its scanners. RecoveryStorage observes the
original control record for flush/ACK; public subscriptions receive its TimeTick
confirmation without target/config details. The old AlterWALState reopen protocol
is deprecated.

All backends implement the same interface:
- `Append(ctx, msg) → MessageID` — Persist a message and return its backend-specific ID.
- `Read(ctx, opts) → ScannerImpls` — Create a scanner starting from a delivery policy (All, StartFrom or StartAfter). Latest is deprecated and returns an error.
- `Truncate(ctx, messageID)` — Remove messages up to the given ID (log compaction).

## Historical subscriptions and handle lifetime

`All` starts from the earliest retained record in the initially selected physical
backend (the current backend for autowalimpls), then follows subsequent switch
links. It does not discover older backends. Use a checkpoint MessageID with
From/After when the subscription must include history preceding a switch.

From/After subscriptions select the backend encoded in their position and follow
AlterWAL target anchors. Deployment operations prevent upgrades from overlapping
AlterWAL and eliminate legacy-switch historical subscription requirements before
enabling hot switching. No persisted backend-membership compatibility list or
catalog lookup is added to this read path; see the AlterWAL design.

Auto keeps one current writer and one historical reader handle per backend. Retiring
an RW handle changes its role to historical reader without reopening it or changing
its access mode. A later switch back can reuse it. If a backend already has an RO
reader when it becomes current, its new RW handle replaces the RO handle, which
is then closed. Physical RO handles do not own shared opener resources or scanner
connections, so existing scanners continue until their own close or handoff.
Other historical handles are retained until auto closes; there is no idle eviction.
The current handle is not duplicated in the historical map, and every owned
handle closes once.

Physical Open runs outside the current-writer mutex and serializes acquisition
to avoid duplicate writers fencing one another. Waiting for acquisition is
cancellable, so closing a reader does not wait for another caller's slow Open.
Latest is rejected before reader creation; there is no Latest-specific switch
state or retry loop.

## Append retry and writer lifetime

`autowalimpls` retries ordinary backend errors without a retry-count or elapsed-time
limit. It pins the physical backend and message for the duration of each retry loop. One logical append keeps its allocated local TimeTick across backend attempts.
A backend attempt can persist a record and lose its response; retry copies are
handled by the scanner's physical TimeTick deduplication and chunk assembly.

The caller's cancellation is removed with `context.WithoutCancel`; the append context
is bound to the WAL's availability instead. Cancellation of that context ends the
writer lifecycle and requires WAL recovery before writes resume. `ErrFenced` likewise
means the old writer has lost write authority. A final cancellation/fencing error
must not be modeled as a failed request on an otherwise reusable healthy writer.
Recovery, rather than the error result, determines which records became durable.

This contract lets interceptors rely on the existing retry and recovery paths;
they do not need their own Commit retry state machine or a context-carried callback
to fence the WAL. `autowalimpls` checks the WAL-owned append context itself;
a backend `context.Canceled` or `context.DeadlineExceeded` error alone does not
end the retry loop. Independent per-attempt cancellation/timeouts on a live writer
are retried. `ErrFenced` still terminates immediately because write authority is lost.
Tests should cancel the actual WAL lifecycle when exercising final cancellation.

The adaptor records one trace span around each call into `autowalimpls.Append`
(one per chunk when chunking is enabled), covering its complete retry sequence.
No error is injected after a successful auto append: interceptors must finish
their in-memory transitions once persistence succeeds. Backend lost-acknowledgement
retry tests inject errors in the mocked physical implementation, after recording
the message. No observation wrapper or callback is passed into auto.
Auto owns the checkpoint writer from the start of recovery. RecoveryStorage
selects each linked writer through `SwitchWriterForRecovery`, then writes its new
RecoveryBarrier through auto; ordinary historical readers never change the writer.
The builder does not maintain a second handle cache or transfer writers on completion.
The adaptor directly calls the same auto WAL. Retry helpers are private to
`autowalimpls.Append`; there is no separate retry wrapper. Test backends use the
same auto construction path before recovery.

Validation rejected before backend append is a separate case: no record was written,
so the relevant producer/transaction protocol handles that rejection directly.

## Key Packages

- `pkg/streaming/walimpls/` — `WALImpls` interface, Kafka/Pulsar/RMQ/Woodpecker implementations
- `pkg/streaming/walimpls/impls/autowalimpls/retry.go` — physical retries, backoff, retry metrics and logs
- `internal/streamingnode/server/wal/adaptor/wal_adaptor.go` — logical append context, outer trace and writer fencing
- `internal/streamingnode/server/wal/adaptor/scanner_adaptor.go` — consumer ordering, physical deduplication and transaction assembly
