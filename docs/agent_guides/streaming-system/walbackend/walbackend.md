# WAL Backend

The WAL backend is the durable storage layer for WAL entries. Each PChannel maps 1:1 to a backend topic/partition. The backend is pluggable — implementations share a common `WALImpls` interface for append, read, and truncate operations.

## Supported Backends

- **Kafka**: Production-grade distributed log. Uses confluent-kafka-go. Message properties stored as Kafka headers.
- **Pulsar**: Alternative distributed messaging. Uses apache/pulsar-client-go. Async producer with exponential backoff initialization.
- **Woodpecker**: Milvus-native log storage (zilliztech/woodpecker). Supports writer fencing via `ErrFenced`.
- **RocksMQ (RMQ)**: In-process RocksDB-backed queue for standalone deployment. No network overhead but single-node only.

## WALImpls Interface

All backends implement the same interface:
- `Append(ctx, msg) → MessageID` — Persist a message and return its backend-specific ID.
- `Read(ctx, opts) → ScannerImpls` — Create a scanner starting from a delivery policy (earliest, latest, or specific MessageID).
- `Truncate(ctx, messageID)` — Remove messages up to the given ID (log compaction).

## Append retry and writer lifetime

The WAL adaptor retries ordinary backend errors without a retry-count or elapsed-time
limit. One logical append keeps its allocated local TimeTick across backend attempts.
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
to fence the WAL. `appendOneWithRetry` checks the outer append context itself;
a backend `context.Canceled` or `context.DeadlineExceeded` error alone does not
end the retry loop. Independent per-attempt cancellation/timeouts on a live writer
are retried. `ErrFenced` still terminates immediately because write authority is lost.
Tests should cancel the actual WAL lifecycle when exercising final cancellation.

Validation rejected before backend append is a separate case: no record was written,
so the relevant producer/transaction protocol handles that rejection directly.

## Key Packages

- `pkg/streaming/walimpls/` — `WALImpls` interface, Kafka/Pulsar/RMQ/Woodpecker implementations
- `internal/streamingnode/server/wal/adaptor/wal_adaptor.go` — logical append context, backend retry and writer fencing
- `internal/streamingnode/server/wal/adaptor/scanner_adaptor.go` — consumer ordering, physical deduplication and transaction assembly
