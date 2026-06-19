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

## Key Packages

- `pkg/streaming/walimpls/` — `WALImpls` interface, Kafka/Pulsar/RMQ/Woodpecker implementations
