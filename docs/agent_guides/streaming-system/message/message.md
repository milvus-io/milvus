# Message Model

Every WAL entry is a **Message** — the fundamental data unit flowing through all WAL components. A message consists of a typed payload (protobuf-encoded header + body) and key-value properties (`map[string]string`, reserved keys prefixed with `_`).

## Message Lifecycle

Messages transition through three stages:

- **BroadcastMutableMessage**: Created by the Broadcaster for messages that target multiple VChannels (DDL/DCL/WALInternal broadcasts). Carries a `BroadcastHeader` with the list of target VChannels and ResourceKeys. `SplitIntoMutableMessage()` splits it into per-VChannel MutableMessages for individual WAL append.

- **MutableMessage**: The pre-append state. Properties can be modified by the WAL interceptor chain (attaching TimeTick, LastConfirmed, TxnContext, WALTerm, etc.). Created either by client-side builders (DML) or by splitting a BroadcastMutableMessage. Transitions to ImmutableMessage via `IntoImmutableMessage(msgID)` after WAL persistence.

- **ImmutableMessage**: The post-append, read-only state. Carries a backend-assigned MessageID and LastConfirmedMessageID. Returned by WAL Read/Consume operations. For transactions, multiple ImmutableMessages are assembled into an **ImmutableTxnMessage** (Begin + body messages + Commit) by the consumer-side TxnBuffer.

- **ReplicateMutableMessage**: Created from a source cluster's ImmutableMessage for cross-cluster replication. Attaches a `ReplicateHeader` preserving the source cluster's original Message Properties, then re-enters the local WAL as a MutableMessage. Replication honors message-level compatibility markers such as `Unreplicable`.

## Key Properties

| Property | Description |
|----------|-------------|
| **Version** | Payload format version, bound at build time for compatibility. `VersionOld`(0): legacy format before StreamingNode, to be removed in future. `V1`(1): payload still uses msgstream serialization. `V2`(2): payload fully independent of msgstream. |
| **MessageType** | The kind of message. Determines how payload is decoded. |
| **VChannel** | Target virtual channel. Empty means visible to all VChannels on the PChannel. |
| **IsPChannelLevel** | Marks cluster-level broadcast messages handled at PChannel scope. |
| **BroadcastHeader** | Broadcast metadata for messages targeting multiple VChannels. See Broadcaster. |
| **TimeTick** | PChannel-level log sequence number. See TimeTick. |
| **LastConfirmedMessageID** | Reading from this MessageID guarantees all subsequent messages have TimeTick greater than this message's TimeTick (including txn messages). |
| **TxnContext** | Links the message to a transaction. Nil if non-transactional. |
| **ReplicateHeader** | Source cluster's original message metadata for cross-cluster replication. |
| **Unreplicable** (`_ur`) | Marks this concrete message as unsafe for cross-cluster replication. Replication skips only messages carrying this property; absence means replicable, including old WAL messages written before the marker existed. |
| **MessageID** | Backend-assigned unique identifier. |
| **PChannel** | The PChannel this message belongs to. |

## Message Semantic Docs

- [Collection Messages](message-semantic-collection.md) — DDL, partition, index, snapshot, import, DML, segment, load config
- [Alias Messages](message-semantic-alias.md) — alias create/drop
- [Database Messages](message-semantic-database.md) — database lifecycle
- [RBAC Messages](message-semantic-rbac.md) — users, roles, privileges
- [Transaction Messages](message-semantic-txn.md) — begin, commit, rollback
- [Cluster Messages](message-semantic-cluster.md) — global barriers, replication config, resource groups
- [TimeTick Message](message-semantic-time-tick.md) — visibility barrier

## Header vs Body

Every specialized message has a **header** and a **body**, both protobuf. Their storage is different:

- `EncodeProto` stores the header as base64 in the `_h` property. Each `AsSpecialized*` call decodes it, and log output decodes it again (`marshalSpecializedHeader`).
- The body is the payload. It is decoded only when a caller calls `Body()`/`MustBody()`. When cipher is enabled, only the payload is encrypted.

Use these rules to choose where a new field goes:

| # | Rule | Reason | Example in code |
|---|------|--------|-----------------|
| 1 | Put a field in the header only if the WAL infrastructure must read or write it without a body decode. The WAL infrastructure is the interceptors, RecoveryStorage, the flusher, CDC and the broadcast ack callback dispatch. This includes identity and ownership IDs, values that the WAL assigns during append, and fingerprints or decision facts used for validation. Put the business content that the final consumer applies in the body. | Every access decodes the header. Only a consumer that needs the content decodes the body. | `CreateSegmentMessageHeader.segment_id`, `CommitImportMessageHeader.job_id` (read by the flusher), `RestoreSnapshotMessageHeader.snapshot_fingerprint` |
| 2 | The header size must not grow with user data volume. A header field must have a constant size, or a small bound set by the system. Put schemas, rows, expressions and file lists in the body. | Each decode of the header costs time in proportion to its size, and each log line of the message contains it. | `ManualFlushMessageHeader.segment_ids` is bounded by the growing segments of one VChannel. The insert rows are in the body. |
| 3 | Put secrets and user data only in the body. | Cipher encrypts only the payload. The header stays plaintext in the WAL backend and in logs. | `InsertMessageHeader` has only IDs and row counts. |
| 4 | Put a field that the WAL writes during append in the header. | `OverwriteHeader` encodes one property again. `OverwriteBody` marshals the full body again, and encrypts it again when cipher is enabled. | The shard interceptor fills `flushed_segment_ids` and `segment_ids` with `OverwriteHeader`. |
| 5 | Put metadata that applies to many message types in a message property (`properties.go`), not in a specialized header. | A property is readable without the knowledge of the message type. | The idempotency key `_ik` is a property, not a field of `InsertMessageHeader`. |

If rule 1 and rule 2 or rule 3 point to different places, rules 2 and 3 win. Put a small handle (an ID or a fingerprint) in the header and put the full content in the body.

## Adding a New Message Type

New message types **MUST** be defined via `codegen/reflect_info.json` and `pkg/streaming/util/message/codegen/`. Do not manually write builder or type-conversion functions.

## Key Packages

- `pkg/streaming/util/message/` — Message types, builders, properties, codegen, legacy adaptor
