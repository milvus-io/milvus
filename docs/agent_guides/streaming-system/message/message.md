# Message Model

Every WAL entry is a **Message** — the fundamental data unit flowing through all WAL components. A message consists of a typed payload (protobuf-encoded header + body) and key-value properties (`map[string]string`, reserved keys prefixed with `_`).

## Message Lifecycle

Messages transition through three stages:

- **BroadcastMutableMessage**: Created by the Broadcaster for messages that target multiple VChannels (DDL/DCL/WALInternal broadcasts). Carries a `BroadcastHeader` with the list of target VChannels and ResourceKeys. `SplitIntoMutableMessage()` splits it into per-VChannel MutableMessages for individual WAL append.

- **MutableMessage**: The pre-append state. Properties can be modified by the WAL interceptor chain (attaching TimeTick, LastConfirmed, TxnContext, WALTerm, etc.). Created either by client-side builders (DML) or by splitting a BroadcastMutableMessage. Transitions to ImmutableMessage via `IntoImmutableMessage(msgID)` after WAL persistence.

- **ImmutableMessage**: The post-append, read-only state. Carries a backend-assigned MessageID and LastConfirmedMessageID. Returned by WAL Read/Consume operations. For transactions, multiple ImmutableMessages are assembled into an **ImmutableTxnMessage** (Begin + body messages + Commit) by the consumer-side TxnBuffer.

- **ReplicateMutableMessage**: Created from a source cluster's ImmutableMessage for cross-cluster replication. Attaches a `ReplicateHeader` preserving the source cluster's original Message Properties, then re-enters the local WAL as a MutableMessage,

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

## Adding a New Message Type

New message types **MUST** be defined via `codegen/reflect_info.json` and `pkg/streaming/util/message/codegen/`. Do not manually write builder or type-conversion functions.

## Key Packages

- `pkg/streaming/util/message/` — Message types, builders, properties, codegen, legacy adaptor
