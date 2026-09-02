# TransformLog View Design Index

The original TransformLog View Module design described TransformLog as an
independent RecoveryStorage module with a materialized frontier used by
BroadcastAck. That ownership and checkpoint model has been superseded.

Current design is split across:

- [TransformLog Design](transformlog/transform_log.md): storage layout, chunk
  durability, subscription, materialization, truncation, and recovery;
- [WAL Message Ack Design](message_ack.md): per-message reference-count
  completion and checkpoint gating;
- [Broadcast Ack Module](broadcast_ack_module.md): one-shot Owner exclusive
  callbacks, ResourceKey partial ordering, and retry semantics;
- [StreamingNode VChannel WAL Input View](streamingnode_vchannel_wal_view.md):
  QueryRuntime preparation from the VChannel-owned TransformLog stream.

The current ownership is:

```text
PChannelRecoveryManager
  -> VChannelRecoveryModule
       -> TransformLog
```

TransformLog is not an independently registered `moduleapi.Module`.
`TransformLogModuleSnapshot` remains a catalog/snapshot classification name and
does not imply runtime module ownership.

The authoritative completion rules are:

1. TransformLog calls `Clone()` when a message requires chunk flush work and
   stores the returned retained immutable message with the buffered entry or
   barrier.
2. The handle releases after the chunk covering the message is durable and
   committed into in-memory TransformLog state.
3. The handle does not wait for L0 materialization.
4. TransformLog installs metadata changes and marks itself dirty before
   releasing the handle.
5. RecoveryStorage persists the TransformLog DirtySnapshot before the frozen
   batch checkpoint that includes the completed message.
