# TransformLog View Design Index

TransformLog is a VChannel-owned component, not an independent top-level
RecoveryStorage module.

The design is split across:

- [TransformLog Design](transformlog/transform_log.md): storage layout,
  durability, subscription, materialization, truncation, and recovery;
- [WAL Message Ack Design](message_ack.md): retained-message completion and
  global checkpoint gating;
- [Recovery Tail Controller](recovery-tail-controller.md): VChannel-scoped
  `PersistThrough` requests;
- [Broadcast Ack Module](broadcast_ack_module.md): Coordinator Ack ownership;
- [StreamingNode VChannel WAL Input View](streamingnode_vchannel_wal_view.md):
  QueryRuntime preparation from VChannel state.

```text
PChannelRecoveryManager
  -> VChannelRecoveryModule
       -> TransformLog
```

TransformLog releases a retained message only after the covering chunk is
durable and its stable metadata is dirty. L0 materialization is a downstream
operation and does not gate source-message completion.
