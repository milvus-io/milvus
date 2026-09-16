# TransformLog View Design Index

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

TransformLog is a VChannel-owned component, not an independent top-level
RecoveryStorage module.

The design is split across:

- [TransformLog Design](transform_log.md): copied consumer window, L1 safety
  bound, L0 materialization, and recovery;
- [WALSummary](summary.md): chunk layout, durability, confirmation, and retention;
- [WAL Message Ack Design](message_ack.md): retained-message completion and
  global checkpoint gating;
- [Recovery Tail Controller](recovery-tail-controller.md): VChannel-scoped
  `RequestPersistThrough` calls;
- [Broadcast Ack Module](broadcast_ack_module.md): Coordinator Ack ownership;
- [StreamingNode VChannel WAL Input View](streamingnode_vchannel_wal_view.md):
  QueryRuntime preparation from VChannel state.

```text
PChannelRecoveryManager
  -> VChannelRecoveryModule
       -> TransformLog
```

TransformLog consumes summary records during recovery and supplies the
materialization frontier used for GC. It does not own summary persistence or
retain WAL handles. RecoveryStorage separately combines the tracker frontier
with `WALSummary.LastAcked()` before publishing a checkpoint.
