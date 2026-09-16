# Transform Storage And Consumer Design Index

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

The agreed design separates shared storage, L0 materialization, and subscriptions:

- [WALSummary](summary.md): sole record storage, bounded reads, readable
  coverage, durability, confirmation, and retention.
- [L0 Materializer](l0_materializer.md): VChannel-owned component that observes
  window boundaries and reads Summary to produce L0. Implemented in `vchannel/l0materializer`, replacing the former copied window.
- [TransformLog Subscription Adaptor](transform_log.md): future read-only
  wrapper over Summary, outside this PR; no observation or materialization.

```text
RecoveryStorage -> WALSummary
                -> PChannelRecoveryManager
                     -> VChannelRecoveryModule
                          -> L0Materializer -> Summary reads

Future TransformLog adaptor -> the same Summary reads
```

The materializer's durable cursor is carried by VChannelMeta; it has no separate
catalog. Neither materialization nor subscription delivery retains WAL handles.
RecoveryStorage combines Tracker completion with `WALSummary.LastAcked()` before
publishing the global checkpoint.

Related contracts:

- [WAL Message Ack Design](message_ack.md)
- [Recovery Tail Controller](recovery-tail-controller.md)
- [Broadcast Ack Module](broadcast_ack_module.md)
- [StreamingNode VChannel WAL Input View](streamingnode_vchannel_wal_view.md)
