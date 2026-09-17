# Transform Storage And Consumer Design Index

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

**Current runtime:** [WAL L0 Materializer](l0_materializer.md) retains Delete
handles for legacy query recovery. The [Summary consumer](summary_l0_materializer.md)
is retained for future QueryView wiring; the two implementations are not run together.

The agreed design separates shared storage, L0 materialization, and subscriptions:

- [WALSummary](summary.md): sole record storage, bounded reads, readable
  coverage, durability, confirmation, and retention.
- [WAL L0 Materializer](l0_materializer.md): current VChannel-owned consumer
  retaining WAL Delete handles through L0 output. The
  [Summary consumer](summary_l0_materializer.md) is retained for future wiring.
- [TransformLog Subscription Adaptor](transform_log.md): future read-only
  wrapper over Summary, outside this PR; no observation or materialization.

```text
RecoveryStorage -> WALSummary
                -> PChannelRecoveryManager
                     -> VChannelRecoveryModule
                          -> WALMaterializer -> retained WAL Delete batches

Future TransformLog adaptor -> the same Summary reads
```

The materializer's durable cursor is carried by VChannelMeta; it has no separate
catalog. The current WAL consumer retains Delete and explicit Flush handles.
Future subscriptions do not participate in message completion.
RecoveryStorage combines Tracker completion with `WALSummary.LastAcked()` before
publishing the global checkpoint.

Related contracts:

- [WAL Message Ack Design](message_ack.md)
- [Recovery Tail Controller](recovery-tail-controller.md)
- [Broadcast Ack Module](broadcast_ack_module.md)
- [StreamingNode VChannel WAL Input View](streamingnode_vchannel_wal_view.md)
