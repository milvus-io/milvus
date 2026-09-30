# TransformLog Design Index

The canonical [TransformLog Subscription Adaptor](../transform_log.md) describes
the read-only wrapper over WALSummary: streams, subscriptions, Entry /
SyncUp delivery, and resume semantics. It has no ObserveMessage or storage path.
Local bounded SN bootstrap replay is implemented and wired into QueryRuntime
preparation. The local unbounded adaptor also provides VChannel-scoped
notifications; remote transport and QN continuous-subscription integration remain
planned. See the canonical document for current limitations and follow-up work.

Related contracts:

- [WAL L0 Materializer](../l0_materializer.md): the active VChannel-owned
  materializer, retaining WAL Delete handles independently of subscriptions.
- [Summary L0 consumer](../summary_l0_materializer.md): retained for future
  wiring, using bounded Summary reads; not active alongside the WAL consumer.
- [WALSummary](../summary.md): shared storage, bounded reads, readable coverage,
  manifest publication, LastAcked, and retention.
- [Message Ack](../message_ack.md): successful handle completion and poison.
- [Checkpoint Persistence](../checkpoint-persistence.md): publication bounded
  by both Tracker completion and Summary confirmation.

This path remains a link target. The former independent TransformLog storage
and combined subscription/materialization model are superseded; maintain the
canonical documents rather than a second copy.
