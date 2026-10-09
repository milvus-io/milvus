# TransformLog Design Index

The canonical [TransformLog Subscription Adaptor](../transform_log.md) describes
the future read-only wrapper over WALSummary: streams, subscriptions, Entry /
SyncUp delivery, and resume semantics. It has no ObserveMessage or storage path.
Subscription integration is outside the current recovery-storage PR.

Related contracts:

- [L0 Materializer](../l0_materializer.md): independent VChannel materialization
  windows, L1 safety bound, bounded Summary reads, L0 output, and recovery.
- [WALSummary](../summary.md): shared storage, bounded reads, readable coverage,
  manifest publication, LastAcked, and retention.
- [Message Ack](../message_ack.md): successful handle completion and poison.
- [Checkpoint Persistence](../checkpoint-persistence.md): publication bounded
  by both Tracker completion and Summary confirmation.

This path remains a link target. The former independent TransformLog storage
and combined subscription/materialization model are superseded; maintain the
canonical documents rather than a second copy.
