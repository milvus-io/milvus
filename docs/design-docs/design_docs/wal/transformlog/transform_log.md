# TransformLog Design Index

The canonical consumer design is [TransformLog Design](../transform_log.md).
It defines the copied materialization window, VChannel barrier handling, L1
safety bound, L0 output, and recovery. TransformLog retains no WAL message handle.

Related contracts:

- [WALSummary](../summary.md): chunk storage, manifest publication, LastAcked,
  independent backlog progress, and retention.
- [Message Ack](../message_ack.md): successful handle completion and poison.
- [Checkpoint Persistence](../checkpoint-persistence.md): publication bounded
  by both Tracker completion and Summary confirmation.

This path is retained as a link target. The former duplicate body described
superseded ownership and barrier behavior; maintain the canonical document
instead of a second copy.
