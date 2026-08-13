// Package recovery rebuilds and durably persists the per-pchannel WAL recovery
// state of a streaming node: vchannel/segment assignments, the consume
// checkpoint, and the write summary store.
//
// # The summary store
//
// The summary store is a business-agnostic record of what a pchannel durably
// wrote. It is deliberately NOT named after any consumer: it stores committed
// write facts (source message id, timetick, primary keys, and whatever extra
// payload a write carried), and applications build their own views on top.
// Today the idempotency interceptor materializes a write-dedup window from
// these entries; a primary-key index view is the second planned consumer, which
// is why every logical meta is scoped by a view_type.
//
// Layout, from the outside in:
//
//   - One object per generation, at pchannel level:
//     <minio-root>/streamingnode/summary-store/<pchannel>/chunks/<generation>-<term>
//   - Inside that object: a footer indexing N per-vchannel chunks, each holding
//     that vchannel's committedWriteRecord list, its source message-id/timetick
//     range, and its own checksum.
//   - In etcd: one PChannelSummaryMeta per pchannel (generations, manifest,
//     owner term) plus one VChannelSummaryMeta per (view_type, vchannel)
//     recording that view's checkpoint hint and the oldest generation it still
//     needs.
//
// Naming convention in this package: "summary" is the durable, application-
// neutral data; "window" belongs to the idempotency interceptor, which is the
// application that turns summary entries into a dedup window with a TTL and a
// byte cap. A type or file here should never be named after the window.
//
// # File groups
//
// Recovery core — the recovery storage and its lifecycle:
//   - recovery_storage.go / recovery_storage_impl.go — recoveryStorageImpl + RecoverySnapshot
//   - recovery_background_task.go / recovery_persisted.go / recovery_stream.go — persist loop & replay
//   - segment_recovery_info.go / vchannel_recovery_info.go — per-segment / per-vchannel state
//   - config.go / metrics.go / checkpoint_util.go — config, metrics, checkpoint helpers (shared)
//
// Summary store — the in-memory + durable write summary (summary_* files):
//   - summary_manager.go — summaryManager, the entry point owned by recoveryStorageImpl
//   - summary_background_task.go — periodic snapshot/clean task
//   - summary_vchannel_state.go — per-vchannel summary state machine
//   - summary_store_recover.go / summary_store_persist.go — pchannel chunk store recover / persist paths
//   - summary_store_record.go — the committed-write record data model
//   - summary_store_codec.go — chunk serialization + checksums
//   - summary_store_errors.go — summary-store corruption / fencing sentinels
//   - summary_store_manifest.go — generation-to-term manifest
//   - summary_checkpoint.go / summary_cleaner.go — consume-checkpoint clamping & chunk GC
package recovery
