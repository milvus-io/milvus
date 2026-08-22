// Package recovery rebuilds and durably persists the per-pchannel WAL recovery
// state of a streaming node: vchannel/segment assignments, the consume
// checkpoint, and the write summary store.
//
// # The summary store
//
// The summary store is a consumer-agnostic record of what a pchannel durably
// wrote. It is deliberately NOT named after any consumer: it stores committed
// write facts (source message id, timetick, last-confirmed position, primary
// keys, and whatever a view adds on top), and applications build their own
// views from them. Today the idempotency interceptor materializes a write-dedup
// window; a primary-key index is the second planned consumer.
//
// Layout, from the outside in:
//
//   - In etcd, one PChannelSummaryMeta per pchannel, carrying the owner's WAL
//     assignment term and nothing else. It is not a checkpoint and not an
//     inventory: it only gives recovery a floor when it probes for the newest
//     manifest.
//   - In object storage, one manifest per term:
//     <root>/streamingnode/summary-store/<pchannel>/<pchannel>.manifest.<term>
//     It lists every chunk recovery may read and every chunk queued for
//     deletion. It is the only index into the chunk set.
//   - In object storage, one chunk object per generation:
//     <root>/streamingnode/summary-store/<pchannel>/chunks/chunk.<gen>.term<term>.psc
//
// Inside a chunk, a vchannel's records are stored as SEPARATE SECTIONS rather
// than one message — the idempotency annotation (key, row offsets) in one, the
// write itself (identity and primary keys) in another — each located by its own
// {offset, length, record_count} ref in the footer. A consumer range-reads only
// the section it needs, which is what lets a future primary-key index reuse the
// stored primary keys instead of writing its own copy. Sections of one vchannel
// correspond by position; they are built in one pass over one sorted slice, so
// they cannot disagree.
//
// A physical WAL position is recorded only at pchannel level. Everything below
// it is addressed by timetick.
//
// There is no persist watermark. A chunk is written synchronously from the WAL
// checkpoint's dirty persist, BEFORE that checkpoint is saved, so the consume
// checkpoint is itself the boundary between what a chunk holds and what the WAL
// still holds. Nothing records a second position, nothing clamps the
// checkpoint, and recovery never rewinds.
//
// # Model boundaries
//
// For everything that crosses a storage boundary, the generated protobuf types
// ARE the model. What never crosses one does NOT get a message in
// streaming.proto: SummaryRecord and VChannelSummarySnapshot are plain Go
// structs, built and consumed in-process, and a proto there would advertise a
// wire contract that does not exist. SummaryRecord is also why the section split
// lives only in the codec — in memory there is no partial consumer, so whoever
// holds a record holds all of it.
//
// Naming convention in this package: "summary" is the durable, consumer-neutral
// data; "window" belongs to the idempotency interceptor, which is the consumer
// that turns summary records into a dedup window with a byte cap. A type or file
// here should never be named after the window.
//
// # File groups
//
// Recovery core — the recovery storage and its lifecycle:
//   - recovery_storage.go / recovery_storage_impl.go — recoveryStorageImpl + RecoverySnapshot
//   - recovery_background_task.go / recovery_persisted.go / recovery_stream.go — persist loop & replay
//   - segment_recovery_info.go / vchannel_recovery_info.go — per-segment / per-vchannel state
//   - config.go / metrics.go / checkpoint_util.go — config, metrics, checkpoint helpers (shared)
//
// Summary store — the in-memory staging plus the durable store (summary_* files):
//   - summary_manager.go — summaryManager, the entry point owned by recoveryStorageImpl
//   - summary_background_task.go — the gc loop, and the persist called from the checkpoint
//   - summary_vchannel_state.go — per-vchannel staging buffer
//   - summary_store_record.go — SummaryRecord, the in-memory write fact
//   - summary_store_recover.go / summary_store_persist.go — recover / persist paths
//   - summary_store_codec.go — chunk framing and the section split
//   - summary_store_manifest.go — the manifest, retention, and the gc queue
//   - summary_store_meta.go — the etcd meta and its CAS
//   - summary_store_errors.go — corruption / fencing sentinels
//   - summary_cleaner.go — chunk gc, and the drop path when idempotency is off
package recovery
