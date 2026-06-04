// Package recovery rebuilds and durably persists the per-pchannel WAL recovery
// state of a streaming node: vchannel/segment assignments, the consume
// checkpoint, and the idempotency write-dedup window.
//
// The files fall into three groups:
//
// Recovery core — the recovery storage and its lifecycle:
//   - recovery_storage.go / recovery_storage_impl.go — recoveryStorageImpl + RecoverySnapshot
//   - recovery_background_task.go / recovery_persisted.go / recovery_stream.go — persist loop & replay
//   - segment_recovery_info.go / vchannel_recovery_info.go — per-segment / per-vchannel state
//   - config.go / metrics.go / checkpoint_util.go — config, metrics, checkpoint helpers (shared)
//
// Idempotency window — the in-memory + durable dedup window the idempotency
// interceptor relies on across restarts (window_* files):
//   - window_manager.go — windowManager, the entry point owned by recoveryStorageImpl
//   - window_background_task.go — periodic snapshot/clean task
//   - window_vchannel_state.go — per-vchannel window state machine
//   - window_store_recover.go / window_store_persist.go — pchannel chunk store recover / persist paths
//   - window_store_record.go — the committed-write record data model
//   - window_store_codec.go — chunk serialization + checksums
//   - window_store_errors.go — window-store corruption sentinel
//   - window_checkpoint.go / window_cleaner.go — window consume-checkpoint clamping & chunk GC
package recovery
