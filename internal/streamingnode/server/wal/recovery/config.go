package recovery

import (
	"time"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// newConfig creates a new config for the recovery module.
func newConfig() *config {
	params := paramtable.Get()
	persistInterval := params.StreamingCfg.WALRecoveryPersistInterval.GetAsDurationByParse()
	maxDirtyMessages := params.StreamingCfg.WALRecoveryMaxDirtyMessage.GetAsInt()
	gracefulTimeout := params.StreamingCfg.WALRecoveryGracefulCloseTimeout.GetAsDurationByParse()
	cfg := &config{
		persistInterval:             persistInterval,
		maxDirtyMessages:            maxDirtyMessages,
		gracefulTimeout:             gracefulTimeout,
		idempotencyEnabled:          params.StreamingCfg.IdempotencyEnabled.GetAsBool(),
		idempotencyWindowTTL:        params.StreamingCfg.IdempotencyWindowTTL.GetAsDurationByParse(),
		idempotencyMinEntries:       params.StreamingCfg.IdempotencyMinEntriesPerWindow.GetAsInt(),
		idempotencyMaxEntries:       params.StreamingCfg.IdempotencyMaxEntriesPerWindow.GetAsInt(),
		idempotencySnapshotInterval: params.StreamingCfg.IdempotencySnapshotInterval.GetAsDurationByParse(),
	}
	if err := cfg.validate(); err != nil {
		panic(err)
	}
	return cfg
}

// config is the configuration for the recovery module.
type config struct {
	persistInterval             time.Duration // persistInterval is the interval to persist the dirty recovery snapshot.
	maxDirtyMessages            int           // maxDirtyMessages is the maximum number of dirty messages to be persisted.
	gracefulTimeout             time.Duration // gracefulTimeout is the timeout for graceful close of recovery module.
	idempotencyEnabled          bool          // idempotencyEnabled gates all idempotency-window machinery (recovery, bootstrap, in-memory windows).
	idempotencyWindowTTL        time.Duration // idempotencyWindowTTL is the TTL for evicting entries during recovery.
	idempotencyMinEntries       int           // idempotencyMinEntries is the minimum entries to keep per window during eviction.
	idempotencyMaxEntries       int           // idempotencyMaxEntries is the hard cap per window during eviction.
	idempotencySnapshotInterval time.Duration // idempotencySnapshotInterval is the interval to persist idempotency window snapshots. Non-positive disables periodic window snapshots.
}

func (cfg *config) validate() error {
	if cfg.persistInterval <= 0 {
		return status.NewInvalidArgument("persist interval must be greater than 0")
	}
	if cfg.maxDirtyMessages <= 0 {
		return status.NewInvalidArgument("max dirty messages must be greater than 0")
	}
	if cfg.gracefulTimeout <= 0 {
		return status.NewInvalidArgument("graceful timeout must be greater than 0")
	}
	// When idempotency is enabled the window background task is the only thing that
	// persists and evicts window entries; a non-positive snapshot interval disables
	// its ticker, so the in-memory windows would grow without bound until OOM.
	if cfg.idempotencyEnabled && cfg.idempotencySnapshotInterval <= 0 {
		return status.NewInvalidArgument("idempotency snapshot interval must be greater than 0 when idempotency is enabled")
	}
	// The live interceptor window only evicts on a positive TTL or a positive max
	// entries cap; with both non-positive it would grow without bound per key.
	if cfg.idempotencyEnabled && cfg.idempotencyWindowTTL <= 0 && cfg.idempotencyMaxEntries <= 0 {
		return status.NewInvalidArgument("idempotency window TTL or max entries per window must be greater than 0 when idempotency is enabled")
	}
	return nil
}
