package recovery

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
	cfg.sanitizeIdempotency()
	if err := cfg.validate(); err != nil {
		panic(err)
	}
	return cfg
}

// sanitizeIdempotency repairs invalid combinations of the runtime-tunable
// idempotency parameters by falling back to their defaults with a warning,
// instead of panicking the WAL open into a crash loop over an operator typo
// (e.g. windowTTL: 0s with maxEntriesPerWindow left at its 0 default). The
// same fallback is applied by the idempotency interceptor builder — keep both
// in sync.
func (cfg *config) sanitizeIdempotency() {
	if !cfg.idempotencyEnabled {
		return
	}
	params := paramtable.Get()
	// A non-positive snapshot interval would disable the only task that persists
	// and evicts recovery-side window entries, growing memory without bound.
	if cfg.idempotencySnapshotInterval <= 0 {
		fallback := defaultDuration(&params.StreamingCfg.IdempotencySnapshotInterval)
		mlog.Warn(context.TODO(), "non-positive idempotency snapshot interval; falling back to default",
			mlog.Duration("configured", cfg.idempotencySnapshotInterval),
			mlog.Duration("fallback", fallback))
		cfg.idempotencySnapshotInterval = fallback
	}
	// With neither a TTL nor a max entry cap the live window would grow without
	// bound per key; fall back to the default TTL.
	if cfg.idempotencyWindowTTL <= 0 && cfg.idempotencyMaxEntries <= 0 {
		fallback := defaultDuration(&params.StreamingCfg.IdempotencyWindowTTL)
		mlog.Warn(context.TODO(), "idempotency window has neither a positive TTL nor a positive max entry cap; falling back to default TTL",
			mlog.Duration("configuredTTL", cfg.idempotencyWindowTTL),
			mlog.Duration("fallbackTTL", fallback))
		cfg.idempotencyWindowTTL = fallback
	}
}

// defaultDuration parses a ParamItem's declared default; the defaults are
// compile-time literals, so a parse failure is unreachable (paramtable itself
// panics on it in the regular read path).
func defaultDuration(item *paramtable.ParamItem) time.Duration {
	fallback, err := time.ParseDuration(item.DefaultValue)
	if err != nil {
		panic(err)
	}
	return fallback
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
	// The idempotency parameter combinations are repaired by sanitizeIdempotency
	// (fallback + warning) instead of validated here: they are runtime-tunable
	// operator knobs, and a panic here would put every WAL open on this node into
	// a crash loop over a config typo.
	return nil
}
