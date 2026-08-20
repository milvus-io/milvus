package recovery

import (
	"context"
	"strconv"
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
		persistInterval:              persistInterval,
		maxDirtyMessages:             maxDirtyMessages,
		gracefulTimeout:              gracefulTimeout,
		idempotencyEnabled:           params.StreamingCfg.IdempotencyEnabled.GetAsBool(),
		idempotencyMinRetainedBytes:  int(params.StreamingCfg.IdempotencyMinRetainedBytes.GetAsSize()),
		idempotencyRetentionTTL:      params.StreamingCfg.IdempotencyRetentionTTL.GetAsDurationByParse(),
		idempotencyMaxRetainedChunks: params.StreamingCfg.IdempotencyMaxRetainedChunks.GetAsInt(),
		idempotencyGCInterval:        params.StreamingCfg.IdempotencyGCInterval.GetAsDurationByParse(),
	}
	cfg.sanitizeIdempotency()
	if err := cfg.validate(); err != nil {
		panic(err)
	}
	return cfg
}

// sanitizeIdempotency repairs unusable values by falling back to their defaults
// with a warning, instead of panicking the WAL open into a crash loop over an
// operator typo.
func (cfg *config) sanitizeIdempotency() {
	if !cfg.idempotencyEnabled {
		return
	}
	params := paramtable.Get()
	// A non-positive interval would stop gc entirely, so released chunks would
	// accumulate in object storage forever.
	if cfg.idempotencyGCInterval <= 0 {
		fallback := defaultDuration(&params.StreamingCfg.IdempotencyGCInterval)
		mlog.Warn(context.TODO(), "non-positive idempotency gc interval; falling back to default",
			mlog.Duration("configured", cfg.idempotencyGCInterval),
			mlog.Duration("fallback", fallback))
		cfg.idempotencyGCInterval = fallback
	}
	// Retention is the only bound on the durable chunk set; without it the store
	// would keep every generation forever.
	// The floor is what makes retention survive an outage; without it a TTL alone
	// would empty the store exactly when a resuming client needs it.
	if cfg.idempotencyMinRetainedBytes <= 0 {
		fallback := defaultInt(&params.StreamingCfg.IdempotencyMinRetainedBytes)
		mlog.Warn(context.TODO(), "non-positive idempotency retention floor; falling back to default",
			mlog.Int("configured", cfg.idempotencyMinRetainedBytes),
			mlog.Int("fallback", fallback))
		cfg.idempotencyMinRetainedBytes = fallback
	}
	// Without the cap, a workload that writes little per persist keeps a great
	// many tiny chunks inside the floor, and recovery has to read every one.
	if cfg.idempotencyMaxRetainedChunks <= 0 {
		fallback := defaultInt(&params.StreamingCfg.IdempotencyMaxRetainedChunks)
		mlog.Warn(context.TODO(), "non-positive idempotency retained chunk cap; falling back to default",
			mlog.Int("configured", cfg.idempotencyMaxRetainedChunks),
			mlog.Int("fallback", fallback))
		cfg.idempotencyMaxRetainedChunks = fallback
	}
}

// defaultInt parses a ParamItem's declared default; the defaults are
// compile-time literals, so a parse failure is unreachable.
func defaultInt(item *paramtable.ParamItem) int {
	value, err := strconv.Atoi(item.DefaultValue)
	if err != nil {
		panic(err)
	}
	return value
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
	persistInterval  time.Duration // persistInterval is the interval to persist the dirty recovery snapshot.
	maxDirtyMessages int           // maxDirtyMessages is the maximum number of dirty messages to be persisted.
	gracefulTimeout  time.Duration // gracefulTimeout is the timeout for graceful close of recovery module.

	idempotencyEnabled bool // idempotencyEnabled gates all summary-store machinery (recovery, bootstrap, in-memory summaries).
	// idempotencyMinRetainedBytes is the FLOOR of durable retention, in bytes of
	// chunk objects. Chunks inside it survive even past the TTL, so an outage of
	// any length still leaves the most recent writes recoverable.
	//
	// It is accounted per OBJECT, not per vchannel: a chunk is retained or released
	// whole, so the object is both what it costs to keep and what recovery pays to
	// read. How much of what the store hands over a consumer then caches in memory
	// is the consumer's own bound (the window's byte cap), not this one.
	idempotencyMinRetainedBytes int
	// idempotencyRetentionTTL is the normal expiry. A chunk is released only when
	// it is older than this AND outside the floor above.
	idempotencyRetentionTTL time.Duration
	// idempotencyMaxRetainedChunks is the CAP on how many chunks stay retained, and
	// it overrides the floor. The floor bounds bytes, which is not what recovery
	// pays for: recovery pays per chunk. Without this, a workload writing little
	// per persist grows the manifest and the replay without limit.
	idempotencyMaxRetainedChunks int
	// idempotencyGCInterval is how often queued chunk deletions are swept. There
	// is no periodic persist: chunks are written synchronously with the WAL
	// consume checkpoint.
	idempotencyGCInterval time.Duration
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
