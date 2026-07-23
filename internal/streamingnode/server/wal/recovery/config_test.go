package recovery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestNewConfig(t *testing.T) {
	// Mock paramtable values
	paramtable.Init()
	params := paramtable.Get()
	defer params.Reset(params.StreamingCfg.IdempotencySnapshotInterval.Key)
	cfg := newConfig()

	assert.Equal(t, 10*time.Second, cfg.persistInterval)
	assert.Equal(t, 10*time.Second, cfg.idempotencySnapshotInterval)
	assert.Equal(t, 100, cfg.maxDirtyMessages)
	assert.Equal(t, 3*time.Second, cfg.gracefulTimeout)
	assert.False(t, cfg.idempotencyEnabled)
}

func TestNewConfigKeepsRecoveryAndIdempotencyIntervalsIndependent(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	defer params.Reset(params.StreamingCfg.IdempotencySnapshotInterval.Key)

	params.Save(params.StreamingCfg.IdempotencySnapshotInterval.Key, "2s")
	cfg := newConfig()

	assert.Equal(t, 10*time.Second, cfg.persistInterval)
	assert.Equal(t, 2*time.Second, cfg.idempotencySnapshotInterval)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name                        string
		persistInterval             time.Duration
		maxDirtyMessages            int
		gracefulTimeout             time.Duration
		idempotencyEnabled          bool
		idempotencySnapshotInterval time.Duration
		idempotencyWindowTTL        time.Duration
		idempotencyMaxEntries       int
		expectError                 bool
	}{
		{"ValidConfig", 10 * time.Second, 100, 5 * time.Second, false, 0, 0, 0, false},
		{"InvalidPersistInterval", 0, 100, 5 * time.Second, false, 0, 0, 0, true},
		{"InvalidMaxDirtyMessages", 10 * time.Second, 0, 5 * time.Second, false, 0, 0, 0, true},
		{"InvalidGracefulTimeout", 10 * time.Second, 100, 0, false, 0, 0, 0, true},
		// Invalid idempotency parameter combinations are repaired by
		// sanitizeIdempotency (see TestSanitizeIdempotencyFallsBack), not rejected
		// by validate: they are runtime-tunable operator knobs and a panic here
		// would crash-loop every WAL open on the node.
		{"IdempotencyEnabledValidConfig", 10 * time.Second, 100, 5 * time.Second, true, 10 * time.Second, 10 * time.Minute, 0, false},
		{"IdempotencyEnabledZeroSnapshotInterval", 10 * time.Second, 100, 5 * time.Second, true, 0, 10 * time.Minute, 0, false},
		{"IdempotencyEnabledNoTTLNoMaxEntries", 10 * time.Second, 100, 5 * time.Second, true, 10 * time.Second, 0, 0, false},
		// Disabled idempotency tolerates fully non-positive window config.
		{"IdempotencyDisabledUnboundedWindow", 10 * time.Second, 100, 5 * time.Second, false, 0, 0, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config{
				persistInterval:             tt.persistInterval,
				maxDirtyMessages:            tt.maxDirtyMessages,
				gracefulTimeout:             tt.gracefulTimeout,
				idempotencyEnabled:          tt.idempotencyEnabled,
				idempotencySnapshotInterval: tt.idempotencySnapshotInterval,
				idempotencyWindowTTL:        tt.idempotencyWindowTTL,
				idempotencyMaxEntries:       tt.idempotencyMaxEntries,
			}
			err := cfg.validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSanitizeIdempotencyFallsBack(t *testing.T) {
	paramtable.Init()

	// Both knobs invalid: fall back to the parameter defaults with a warning.
	cfg := &config{idempotencyEnabled: true, idempotencySnapshotInterval: 0, idempotencyWindowTTL: 0, idempotencyMaxEntries: 0}
	cfg.sanitizeIdempotency()
	assert.Equal(t, 10*time.Second, cfg.idempotencySnapshotInterval)
	assert.Equal(t, 10*time.Minute, cfg.idempotencyWindowTTL)

	// An explicit max entry cap makes windowTTL: 0s a valid "count-capped only"
	// configuration; nothing is rewritten.
	cfg = &config{idempotencyEnabled: true, idempotencySnapshotInterval: 5 * time.Second, idempotencyWindowTTL: 0, idempotencyMaxEntries: 100}
	cfg.sanitizeIdempotency()
	assert.Equal(t, time.Duration(0), cfg.idempotencyWindowTTL)
	assert.Equal(t, 5*time.Second, cfg.idempotencySnapshotInterval)

	// Disabled idempotency is left untouched.
	cfg = &config{idempotencyEnabled: false}
	cfg.sanitizeIdempotency()
	assert.Zero(t, cfg.idempotencyWindowTTL)
	assert.Zero(t, cfg.idempotencySnapshotInterval)

	// End-to-end: an operator explicitly zeroing BOTH windowTTL and
	// maxEntriesPerWindow must not panic the WAL open — the TTL falls back to
	// its default with a warning. (windowTTL: 0s alone is now a legitimate
	// count-capped configuration, since maxEntriesPerWindow defaults to 10000.)
	params := paramtable.Get()
	params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "true")
	params.Save(params.StreamingCfg.IdempotencyWindowTTL.Key, "0s")
	params.Save(params.StreamingCfg.IdempotencyMaxEntriesPerWindow.Key, "0")
	params.Save(params.StreamingCfg.IdempotencyMaxBytesPerWindow.Key, "0")
	defer func() {
		params.Reset(params.StreamingCfg.IdempotencyEnabled.Key)
		params.Reset(params.StreamingCfg.IdempotencyWindowTTL.Key)
		params.Reset(params.StreamingCfg.IdempotencyMaxEntriesPerWindow.Key)
		params.Reset(params.StreamingCfg.IdempotencyMaxBytesPerWindow.Key)
	}()
	assert.NotPanics(t, func() {
		cfg := newConfig()
		assert.Equal(t, 10*time.Minute, cfg.idempotencyWindowTTL)
	})
}
