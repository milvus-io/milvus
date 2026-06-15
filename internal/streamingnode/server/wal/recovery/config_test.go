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
		// Idempotency enabled requires a positive snapshot interval, otherwise the
		// window background task never persists and the windows grow without bound.
		{"IdempotencyEnabledValidConfig", 10 * time.Second, 100, 5 * time.Second, true, 10 * time.Second, 10 * time.Minute, 0, false},
		{"IdempotencyEnabledZeroSnapshotInterval", 10 * time.Second, 100, 5 * time.Second, true, 0, 10 * time.Minute, 0, true},
		{"IdempotencyEnabledNegativeSnapshotInterval", 10 * time.Second, 100, 5 * time.Second, true, -1, 10 * time.Minute, 0, true},
		// Idempotency enabled requires a positive window TTL or max entries, otherwise
		// the live in-memory window has no bound.
		{"IdempotencyEnabledWindowTTLOnly", 10 * time.Second, 100, 5 * time.Second, true, 10 * time.Second, 10 * time.Minute, 0, false},
		{"IdempotencyEnabledMaxEntriesOnly", 10 * time.Second, 100, 5 * time.Second, true, 10 * time.Second, 0, 100, false},
		{"IdempotencyEnabledNoTTLNoMaxEntries", 10 * time.Second, 100, 5 * time.Second, true, 10 * time.Second, 0, 0, true},
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
