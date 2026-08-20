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
	defer params.Reset(params.StreamingCfg.IdempotencyGCInterval.Key)
	cfg := newConfig()

	assert.Equal(t, 10*time.Second, cfg.persistInterval)
	assert.Equal(t, 10*time.Second, cfg.idempotencyGCInterval)
	assert.Equal(t, 100, cfg.maxDirtyMessages)
	assert.Equal(t, 3*time.Second, cfg.gracefulTimeout)
	assert.False(t, cfg.idempotencyEnabled)
}

func TestNewConfigKeepsRecoveryAndIdempotencyIntervalsIndependent(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	defer params.Reset(params.StreamingCfg.IdempotencyGCInterval.Key)

	params.Save(params.StreamingCfg.IdempotencyGCInterval.Key, "2s")
	cfg := newConfig()

	assert.Equal(t, 10*time.Second, cfg.persistInterval)
	assert.Equal(t, 2*time.Second, cfg.idempotencyGCInterval)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name                        string
		persistInterval             time.Duration
		maxDirtyMessages            int
		gracefulTimeout             time.Duration
		idempotencyEnabled          bool
		idempotencyGCInterval       time.Duration
		idempotencyRetentionTTL     time.Duration
		idempotencyMinRetainedBytes int
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
		{"IdempotencyEnabledZeroGCInterval", 10 * time.Second, 100, 5 * time.Second, true, 0, 10 * time.Minute, 0, false},
		{"IdempotencyEnabledNoTTLNoFloor", 10 * time.Second, 100, 5 * time.Second, true, 10 * time.Second, 0, 0, false},
		// Disabled idempotency tolerates fully non-positive summary config.
		{"IdempotencyDisabledUnboundedWindow", 10 * time.Second, 100, 5 * time.Second, false, 0, 0, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config{
				persistInterval:             tt.persistInterval,
				maxDirtyMessages:            tt.maxDirtyMessages,
				gracefulTimeout:             tt.gracefulTimeout,
				idempotencyEnabled:          tt.idempotencyEnabled,
				idempotencyGCInterval:       tt.idempotencyGCInterval,
				idempotencyRetentionTTL:     tt.idempotencyRetentionTTL,
				idempotencyMinRetainedBytes: tt.idempotencyMinRetainedBytes,
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

	// Every knob invalid: fall back to the parameter defaults with a warning
	// rather than crash-looping the WAL open over an operator typo.
	cfg := &config{idempotencyEnabled: true}
	cfg.sanitizeIdempotency()
	assert.Equal(t, 10*time.Second, cfg.idempotencyGCInterval)
	assert.Equal(t, 64*1024*1024, cfg.idempotencyMinRetainedBytes)
	assert.Positive(t, cfg.idempotencyChunkTargetBytes)
	assert.Positive(t, cfg.idempotencyManifestChunkInterval)

	// The retention floor is what keeps the store non-empty after an outage, so
	// a non-positive value is repaired rather than honoured.
	cfg = &config{idempotencyEnabled: true, idempotencyMinRetainedBytes: 0}
	cfg.sanitizeIdempotency()
	assert.Positive(t, cfg.idempotencyMinRetainedBytes)

	// Explicit values are left alone.
	cfg = &config{
		idempotencyEnabled:               true,
		idempotencyGCInterval:            5 * time.Second,
		idempotencyMinRetainedBytes:      100,
		idempotencyChunkTargetBytes:      200,
		idempotencyManifestChunkInterval: 3,
	}
	cfg.sanitizeIdempotency()
	assert.Equal(t, 5*time.Second, cfg.idempotencyGCInterval)
	assert.Equal(t, 100, cfg.idempotencyMinRetainedBytes)

	// Disabled idempotency is left untouched.
	cfg = &config{idempotencyEnabled: false}
	cfg.sanitizeIdempotency()
	assert.Zero(t, cfg.idempotencyGCInterval)
	assert.Zero(t, cfg.idempotencyMinRetainedBytes)
}
