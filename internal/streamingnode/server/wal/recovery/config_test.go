package recovery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestNewConfig(t *testing.T) {
	// Mock paramtable values
	paramtable.Init()
	cfg := newConfig()

	assert.Equal(t, 10*time.Second, cfg.persistInterval)
	assert.Equal(t, 100, cfg.maxDirtyMessages)
	assert.Equal(t, 3*time.Second, cfg.gracefulTimeout)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name             string
		persistInterval  time.Duration
		maxDirtyMessages int
		gracefulTimeout  time.Duration
		expectError      bool
	}{
		{"ValidConfig", 10 * time.Second, 100, 5 * time.Second, false},
		{"InvalidPersistInterval", 0, 100, 5 * time.Second, true},
		{"InvalidMaxDirtyMessages", 10 * time.Second, 0, 5 * time.Second, true},
		{"InvalidGracefulTimeout", 10 * time.Second, 100, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config{
				persistInterval:  tt.persistInterval,
				maxDirtyMessages: tt.maxDirtyMessages,
				gracefulTimeout:  tt.gracefulTimeout,
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

func TestTruncatorConfig(t *testing.T) {
	// Mock paramtable values
	paramtable.Init()
	cfg := newTruncatorConfig()

	assert.Equal(t, 1*time.Minute, cfg.sampleInterval)
	assert.Equal(t, 5*time.Minute, cfg.retentionInterval)
}
