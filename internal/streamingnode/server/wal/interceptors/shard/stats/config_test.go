package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestNewStatConfig(t *testing.T) {
	paramtable.Init()
	cfg := newStatsConfig()
	assert.Greater(t, cfg.memoryThreshold, float64(0))
	assert.Greater(t, cfg.growingBytesHWM, int64(0))
	assert.Greater(t, cfg.growingBytesLWM, int64(0))
	assert.Greater(t, cfg.l1MaxIdleTime, time.Duration(0))
	assert.Greater(t, cfg.l1MaxLifetime, time.Duration(0))
	assert.Greater(t, cfg.l1MinSizeFromIdleTime, int64(0))
}

func TestStatsConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  statsConfig
		wantErr bool
	}{
		{
			name: "ValidConfig",
			config: statsConfig{
				maxBinlogFileNum:      100,
				memoryThreshold:       1024,
				growingBytesHWM:       2048,
				growingBytesLWM:       1024,
				l1MaxLifetime:         time.Hour,
				l1MaxIdleTime:         time.Minute,
				l1MinSizeFromIdleTime: 512,
				l0MaxLifetime:         10 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "InvalidMemoryWatermark",
			config: statsConfig{
				maxBinlogFileNum:      100,
				memoryThreshold:       0,
				growingBytesHWM:       2048,
				growingBytesLWM:       1024,
				l1MaxLifetime:         time.Hour,
				l1MaxIdleTime:         time.Minute,
				l1MinSizeFromIdleTime: 512,
				l0MaxLifetime:         10 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "InvalidGrowingBytesHWM",
			config: statsConfig{
				maxBinlogFileNum:      100,
				memoryThreshold:       1024,
				growingBytesHWM:       0,
				growingBytesLWM:       1024,
				l1MaxLifetime:         time.Hour,
				l1MaxIdleTime:         time.Minute,
				l1MinSizeFromIdleTime: 512,
				l0MaxLifetime:         10 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "GrowingBytesHWM_LessThan_LWM",
			config: statsConfig{
				maxBinlogFileNum:      100,
				memoryThreshold:       1024,
				growingBytesHWM:       1024,
				growingBytesLWM:       2048,
				l1MaxLifetime:         time.Hour,
				l1MaxIdleTime:         time.Minute,
				l1MinSizeFromIdleTime: 512,
				l0MaxLifetime:         10 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "InvalidMaxLifetime",
			config: statsConfig{
				maxBinlogFileNum:      100,
				memoryThreshold:       1024,
				growingBytesHWM:       2048,
				growingBytesLWM:       1024,
				l1MaxLifetime:         0,
				l1MaxIdleTime:         time.Minute,
				l1MinSizeFromIdleTime: 512,
				l0MaxLifetime:         10 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "InvalidMaxIdleTime",
			config: statsConfig{
				maxBinlogFileNum:      100,
				memoryThreshold:       1024,
				growingBytesHWM:       2048,
				growingBytesLWM:       1024,
				l1MaxLifetime:         time.Hour,
				l1MaxIdleTime:         0,
				l1MinSizeFromIdleTime: 512,
				l0MaxLifetime:         10 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "InvalidMinSizeFromIdleTime",
			config: statsConfig{
				maxBinlogFileNum:      100,
				memoryThreshold:       1024,
				growingBytesHWM:       2048,
				growingBytesLWM:       1024,
				l1MaxLifetime:         time.Hour,
				l1MaxIdleTime:         time.Minute,
				l1MinSizeFromIdleTime: 0,
				l0MaxLifetime:         10 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "InvalidMaxBinlogFileNum",
			config: statsConfig{
				maxBinlogFileNum:      0,
				memoryThreshold:       1024,
				growingBytesHWM:       2048,
				growingBytesLWM:       1024,
				l1MaxLifetime:         time.Hour,
				l1MaxIdleTime:         time.Minute,
				l1MinSizeFromIdleTime: 0,
				l0MaxLifetime:         10 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "InvalidL0MaxLifetime",
			config: statsConfig{
				maxBinlogFileNum:      100,
				memoryThreshold:       1024,
				growingBytesHWM:       2048,
				growingBytesLWM:       1024,
				l1MaxLifetime:         time.Hour,
				l1MaxIdleTime:         time.Minute,
				l1MinSizeFromIdleTime: 512,
				l0MaxLifetime:         0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
