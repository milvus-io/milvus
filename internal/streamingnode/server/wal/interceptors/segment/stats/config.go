package stats

import (
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newStatsConfig creates a new config for the stats manager.
func newStatsConfig() statsConfig {
	params := paramtable.Get()
	memoryTheshold := params.StreamingCfg.NodeMemoryUsageThreshold.GetAsFloat()
	hwmThreshold := params.StreamingCfg.NodeGrowingSegmentBytesHwmThreshold.GetAsFloat()
	lwmThreshold := params.StreamingCfg.NodeGrowingSegmentBytesLwmThreshold.GetAsFloat()
	memory := float64(hardware.GetMemoryCount())

	segmentMaxBinlogFileNum := paramtable.Get().DataCoordCfg.SegmentMaxBinlogFileNumber.GetAsInt()
	maxLifetime := paramtable.Get().DataCoordCfg.SegmentMaxLifetime.GetAsDuration(time.Second)
	maxIdleTime := paramtable.Get().DataCoordCfg.SegmentMaxIdleTime.GetAsDuration(time.Second)
	minSizeFromIdleTime := paramtable.Get().DataCoordCfg.SegmentMinSizeFromIdleToSealed.GetAsInt64() * 1024 * 1024
	return statsConfig{
		maxBinlogFileNum:    segmentMaxBinlogFileNum,
		memoryThreshold:     memoryTheshold,
		growingBytesHWM:     int64(hwmThreshold * memory),
		growingBytesLWM:     int64(lwmThreshold * memory),
		maxLifetime:         maxLifetime,
		maxIdleTime:         maxIdleTime,
		minSizeFromIdleTime: minSizeFromIdleTime,
	}
}

// statsConfig is the configuration for the stats manager.
type statsConfig struct {
	maxBinlogFileNum    int
	memoryThreshold     float64
	growingBytesHWM     int64
	growingBytesLWM     int64
	maxLifetime         time.Duration
	maxIdleTime         time.Duration
	minSizeFromIdleTime int64
}

// Validate checks if the config is valid.
func (c statsConfig) Validate() error {
	if c.memoryThreshold <= 0 ||
		c.growingBytesHWM <= 0 ||
		c.growingBytesLWM <= 0 ||
		c.growingBytesHWM <= c.growingBytesLWM ||
		c.maxLifetime <= 0 ||
		c.maxIdleTime <= 0 ||
		c.minSizeFromIdleTime <= 0 ||
		c.maxBinlogFileNum <= 0 {
		return errors.Errorf("invalid stats config, cfg: %+v", c)
	}
	return nil
}
