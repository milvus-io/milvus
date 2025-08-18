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
	memoryTheshold := params.StreamingCfg.FlushMemoryThreshold.GetAsFloat()
	hwmThreshold := params.StreamingCfg.FlushGrowingSegmentBytesHwmThreshold.GetAsFloat()
	lwmThreshold := params.StreamingCfg.FlushGrowingSegmentBytesLwmThreshold.GetAsFloat()
	memory := float64(hardware.GetMemoryCount())

	segmentMaxBinlogFileNum := paramtable.Get().DataCoordCfg.SegmentMaxBinlogFileNumber.GetAsInt()
	l1MaxLifetime := paramtable.Get().DataCoordCfg.SegmentMaxLifetime.GetAsDuration(time.Second)
	l1MaxIdleTime := paramtable.Get().DataCoordCfg.SegmentMaxIdleTime.GetAsDuration(time.Second)
	l1MinSizeFromIdleTime := paramtable.Get().DataCoordCfg.SegmentMinSizeFromIdleToSealed.GetAsInt64() * 1024 * 1024

	l0MaxLifetime := params.StreamingCfg.FlushL0MaxLifetime.GetAsDurationByParse()
	return statsConfig{
		maxBinlogFileNum:      segmentMaxBinlogFileNum,
		memoryThreshold:       memoryTheshold,
		growingBytesHWM:       int64(hwmThreshold * memory),
		growingBytesLWM:       int64(lwmThreshold * memory),
		l1MaxLifetime:         l1MaxLifetime,
		l1MaxIdleTime:         l1MaxIdleTime,
		l1MinSizeFromIdleTime: l1MinSizeFromIdleTime,
		l0MaxLifetime:         l0MaxLifetime,
	}
}

// statsConfig is the configuration for the stats manager.
type statsConfig struct {
	maxBinlogFileNum      int
	memoryThreshold       float64
	growingBytesHWM       int64
	growingBytesLWM       int64
	l1MaxLifetime         time.Duration
	l1MaxIdleTime         time.Duration
	l1MinSizeFromIdleTime int64
	l0MaxLifetime         time.Duration
}

// Validate checks if the config is valid.
func (c statsConfig) Validate() error {
	if c.memoryThreshold <= 0 ||
		c.growingBytesHWM <= 0 ||
		c.growingBytesLWM <= 0 ||
		c.growingBytesHWM <= c.growingBytesLWM ||
		c.l1MaxLifetime <= 0 ||
		c.l1MaxIdleTime <= 0 ||
		c.l1MinSizeFromIdleTime <= 0 ||
		c.maxBinlogFileNum <= 0 ||
		c.l0MaxLifetime <= 0 {
		return errors.Errorf("invalid stats config, cfg: %+v", c)
	}
	return nil
}
