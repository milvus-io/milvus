package stats

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// aggregatedMetrics is the metrics of a channel.
type aggregatedMetrics struct {
	Insert ModifiedMetrics
	Delete ModifiedMetrics
}

// newAggregatedMetrics creates a new aggregated metrics.
func newAggregatedMetrics() *aggregatedMetrics {
	return &aggregatedMetrics{
		Insert: ModifiedMetrics{},
		Delete: ModifiedMetrics{},
	}
}

// IsZero returns true if the metrics is zero.
func (c *aggregatedMetrics) IsZero() bool {
	return c.Insert.IsZero() && c.Delete.IsZero()
}

// Total returns the total metrics of a channel.
func (c *aggregatedMetrics) Total() ModifiedMetrics {
	return ModifiedMetrics{
		Rows:       c.Insert.Rows + c.Delete.Rows,
		BinarySize: c.Insert.BinarySize + c.Delete.BinarySize,
	}
}

// Collect collects the metrics of a level.
func (c *aggregatedMetrics) Collect(level datapb.SegmentLevel, metrics ModifiedMetrics) {
	switch level {
	case datapb.SegmentLevel_L0:
		c.Delete.Collect(metrics)
	case datapb.SegmentLevel_L1:
		c.Insert.Collect(metrics)
	default:
		panicIfInvalidLevel(level)
	}
}

// Subtract subtracts the metrics of a level.
func (c *aggregatedMetrics) Subtract(level datapb.SegmentLevel, metrics ModifiedMetrics) {
	switch level {
	case datapb.SegmentLevel_L0:
		c.Delete.Subtract(metrics)
	case datapb.SegmentLevel_L1:
		c.Insert.Subtract(metrics)
	default:
		panicIfInvalidLevel(level)
	}
}

// GetByLevel returns the metrics of a level.
func (c *aggregatedMetrics) Get(level datapb.SegmentLevel) ModifiedMetrics {
	switch level {
	case datapb.SegmentLevel_L0:
		return c.Delete
	case datapb.SegmentLevel_L1:
		return c.Insert
	default:
		panicIfInvalidLevel(level)
	}
	return ModifiedMetrics{}
}

// panicIfInvalidLevel panics if the level is invalid.
func panicIfInvalidLevel(level datapb.SegmentLevel) {
	panic(fmt.Sprintf("invalid level: %s", level))
}
