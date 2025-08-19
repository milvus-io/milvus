package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func TestNewAggregatedMetrics(t *testing.T) {
	am := newAggregatedMetrics()
	assert.NotNil(t, am)
	assert.True(t, am.IsZero())
}

func TestAggregatedMetrics_CollectAndSubtract(t *testing.T) {
	am := newAggregatedMetrics()
	metricsL0 := ModifiedMetrics{Rows: 10, BinarySize: 100}
	metricsL1 := ModifiedMetrics{Rows: 20, BinarySize: 200}

	// Collect L0 (should go to Delete)
	am.Collect(datapb.SegmentLevel_L0, metricsL0)
	assert.Equal(t, metricsL0, am.Delete)
	assert.True(t, am.Insert.IsZero())

	// Collect L1 (should go to Insert)
	am.Collect(datapb.SegmentLevel_L1, metricsL1)
	assert.Equal(t, metricsL1, am.Insert)
	assert.Equal(t, metricsL0, am.Delete)

	// Subtract L0 (should zero Delete)
	am.Subtract(datapb.SegmentLevel_L0, metricsL0)
	assert.True(t, am.Delete.IsZero())
	assert.Equal(t, metricsL1, am.Insert)

	// Subtract L1 (should zero Insert)
	am.Subtract(datapb.SegmentLevel_L1, metricsL1)
	assert.True(t, am.Insert.IsZero())
	assert.True(t, am.Delete.IsZero())
	assert.True(t, am.IsZero())
}

func TestAggregatedMetrics_PanicIfInvalidLevel(t *testing.T) {
	am := newAggregatedMetrics()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for invalid level, but did not panic")
		}
	}()
	// Use an invalid level
	am.Collect(datapb.SegmentLevel(-1), ModifiedMetrics{Rows: 1, BinarySize: 1})
}
