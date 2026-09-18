package utils

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// setSizeMetricForTest switches dataCoord.segment.sizeMetric for the test and
// restores the previous value on cleanup.
func setSizeMetricForTest(t *testing.T, metric string) {
	t.Helper()
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SizeMetric.Key, metric)
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SizeMetric.Key) })
}

func TestStatsConvention(t *testing.T) {
	assert.Nil(t, NewProtoFromSegmentStat(nil))
	stat := &SegmentStats{
		Modified: ModifiedMetrics{
			Rows:       1,
			BinarySize: 2,
		},
		MaxBinarySize:    2,
		CreateTime:       time.Now(),
		LastModifiedTime: time.Now(),
		BinLogCounter:    3,
	}
	pb := NewProtoFromSegmentStat(stat)
	assert.Equal(t, stat.MaxBinarySize, pb.MaxBinarySize)
	assert.Equal(t, stat.Modified.Rows, pb.ModifiedRows)
	assert.Equal(t, stat.Modified.BinarySize, pb.ModifiedBinarySize)
	assert.Equal(t, stat.CreateTime.Unix(), pb.CreateTimestamp)
	assert.Equal(t, stat.LastModifiedTime.Unix(), pb.LastModifiedTimestamp)
	assert.Equal(t, stat.BinLogCounter, pb.BinlogCounter)

	stat2 := NewSegmentStatFromProto(pb)
	assert.Equal(t, stat.MaxBinarySize, stat2.MaxBinarySize)
	assert.Equal(t, stat.Modified.Rows, stat2.Modified.Rows)
	assert.Equal(t, stat.Modified.BinarySize, stat2.Modified.BinarySize)
	assert.Equal(t, stat.CreateTime.Unix(), stat2.CreateTime.Unix())
	assert.Equal(t, stat.LastModifiedTime.Unix(), stat2.LastModifiedTime.Unix())
	assert.Equal(t, stat.BinLogCounter, stat2.BinLogCounter)

	stat3 := stat2.Copy()
	stat3.Modified.Subtract(ModifiedMetrics{
		Rows:       1,
		BinarySize: 2,
	})
	assert.Equal(t, stat3.Modified.Rows, stat2.Modified.Rows-1)
	assert.Equal(t, stat3.Modified.BinarySize, stat2.Modified.BinarySize-2)
	assert.Equal(t, stat.Modified.Rows, stat2.Modified.Rows)
	assert.Equal(t, stat.Modified.BinarySize, stat2.Modified.BinarySize)
	assert.Panics(t, func() {
		stat3.Modified.Rows = 0
		stat3.Modified.Subtract(ModifiedMetrics{
			Rows:       1,
			BinarySize: 0,
		})
	})
	assert.Panics(t, func() {
		stat3.Modified.BinarySize = 0
		stat3.Modified.Subtract(ModifiedMetrics{
			Rows:       0,
			BinarySize: 1,
		})
	})

	stat4 := NewSegmentStatFromProto(nil)
	assert.Nil(t, stat4)
}

func TestNewSegmentStatFromProtoPreservesCreateSegmentTimeTick(t *testing.T) {
	pb := &streamingpb.SegmentAssignmentStat{
		CreateSegmentTimeTick: 10086,
	}

	stat := NewSegmentStatFromProto(pb)
	assert.Equal(t, uint64(10086), stat.CreateSegmentTimeTick)

	roundTrip := NewProtoFromSegmentStat(stat)
	assert.Equal(t, uint64(10086), roundTrip.GetCreateSegmentTimeTick())
}

func TestRecoveredOverTargetSegmentShouldBeSealed(t *testing.T) {
	stat := NewSegmentStatFromProto(&streamingpb.SegmentAssignmentStat{
		MaxRows:            100,
		MaxBinarySize:      400,
		ModifiedRows:       101,
		ModifiedBinarySize: 401,
	})

	assert.False(t, stat.ReachLimit)
	assert.True(t, stat.ShouldBeSealed())
}

func TestSegmentStats(t *testing.T) {
	now := time.Now()
	stat := &SegmentStats{
		Modified: ModifiedMetrics{
			Rows:       100,
			BinarySize: 200,
		},
		MaxRows:           math.MaxUint64,
		MaxBinarySize:     400,
		CreateTime:        now,
		LastModifiedTime:  now,
		BinLogCounter:     3,
		BinLogFileCounter: 4,
	}

	insert1 := ModifiedMetrics{
		Rows:       60,
		BinarySize: 120,
	}
	inserted := stat.AllocRows(insert1)
	assert.True(t, inserted)
	assert.Equal(t, stat.Modified.Rows, uint64(160))
	assert.Equal(t, stat.Modified.BinarySize, uint64(320))
	assert.True(t, time.Now().After(now))
	assert.False(t, stat.IsEmpty())
	assert.False(t, stat.ShouldBeSealed())

	insert1 = ModifiedMetrics{
		Rows:       100,
		BinarySize: 100,
	}
	inserted = stat.AllocRows(insert1)
	assert.True(t, inserted)
	assert.Equal(t, stat.Modified.Rows, uint64(260))
	assert.Equal(t, stat.Modified.BinarySize, uint64(420))
	assert.False(t, stat.IsEmpty())
	assert.True(t, stat.ShouldBeSealed())

	modifiedAfterCrossing := stat.Modified
	inserted = stat.AllocRows(ModifiedMetrics{Rows: 1, BinarySize: 1})
	assert.False(t, inserted)
	assert.Equal(t, modifiedAfterCrossing, stat.Modified)

	stat.UpdateOnSync(SyncOperationMetrics{
		BinLogCounterIncr:     4,
		BinLogFileCounterIncr: 9,
	})
	assert.Equal(t, uint64(7), stat.BinLogCounter)
	assert.Equal(t, uint64(13), stat.BinLogFileCounter)
}

func TestIsZero(t *testing.T) {
	// Test zero insert metrics
	zeroInsert := ModifiedMetrics{}
	assert.True(t, zeroInsert.IsZero())

	// Test non-zero insert metrics
	nonZeroInsert := ModifiedMetrics{
		Rows:       1,
		BinarySize: 2,
	}
	assert.False(t, nonZeroInsert.IsZero())
}

func TestOversizeFirstAllocIsAcceptedAndSealed(t *testing.T) {
	now := time.Now()
	stat := &SegmentStats{
		Modified:         ModifiedMetrics{},
		MaxRows:          100,
		MaxBinarySize:    400,
		CreateTime:       now,
		LastModifiedTime: now,
	}
	// An oversized first logical batch is accepted because the segment limit is
	// a sealing threshold, then the segment is sealed immediately.
	inserted := stat.AllocRows(ModifiedMetrics{
		Rows:       1,
		BinarySize: 401,
	})
	assert.True(t, inserted)
	assert.Equal(t, ModifiedMetrics{Rows: 1, BinarySize: 401}, stat.Modified)
	assert.False(t, stat.IsEmpty())
	assert.True(t, stat.ShouldBeSealed())
	assert.Zero(t, stat.BinaryCanBeAssign())

	modifiedAfterCrossing := stat.Modified
	inserted = stat.AllocRows(ModifiedMetrics{Rows: 1, BinarySize: 1})
	assert.False(t, inserted)
	assert.Equal(t, modifiedAfterCrossing, stat.Modified)

	rowLimitedStat := &SegmentStats{
		MaxRows:       1,
		MaxBinarySize: 400,
	}
	inserted = rowLimitedStat.AllocRows(ModifiedMetrics{
		Rows:       2,
		BinarySize: 1,
	})
	assert.True(t, inserted)
	assert.True(t, rowLimitedStat.ShouldBeSealed())
	assert.Zero(t, rowLimitedStat.RowsCanBeAssign())

	modifiedAfterRowCrossing := rowLimitedStat.Modified
	inserted = rowLimitedStat.AllocRows(ModifiedMetrics{Rows: 1, BinarySize: 1})
	assert.False(t, inserted)
	assert.Equal(t, modifiedAfterRowCrossing, rowLimitedStat.Modified)
}

func TestSegmentStatsExactLimitDoesNotSealBeforeFirstCrossingAllocation(t *testing.T) {
	stat := &SegmentStats{
		MaxRows:       2,
		MaxBinarySize: 400,
	}

	inserted := stat.AllocRows(ModifiedMetrics{Rows: 2, BinarySize: 400})
	assert.True(t, inserted)
	assert.Equal(t, ModifiedMetrics{Rows: 2, BinarySize: 400}, stat.Modified)
	assert.False(t, stat.ShouldBeSealed())
	assert.Zero(t, stat.RowsCanBeAssign())
	assert.Zero(t, stat.BinaryCanBeAssign())

	inserted = stat.AllocRows(ModifiedMetrics{Rows: 1, BinarySize: 1})
	assert.True(t, inserted)
	assert.Equal(t, ModifiedMetrics{Rows: 3, BinarySize: 401}, stat.Modified)
	assert.True(t, stat.ShouldBeSealed())

	modifiedAfterCrossing := stat.Modified
	inserted = stat.AllocRows(ModifiedMetrics{Rows: 1, BinarySize: 1})
	assert.False(t, inserted)
	assert.Equal(t, modifiedAfterCrossing, stat.Modified)
}

func TestAllocRowsSealBudgetUsesSealSize(t *testing.T) {
	setSizeMetricForTest(t, typeutil.SizeMetricMainIndex)
	stat := &SegmentStats{
		MaxBinarySize: 100,
	}
	// SealSize is measured in the active metric; BinarySize is the whole-row
	// payload. The seal budget check must use SealSize, not BinarySize.
	inserted := stat.AllocRows(ModifiedMetrics{
		Rows:       10,
		BinarySize: 90, // whole-row bytes, far below the budget
		SealSize:   60, // main-column bytes
	})
	assert.True(t, inserted)
	assert.False(t, stat.ShouldBeSealed())

	// Another insert whose seal budget crosses the limit seals the segment
	// even though the whole-row bytes remain below the budget. The crossing
	// allocation itself is accepted once, and the segment seals afterwards.
	inserted = stat.AllocRows(ModifiedMetrics{
		Rows:       10,
		BinarySize: 10,
		SealSize:   50,
	})
	assert.True(t, inserted)
	assert.True(t, stat.ShouldBeSealed())
	assert.Equal(t, uint64(110), stat.Modified.SealSize)
}

func TestAllocRowsCeilingSealsOnWholeRowBytes(t *testing.T) {
	setSizeMetricForTest(t, typeutil.SizeMetricMainIndex)
	stat := &SegmentStats{
		MaxBinarySize:      1000,
		MaxFullSegmentSize: 200,
	}
	// First insert fits both the budget and the ceiling.
	inserted := stat.AllocRows(ModifiedMetrics{
		Rows:       5,
		BinarySize: 150,
		SealSize:   10,
	})
	assert.True(t, inserted)
	assert.False(t, stat.ShouldBeSealed())

	// A second insert stays far below the seal budget (SealSize) but crosses
	// the whole-row ceiling (BinarySize); the segment must seal.
	inserted = stat.AllocRows(ModifiedMetrics{
		Rows:       5,
		BinarySize: 60,
		SealSize:   10,
	})
	assert.False(t, inserted)
	assert.True(t, stat.ShouldBeSealed())
}

func TestAllocRowsRowCap(t *testing.T) {
	stat := &SegmentStats{
		MaxRows:       100,
		MaxBinarySize: 1000,
	}
	inserted := stat.AllocRows(ModifiedMetrics{Rows: 60, BinarySize: 10, SealSize: 10})
	assert.True(t, inserted)
	assert.False(t, stat.ShouldBeSealed())
	// The second insert crosses the row cap; it is accepted as the single
	// crossing allocation and the segment seals afterwards.
	inserted = stat.AllocRows(ModifiedMetrics{Rows: 50, BinarySize: 10, SealSize: 10})
	assert.True(t, inserted)
	assert.True(t, stat.ShouldBeSealed())
}

func TestModifiedMetricsCollectSubtractSealSize(t *testing.T) {
	m := ModifiedMetrics{Rows: 1, BinarySize: 2, SealSize: 3}
	other := ModifiedMetrics{Rows: 1, BinarySize: 2, SealSize: 3}
	m.Collect(other)
	assert.Equal(t, uint64(2), m.Rows)
	assert.Equal(t, uint64(4), m.BinarySize)
	assert.Equal(t, uint64(6), m.SealSize)
	m.Subtract(other)
	assert.Equal(t, uint64(1), m.Rows)
	assert.Equal(t, uint64(2), m.BinarySize)
	assert.Equal(t, uint64(3), m.SealSize)
	assert.Panics(t, func() {
		m.Subtract(ModifiedMetrics{SealSize: 100})
	})
}

func TestSealBudgetCanBeAssignSaturatesAfterRecovery(t *testing.T) {
	// After recovery SealSize is 0 and Modified.BinarySize is whole-row. For a
	// mainIndex-metric segment the whole-row bytes can already exceed the
	// main-column budget; the capacity must saturate to 0 instead of
	// underflowing to a huge value that would disable the size seal.
	stat := &SegmentStats{
		Modified:      ModifiedMetrics{Rows: 1000, BinarySize: 500},
		MaxBinarySize: 100,
	}
	assert.Equal(t, uint64(0), stat.SealBudgetCanBeAssign())

	stat.Modified.BinarySize = 50
	assert.Equal(t, uint64(50), stat.SealBudgetCanBeAssign())
}

func TestAllocRowsCeilingSaturatesWhenWholeRowExceedsCeiling(t *testing.T) {
	setSizeMetricForTest(t, typeutil.SizeMetricMainIndex)
	// The ceiling is recomputed from config on recovery; if it is lowered below
	// the segment's existing whole-row bytes, the ceiling capacity must
	// saturate to 0 so the segment seals immediately instead of the check being
	// bypassed by an underflow.
	stat := &SegmentStats{
		Modified:           ModifiedMetrics{Rows: 10, BinarySize: 100},
		MaxBinarySize:      1000,
		MaxFullSegmentSize: 64,
	}
	// The segment already exceeds the ceiling: any further insert is rejected
	// and the segment is marked to seal.
	inserted := stat.AllocRows(ModifiedMetrics{Rows: 1, BinarySize: 1, SealSize: 1})
	assert.False(t, inserted)
	assert.True(t, stat.ShouldBeSealed())
}

func TestSealSizeRoundTripPersistsInRecoveryMeta(t *testing.T) {
	setSizeMetricForTest(t, typeutil.SizeMetricMainIndex)
	stat := &SegmentStats{
		Modified: ModifiedMetrics{
			Rows:       10,
			BinarySize: 200,
			SealSize:   60,
		},
		MaxBinarySize:         100,
		CreateTime:            time.Now(),
		LastModifiedTime:      time.Now(),
		BinLogCounter:         3,
		CreateSegmentTimeTick: 7,
		Level:                 datapb.SegmentLevel_L1,
	}

	pb := NewProtoFromSegmentStat(stat)
	assert.Equal(t, uint64(60), pb.GetModifiedSealSize())

	recovered := NewSegmentStatFromProto(pb)
	assert.Equal(t, uint64(60), recovered.Modified.SealSize)
	assert.Equal(t, uint64(200), recovered.Modified.BinarySize)
}

func TestSealSizeConsumedOnlyUnderMainIndex(t *testing.T) {
	paramtable.Init()
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SizeMetric.Key) })

	// A persisted main-index SealSize must never be consumed as whole-row bytes
	// when the metric is wholeRow (cross-restart metric-switch, C4).
	stat := &SegmentStats{
		Modified:      ModifiedMetrics{Rows: 10, BinarySize: 500, SealSize: 20},
		MaxBinarySize: 100,
	}

	// wholeRow (default): the seal budget compares whole-row BinarySize.
	assert.Equal(t, uint64(0), stat.SealBudgetCanBeAssign()) // 500 >= 100, saturated
	assert.False(t, stat.canAssign(ModifiedMetrics{Rows: 1, BinarySize: 10, SealSize: 5}))

	// mainIndex: the seal budget compares main-column SealSize.
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SizeMetric.Key, typeutil.SizeMetricMainIndex)
	assert.Equal(t, uint64(80), stat.SealBudgetCanBeAssign()) // 100 - 20
}

func TestRecoveredWithoutSealSizeStartsFromFullBudget(t *testing.T) {
	setSizeMetricForTest(t, typeutil.SizeMetricMainIndex)
	// Pre-upgrade segments have no persisted SealSize. They start from a full
	// main-column budget (falling back to whole-row bytes when accounting), so
	// they are not spuriously sealed on recovery; the persisted row cap and the
	// whole-row ceiling still bound them.
	stat := NewSegmentStatFromProto(&streamingpb.SegmentAssignmentStat{
		MaxRows:            math.MaxUint64,
		MaxBinarySize:      100,
		ModifiedRows:       10,
		ModifiedBinarySize: 50,
	})
	assert.False(t, stat.ShouldBeSealed())
	assert.Equal(t, uint64(50), stat.SealBudgetCanBeAssign())
}
