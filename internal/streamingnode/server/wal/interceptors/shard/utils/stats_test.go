package utils

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

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
