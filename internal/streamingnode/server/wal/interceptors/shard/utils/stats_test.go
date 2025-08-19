package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func TestSegmentStats(t *testing.T) {
	now := time.Now()
	stat := &SegmentStats{
		Modified: ModifiedMetrics{
			Rows:       100,
			BinarySize: 200,
		},
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
	assert.False(t, inserted)
	assert.Equal(t, stat.Modified.Rows, uint64(160))
	assert.Equal(t, stat.Modified.BinarySize, uint64(320))
	assert.False(t, stat.IsEmpty())
	assert.True(t, stat.ShouldBeSealed())

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

func TestOversizeAlloc(t *testing.T) {
	now := time.Now()
	stat := &SegmentStats{
		Modified:         ModifiedMetrics{},
		MaxBinarySize:    400,
		CreateTime:       now,
		LastModifiedTime: now,
	}
	// Try to alloc a oversized insert metrics.
	inserted := stat.AllocRows(ModifiedMetrics{
		BinarySize: 401,
	})
	assert.False(t, inserted)
	assert.True(t, stat.IsEmpty())
	assert.False(t, stat.ShouldBeSealed())
}
