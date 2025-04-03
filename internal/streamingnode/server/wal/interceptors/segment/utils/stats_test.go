package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStatsConvention(t *testing.T) {
	assert.Nil(t, NewProtoFromSegmentStat(nil))
	stat := &SegmentStats{
		Insert: InsertMetrics{
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
	assert.Equal(t, stat.Insert.Rows, pb.InsertedRows)
	assert.Equal(t, stat.Insert.BinarySize, pb.InsertedBinarySize)
	assert.Equal(t, stat.CreateTime.UnixNano(), pb.CreateTimestampNanoseconds)
	assert.Equal(t, stat.LastModifiedTime.UnixNano(), pb.LastModifiedTimestampNanoseconds)
	assert.Equal(t, stat.BinLogCounter, pb.BinlogCounter)

	stat2 := NewSegmentStatFromProto(pb)
	assert.Equal(t, stat.MaxBinarySize, stat2.MaxBinarySize)
	assert.Equal(t, stat.Insert.Rows, stat2.Insert.Rows)
	assert.Equal(t, stat.Insert.BinarySize, stat2.Insert.BinarySize)
	assert.Equal(t, stat.CreateTime.UnixNano(), stat2.CreateTime.UnixNano())
	assert.Equal(t, stat.LastModifiedTime.UnixNano(), stat2.LastModifiedTime.UnixNano())
	assert.Equal(t, stat.BinLogCounter, stat2.BinLogCounter)

	stat3 := stat2.Copy()
	stat3.Insert.Subtract(InsertMetrics{
		Rows:       1,
		BinarySize: 2,
	})
	assert.Equal(t, stat3.Insert.Rows, stat2.Insert.Rows-1)
	assert.Equal(t, stat3.Insert.BinarySize, stat2.Insert.BinarySize-2)
	assert.Equal(t, stat.Insert.Rows, stat2.Insert.Rows)
	assert.Equal(t, stat.Insert.BinarySize, stat2.Insert.BinarySize)
	assert.Panics(t, func() {
		stat3.Insert.Rows = 0
		stat3.Insert.Subtract(InsertMetrics{
			Rows:       1,
			BinarySize: 0,
		})
	})
	assert.Panics(t, func() {
		stat3.Insert.BinarySize = 0
		stat3.Insert.Subtract(InsertMetrics{
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
		Insert: InsertMetrics{
			Rows:       100,
			BinarySize: 200,
		},
		MaxBinarySize:     400,
		CreateTime:        now,
		LastModifiedTime:  now,
		BinLogCounter:     3,
		BinLogFileCounter: 4,
	}

	insert1 := InsertMetrics{
		Rows:       60,
		BinarySize: 120,
	}
	inserted := stat.AllocRows(insert1)
	assert.True(t, inserted)
	assert.Equal(t, stat.Insert.Rows, uint64(160))
	assert.Equal(t, stat.Insert.BinarySize, uint64(320))
	assert.True(t, time.Now().After(now))
	assert.False(t, stat.IsEmpty())
	assert.False(t, stat.ShouldBeSealed())

	insert1 = InsertMetrics{
		Rows:       100,
		BinarySize: 100,
	}
	inserted = stat.AllocRows(insert1)
	assert.False(t, inserted)
	assert.Equal(t, stat.Insert.Rows, uint64(160))
	assert.Equal(t, stat.Insert.BinarySize, uint64(320))
	assert.False(t, stat.IsEmpty())
	assert.True(t, stat.ShouldBeSealed())

	stat.UpdateOnSync(SyncOperationMetrics{
		BinLogCounterIncr:     4,
		BinLogFileCounterIncr: 9,
	})
	assert.Equal(t, uint64(7), stat.BinLogCounter)
	assert.Equal(t, uint64(13), stat.BinLogFileCounter)
}

func TestOversizeAlloc(t *testing.T) {
	now := time.Now()
	stat := &SegmentStats{
		Insert:           InsertMetrics{},
		MaxBinarySize:    400,
		CreateTime:       now,
		LastModifiedTime: now,
	}
	// Try to alloc a oversized insert metrics.
	inserted := stat.AllocRows(InsertMetrics{
		BinarySize: 401,
	})
	assert.False(t, inserted)
	assert.True(t, stat.IsEmpty())
	assert.False(t, stat.ShouldBeSealed())
}
