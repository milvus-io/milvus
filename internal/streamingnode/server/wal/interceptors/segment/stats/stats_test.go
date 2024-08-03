package stats

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
}

func TestSegmentStats(t *testing.T) {
	now := time.Now()
	stat := &SegmentStats{
		Insert: InsertMetrics{
			Rows:       100,
			BinarySize: 200,
		},
		MaxBinarySize:    400,
		CreateTime:       now,
		LastModifiedTime: now,
		BinLogCounter:    3,
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

	insert1 = InsertMetrics{
		Rows:       100,
		BinarySize: 100,
	}
	inserted = stat.AllocRows(insert1)
	assert.False(t, inserted)
	assert.Equal(t, stat.Insert.Rows, uint64(160))
	assert.Equal(t, stat.Insert.BinarySize, uint64(320))

	stat.UpdateOnFlush(FlushOperationMetrics{
		BinLogCounter: 4,
	})
	assert.Equal(t, uint64(4), stat.BinLogCounter)
}
