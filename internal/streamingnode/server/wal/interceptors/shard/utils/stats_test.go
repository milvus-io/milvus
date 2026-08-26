package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func TestAllocRowsSealBudgetUsesSealSize(t *testing.T) {
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
	// even though the whole-row bytes remain below the budget.
	inserted = stat.AllocRows(ModifiedMetrics{
		Rows:       10,
		BinarySize: 10,
		SealSize:   50,
	})
	assert.False(t, inserted)
	assert.True(t, stat.ShouldBeSealed())
	assert.Equal(t, uint64(60), stat.Modified.SealSize)
}

func TestAllocRowsCeilingSealsOnWholeRowBytes(t *testing.T) {
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
	inserted = stat.AllocRows(ModifiedMetrics{Rows: 50, BinarySize: 10, SealSize: 10})
	assert.False(t, inserted)
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

func TestBackfillSealSizeFromSchema(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 10, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
	}}

	t.Run("backfills rows × perRecord", func(t *testing.T) {
		stat := &SegmentStats{Modified: ModifiedMetrics{Rows: 100, BinarySize: 4096}}
		BackfillSealSizeFromSchema(stat, schema)
		assert.Equal(t, uint64(100*8*4), stat.Modified.SealSize)
	})

	t.Run("no-op when SealSize already present", func(t *testing.T) {
		stat := &SegmentStats{Modified: ModifiedMetrics{Rows: 100, BinarySize: 4096, SealSize: 42}}
		BackfillSealSizeFromSchema(stat, schema)
		assert.Equal(t, uint64(42), stat.Modified.SealSize)
	})

	t.Run("no-op for nil schema or no rows", func(t *testing.T) {
		stat := &SegmentStats{Modified: ModifiedMetrics{Rows: 100, BinarySize: 4096}}
		BackfillSealSizeFromSchema(stat, nil)
		assert.Zero(t, stat.Modified.SealSize)
		stat2 := &SegmentStats{}
		BackfillSealSizeFromSchema(stat2, schema)
		assert.Zero(t, stat2.Modified.SealSize)
	})

	t.Run("no-op for sparse-only schema", func(t *testing.T) {
		sparseSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 10, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		}}
		stat := &SegmentStats{Modified: ModifiedMetrics{Rows: 100, BinarySize: 4096}}
		BackfillSealSizeFromSchema(stat, sparseSchema)
		assert.Zero(t, stat.Modified.SealSize)
	})
}
