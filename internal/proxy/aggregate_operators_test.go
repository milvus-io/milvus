package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func TestNewAggregate(t *testing.T) {
	tests := []struct {
		name          string
		aggregateName string
		fieldID       int64
		originalName  string
		expectError   bool
		expectType    string
	}{
		{"count aggregate", "count", 101, "count(f1)", false, "count"},
		{"sum aggregate", "sum", 102, "sum(f2)", false, "sum"},
		{"avg aggregate", "avg", 103, "avg(f3)", false, "avg"},
		{"min aggregate", "min", 104, "min(f4)", false, "min"},
		{"max aggregate", "max", 105, "max(f5)", false, "max"},
		{"invalid aggregate", "invalid", 106, "invalid(f6)", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg, err := agg.NewAggregate(tt.aggregateName, tt.fieldID, tt.originalName)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, agg)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, agg)
				assert.Equal(t, tt.expectType, agg.Name())
				assert.Equal(t, tt.fieldID, agg.FieldID())
				assert.Equal(t, tt.originalName, agg.OriginalName())
			}
		})
	}
}

func TestFromPB(t *testing.T) {
	tests := []struct {
		name        string
		pb          *planpb.Aggregate
		expectError bool
		expectType  string
	}{
		{"count from pb", &planpb.Aggregate{Op: planpb.AggregateOp_count, FieldId: 101}, false, "count"},
		{"sum from pb", &planpb.Aggregate{Op: planpb.AggregateOp_sum, FieldId: 102}, false, "sum"},
		{"avg from pb", &planpb.Aggregate{Op: planpb.AggregateOp_avg, FieldId: 103}, false, "avg"},
		{"min from pb", &planpb.Aggregate{Op: planpb.AggregateOp_min, FieldId: 104}, false, "min"},
		{"max from pb", &planpb.Aggregate{Op: planpb.AggregateOp_max, FieldId: 105}, false, "max"},
		{"invalid from pb", &planpb.Aggregate{Op: planpb.AggregateOp(999), FieldId: 106}, true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg, err := agg.FromPB(tt.pb)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, agg)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, agg)
				assert.Equal(t, tt.expectType, agg.Name())
				assert.Equal(t, tt.pb.GetFieldId(), agg.FieldID())
			}
		})
	}
}

func TestSumAggregate_Update(t *testing.T) {
	sumAgg, _ := agg.NewAggregate("sum", 101, "sum(f1)")

	// Create a row to test aggregation
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // key
		agg.NewFieldValue(int64(10)), // value to sum
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(20)), // value to sum
	})

	// Update using Row's UpdateFieldValue
	row1.UpdateFieldValue(row2, 1, sumAgg)
	assert.Equal(t, int64(30), row1.ValAt(1))
}

func TestCountAggregate_Update(t *testing.T) {
	countAgg, _ := agg.NewAggregate("count", 102, "count(f2)")

	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)), // key
		agg.NewFieldValue(int64(1)), // count value
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)), // same key
		agg.NewFieldValue(int64(1)), // count value
	})

	row1.UpdateFieldValue(row2, 1, countAgg)
	assert.Equal(t, int64(2), row1.ValAt(1))
}

func TestMinAggregate_Update(t *testing.T) {
	minAgg, _ := agg.NewAggregate("min", 103, "min(f3)")

	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // key
		agg.NewFieldValue(int64(10)), // value
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)), // same key
		agg.NewFieldValue(int64(5)), // smaller value
	})
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(20)), // larger value
	})

	row1.UpdateFieldValue(row2, 1, minAgg)
	assert.Equal(t, int64(5), row1.ValAt(1))

	row1.UpdateFieldValue(row3, 1, minAgg)
	assert.Equal(t, int64(5), row1.ValAt(1)) // should still be 5
}

func TestMaxAggregate_Update(t *testing.T) {
	maxAgg, _ := agg.NewAggregate("max", 104, "max(f4)")

	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // key
		agg.NewFieldValue(int64(10)), // value
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(20)), // larger value
	})
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)), // same key
		agg.NewFieldValue(int64(5)), // smaller value
	})

	row1.UpdateFieldValue(row2, 1, maxAgg)
	assert.Equal(t, int64(20), row1.ValAt(1))

	row1.UpdateFieldValue(row3, 1, maxAgg)
	assert.Equal(t, int64(20), row1.ValAt(1)) // should still be 20
}

func TestAvgAggregate_Update_Int64(t *testing.T) {
	avgAgg, _ := agg.NewAggregate("avg", 105, "avg(f5)")

	// Test avg aggregation through Row operations
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // key
		agg.NewFieldValue(int64(10)), // value
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(20)), // value
	})
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(30)), // value
	})

	// Update with first value
	row1.UpdateFieldValue(row2, 1, avgAgg)
	// Update with second value
	row1.UpdateFieldValue(row3, 1, avgAgg)

	// Verify the state is stored (we can't directly access avgState, but we can verify it's not nil)
	val := row1.ValAt(1)
	assert.NotNil(t, val)
	// The value should be an avgState struct (unexported, so we just verify it exists)
}

func TestAvgAggregate_Update_Float64(t *testing.T) {
	avgAgg, _ := agg.NewAggregate("avg", 106, "avg(f6)")

	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),      // key
		agg.NewFieldValue(float64(10.5)), // value
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),      // same key
		agg.NewFieldValue(float64(20.5)), // value
	})

	row1.UpdateFieldValue(row2, 1, avgAgg)
	val := row1.ValAt(1)
	assert.NotNil(t, val)
}

func TestAvgAggregate_Update_MergeStates(t *testing.T) {
	avgAgg, _ := agg.NewAggregate("avg", 107, "avg(f7)")

	// Create first aggregated row
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // key
		agg.NewFieldValue(int64(10)), // value
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(20)), // value
	})
	row1.UpdateFieldValue(row2, 1, avgAgg)

	// Create second aggregated row (simulating merge from another segment)
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(30)), // value
	})
	row4 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(40)), // value
	})
	row3.UpdateFieldValue(row4, 1, avgAgg)

	// Merge the two aggregated states
	row1.UpdateFieldValue(row3, 1, avgAgg)
	val := row1.ValAt(1)
	assert.NotNil(t, val)
}

func TestAccumulateFieldValue(t *testing.T) {
	tests := []struct {
		name      string
		targetVal interface{}
		newVal    interface{}
		expectErr bool
	}{
		{"int64", int64(10), int64(20), false},
		{"int32", int32(10), int32(20), false},
		{"float64", float64(10.5), float64(20.5), false},
		{"float32", float32(10.5), float32(20.5), false},
		{"nil target", nil, int64(10), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := agg.NewFieldValue(tt.targetVal)
			newVal := agg.NewFieldValue(tt.newVal)
			err := agg.AccumulateFieldValue(target, newVal)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// Verify value was updated (we can't directly access .val, but we can use it in a Row)
				row := agg.NewRow([]*agg.FieldValue{target})
				assert.NotNil(t, row.ValAt(0))
			}
		})
	}
}

func TestAggregatesToPB(t *testing.T) {
	countAgg, _ := agg.NewAggregate("count", 101, "count(f1)")
	sumAgg, _ := agg.NewAggregate("sum", 102, "sum(f2)")
	avgAgg, _ := agg.NewAggregate("avg", 103, "avg(f3)")
	minAgg, _ := agg.NewAggregate("min", 104, "min(f4)")
	maxAgg, _ := agg.NewAggregate("max", 105, "max(f5)")

	aggs := []agg.AggregateBase{
		countAgg,
		sumAgg,
		avgAgg,
		minAgg,
		maxAgg,
	}

	pbAggs := agg.AggregatesToPB(aggs)
	assert.Equal(t, len(aggs), len(pbAggs))
	assert.Equal(t, planpb.AggregateOp_count, pbAggs[0].Op)
	assert.Equal(t, planpb.AggregateOp_sum, pbAggs[1].Op)
	assert.Equal(t, planpb.AggregateOp_avg, pbAggs[2].Op)
	assert.Equal(t, planpb.AggregateOp_min, pbAggs[3].Op)
	assert.Equal(t, planpb.AggregateOp_max, pbAggs[4].Op)
}

func TestRow(t *testing.T) {
	fieldValues := []*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(100)),
		agg.NewFieldValue(float32(3.14)),
	}

	row := agg.NewRow(fieldValues)
	assert.Equal(t, 3, row.ColumnCount())
	assert.Equal(t, int32(1), row.ValAt(0))
	assert.Equal(t, int64(100), row.ValAt(1))
	assert.Equal(t, float32(3.14), row.ValAt(2))

	// Test Equal
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(100)),
		agg.NewFieldValue(float32(3.14)),
	})
	assert.True(t, row.Equal(row2, 2)) // Compare first 2 columns

	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(2)),
		agg.NewFieldValue(int64(100)),
		agg.NewFieldValue(float32(3.14)),
	})
	assert.False(t, row.Equal(row3, 2))
}

func TestBucket(t *testing.T) {
	bucket := agg.NewBucket()
	assert.Equal(t, 0, bucket.RowCount())

	// Add first row
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(100)),
	})
	bucket.AddRow(row1)
	assert.Equal(t, 1, bucket.RowCount())

	// Add second row
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(2)),
		agg.NewFieldValue(int64(200)),
	})
	bucket.AddRow(row2)
	assert.Equal(t, 2, bucket.RowCount())

	// Test Find
	idx := bucket.Find(row1, 1)
	assert.Equal(t, 0, idx)

	idx = bucket.Find(row2, 1)
	assert.Equal(t, 1, idx)

	// Test RowAt
	retrievedRow := bucket.RowAt(0)
	assert.True(t, row1.Equal(retrievedRow, 2))
}

func TestBucket_Accumulate(t *testing.T) {
	bucket := agg.NewBucket()

	// Add initial row
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // key
		agg.NewFieldValue(int64(10)), // agg value
	})
	bucket.AddRow(row1)

	// Create aggregate
	sumAgg, _ := agg.NewAggregate("sum", 101, "sum(f1)")
	aggs := []agg.AggregateBase{sumAgg}

	// Create new row with same key but different agg value
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(20)), // new agg value
	})

	// Accumulate
	err := bucket.Accumulate(row2, 0, 1, aggs)
	assert.NoError(t, err)

	// Verify accumulation
	resultRow := bucket.RowAt(0)
	assert.Equal(t, int64(30), resultRow.ValAt(1))
}

func TestAvgAggregate_ToPB(t *testing.T) {
	avgAgg, _ := agg.NewAggregate("avg", 103, "avg(f3)")
	pb := avgAgg.ToPB()
	assert.NotNil(t, pb)
	assert.Equal(t, planpb.AggregateOp_avg, pb.Op)
	assert.Equal(t, int64(103), pb.FieldId)
}

func TestAvgAggregate_FieldID_OriginalName(t *testing.T) {
	avgAgg, _ := agg.NewAggregate("avg", 103, "avg(f3)")
	assert.Equal(t, int64(103), avgAgg.FieldID())
	assert.Equal(t, "avg(f3)", avgAgg.OriginalName())
	assert.Equal(t, "avg", avgAgg.Name())
}
