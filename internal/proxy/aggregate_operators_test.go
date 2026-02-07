package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

type AggregateOperatorsSuite struct {
	suite.Suite
}

func (s *AggregateOperatorsSuite) TestNewAggregate() {
	t := s.T()
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
			aggs, err := agg.NewAggregate(tt.aggregateName, tt.fieldID, tt.originalName, schemapb.DataType_Int64)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, aggs)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, aggs)
				if tt.aggregateName == "avg" {
					// avg returns sum and count aggregates
					assert.Equal(t, 2, len(aggs))
					assert.Equal(t, "sum", aggs[0].Name())
					assert.Equal(t, "count", aggs[1].Name())
					assert.Equal(t, tt.fieldID, aggs[0].FieldID())
					assert.Equal(t, tt.fieldID, aggs[1].FieldID())
					assert.Equal(t, tt.originalName, aggs[0].OriginalName())
					assert.Equal(t, tt.originalName, aggs[1].OriginalName())
				} else {
					// other aggregates return single element slice
					assert.Equal(t, 1, len(aggs))
					assert.Equal(t, tt.expectType, aggs[0].Name())
					assert.Equal(t, tt.fieldID, aggs[0].FieldID())
					assert.Equal(t, tt.originalName, aggs[0].OriginalName())
				}
			}
		})
	}
}

func (s *AggregateOperatorsSuite) TestFromPB() {
	t := s.T()
	tests := []struct {
		name        string
		pb          *planpb.Aggregate
		expectError bool
		expectType  string
	}{
		{"count from pb", &planpb.Aggregate{Op: planpb.AggregateOp_count, FieldId: 101}, false, "count"},
		{"sum from pb", &planpb.Aggregate{Op: planpb.AggregateOp_sum, FieldId: 102}, false, "sum"},
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

func (s *AggregateOperatorsSuite) TestSumAggregate_Update() {
	t := s.T()
	sumAggs, _ := agg.NewAggregate("sum", 101, "sum(f1)", schemapb.DataType_Int64)
	sumAgg := sumAggs[0]

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

func (s *AggregateOperatorsSuite) TestCountAggregate_Update() {
	t := s.T()
	countAggs, _ := agg.NewAggregate("count", 102, "count(f2)", schemapb.DataType_None)
	countAgg := countAggs[0]

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

func (s *AggregateOperatorsSuite) TestMinAggregate_Update() {
	t := s.T()
	minAggs, _ := agg.NewAggregate("min", 103, "min(f3)", schemapb.DataType_Int64)
	minAgg := minAggs[0]

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

func (s *AggregateOperatorsSuite) TestMaxAggregate_Update() {
	t := s.T()
	maxAggs, _ := agg.NewAggregate("max", 104, "max(f4)", schemapb.DataType_Int64)
	maxAgg := maxAggs[0]

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

func (s *AggregateOperatorsSuite) TestAvgAggregate_Update_Int64() {
	t := s.T()
	avgAggs, _ := agg.NewAggregate("avg", 105, "avg(f5)", schemapb.DataType_Int64)
	// avg returns sum and count aggregates
	assert.Equal(t, 2, len(avgAggs))
	sumAgg := avgAggs[0]
	countAgg := avgAggs[1]

	// Test avg aggregation through Row operations (using sum and count)
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // key
		agg.NewFieldValue(int64(10)), // sum value
		agg.NewFieldValue(int64(1)),  // count value
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(20)), // sum value
		agg.NewFieldValue(int64(1)),  // count value
	})
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(30)), // sum value
		agg.NewFieldValue(int64(1)),  // count value
	})

	// Update with first value (sum and count)
	row1.UpdateFieldValue(row2, 1, sumAgg)
	row1.UpdateFieldValue(row2, 2, countAgg)
	// Update with second value
	row1.UpdateFieldValue(row3, 1, sumAgg)
	row1.UpdateFieldValue(row3, 2, countAgg)

	// Verify sum and count
	assert.Equal(t, int64(60), row1.ValAt(1)) // sum: 10+20+30
	assert.Equal(t, int64(3), row1.ValAt(2))  // count: 1+1+1
}

func (s *AggregateOperatorsSuite) TestAvgAggregate_Update_Float64() {
	t := s.T()
	avgAggs, _ := agg.NewAggregate("avg", 106, "avg(f6)", schemapb.DataType_Double)
	sumAgg := avgAggs[0]
	countAgg := avgAggs[1]

	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),      // key
		agg.NewFieldValue(float64(10.5)), // sum value
		agg.NewFieldValue(int64(1)),      // count value
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),      // same key
		agg.NewFieldValue(float64(20.5)), // sum value
		agg.NewFieldValue(int64(1)),      // count value
	})

	row1.UpdateFieldValue(row2, 1, sumAgg)
	row1.UpdateFieldValue(row2, 2, countAgg)
	assert.Equal(t, float64(31.0), row1.ValAt(1)) // sum: 10.5+20.5
	assert.Equal(t, int64(2), row1.ValAt(2))      // count: 1+1
}

func (s *AggregateOperatorsSuite) TestAvgAggregate_Update_MergeStates() {
	t := s.T()
	avgAggs, _ := agg.NewAggregate("avg", 107, "avg(f7)", schemapb.DataType_Int64)
	sumAgg := avgAggs[0]
	countAgg := avgAggs[1]

	// Create first aggregated row
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // key
		agg.NewFieldValue(int64(10)), // sum value
		agg.NewFieldValue(int64(1)),  // count value
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(20)), // sum value
		agg.NewFieldValue(int64(1)),  // count value
	})
	row1.UpdateFieldValue(row2, 1, sumAgg)
	row1.UpdateFieldValue(row2, 2, countAgg)

	// Create second aggregated row (simulating merge from another segment)
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(30)), // sum value
		agg.NewFieldValue(int64(1)),  // count value
	})
	row4 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // same key
		agg.NewFieldValue(int64(40)), // sum value
		agg.NewFieldValue(int64(1)),  // count value
	})
	row3.UpdateFieldValue(row4, 1, sumAgg)
	row3.UpdateFieldValue(row4, 2, countAgg)

	// Merge the two aggregated states
	row1.UpdateFieldValue(row3, 1, sumAgg)
	row1.UpdateFieldValue(row3, 2, countAgg)
	assert.Equal(t, int64(100), row1.ValAt(1)) // sum: 10+20+30+40
	assert.Equal(t, int64(4), row1.ValAt(2))   // count: 1+1+1+1
}

func (s *AggregateOperatorsSuite) TestAccumulateFieldValue() {
	t := s.T()
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

func (s *AggregateOperatorsSuite) TestAggregatesToPB() {
	t := s.T()
	countAggs, _ := agg.NewAggregate("count", 101, "count(f1)", schemapb.DataType_None)
	sumAggs, _ := agg.NewAggregate("sum", 102, "sum(f2)", schemapb.DataType_Int64)
	avgAggs, _ := agg.NewAggregate("avg", 103, "avg(f3)", schemapb.DataType_Int64)
	minAggs, _ := agg.NewAggregate("min", 104, "min(f4)", schemapb.DataType_Int64)
	maxAggs, _ := agg.NewAggregate("max", 105, "max(f5)", schemapb.DataType_Int64)

	aggs := []agg.AggregateBase{
		countAggs[0],
		sumAggs[0],
		avgAggs[0], // sum aggregate for avg
		avgAggs[1], // count aggregate for avg
		minAggs[0],
		maxAggs[0],
	}

	pbAggs := agg.AggregatesToPB(aggs)
	assert.Equal(t, len(aggs), len(pbAggs))
	assert.Equal(t, planpb.AggregateOp_count, pbAggs[0].Op)
	assert.Equal(t, planpb.AggregateOp_sum, pbAggs[1].Op)
	assert.Equal(t, planpb.AggregateOp_sum, pbAggs[2].Op)   // avg's sum
	assert.Equal(t, planpb.AggregateOp_count, pbAggs[3].Op) // avg's count
	assert.Equal(t, planpb.AggregateOp_min, pbAggs[4].Op)
	assert.Equal(t, planpb.AggregateOp_max, pbAggs[5].Op)
}

func (s *AggregateOperatorsSuite) TestRow() {
	t := s.T()
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

func (s *AggregateOperatorsSuite) TestBucket() {
	t := s.T()
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

func (s *AggregateOperatorsSuite) TestBucket_Accumulate() {
	t := s.T()
	bucket := agg.NewBucket()

	// Add initial row
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),  // key
		agg.NewFieldValue(int64(10)), // agg value
	})
	bucket.AddRow(row1)

	// Create aggregate
	sumAggs, _ := agg.NewAggregate("sum", 101, "sum(f1)", schemapb.DataType_Int64)
	aggs := []agg.AggregateBase{sumAggs[0]}

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

func (s *AggregateOperatorsSuite) TestAvgAggregate_ToPB() {
	t := s.T()
	avgAggs, _ := agg.NewAggregate("avg", 103, "avg(f3)", schemapb.DataType_Int64)
	// avg returns sum and count, check both
	sumPB := avgAggs[0].ToPB()
	countPB := avgAggs[1].ToPB()
	assert.NotNil(t, sumPB)
	assert.NotNil(t, countPB)
	assert.Equal(t, planpb.AggregateOp_sum, sumPB.Op)
	assert.Equal(t, planpb.AggregateOp_count, countPB.Op)
	assert.Equal(t, int64(103), sumPB.FieldId)
	assert.Equal(t, int64(103), countPB.FieldId)
}

func (s *AggregateOperatorsSuite) TestAvgAggregate_FieldID_OriginalName() {
	t := s.T()
	avgAggs, _ := agg.NewAggregate("avg", 103, "avg(f3)", schemapb.DataType_Int64)
	sumAgg := avgAggs[0]
	countAgg := avgAggs[1]
	assert.Equal(t, int64(103), sumAgg.FieldID())
	assert.Equal(t, int64(103), countAgg.FieldID())
	assert.Equal(t, "avg(f3)", sumAgg.OriginalName())
	assert.Equal(t, "avg(f3)", countAgg.OriginalName())
	assert.Equal(t, "sum", sumAgg.Name())
	assert.Equal(t, "count", countAgg.Name())
}

// ==================== Null Handling Tests ====================

// Test FieldValue null operations: NewNullFieldValue, IsNull, SetNull
func (s *AggregateOperatorsSuite) TestFieldValue_NullOperations() {
	t := s.T()

	// Test NewNullFieldValue
	nullFv := agg.NewNullFieldValue()
	assert.True(t, nullFv.IsNull(), "NewNullFieldValue should create a null field value")

	// Test NewFieldValue creates non-null
	nonNullFv := agg.NewFieldValue(int64(100))
	assert.False(t, nonNullFv.IsNull(), "NewFieldValue should create a non-null field value")

	// Test SetNull
	fv := agg.NewFieldValue(int64(50))
	assert.False(t, fv.IsNull())
	fv.SetNull()
	assert.True(t, fv.IsNull(), "SetNull should make the field value null")

	// Test nil value is different from null
	nilValFv := agg.NewFieldValue(nil)
	assert.False(t, nilValFv.IsNull(), "nil value should not be considered null (backward compatibility)")
}

// Test Row.FieldValueAt returns the FieldValue
func (s *AggregateOperatorsSuite) TestRow_FieldValueAt() {
	t := s.T()

	fv1 := agg.NewFieldValue(int32(1))
	fv2 := agg.NewNullFieldValue()
	fv3 := agg.NewFieldValue(int64(100))

	row := agg.NewRow([]*agg.FieldValue{fv1, fv2, fv3})

	assert.False(t, row.FieldValueAt(0).IsNull())
	assert.True(t, row.FieldValueAt(1).IsNull())
	assert.False(t, row.FieldValueAt(2).IsNull())
}

// Test Row.Equal with null values - null == null should be equal for grouping
func (s *AggregateOperatorsSuite) TestRow_Equal_WithNulls() {
	t := s.T()

	// Both rows have null at same position - should be equal
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewNullFieldValue(),
		agg.NewFieldValue(int64(100)),
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewNullFieldValue(),
		agg.NewFieldValue(int64(200)),
	})
	assert.True(t, row1.Equal(row2, 1), "null == null should be considered equal for grouping")

	// One row has null, other has value - should NOT be equal
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(100)),
	})
	row4 := agg.NewRow([]*agg.FieldValue{
		agg.NewNullFieldValue(),
		agg.NewFieldValue(int64(100)),
	})
	assert.False(t, row3.Equal(row4, 1), "null != non-null should be unequal")

	// Multiple null keys - should be equal
	row5 := agg.NewRow([]*agg.FieldValue{
		agg.NewNullFieldValue(),
		agg.NewNullFieldValue(),
		agg.NewFieldValue(int64(100)),
	})
	row6 := agg.NewRow([]*agg.FieldValue{
		agg.NewNullFieldValue(),
		agg.NewNullFieldValue(),
		agg.NewFieldValue(int64(200)),
	})
	assert.True(t, row5.Equal(row6, 2), "multiple null keys should be equal")

	// Mixed: first key null (equal), second key different - should NOT be equal
	row7 := agg.NewRow([]*agg.FieldValue{
		agg.NewNullFieldValue(),
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(100)),
	})
	row8 := agg.NewRow([]*agg.FieldValue{
		agg.NewNullFieldValue(),
		agg.NewFieldValue(int32(2)),
		agg.NewFieldValue(int64(100)),
	})
	assert.False(t, row7.Equal(row8, 2), "same null first key but different second key should be unequal")
}

// Test AccumulateFieldValue with null values
func (s *AggregateOperatorsSuite) TestAccumulateFieldValue_WithNulls() {
	t := s.T()

	// Case 1: new value is null - should skip (target unchanged)
	target1 := agg.NewFieldValue(int64(10))
	nullNew := agg.NewNullFieldValue()
	err := agg.AccumulateFieldValue(target1, nullNew)
	assert.NoError(t, err)
	row1 := agg.NewRow([]*agg.FieldValue{target1})
	assert.Equal(t, int64(10), row1.ValAt(0), "accumulating null should not change target")

	// Case 2: target is null, new is not null - should initialize target
	target2 := agg.NewNullFieldValue()
	nonNullNew := agg.NewFieldValue(int64(20))
	err = agg.AccumulateFieldValue(target2, nonNullNew)
	assert.NoError(t, err)
	assert.False(t, target2.IsNull(), "target should no longer be null after accumulating non-null value")
	row2 := agg.NewRow([]*agg.FieldValue{target2})
	assert.Equal(t, int64(20), row2.ValAt(0))

	// Case 3: both are null - should remain null (skip)
	target3 := agg.NewNullFieldValue()
	nullNew3 := agg.NewNullFieldValue()
	err = agg.AccumulateFieldValue(target3, nullNew3)
	assert.NoError(t, err)
	assert.True(t, target3.IsNull(), "accumulating null to null should remain null")

	// Case 4: target not null, new not null - normal accumulation
	target4 := agg.NewFieldValue(int64(10))
	nonNullNew4 := agg.NewFieldValue(int64(30))
	err = agg.AccumulateFieldValue(target4, nonNullNew4)
	assert.NoError(t, err)
	row4 := agg.NewRow([]*agg.FieldValue{target4})
	assert.Equal(t, int64(40), row4.ValAt(0))
}

// Test MinAggregate.Update with null values
func (s *AggregateOperatorsSuite) TestMinAggregate_Update_WithNulls() {
	t := s.T()
	minAggs, _ := agg.NewAggregate("min", 103, "min(f3)", schemapb.DataType_Int64)
	minAgg := minAggs[0]

	// Case 1: new value is null - should skip
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(10)),
	})
	rowNull := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	row1.UpdateFieldValue(rowNull, 1, minAgg)
	assert.Equal(t, int64(10), row1.ValAt(1), "min should skip null values")

	// Case 2: target is null, new is not null - should initialize
	rowNullTarget := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(20)),
	})
	rowNullTarget.UpdateFieldValue(row2, 1, minAgg)
	assert.Equal(t, int64(20), rowNullTarget.ValAt(1), "min should initialize from non-null when target is null")
	assert.False(t, rowNullTarget.FieldValueAt(1).IsNull())

	// Case 3: after initialization, normal min comparison
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(5)),
	})
	rowNullTarget.UpdateFieldValue(row3, 1, minAgg)
	assert.Equal(t, int64(5), rowNullTarget.ValAt(1), "min should update to smaller value")
}

// Test MaxAggregate.Update with null values
func (s *AggregateOperatorsSuite) TestMaxAggregate_Update_WithNulls() {
	t := s.T()
	maxAggs, _ := agg.NewAggregate("max", 104, "max(f4)", schemapb.DataType_Int64)
	maxAgg := maxAggs[0]

	// Case 1: new value is null - should skip
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(10)),
	})
	rowNull := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	row1.UpdateFieldValue(rowNull, 1, maxAgg)
	assert.Equal(t, int64(10), row1.ValAt(1), "max should skip null values")

	// Case 2: target is null, new is not null - should initialize
	rowNullTarget := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(20)),
	})
	rowNullTarget.UpdateFieldValue(row2, 1, maxAgg)
	assert.Equal(t, int64(20), rowNullTarget.ValAt(1), "max should initialize from non-null when target is null")
	assert.False(t, rowNullTarget.FieldValueAt(1).IsNull())

	// Case 3: after initialization, normal max comparison
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(50)),
	})
	rowNullTarget.UpdateFieldValue(row3, 1, maxAgg)
	assert.Equal(t, int64(50), rowNullTarget.ValAt(1), "max should update to larger value")
}

// Test SumAggregate.Update with null values (uses AccumulateFieldValue)
func (s *AggregateOperatorsSuite) TestSumAggregate_Update_WithNulls() {
	t := s.T()
	sumAggs, _ := agg.NewAggregate("sum", 101, "sum(f1)", schemapb.DataType_Int64)
	sumAgg := sumAggs[0]

	// Case 1: new value is null - should skip
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(10)),
	})
	rowNull := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	row1.UpdateFieldValue(rowNull, 1, sumAgg)
	assert.Equal(t, int64(10), row1.ValAt(1), "sum should skip null values")

	// Case 2: target is null, new is not null - should initialize
	rowNullTarget := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(20)),
	})
	rowNullTarget.UpdateFieldValue(row2, 1, sumAgg)
	assert.Equal(t, int64(20), rowNullTarget.ValAt(1))
	assert.False(t, rowNullTarget.FieldValueAt(1).IsNull())

	// Case 3: after initialization, normal sum
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(30)),
	})
	rowNullTarget.UpdateFieldValue(row3, 1, sumAgg)
	assert.Equal(t, int64(50), rowNullTarget.ValAt(1))
}

// Test Bucket operations with null grouping keys
func (s *AggregateOperatorsSuite) TestBucket_WithNullKeys() {
	t := s.T()
	bucket := agg.NewBucket()

	// Add row with null key
	rowNull := agg.NewRow([]*agg.FieldValue{
		agg.NewNullFieldValue(),
		agg.NewFieldValue(int64(100)),
	})
	bucket.AddRow(rowNull)

	// Add row with non-null key
	rowNonNull := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(200)),
	})
	bucket.AddRow(rowNonNull)

	// Find row with null key
	searchNull := agg.NewRow([]*agg.FieldValue{
		agg.NewNullFieldValue(),
		agg.NewFieldValue(int64(999)),
	})
	idx := bucket.Find(searchNull, 1)
	assert.Equal(t, 0, idx, "should find row with null key")

	// Find row with non-null key
	searchNonNull := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(999)),
	})
	idx = bucket.Find(searchNonNull, 1)
	assert.Equal(t, 1, idx, "should find row with non-null key")

	// Search for non-existent null row should not match non-null rows
	bucket2 := agg.NewBucket()
	bucket2.AddRow(rowNonNull)
	idx = bucket2.Find(searchNull, 1)
	assert.Equal(t, -1, idx, "null key should not match non-null key")
}

// Test Bucket.Accumulate with null values
func (s *AggregateOperatorsSuite) TestBucket_Accumulate_WithNulls() {
	t := s.T()
	bucket := agg.NewBucket()

	// Add initial row with null aggregate value
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	bucket.AddRow(row1)

	sumAggs, _ := agg.NewAggregate("sum", 101, "sum(f1)", schemapb.DataType_Int64)
	aggs := []agg.AggregateBase{sumAggs[0]}

	// Accumulate non-null value to null target
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(20)),
	})
	err := bucket.Accumulate(row2, 0, 1, aggs)
	assert.NoError(t, err)

	resultRow := bucket.RowAt(0)
	assert.Equal(t, int64(20), resultRow.ValAt(1))
	assert.False(t, resultRow.FieldValueAt(1).IsNull())

	// Accumulate null value - should not change
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	err = bucket.Accumulate(row3, 0, 1, aggs)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), resultRow.ValAt(1), "accumulating null should not change sum")
}

// Test FieldAccessor.IsNullAt for various types
func (s *AggregateOperatorsSuite) TestFieldAccessor_IsNullAt() {
	t := s.T()

	// Test Int64FieldAccessor with validity data
	int64Accessor, err := agg.NewFieldAccessor(schemapb.DataType_Int64)
	assert.NoError(t, err)
	fieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{10, 20, 30, 40},
					},
				},
			},
		},
		ValidData: []bool{true, false, true, false}, // 2nd and 4th are null
	}
	int64Accessor.SetVals(fieldData)
	assert.False(t, int64Accessor.IsNullAt(0), "index 0 should be valid")
	assert.True(t, int64Accessor.IsNullAt(1), "index 1 should be null")
	assert.False(t, int64Accessor.IsNullAt(2), "index 2 should be valid")
	assert.True(t, int64Accessor.IsNullAt(3), "index 3 should be null")

	// Test Int32FieldAccessor without validity data (all valid)
	int32Accessor, err := agg.NewFieldAccessor(schemapb.DataType_Int32)
	assert.NoError(t, err)
	fieldDataNoValid := &schemapb.FieldData{
		Type: schemapb.DataType_Int32,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{1, 2, 3},
					},
				},
			},
		},
		// No ValidData means all values are valid
	}
	int32Accessor.SetVals(fieldDataNoValid)
	assert.False(t, int32Accessor.IsNullAt(0), "without ValidData, all should be valid")
	assert.False(t, int32Accessor.IsNullAt(1), "without ValidData, all should be valid")
	assert.False(t, int32Accessor.IsNullAt(2), "without ValidData, all should be valid")

	// Test StringFieldAccessor with validity data
	stringAccessor, err := agg.NewFieldAccessor(schemapb.DataType_VarChar)
	assert.NoError(t, err)
	stringFieldData := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"a", "b", "c"},
					},
				},
			},
		},
		ValidData: []bool{false, true, false}, // 1st and 3rd are null
	}
	stringAccessor.SetVals(stringFieldData)
	assert.True(t, stringAccessor.IsNullAt(0), "index 0 should be null")
	assert.False(t, stringAccessor.IsNullAt(1), "index 1 should be valid")
	assert.True(t, stringAccessor.IsNullAt(2), "index 2 should be null")

	// Test Float64FieldAccessor with validity data
	float64Accessor, err := agg.NewFieldAccessor(schemapb.DataType_Double)
	assert.NoError(t, err)
	floatFieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Double,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: []float64{1.1, 2.2},
					},
				},
			},
		},
		ValidData: []bool{true, false},
	}
	float64Accessor.SetVals(floatFieldData)
	assert.False(t, float64Accessor.IsNullAt(0))
	assert.True(t, float64Accessor.IsNullAt(1))

	// Test BoolFieldAccessor with validity data
	boolAccessor, err := agg.NewFieldAccessor(schemapb.DataType_Bool)
	assert.NoError(t, err)
	boolFieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Bool,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: []bool{true, false, true},
					},
				},
			},
		},
		ValidData: []bool{true, true, false},
	}
	boolAccessor.SetVals(boolFieldData)
	assert.False(t, boolAccessor.IsNullAt(0))
	assert.False(t, boolAccessor.IsNullAt(1))
	assert.True(t, boolAccessor.IsNullAt(2))
}

// Test FieldAccessor.Hash returns nullHashValue for null entries
func (s *AggregateOperatorsSuite) TestFieldAccessor_Hash_NullValue() {
	t := s.T()
	const nullHashValue uint64 = 0x9E3779B97F4A7C15

	// Test Int64FieldAccessor Hash with null
	int64Accessor, _ := agg.NewFieldAccessor(schemapb.DataType_Int64)
	fieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{100, 200},
					},
				},
			},
		},
		ValidData: []bool{true, false}, // 2nd is null
	}
	int64Accessor.SetVals(fieldData)

	hash0 := int64Accessor.Hash(0)
	hash1 := int64Accessor.Hash(1)

	assert.NotEqual(t, nullHashValue, hash0, "valid value should not have nullHashValue")
	assert.Equal(t, nullHashValue, hash1, "null value should return nullHashValue")

	// Test StringFieldAccessor Hash with null
	stringAccessor, _ := agg.NewFieldAccessor(schemapb.DataType_VarChar)
	stringFieldData := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"hello", "world"},
					},
				},
			},
		},
		ValidData: []bool{false, true}, // 1st is null
	}
	stringAccessor.SetVals(stringFieldData)

	stringHash0 := stringAccessor.Hash(0)
	stringHash1 := stringAccessor.Hash(1)

	assert.Equal(t, nullHashValue, stringHash0, "null string should return nullHashValue")
	assert.NotEqual(t, nullHashValue, stringHash1, "valid string should not have nullHashValue")
}

// Test AssembleSingleValue with null FieldValue
func (s *AggregateOperatorsSuite) TestAssembleSingleValue_WithNull() {
	t := s.T()

	// Test null Int64 value
	int64FieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{},
					},
				},
			},
		},
	}
	nullFv := agg.NewNullFieldValue()
	err := agg.AssembleSingleValue(nullFv, int64FieldData)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(int64FieldData.ValidData))
	assert.False(t, int64FieldData.ValidData[0], "null should have ValidData=false")
	assert.Equal(t, 1, len(int64FieldData.GetScalars().GetLongData().GetData()))
	assert.Equal(t, int64(0), int64FieldData.GetScalars().GetLongData().GetData()[0], "null should have default value 0")

	// Test non-null Int64 value
	nonNullFv := agg.NewFieldValue(int64(42))
	err = agg.AssembleSingleValue(nonNullFv, int64FieldData)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(int64FieldData.ValidData))
	assert.True(t, int64FieldData.ValidData[1], "non-null should have ValidData=true")
	assert.Equal(t, int64(42), int64FieldData.GetScalars().GetLongData().GetData()[1])

	// Test null String value
	stringFieldData := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{},
					},
				},
			},
		},
	}
	nullStrFv := agg.NewNullFieldValue()
	err = agg.AssembleSingleValue(nullStrFv, stringFieldData)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(stringFieldData.ValidData))
	assert.False(t, stringFieldData.ValidData[0])
	assert.Equal(t, "", stringFieldData.GetScalars().GetStringData().GetData()[0], "null string should be empty")

	// Test null Double value
	doubleFieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Double,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: []float64{},
					},
				},
			},
		},
	}
	nullDoubleFv := agg.NewNullFieldValue()
	err = agg.AssembleSingleValue(nullDoubleFv, doubleFieldData)
	assert.NoError(t, err)
	assert.False(t, doubleFieldData.ValidData[0])
	assert.Equal(t, float64(0), doubleFieldData.GetScalars().GetDoubleData().GetData()[0])

	// Test null Bool value
	boolFieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Bool,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: []bool{},
					},
				},
			},
		},
	}
	nullBoolFv := agg.NewNullFieldValue()
	err = agg.AssembleSingleValue(nullBoolFv, boolFieldData)
	assert.NoError(t, err)
	assert.False(t, boolFieldData.ValidData[0])
	assert.False(t, boolFieldData.GetScalars().GetBoolData().GetData()[0], "null bool should be false")
}

// Test AssembleSingleRow with mixed null and non-null values
func (s *AggregateOperatorsSuite) TestAssembleSingleRow_WithMixedNulls() {
	t := s.T()

	row := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
		agg.NewFieldValue(int64(100)),
	})

	fieldDatas := []*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{Data: []int32{}},
					},
				},
			},
		},
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{}},
					},
				},
			},
		},
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{}},
					},
				},
			},
		},
	}

	err := agg.AssembleSingleRow(3, row, fieldDatas)
	assert.NoError(t, err)

	// Check first column (non-null int32)
	assert.True(t, fieldDatas[0].ValidData[0])
	assert.Equal(t, int32(1), fieldDatas[0].GetScalars().GetIntData().GetData()[0])

	// Check second column (null int64)
	assert.False(t, fieldDatas[1].ValidData[0])
	assert.Equal(t, int64(0), fieldDatas[1].GetScalars().GetLongData().GetData()[0])

	// Check third column (non-null int64)
	assert.True(t, fieldDatas[2].ValidData[0])
	assert.Equal(t, int64(100), fieldDatas[2].GetScalars().GetLongData().GetData()[0])
}

// Test Float32FieldAccessor.IsNullAt
func (s *AggregateOperatorsSuite) TestFieldAccessor_Float32_IsNullAt() {
	t := s.T()

	float32Accessor, err := agg.NewFieldAccessor(schemapb.DataType_Float)
	assert.NoError(t, err)
	fieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Float,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: []float32{1.1, 2.2, 3.3},
					},
				},
			},
		},
		ValidData: []bool{true, false, true}, // 2nd is null
	}
	float32Accessor.SetVals(fieldData)
	assert.False(t, float32Accessor.IsNullAt(0), "index 0 should be valid")
	assert.True(t, float32Accessor.IsNullAt(1), "index 1 should be null")
	assert.False(t, float32Accessor.IsNullAt(2), "index 2 should be valid")

	// Test without validity data (all valid)
	fieldDataNoValid := &schemapb.FieldData{
		Type: schemapb.DataType_Float,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: []float32{1.0, 2.0},
					},
				},
			},
		},
	}
	float32Accessor.SetVals(fieldDataNoValid)
	assert.False(t, float32Accessor.IsNullAt(0), "without ValidData, all should be valid")
	assert.False(t, float32Accessor.IsNullAt(1), "without ValidData, all should be valid")
}

// Test TimestamptzFieldAccessor.IsNullAt
func (s *AggregateOperatorsSuite) TestFieldAccessor_Timestamptz_IsNullAt() {
	t := s.T()

	tzAccessor, err := agg.NewFieldAccessor(schemapb.DataType_Timestamptz)
	assert.NoError(t, err)
	fieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Timestamptz,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimestamptzData{
					TimestamptzData: &schemapb.TimestamptzArray{
						Data: []int64{1000, 2000, 3000},
					},
				},
			},
		},
		ValidData: []bool{false, true, false}, // 1st and 3rd are null
	}
	tzAccessor.SetVals(fieldData)
	assert.True(t, tzAccessor.IsNullAt(0), "index 0 should be null")
	assert.False(t, tzAccessor.IsNullAt(1), "index 1 should be valid")
	assert.True(t, tzAccessor.IsNullAt(2), "index 2 should be null")

	// Test without validity data (all valid)
	fieldDataNoValid := &schemapb.FieldData{
		Type: schemapb.DataType_Timestamptz,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimestamptzData{
					TimestamptzData: &schemapb.TimestamptzArray{
						Data: []int64{1000, 2000},
					},
				},
			},
		},
	}
	tzAccessor.SetVals(fieldDataNoValid)
	assert.False(t, tzAccessor.IsNullAt(0), "without ValidData, all should be valid")
	assert.False(t, tzAccessor.IsNullAt(1), "without ValidData, all should be valid")
}

// Test Float32FieldAccessor.Hash returns nullHashValue for null entries
func (s *AggregateOperatorsSuite) TestFieldAccessor_Float32_Hash_NullValue() {
	t := s.T()
	const nullHashValue uint64 = 0x9E3779B97F4A7C15

	float32Accessor, _ := agg.NewFieldAccessor(schemapb.DataType_Float)
	fieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Float,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: []float32{1.5, 2.5},
					},
				},
			},
		},
		ValidData: []bool{true, false}, // 2nd is null
	}
	float32Accessor.SetVals(fieldData)

	hash0 := float32Accessor.Hash(0)
	hash1 := float32Accessor.Hash(1)

	assert.NotEqual(t, nullHashValue, hash0, "valid value should not have nullHashValue")
	assert.Equal(t, nullHashValue, hash1, "null value should return nullHashValue")
}

// Test TimestamptzFieldAccessor.Hash returns nullHashValue for null entries
func (s *AggregateOperatorsSuite) TestFieldAccessor_Timestamptz_Hash_NullValue() {
	t := s.T()
	const nullHashValue uint64 = 0x9E3779B97F4A7C15

	tzAccessor, _ := agg.NewFieldAccessor(schemapb.DataType_Timestamptz)
	fieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Timestamptz,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimestamptzData{
					TimestamptzData: &schemapb.TimestamptzArray{
						Data: []int64{1000, 2000},
					},
				},
			},
		},
		ValidData: []bool{false, true}, // 1st is null
	}
	tzAccessor.SetVals(fieldData)

	hash0 := tzAccessor.Hash(0)
	hash1 := tzAccessor.Hash(1)

	assert.Equal(t, nullHashValue, hash0, "null value should return nullHashValue")
	assert.NotEqual(t, nullHashValue, hash1, "valid value should not have nullHashValue")
}

// Test AssembleSingleValue with null Float32 value
func (s *AggregateOperatorsSuite) TestAssembleSingleValue_WithNull_Float32() {
	t := s.T()

	// Test null Float32 value
	floatFieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Float,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: []float32{},
					},
				},
			},
		},
	}
	nullFv := agg.NewNullFieldValue()
	err := agg.AssembleSingleValue(nullFv, floatFieldData)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(floatFieldData.ValidData))
	assert.False(t, floatFieldData.ValidData[0], "null should have ValidData=false")
	assert.Equal(t, float32(0), floatFieldData.GetScalars().GetFloatData().GetData()[0], "null should have default value 0")

	// Test non-null Float32 value
	nonNullFv := agg.NewFieldValue(float32(3.14))
	err = agg.AssembleSingleValue(nonNullFv, floatFieldData)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(floatFieldData.ValidData))
	assert.True(t, floatFieldData.ValidData[1], "non-null should have ValidData=true")
	assert.Equal(t, float32(3.14), floatFieldData.GetScalars().GetFloatData().GetData()[1])
}

// Test AssembleSingleValue with null Timestamptz value
func (s *AggregateOperatorsSuite) TestAssembleSingleValue_WithNull_Timestamptz() {
	t := s.T()

	// Test null Timestamptz value
	tzFieldData := &schemapb.FieldData{
		Type: schemapb.DataType_Timestamptz,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimestamptzData{
					TimestamptzData: &schemapb.TimestamptzArray{
						Data: []int64{},
					},
				},
			},
		},
	}
	nullFv := agg.NewNullFieldValue()
	err := agg.AssembleSingleValue(nullFv, tzFieldData)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tzFieldData.ValidData))
	assert.False(t, tzFieldData.ValidData[0], "null should have ValidData=false")
	assert.Equal(t, int64(0), tzFieldData.GetScalars().GetTimestamptzData().GetData()[0], "null should have default value 0")

	// Test non-null Timestamptz value
	nonNullFv := agg.NewFieldValue(int64(1234567890))
	err = agg.AssembleSingleValue(nonNullFv, tzFieldData)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tzFieldData.ValidData))
	assert.True(t, tzFieldData.ValidData[1], "non-null should have ValidData=true")
	assert.Equal(t, int64(1234567890), tzFieldData.GetScalars().GetTimestamptzData().GetData()[1])
}

// Test CountAggregate with null values - count should count all rows including nulls
func (s *AggregateOperatorsSuite) TestCountAggregate_WithNulls() {
	t := s.T()
	countAggs, _ := agg.NewAggregate("count", 102, "count(f2)", schemapb.DataType_Int64)
	countAgg := countAggs[0]

	// Count starts at 1 and increments regardless of null values
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(1)), // count = 1
	})

	// Update with a null value - count should still increment
	rowNull := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})

	// CountAggregate uses AccumulateFieldValue which skips nulls
	// But for count, the value passed is the count itself (not the field being counted)
	// Let's verify the count behavior
	row1.UpdateFieldValue(rowNull, 1, countAgg)
	// With null handling, if new value is null, it skips accumulation
	// So count remains 1
	assert.Equal(t, int64(1), row1.ValAt(1), "count should remain 1 when accumulating null")

	// Update with non-null count value
	row2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(1)), // count = 1
	})
	row1.UpdateFieldValue(row2, 1, countAgg)
	assert.Equal(t, int64(2), row1.ValAt(1), "count should be 2 after accumulating non-null count")

	// Test when target is null and new is non-null
	rowNullTarget := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	row3 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(5)),
	})
	rowNullTarget.UpdateFieldValue(row3, 1, countAgg)
	assert.Equal(t, int64(5), rowNullTarget.ValAt(1), "count should initialize from non-null when target is null")
	assert.False(t, rowNullTarget.FieldValueAt(1).IsNull())
}

// Test AvgAggregate with null values - avg is sum/count, both should handle nulls
func (s *AggregateOperatorsSuite) TestAvgAggregate_WithNulls() {
	t := s.T()
	// avg returns two aggregates: sum and count
	avgAggs, _ := agg.NewAggregate("avg", 103, "avg(f3)", schemapb.DataType_Int64)
	assert.Equal(t, 2, len(avgAggs), "avg should return sum and count aggregates")
	sumAgg := avgAggs[0]
	countAgg := avgAggs[1]

	// Initial row: sum=100, count=2
	row1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),   // grouping key
		agg.NewFieldValue(int64(100)), // sum
		agg.NewFieldValue(int64(2)),   // count
	})

	// Row with null sum - should skip sum accumulation
	rowNullSum := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),     // null sum
		agg.NewFieldValue(int64(1)), // count = 1
	})

	row1.UpdateFieldValue(rowNullSum, 1, sumAgg)
	row1.UpdateFieldValue(rowNullSum, 2, countAgg)
	assert.Equal(t, int64(100), row1.ValAt(1), "sum should remain 100 when accumulating null")
	assert.Equal(t, int64(3), row1.ValAt(2), "count should be 3 after accumulating non-null count")

	// Row with null count - should skip count accumulation
	rowNullCount := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(50)), // sum = 50
		agg.NewNullFieldValue(),      // null count
	})

	row1.UpdateFieldValue(rowNullCount, 1, sumAgg)
	row1.UpdateFieldValue(rowNullCount, 2, countAgg)
	assert.Equal(t, int64(150), row1.ValAt(1), "sum should be 150 after accumulating non-null sum")
	assert.Equal(t, int64(3), row1.ValAt(2), "count should remain 3 when accumulating null")

	// Row with both non-null
	rowNonNull := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(50)),
		agg.NewFieldValue(int64(2)),
	})
	row1.UpdateFieldValue(rowNonNull, 1, sumAgg)
	row1.UpdateFieldValue(rowNonNull, 2, countAgg)
	assert.Equal(t, int64(200), row1.ValAt(1), "sum should be 200")
	assert.Equal(t, int64(5), row1.ValAt(2), "count should be 5")

	// Test initialization from null target
	rowNullTarget := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
		agg.NewNullFieldValue(),
	})
	rowInit := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewFieldValue(int64(25)),
		agg.NewFieldValue(int64(1)),
	})
	rowNullTarget.UpdateFieldValue(rowInit, 1, sumAgg)
	rowNullTarget.UpdateFieldValue(rowInit, 2, countAgg)
	assert.Equal(t, int64(25), rowNullTarget.ValAt(1), "sum should initialize to 25")
	assert.Equal(t, int64(1), rowNullTarget.ValAt(2), "count should initialize to 1")
	assert.False(t, rowNullTarget.FieldValueAt(1).IsNull())
	assert.False(t, rowNullTarget.FieldValueAt(2).IsNull())
}

// Test all null scenario - when all values are null, result should remain null
func (s *AggregateOperatorsSuite) TestAggregate_AllNulls() {
	t := s.T()

	// Test sum with all nulls
	sumAggs, _ := agg.NewAggregate("sum", 101, "sum(f1)", schemapb.DataType_Int64)
	sumAgg := sumAggs[0]

	rowTarget := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	rowNull1 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	rowNull2 := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})

	rowTarget.UpdateFieldValue(rowNull1, 1, sumAgg)
	rowTarget.UpdateFieldValue(rowNull2, 1, sumAgg)
	assert.True(t, rowTarget.FieldValueAt(1).IsNull(), "sum of all nulls should remain null")

	// Test min with all nulls
	minAggs, _ := agg.NewAggregate("min", 102, "min(f2)", schemapb.DataType_Int64)
	minAgg := minAggs[0]

	rowTargetMin := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	rowTargetMin.UpdateFieldValue(rowNull1, 1, minAgg)
	rowTargetMin.UpdateFieldValue(rowNull2, 1, minAgg)
	assert.True(t, rowTargetMin.FieldValueAt(1).IsNull(), "min of all nulls should remain null")

	// Test max with all nulls
	maxAggs, _ := agg.NewAggregate("max", 103, "max(f3)", schemapb.DataType_Int64)
	maxAgg := maxAggs[0]

	rowTargetMax := agg.NewRow([]*agg.FieldValue{
		agg.NewFieldValue(int32(1)),
		agg.NewNullFieldValue(),
	})
	rowTargetMax.UpdateFieldValue(rowNull1, 1, maxAgg)
	rowTargetMax.UpdateFieldValue(rowNull2, 1, maxAgg)
	assert.True(t, rowTargetMax.FieldValueAt(1).IsNull(), "max of all nulls should remain null")
}

func TestAggregateOperators(t *testing.T) {
	suite.Run(t, new(AggregateOperatorsSuite))
}
