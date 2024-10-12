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

func TestAggregateOperators(t *testing.T) {
	suite.Run(t, new(AggregateOperatorsSuite))
}
