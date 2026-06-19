package agg

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func TestAggregateTerminateReturnsSingleSlotValue(t *testing.T) {
	tests := []struct {
		name      string
		aggregate AggregateBase
		value     any
	}{
		{name: "sum", aggregate: &SumAggregate{}, value: int64(10)},
		{name: "count", aggregate: &CountAggregate{}, value: int64(3)},
		{name: "min", aggregate: &MinAggregate{}, value: "a"},
		{name: "max", aggregate: &MaxAggregate{}, value: float64(9.5)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.aggregate.Terminate([]*FieldValue{NewFieldValue(test.value)})
			require.NoError(t, err)
			require.Equal(t, test.value, result)
		})
	}
}

func TestAggregateTerminateReturnsNilForNullSingleSlot(t *testing.T) {
	tests := []struct {
		name      string
		aggregate AggregateBase
	}{
		{name: "sum", aggregate: &SumAggregate{}},
		{name: "count", aggregate: &CountAggregate{}},
		{name: "min", aggregate: &MinAggregate{}},
		{name: "max", aggregate: &MaxAggregate{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.aggregate.Terminate([]*FieldValue{NewNullFieldValue()})
			require.NoError(t, err)
			require.Nil(t, result)
		})
	}
}

func TestNewAggregateAvgReturnsPhysicalSumAndCount(t *testing.T) {
	aggregates, err := NewAggregate("avg", 100, "avg(value)", schemapb.DataType_Int64)
	require.NoError(t, err)
	require.Len(t, aggregates, 2)
	require.IsType(t, &SumAggregate{}, aggregates[0])
	require.IsType(t, &CountAggregate{}, aggregates[1])
	require.Equal(t, kSum, aggregates[0].Name())
	require.Equal(t, kCount, aggregates[1].Name())
}

func TestMinAggregateUpdateOrderedTypes(t *testing.T) {
	tests := []struct {
		name     string
		target   any
		newValue any
		expected any
	}{
		{name: "int", target: int(10), newValue: int(3), expected: int(3)},
		{name: "int32", target: int32(10), newValue: int32(3), expected: int32(3)},
		{name: "int64", target: int64(10), newValue: int64(3), expected: int64(3)},
		{name: "float32", target: float32(10.5), newValue: float32(3.5), expected: float32(3.5)},
		{name: "float64", target: float64(10.5), newValue: float64(3.5), expected: float64(3.5)},
		{name: "string", target: "b", newValue: "a", expected: "a"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := NewFieldValue(test.target)

			require.NoError(t, (&MinAggregate{}).Update(target, NewFieldValue(test.newValue)))

			require.Equal(t, test.expected, target.Value())
		})
	}
}

func TestMaxAggregateUpdateOrderedTypes(t *testing.T) {
	tests := []struct {
		name     string
		target   any
		newValue any
		expected any
	}{
		{name: "int", target: int(3), newValue: int(10), expected: int(10)},
		{name: "int32", target: int32(3), newValue: int32(10), expected: int32(10)},
		{name: "int64", target: int64(3), newValue: int64(10), expected: int64(10)},
		{name: "float32", target: float32(3.5), newValue: float32(10.5), expected: float32(10.5)},
		{name: "float64", target: float64(3.5), newValue: float64(10.5), expected: float64(10.5)},
		{name: "string", target: "a", newValue: "b", expected: "b"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := NewFieldValue(test.target)

			require.NoError(t, (&MaxAggregate{}).Update(target, NewFieldValue(test.newValue)))

			require.Equal(t, test.expected, target.Value())
		})
	}
}

func TestMinMaxAggregateUpdateDoesNotReplaceWhenComparisonHasNaN(t *testing.T) {
	tests := []struct {
		name        string
		aggregate   AggregateBase
		target      float64
		newValue    float64
		expected    float64
		expectedNaN bool
	}{
		{name: "min new NaN", aggregate: &MinAggregate{}, target: 1, newValue: math.NaN(), expected: 1},
		{name: "min target NaN", aggregate: &MinAggregate{}, target: math.NaN(), newValue: 1, expectedNaN: true},
		{name: "max new NaN", aggregate: &MaxAggregate{}, target: 1, newValue: math.NaN(), expected: 1},
		{name: "max target NaN", aggregate: &MaxAggregate{}, target: math.NaN(), newValue: 1, expectedNaN: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := NewFieldValue(test.target)

			require.NoError(t, test.aggregate.Update(target, NewFieldValue(test.newValue)))

			if test.expectedNaN {
				require.True(t, math.IsNaN(target.Value().(float64)))
				return
			}
			require.Equal(t, test.expected, target.Value())
		})
	}
}

func TestMinAggregateUpdateRejectsMismatchedType(t *testing.T) {
	target := NewFieldValue(int64(1))

	err := (&MinAggregate{}).Update(target, NewFieldValue(int32(1)))

	require.Error(t, err)
	require.Contains(t, err.Error(), "type mismatch: target is int64, new is int32")
}

func TestMaxAggregateUpdateRejectsUnsupportedType(t *testing.T) {
	target := NewFieldValue(true)

	err := (&MaxAggregate{}).Update(target, NewFieldValue(false))

	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type for max aggregation: bool")
}

func TestSingleSlotAggregateUpdateState(t *testing.T) {
	tests := []struct {
		name      string
		aggregate AggregateBase
		values    []*FieldValue
		expected  any
	}{
		{name: "sum", aggregate: &SumAggregate{}, values: []*FieldValue{NewFieldValue(int64(10)), NewFieldValue(int64(20))}, expected: int64(30)},
		{name: "count", aggregate: &CountAggregate{}, values: []*FieldValue{NewFieldValue(int64(10)), NewNullFieldValue(), NewFieldValue(int64(20))}, expected: int64(2)},
		{name: "min", aggregate: &MinAggregate{}, values: []*FieldValue{NewFieldValue(int64(10)), NewFieldValue(int64(3))}, expected: int64(3)},
		{name: "max", aggregate: &MaxAggregate{}, values: []*FieldValue{NewFieldValue(int64(10)), NewFieldValue(int64(30))}, expected: int64(30)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := test.aggregate.NewState()

			for _, value := range test.values {
				require.NoError(t, test.aggregate.UpdateState(state, value))
			}

			result, err := test.aggregate.Terminate(state)
			require.NoError(t, err)
			require.Equal(t, test.expected, result)
		})
	}
}

func TestSingleSlotAggregateUpdateStateRejectsBadSlotWidth(t *testing.T) {
	tests := []struct {
		name      string
		aggregate AggregateBase
	}{
		{name: "sum", aggregate: &SumAggregate{}},
		{name: "count", aggregate: &CountAggregate{}},
		{name: "min", aggregate: &MinAggregate{}},
		{name: "max", aggregate: &MaxAggregate{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.aggregate.UpdateState(nil, NewFieldValue(int64(1)))

			require.Error(t, err)
			require.Contains(t, err.Error(), "expects 1 accumulator slot")
		})
	}
}

func TestAggregateMetadataAndPB(t *testing.T) {
	tests := []struct {
		name                 string
		aggregate            AggregateBase
		expectedName         string
		expectedOriginalName string
		expectedOp           planpb.AggregateOp
	}{
		{name: "sum", aggregate: &SumAggregate{fieldID: 100, originalName: "sum(v)"}, expectedName: kSum, expectedOriginalName: "sum(v)", expectedOp: planpb.AggregateOp_sum},
		{name: "count", aggregate: &CountAggregate{fieldID: 100, originalName: "count(v)"}, expectedName: kCount, expectedOriginalName: "count(v)", expectedOp: planpb.AggregateOp_count},
		{name: "min", aggregate: &MinAggregate{fieldID: 100, originalName: "min(v)"}, expectedName: kMin, expectedOriginalName: "min(v)", expectedOp: planpb.AggregateOp_min},
		{name: "max", aggregate: &MaxAggregate{fieldID: 100, originalName: "max(v)"}, expectedName: kMax, expectedOriginalName: "max(v)", expectedOp: planpb.AggregateOp_max},
		{name: "avg", aggregate: NewAvgAggregate(100, "avg(v)"), expectedName: kAvg, expectedOriginalName: "avg(v)", expectedOp: planpb.AggregateOp_avg},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expectedName, test.aggregate.Name())
			require.Equal(t, int64(100), test.aggregate.FieldID())
			require.Equal(t, test.expectedOriginalName, test.aggregate.OriginalName())

			pb := test.aggregate.ToPB()
			require.Equal(t, test.expectedOp, pb.GetOp())
			require.Equal(t, int64(100), pb.GetFieldId())
		})
	}
}

func TestAvgAggregateOwnsStateLifecycle(t *testing.T) {
	avg := NewAvgAggregate(100, "avg(value)")

	state := avg.NewState()
	require.Len(t, state, 2)

	require.NoError(t, avg.UpdateState(state, NewFieldValue(int64(10))))
	require.NoError(t, avg.UpdateState(state, NewFieldValue(int64(20))))

	result, err := avg.Terminate(state)
	require.NoError(t, err)
	require.Equal(t, float64(15), result)
}

func TestAvgAggregateRejectsDirectUpdate(t *testing.T) {
	err := NewAvgAggregate(100, "avg(value)").Update(NewNullFieldValue(), NewFieldValue(int64(1)))

	require.Error(t, err)
	require.Contains(t, err.Error(), "avg aggregate updates require aggregate state slots")
}

func TestAvgAggregateUpdateStateRejectsBadSlotWidth(t *testing.T) {
	err := NewAvgAggregate(100, "avg(value)").UpdateState([]*FieldValue{NewNullFieldValue()}, NewFieldValue(int64(1)))

	require.Error(t, err)
	require.Contains(t, err.Error(), "avg expects 2 accumulator slots")
}

func TestAvgAggregateUpdateStateSkipsNullInput(t *testing.T) {
	avg := NewAvgAggregate(100, "avg(value)")
	state := avg.NewState()

	require.NoError(t, avg.UpdateState(state, NewNullFieldValue()))

	result, err := avg.Terminate(state)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestAvgTerminate(t *testing.T) {
	avg := NewAvgAggregate(100, "avg(value)")

	result, err := avg.Terminate([]*FieldValue{NewFieldValue(int64(61)), NewFieldValue(int64(3))})
	require.NoError(t, err)
	require.InDelta(t, 61.0/3.0, result, 1e-9)
}

func TestAvgTerminateAcceptsNumericAccumulatorTypes(t *testing.T) {
	avg := NewAvgAggregate(100, "avg(value)")
	tests := []struct {
		name  string
		sum   any
		count any
	}{
		{name: "int", sum: int(6), count: int(3)},
		{name: "int32", sum: int32(6), count: int32(3)},
		{name: "int64", sum: int64(6), count: int64(3)},
		{name: "float32", sum: float32(6), count: float32(3)},
		{name: "float64", sum: float64(6), count: float64(3)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := avg.Terminate([]*FieldValue{NewFieldValue(test.sum), NewFieldValue(test.count)})

			require.NoError(t, err)
			require.Equal(t, float64(2), result)
		})
	}
}

func TestAvgTerminateReturnsNilForNullOrZeroCount(t *testing.T) {
	avg := NewAvgAggregate(100, "avg(value)")

	tests := []struct {
		name  string
		slots []*FieldValue
	}{
		{name: "null sum", slots: []*FieldValue{NewNullFieldValue(), NewFieldValue(int64(3))}},
		{name: "null count", slots: []*FieldValue{NewFieldValue(int64(61)), NewNullFieldValue()}},
		{name: "zero count", slots: []*FieldValue{NewFieldValue(int64(61)), NewFieldValue(int64(0))}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := avg.Terminate(test.slots)
			require.NoError(t, err)
			require.Nil(t, result)
		})
	}
}

func TestAggregateTerminateRejectsBadSlotWidth(t *testing.T) {
	_, err := (&SumAggregate{}).Terminate(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expects 1 accumulator slot")

	avg := NewAvgAggregate(100, "avg(value)")
	_, err = avg.Terminate([]*FieldValue{NewFieldValue(int64(1))})
	require.Error(t, err)
	require.Contains(t, err.Error(), "avg expects 2 accumulator slots")
}

func TestAvgTerminateRejectsNonNumericState(t *testing.T) {
	avg := NewAvgAggregate(100, "avg(value)")

	_, err := avg.Terminate([]*FieldValue{NewFieldValue("bad"), NewFieldValue(int64(1))})
	require.Error(t, err)
	require.Contains(t, err.Error(), "avg expects numeric accumulator")
}
