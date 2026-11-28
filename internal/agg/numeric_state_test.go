package agg

import (
	"math"
	"math/big"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestIntegerAggregateStateBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name     string
		values   []int64
		sum      int64
		overflow bool
		avg      float64
	}{
		{"positive overflow", []int64{math.MaxInt64, 1}, 0, true, math.Exp2(62)},
		{"negative overflow", []int64{math.MinInt64, -2}, 0, true, -math.Exp2(62)},
		{"positive cancellation", []int64{math.MaxInt64, 1, -1}, math.MaxInt64, false, float64(math.MaxInt64) / 3},
		{"negative cancellation", []int64{math.MinInt64, -1, 1}, math.MinInt64, false, float64(math.MinInt64) / 3},
		{"wide cancellation", []int64{math.MaxInt64, math.MaxInt64, math.MinInt64, math.MinInt64}, -2, false, -0.5},
		{"round after division", []int64{1 << 53, 1, 0}, 1<<53 + 1, false, 3002399751580331},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Different encounter orders must not change the exact integer result.
			for shift := range tc.values {
				sum, avg := &SumAggregate{}, NewAvgAggregate(100, "avg(v)")
				sumState, avgState := sum.NewState(), avg.NewState()
				for i := range tc.values {
					value := NewFieldValue(tc.values[(i+shift)%len(tc.values)])
					require.NoError(t, sum.UpdateState(sumState, value))
					require.NoError(t, avg.UpdateState(avgState, value))
				}
				require.NoError(t, sum.UpdateState(sumState, NewNullFieldValue()))
				require.NoError(t, avg.UpdateState(avgState, NewNullFieldValue()))
				got, err := sum.Terminate(sumState)
				if tc.overflow {
					require.ErrorIs(t, err, merr.ErrAggregateOverflow)
					require.Nil(t, got)
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.sum, got)
				}
				got, err = avg.Terminate(avgState)
				require.NoError(t, err)
				require.Equal(t, tc.avg, got)
			}
		})
	}
}

func TestIntegerAggregateStateAgainstExactArithmetic(t *testing.T) {
	rng := rand.New(rand.NewSource(53131))
	for range 200 {
		sum, avg := &SumAggregate{}, NewAvgAggregate(100, "avg(v)")
		sumState, avgState := sum.NewState(), avg.NewState()
		exact := new(big.Int)
		count := 1 + rng.Intn(40)
		for range count {
			value := int64(rng.Uint64())
			exact.Add(exact, big.NewInt(value))
			require.NoError(t, sum.UpdateState(sumState, NewFieldValue(value)))
			require.NoError(t, avg.UpdateState(avgState, NewFieldValue(value)))
		}
		got, err := sum.Terminate(sumState)
		if exact.IsInt64() {
			require.NoError(t, err)
			require.Equal(t, exact.Int64(), got)
		} else {
			require.ErrorIs(t, err, merr.ErrAggregateOverflow)
		}
		want, _ := new(big.Rat).SetFrac(exact, big.NewInt(int64(count))).Float64()
		got, err = avg.Terminate(avgState)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestIntegerAggregateStatesAreIndependent(t *testing.T) {
	sum := &SumAggregate{}
	a, b := sum.NewState(), sum.NewState()
	for _, state := range [][]*FieldValue{a, b} {
		for _, value := range []int64{math.MaxInt64, 1} {
			require.NoError(t, sum.UpdateState(state, NewFieldValue(value)))
		}
	}
	require.NoError(t, sum.UpdateState(a, NewFieldValue(int64(-1))))
	got, err := sum.Terminate(a)
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), got)
	_, err = sum.Terminate(b)
	require.ErrorIs(t, err, merr.ErrAggregateOverflow)
	// Finalization must not narrow/mutate a state needed by another metric.
	avg := NewAvgAggregate(100, "avg(v)")
	got, err = avg.Terminate(append(b, NewFieldValue(int64(2))))
	require.NoError(t, err)
	require.Equal(t, math.Exp2(62), got)
	_, err = sum.Terminate(b)
	require.ErrorIs(t, err, merr.ErrAggregateOverflow)
}

func TestNumericStateTypes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []any
		want   any
	}{
		{"int32", []any{int32(math.MaxInt32), int32(1)}, int64(1 << 31)},
		{"int", []any{int(1), int(2)}, int64(3)},
		{"float32", []any{float32(1.5), float32(2)}, float32(3.5)},
		{"float64", []any{float64(1.5), float64(2)}, float64(3.5)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sum := &SumAggregate{}
			state := sum.NewState()
			for _, v := range tc.values {
				require.NoError(t, sum.UpdateState(state, NewFieldValue(v)))
			}
			got, err := sum.Terminate(state)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, values := range [][]any{
		{int64(1), float64(2)},
		{float64(1), int64(2)},
		{int64(math.MaxInt64), int64(1), float64(2)},
	} {
		sum := &SumAggregate{}
		state := sum.NewState()
		for _, v := range values[:len(values)-1] {
			require.NoError(t, sum.UpdateState(state, NewFieldValue(v)))
		}
		require.Error(t, sum.UpdateState(state, NewFieldValue(values[len(values)-1])))
	}
}
