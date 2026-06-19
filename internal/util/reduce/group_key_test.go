package reduce

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashGroupValuesDeterministic(t *testing.T) {
	a := []any{int64(7), "brand", true}
	b := []any{int64(7), "brand", true}
	require.Equal(t, HashGroupValues(a), HashGroupValues(b))
}

func TestHashGroupValuesDifferentOrder(t *testing.T) {
	a := []any{int64(1), int64(2)}
	b := []any{int64(2), int64(1)}
	require.NotEqual(t, HashGroupValues(a), HashGroupValues(b))
}

func TestEqualGroupValuesScalars(t *testing.T) {
	require.True(t, EqualGroupValues([]any{int64(5), "x", true}, []any{int64(5), "x", true}))
	require.False(t, EqualGroupValues([]any{int64(5)}, []any{int64(6)}))
	require.False(t, EqualGroupValues([]any{int64(5)}, []any{int64(5), int64(5)}))
}

func TestEqualGroupValuesNullSemantics(t *testing.T) {
	require.True(t, EqualGroupValues([]any{nil}, []any{nil}))
	require.False(t, EqualGroupValues([]any{nil}, []any{int64(0)}))
	require.False(t, EqualGroupValues([]any{int64(0)}, []any{nil}))
}

func TestEqualGroupValuesNaNNeverEqual(t *testing.T) {
	nan := math.NaN()
	require.False(t, EqualGroupValues([]any{nan}, []any{nan}))
	require.False(t, EqualGroupValues([]any{nan}, []any{float64(1.0)}))
}

func TestHashGroupValuesNullSentinel(t *testing.T) {
	require.Equal(t, HashGroupValues([]any{nil}), HashGroupValues([]any{nil}))
	require.NotEqual(t, HashGroupValues([]any{nil}), HashGroupValues([]any{int64(0)}))
}

func TestNormalizeScalarCollapsesWidth(t *testing.T) {
	require.Equal(t, int64(42), NormalizeScalar(int32(42)))
	require.Equal(t, int64(42), NormalizeScalar(int16(42)))
	require.Equal(t, int64(42), NormalizeScalar(int8(42)))
	require.Equal(t, float64(1.5), NormalizeScalar(float32(1.5)))
	require.Equal(t, "ab", NormalizeScalar([]byte("ab")))
}

func TestMakeCompositeKeyExtractor(t *testing.T) {
	type rawGroupValue struct {
		value string
	}

	extract := MakeCompositeKeyExtractor([]func(int) any{
		func(int) any { return int32(7) },
		nil,
		func(int) any { return []byte("brand") },
		func(int) any { return nil },
		func(int) any { return rawGroupValue{value: "kept"} },
	})

	hash, values := extract(0)

	expectedValues := []any{int64(7), nil, "brand", nil, rawGroupValue{value: "kept"}}
	require.Equal(t, expectedValues, values)
	require.Equal(t, HashGroupValues(expectedValues), hash)

	hash2, values2 := MakeCompositeKeyExtractor([]func(int) any{
		func(int) any { return int64(7) },
		nil,
		func(int) any { return "brand" },
		func(int) any { return nil },
		func(int) any { return rawGroupValue{value: "kept"} },
	})(0)
	require.Equal(t, values, values2)
	require.Equal(t, hash, hash2)
}
