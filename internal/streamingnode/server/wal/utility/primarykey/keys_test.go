package primarykey

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeysLenAndToAny(t *testing.T) {
	require.Zero(t, Keys{}.Len())
	require.Empty(t, Keys{}.ToAny())

	ints := Keys{Kind: KindInt64, Int64Values: []int64{10, 20}}
	require.Equal(t, 2, ints.Len())
	require.Equal(t, []any{int64(10), int64(20)}, ints.ToAny())

	strs := Keys{Kind: KindString, StringValues: []string{"a", "b"}}
	require.Equal(t, 2, strs.Len())
	require.Equal(t, []any{"a", "b"}, strs.ToAny())
}

func TestKeysCloneIsIndependent(t *testing.T) {
	origin := Keys{Kind: KindInt64, Int64Values: []int64{1, 2}}
	cloned := origin.Clone()
	cloned.Int64Values[0] = 100
	require.Equal(t, int64(1), origin.Int64Values[0])
	require.Equal(t, KindInt64, cloned.Kind)
}

func TestKeysAppend(t *testing.T) {
	var keys Keys
	keys.Append(Keys{})
	require.Equal(t, KindNone, keys.Kind)

	keys.Append(Keys{Kind: KindInt64, Int64Values: []int64{1}})
	require.Equal(t, KindInt64, keys.Kind)

	keys.Append(Keys{Kind: KindInt64, Int64Values: []int64{2}})
	require.Equal(t, KindInt64, keys.Kind)
	require.Equal(t, []int64{1, 2}, keys.Int64Values)

	keys.Append(Keys{Kind: KindString, StringValues: []string{"a"}})
	require.Equal(t, KindMixed, keys.Kind)
	require.Equal(t, 3, keys.Len())

	keys.Append(Keys{Kind: KindString, StringValues: []string{"b"}})
	require.Equal(t, KindMixed, keys.Kind)
	require.Equal(t, []any{int64(1), int64(2), "a", "b"}, keys.ToAny())
}
