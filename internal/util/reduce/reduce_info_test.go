package reduce

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestResultInfoBuilderAndAccessors(t *testing.T) {
	info := NewReduceSearchResultInfo(2, 10).
		WithMetricType("L2").
		WithPkType(schemapb.DataType_Int64).
		WithOffset(3).
		WithGroupSize(4).
		WithAdvance(true).
		WithGroupByFieldIds([]int64{101, 102}).
		WithSearchAggregation(true)

	require.Equal(t, int64(2), info.GetNq())
	require.Equal(t, int64(10), info.GetTopK())
	require.Equal(t, "L2", info.GetMetricType())
	require.Equal(t, schemapb.DataType_Int64, info.GetPkType())
	require.Equal(t, int64(3), info.GetOffset())
	require.Equal(t, int64(4), info.GetGroupSize())
	require.True(t, info.GetIsAdvance())
	require.True(t, info.HasGroupBy())
	require.Equal(t, []int64{101, 102}, info.GetGroupByFieldIds())
	require.True(t, info.GetIsSearchAggregation())

	info.SetMetricType("IP")
	require.Equal(t, "IP", info.GetMetricType())
}

func TestResultInfoGroupByFieldIdsFromProto(t *testing.T) {
	t.Run("plural wins", func(t *testing.T) {
		info := NewReduceSearchResultInfo(1, 1).WithGroupByFieldIdsFromProto(101, []int64{102, 103})
		require.Equal(t, []int64{102, 103}, info.GetGroupByFieldIds())
	})

	t.Run("legacy singular used", func(t *testing.T) {
		info := NewReduceSearchResultInfo(1, 1).WithGroupByFieldIdsFromProto(101, nil)
		require.Equal(t, []int64{101}, info.GetGroupByFieldIds())
	})

	t.Run("non positive singular means no group by", func(t *testing.T) {
		info := NewReduceSearchResultInfo(1, 1).WithGroupByFieldIdsFromProto(0, nil)
		require.False(t, info.HasGroupBy())
		require.Nil(t, info.GetGroupByFieldIds())
	})
}

func TestResultInfoEffectiveOffset(t *testing.T) {
	require.Equal(t, int64(5), NewReduceSearchResultInfo(1, 1).WithOffset(5).EffectiveOffset())
	require.Equal(t, int64(0), NewReduceSearchResultInfo(1, 1).WithOffset(5).WithSearchAggregation(true).EffectiveOffset())
}

func TestReduceTypeHelpers(t *testing.T) {
	require.Equal(t, IReduceNoOrder, ToReduceType(0))
	require.Equal(t, IReduceInOrder, ToReduceType(1))
	require.Equal(t, IReduceInOrderForBest, ToReduceType(2))
	require.Equal(t, IReduceNoOrder, ToReduceType(99))

	require.False(t, ShouldStopWhenDrained(IReduceNoOrder))
	require.True(t, ShouldStopWhenDrained(IReduceInOrder))
	require.True(t, ShouldStopWhenDrained(IReduceInOrderForBest))

	require.True(t, ShouldUseInputLimit(IReduceNoOrder))
	require.True(t, ShouldUseInputLimit(IReduceInOrder))
	require.False(t, ShouldUseInputLimit(IReduceInOrderForBest))
}
