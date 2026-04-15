package search_agg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyOrderAndSizeDirect(t *testing.T) {
	t.Run("empty buckets", func(t *testing.T) {
		buckets := []*AggBucketResult{}
		got, err := applyOrderAndSize(buckets, LevelContext{Size: 1})
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("no order normalizes size", func(t *testing.T) {
		buckets := []*AggBucketResult{{Count: 1}, {Count: 2}}
		got, err := applyOrderAndSize(buckets, LevelContext{Size: 0})
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, int64(1), got[0].Count)
	})

	t.Run("count ascending", func(t *testing.T) {
		buckets := []*AggBucketResult{{Count: 3}, {Count: 1}, {Count: 2}}
		got, err := applyOrderAndSize(buckets, LevelContext{
			Size:  3,
			Order: []OrderCriterion{{Key: "_count", Dir: "asc"}},
		})
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2, 3}, []int64{got[0].Count, got[1].Count, got[2].Count})
	})

	t.Run("count descending with truncation", func(t *testing.T) {
		buckets := []*AggBucketResult{{Count: 3}, {Count: 1}, {Count: 2}}
		got, err := applyOrderAndSize(buckets, LevelContext{
			Size:  2,
			Order: []OrderCriterion{{Key: "_count", Dir: "desc"}},
		})
		require.NoError(t, err)
		require.Equal(t, []int64{3, 2}, []int64{got[0].Count, got[1].Count})
	})

	t.Run("composite key descending", func(t *testing.T) {
		buckets := []*AggBucketResult{
			{Key: map[int64]any{1: "a", 2: int64(2)}},
			{Key: map[int64]any{1: "b", 2: int64(1)}},
			{Key: map[int64]any{1: "a", 2: int64(3)}},
		}
		got, err := applyOrderAndSize(buckets, LevelContext{
			OwnFieldIDs: []int64{1, 2},
			Size:        3,
			Order:       []OrderCriterion{{Key: "_key", Dir: "desc"}},
		})
		require.NoError(t, err)
		require.Equal(t, "b", got[0].Key[1])
		require.Equal(t, int64(3), got[1].Key[2])
		require.Equal(t, int64(2), got[2].Key[2])
	})

	t.Run("multi criterion fallback", func(t *testing.T) {
		buckets := []*AggBucketResult{
			{Count: 2, Key: map[int64]any{1: "b"}},
			{Count: 2, Key: map[int64]any{1: "a"}},
			{Count: 1, Key: map[int64]any{1: "c"}},
		}
		got, err := applyOrderAndSize(buckets, LevelContext{
			OwnFieldIDs: []int64{1},
			Size:        3,
			Order:       []OrderCriterion{{Key: "_count", Dir: "desc"}, {Key: "_key", Dir: "asc"}},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b", "c"}, []string{got[0].Key[1].(string), got[1].Key[1].(string), got[2].Key[1].(string)})
	})

	t.Run("metric alias ordering with nil first", func(t *testing.T) {
		buckets := []*AggBucketResult{
			{Metrics: map[string]any{"score": 2.0}},
			{Metrics: map[string]any{}},
			{Metrics: map[string]any{"score": 1.0}},
		}
		got, err := applyOrderAndSize(buckets, LevelContext{
			Size:  3,
			Order: []OrderCriterion{{Key: "score", Dir: "asc"}},
		})
		require.NoError(t, err)
		require.Nil(t, got[0].Metrics["score"])
		require.Equal(t, 1.0, got[1].Metrics["score"])
		require.Equal(t, 2.0, got[2].Metrics["score"])
	})
}

func TestCompareBucketByCriterionDirect(t *testing.T) {
	a := &AggBucketResult{
		Count:   2,
		Key:     map[int64]any{1: "a", 2: int64(1)},
		Metrics: map[string]any{"sum": int64(10)},
	}
	b := &AggBucketResult{
		Count:   3,
		Key:     map[int64]any{1: "a", 2: int64(2)},
		Metrics: map[string]any{"sum": int64(5)},
	}

	cmp, err := compareBucketByCriterion(a, b, []int64{1, 2}, OrderCriterion{Key: "_count"})
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	cmp, err = compareBucketByCriterion(a, b, []int64{1, 2}, OrderCriterion{Key: "_key"})
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	cmp, err = compareBucketByCriterion(a, b, []int64{1, 2}, OrderCriterion{Key: "sum"})
	require.NoError(t, err)
	require.Equal(t, 1, cmp)
}

func TestCompareBucketKeysError(t *testing.T) {
	_, err := compareBucketKeys(
		map[int64]any{1: []byte("a")},
		map[int64]any{1: []byte("b")},
		[]int64{1},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "compare _key field 1 failed")
}
