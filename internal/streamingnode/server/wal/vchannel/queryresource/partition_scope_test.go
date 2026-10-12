package queryresource

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestResolvePartitionsUsesLeasedViewScope(t *testing.T) {
	runtime := NewQueryRuntime()
	runtime.partitionScopes = map[uint64][]int64{1: {10}, 2: {10, 20}, 3: {20}, 4: {}}
	for _, tc := range []struct {
		version         uint64
		requested, want []int64
		failure         bool
	}{
		{1, nil, []int64{10}, false},
		{2, nil, []int64{10, 20}, false},
		{3, nil, []int64{20}, false},
		{1, []int64{20}, nil, true},
		{3, []int64{10}, nil, true},
		{2, []int64{10}, []int64{10}, false},
	} {
		got, err := runtime.ResolvePartitions(tc.version, tc.requested)
		if tc.failure {
			require.ErrorIs(t, err, merr.ErrPartitionNotLoaded)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		}
	}
	_, err := runtime.ResolvePartitions(99, nil)
	require.Error(t, err)
	_, err = runtime.ResolvePartitions(4, nil)
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	// Returned slices must not permit mutation of a retained view's scope.
	got, err := runtime.ResolvePartitions(2, nil)
	require.NoError(t, err)
	got[0] = 99
	require.Equal(t, int64(10), runtime.partitionScopes[2][0])
}
