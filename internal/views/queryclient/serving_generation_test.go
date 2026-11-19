package queryclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type servingTestClient struct {
	empty bool
	epoch uint64
	calls int
}

func (c *servingTestClient) GetQueryViewLoadInfo(context.Context, *querypb.GetQueryViewLoadInfoRequest, ...grpc.CallOption) (*querypb.GetQueryViewLoadInfoResponse, error) {
	c.calls++
	if c.empty {
		return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil
	}
	return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), ShardSetEpoch: c.epoch, ServingShards: []*viewpb.DataViewOfShard{
		{Vchannel: "s", Lineage: &viewpb.QueryViewLineage{Vchannels: []string{"t1", "t2"}, SwitchTimetick: 100}}, {Vchannel: "t1"}, {Vchannel: "t2"},
	}}, nil
}

func TestServingGenerationRetryResolvesTheWholeListAgain(t *testing.T) {
	client := &servingTestClient{epoch: 2}
	r := withServingResolver(nil, client)
	var epochs []uint64
	err := retryServingGeneration(t.Context(), r, 3, 100, func() error {
		shards, err := resolver.ResolveShards(t.Context(), r, 100)
		require.NoError(t, err)
		require.Len(t, shards, 3)
		require.Equal(t, []string{"t1", "t2"}, shards[0].Lineage.VChannels)
		epochs = append(epochs, shards[0].ShardSetEpoch)
		if client.calls == 1 {
			client.epoch = 3
			return viewerror.NewViewInvalidated("stale serving epoch")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 3}, epochs)
	require.Equal(t, 2, client.calls)
}

func TestServingResolverPropagatesEpochThroughEveryShard(t *testing.T) {
	client := &servingTestClient{epoch: 2}
	r := withServingResolver(nil, client)
	shards, err := resolver.ResolveShards(t.Context(), r, 100)
	require.NoError(t, err)
	s := newShardViewQueryClient(3, nil, nil)
	// Ordinary shards prove the transport context; lineage orchestration is
	// already exercised against the actual GetQueryPlan request in group tests.
	shards[0].Lineage = nil
	err = s.fanOutShards(t.Context(), shards, requestReadPoint{}, func(ctx context.Context, index int, read shardReadPoint) error {
		require.Equal(t, shards[index], ctx.Value(servingShardContextKey{}))
		require.Equal(t, shards[index].VChannel, read.vchannel)
		return nil
	})
	require.NoError(t, err)
}

type servingMarkerFallback struct {
	split     bool
	refreshed bool
}

func (r *servingMarkerFallback) ResolveVChannels(context.Context, int64) ([]string, error) {
	return []string{"s"}, nil
}

func (r *servingMarkerFallback) RequiresServingGeneration(context.Context, int64) ([]string, bool, error) {
	return []string{"s"}, r.split, nil
}

func (r *servingMarkerFallback) RefreshServingGeneration(context.Context, int64) {
	r.refreshed = true
	r.split = true
}

func TestNeverSplitReadsKeepMetadataResolverWithoutCoordinatorRPC(t *testing.T) {
	client := &servingTestClient{epoch: 2}
	fallback := &servingMarkerFallback{}
	shards, err := resolver.ResolveShards(t.Context(), withServingResolver(fallback, client), 100)
	require.NoError(t, err)
	require.Equal(t, []resolver.Shard{{VChannel: "s"}}, shards)
	require.Zero(t, client.calls, "successful unsplit reads must not depend on Coordinator availability")
}

func TestStaleUnsplitCacheRefreshesIntoServingGeneration(t *testing.T) {
	client := &servingTestClient{epoch: 2}
	fallback := &servingMarkerFallback{}
	wrapped := withServingResolver(fallback, client)
	attempts := 0
	err := retryServingGeneration(t.Context(), wrapped, 3, 100, func() error {
		attempts++
		shards, err := resolver.ResolveShards(t.Context(), wrapped, 100)
		require.NoError(t, err)
		if attempts == 1 {
			require.Len(t, shards, 1)
			return viewerror.NewViewNotFound("source g0 was preempted")
		}
		require.Len(t, shards, 3)
		require.EqualValues(t, 2, shards[0].ShardSetEpoch)
		return nil
	})
	require.NoError(t, err)
	require.True(t, fallback.refreshed)
	require.Equal(t, 1, client.calls)
}

func TestSplitMetadataNeverFallsBackToWriteRoutingWithoutPublication(t *testing.T) {
	client := &servingTestClient{empty: true}
	fallback := &servingMarkerFallback{split: true}
	shards, err := resolver.ResolveShards(t.Context(), withServingResolver(fallback, client), 100)
	require.Nil(t, shards)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	require.Equal(t, 1, client.calls)
}
