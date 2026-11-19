package resolver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type lineageTestResolver struct {
	staticVChannelProvider
	shards []Shard
	err    error
}

func (r *lineageTestResolver) ResolveVChannels(ctx context.Context, collectionID int64) ([]string, error) {
	return r.GetCollectionVChannels(ctx, collectionID)
}

func (r *lineageTestResolver) ResolveShards(context.Context, int64) ([]Shard, error) {
	return r.shards, r.err
}

func TestResolveShardsWithoutLineageSupport(t *testing.T) {
	provider := &staticVChannelProvider{vchannels: map[int64][]string{100: {"p0_100v0", "p1_100v1"}}}
	shards, err := ResolveShards(context.Background(), NewShardResolverImpl(provider), 100)
	require.NoError(t, err)
	// The default resolver serves every vchannel on its own.
	require.Equal(t, []Shard{{VChannel: "p0_100v0"}, {VChannel: "p1_100v1"}}, shards)

	shards, err = ResolveShards(context.Background(), NewShardResolverImpl(provider), 200)
	require.NoError(t, err)
	require.Empty(t, shards)

	provider.err = context.DeadlineExceeded
	_, err = ResolveShards(context.Background(), NewShardResolverImpl(provider), 100)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestResolveShardsPrefersLineageResolver(t *testing.T) {
	want := []Shard{
		{VChannel: "s", Lineage: &ShardLineage{VChannels: []string{"t1", "t2"}, SwitchTimeTick: 100}},
		{VChannel: "t1"},
		{VChannel: "t2"},
	}
	r := &lineageTestResolver{shards: want}
	shards, err := ResolveShards(context.Background(), r, 100)
	require.NoError(t, err)
	require.Equal(t, want, shards)

	r.err = context.Canceled
	_, err = ResolveShards(context.Background(), r, 100)
	require.ErrorIs(t, err, context.Canceled)
}
