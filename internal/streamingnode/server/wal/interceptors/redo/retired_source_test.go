package redo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type retiredSourceShards struct{ shards.ShardManager }

func (retiredSourceShards) IsSplitFenced(vchannel string) bool { return vchannel == "p0_1v0" }

func TestRetiredSplitSourceRefusesBeforePartitionLookup(t *testing.T) {
	// All partition operations on this minimal recovered source would panic.
	// A stale proxy must get the routing refusal even after allocators are gone.
	r := &redoAppendInterceptor{shardManager: retiredSourceShards{}}
	msg := message.NewInsertMessageBuilderV1().WithVChannel("p0_1v0").
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{}).MustBuildMutable()
	err := r.waitUntilGrowingSegmentReady(context.Background(), msg)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsShardFenced())
	require.False(t, status.AsStreamingError(err).IsUnrecoverable())
}
