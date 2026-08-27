package shard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func newSnapshotTestInterceptor(t *testing.T) (interceptors.Interceptor, *mock_shards.MockShardManager) {
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
	i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	t.Cleanup(i.Close)
	return i, shardManager
}

func newCreateSnapshotMessage(vchannel string, timeTick uint64) message.MutableMessage {
	return message.NewCreateSnapshotMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&messagespb.CreateSnapshotMessageHeader{
			CollectionId: 1,
			Name:         "snap",
		}).
		WithBody(&messagespb.CreateSnapshotMessageBody{}).
		MustBuildMutable().WithTimeTick(timeTick)
}

// TestShardInterceptorCreateSnapshotFences is the whole reason CreateSnapshot is
// broadcast to data vchannels at all: the snapshot's boundary is this message's
// own position, and that only means anything if the growing segments are sealed
// there. Without the fence the boundary is declared but no segment is closed, so
// one segment spans it and the snapshot captures rows from both sides.
func TestShardInterceptorCreateSnapshotFences(t *testing.T) {
	i, shardManager := newSnapshotTestInterceptor(t)
	appender := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	}

	const timeTick = uint64(42)
	var (
		fencedCollection int64
		fencedUntil      uint64
	)
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).
		RunAndReturn(func(collectionID int64, timetick uint64) ([]int64, error) {
			fencedCollection = collectionID
			fencedUntil = timetick
			return []int64{7, 8}, nil
		})

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	msg := newCreateSnapshotMessage("v1", timeTick)

	msgID, err := i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	assert.Equal(t, int64(1), fencedCollection)
	// Fenced at the message's own position, not at some later "now": that is what
	// makes "before the snapshot" mean the same thing to the fence and to
	// DataCoord's membership test.
	assert.Equal(t, timeTick, fencedUntil)

	// The sealed ids are deliberately not written back into the header: nothing
	// downstream consumes them, and WAL recovery re-derives the set from the state
	// it has rebuilt at this position.
}

// TestShardInterceptorCreateSnapshotSkipsControlChannel pins the split of
// duties. The same broadcast puts a copy on the control channel, where it orders
// the DDL, and copies on the data vchannels, where it fences. Fencing off the
// control-channel copy would seal the collection twice per snapshot, at a
// position that has nothing to do with the data.
func TestShardInterceptorCreateSnapshotSkipsControlChannel(t *testing.T) {
	i, _ := newSnapshotTestInterceptor(t)
	appender := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	}

	// No FlushAndFenceSegmentAllocUntil expectation: mockery fails the test if it
	// is called at all.
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	msgID, err := i.DoAppend(ctx, newCreateSnapshotMessage("by-dev-rootcoord-dml_0vcchan", 42), appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)
}
