package balancer_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_manager"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	_ "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/policy"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestBalancer(t *testing.T) {
	paramtable.Init()

	streamingNodeManager := mock_manager.NewMockManagerClient(t)
	streamingNodeManager.EXPECT().WatchNodeChanged(mock.Anything).Return(make(chan struct{}), nil)
	streamingNodeManager.EXPECT().Assign(mock.Anything, mock.Anything).Return(nil)
	streamingNodeManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	streamingNodeManager.EXPECT().CollectAllStatus(mock.Anything).Return(map[int64]*types.StreamingNodeStatus{
		1: {
			StreamingNodeInfo: types.StreamingNodeInfo{
				ServerID: 1,
				Address:  "localhost:1",
			},
		},
		2: {
			StreamingNodeInfo: types.StreamingNodeInfo{
				ServerID: 2,
				Address:  "localhost:2",
			},
		},
		3: {
			StreamingNodeInfo: types.StreamingNodeInfo{
				ServerID: 3,
				Address:  "localhost:3",
			},
		},
		4: {
			StreamingNodeInfo: types.StreamingNodeInfo{
				ServerID: 3,
				Address:  "localhost:3",
			},
			Err: types.ErrStopping,
		},
	}, nil)

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptStreamingManagerClient(streamingNodeManager))
	catalog.EXPECT().ListPChannel(mock.Anything).Unset()
	catalog.EXPECT().ListPChannel(mock.Anything).RunAndReturn(func(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
		return []*streamingpb.PChannelMeta{
			{
				Channel: &streamingpb.PChannelInfo{
					Name: "test-channel-1",
					Term: 1,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				Node:  &streamingpb.StreamingNodeInfo{ServerId: 1},
			},
			{
				Channel: &streamingpb.PChannelInfo{
					Name: "test-channel-2",
					Term: 1,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				Node:  &streamingpb.StreamingNodeInfo{ServerId: 4},
			},
			{
				Channel: &streamingpb.PChannelInfo{
					Name: "test-channel-3",
					Term: 2,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				Node:  &streamingpb.StreamingNodeInfo{ServerId: 2},
			},
		}, nil
	})
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil).Maybe()

	ctx := context.Background()
	b, err := balancer.RecoverBalancer(ctx, "pchannel_count_fair")
	assert.NoError(t, err)
	assert.NotNil(t, b)

	b.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "test-channel-1",
		Term: 1,
	}})
	b.Trigger(ctx)

	doneErr := errors.New("done")
	err = b.WatchChannelAssignments(ctx, func(version typeutil.VersionInt64Pair, relations []types.PChannelInfoAssigned) error {
		// should one pchannel be assigned to per nodes
		nodeIDs := typeutil.NewSet[int64]()
		if len(relations) == 3 {
			for _, status := range relations {
				nodeIDs.Insert(status.Node.ServerID)
			}
			assert.Equal(t, 3, nodeIDs.Len())
			return doneErr
		}
		return nil
	})
	assert.ErrorIs(t, err, doneErr)

	// create a inifite block watcher and can be interrupted by close of balancer.
	f := syncutil.NewFuture[error]()
	go func() {
		err := b.WatchChannelAssignments(context.Background(), func(version typeutil.VersionInt64Pair, relations []types.PChannelInfoAssigned) error {
			return nil
		})
		f.Set(err)
	}()
	time.Sleep(20 * time.Millisecond)
	assert.False(t, f.Ready())

	b.Close()
	assert.ErrorIs(t, f.Get(), balancer.ErrBalancerClosed)
}
