package discover

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_manager"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSendFullAssignmentPublishesSecondaryChannelsAndShardAssignments(t *testing.T) {
	mc := mock_manager.NewMockManagerClient(t)
	mc.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"}, ResourceGroup: "rg1"},
		2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2, Address: "localhost:2"}, ResourceGroup: "rg1"},
	}, nil)
	resource.InitForTest(resource.OptStreamingManagerClient(mc))

	streamServer := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	streamServer.EXPECT().Context().Return(context.Background())
	streamServer.EXPECT().Send(mock.Anything).RunAndReturn(func(resp *streamingpb.AssignmentDiscoverResponse) error {
		fullAssignment := resp.GetFullAssignment()
		assignments := make(map[int64]*streamingpb.StreamingNodeAssignment, len(fullAssignment.Assignments))
		for _, assignment := range fullAssignment.Assignments {
			assignments[assignment.GetNode().GetServerId()] = assignment
		}

		node1Assignment := assignments[1]
		assert.NotNil(t, node1Assignment)
		assert.Equal(t, []string{"rw-channel"}, pchannelNames(node1Assignment.Channels))
		assert.Equal(t, []string{"ro-channel"}, pchannelNames(node1Assignment.SecondaryChannels))
		assert.Equal(t, types.ShardAssignmentInfo{
			PChannelAssignments: []types.PChannelShardAssignment{
				{
					PChannel: "ro-channel",
					Entries: []types.ShardAssignmentEntry{
						{CollectionID: 100, ShardIndex: 1, ReplicaID: 10},
					},
				},
			},
		}, types.NewShardAssignmentInfoFromProto(node1Assignment.GetShardAssignment()))

		node2Assignment := assignments[2]
		assert.NotNil(t, node2Assignment)
		assert.Empty(t, node2Assignment.Channels)
		assert.Empty(t, node2Assignment.SecondaryChannels)
		return nil
	})

	helper := &discoverGrpcServerHelper{
		StreamingCoordAssignmentService_AssignmentDiscoverServer: streamServer,
	}
	err := helper.SendFullAssignment(balancer.WatchChannelAssignmentsCallbackParam{
		Version:            typeutil.VersionInt64Pair{Global: 1, Local: 2},
		CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "pchannel"}},
		Relations: []types.PChannelInfoAssigned{
			{
				Channel: types.PChannelInfo{Name: "rw-channel", Term: 1, AccessMode: types.AccessModeRW},
				Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
			},
			{
				Channel: types.PChannelInfo{Name: "ro-channel", Term: 2, AccessMode: types.AccessModeRO},
				Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
			},
		},
		ShardAssignments: map[int64]types.ShardAssignmentInfo{
			1: {
				PChannelAssignments: []types.PChannelShardAssignment{
					{
						PChannel: "ro-channel",
						Entries: []types.ShardAssignmentEntry{
							{CollectionID: 100, ShardIndex: 1, ReplicaID: 10},
						},
					},
				},
			},
		},
	})
	assert.NoError(t, err)
}

func pchannelNames(channels []*streamingpb.PChannelInfo) []string {
	names := make([]string, 0, len(channels))
	for _, channel := range channels {
		names = append(names, channel.GetName())
	}
	return names
}

func TestAssignmentDiscover(t *testing.T) {
	mc := mock_manager.NewMockManagerClient(t)
	mc.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"}, ResourceGroup: "rg1"},
	}, nil)
	resource.InitForTest(resource.OptStreamingManagerClient(mc))
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		versions := []typeutil.VersionInt64Pair{
			{Global: 1, Local: 2},
			{Global: 1, Local: 3},
		}
		pchans := [][]types.PChannelInfoAssigned{
			{
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
			},
			{
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel2", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
			},
		}
		for i := 0; i < len(versions); i++ {
			cb(balancer.WatchChannelAssignmentsCallbackParam{
				Version:            versions[i],
				CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "pchannel"}},
				Relations:          pchans[i],
			})
		}
		<-ctx.Done()
		return context.Cause(ctx)
	})
	b.EXPECT().MarkAsUnavailable(mock.Anything, mock.Anything).Return(nil)

	streamServer := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	streamServer.EXPECT().Context().Return(context.Background())
	k := 0
	reqs := []*streamingpb.AssignmentDiscoverRequest{
		{
			Command: &streamingpb.AssignmentDiscoverRequest_ReportError{
				ReportError: &streamingpb.ReportAssignmentErrorRequest{
					Pchannel: &streamingpb.PChannelInfo{
						Name: "pchannel",
						Term: 1,
					},
					Err: &streamingpb.StreamingError{
						Code: streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST,
					},
				},
			},
		},
		{
			Command: &streamingpb.AssignmentDiscoverRequest_Close{},
		},
	}
	streamServer.EXPECT().Recv().RunAndReturn(func() (*streamingpb.AssignmentDiscoverRequest, error) {
		if k >= len(reqs) {
			return nil, io.EOF
		}
		req := reqs[k]
		k++
		return req, nil
	})
	streamServer.EXPECT().Send(mock.Anything).Return(nil)
	ads := NewAssignmentDiscoverServer(b, streamServer)
	ads.Execute()
}
