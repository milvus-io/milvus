package balance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestAssignChannelToWALLocatedFirst(t *testing.T) {
	balancer := mock_balancer.NewMockBalancer(t)
	balancer.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb func(typeutil.VersionInt64Pair, []types.PChannelInfoAssigned) error) error {
		versions := []typeutil.VersionInt64Pair{
			{Global: 1, Local: 2},
		}
		pchans := [][]types.PChannelInfoAssigned{
			{
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRW},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel2", Term: 1, AccessMode: types.AccessModeRW},
					Node:    types.StreamingNodeInfo{ServerID: 2, Address: "localhost:1"},
				},
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel3", Term: 1, AccessMode: types.AccessModeRW},
					Node:    types.StreamingNodeInfo{ServerID: 3, Address: "localhost:1"},
				},
			},
		}
		for i := 0; i < len(versions); i++ {
			cb(versions[i], pchans[i])
		}
		<-ctx.Done()
		return context.Cause(ctx)
	})
	snmanager.StaticStreamingNodeManager.SetBalancerReady(balancer)

	channels := []*meta.DmChannel{
		{VchannelInfo: &datapb.VchannelInfo{ChannelName: "pchannel_v1"}},
		{VchannelInfo: &datapb.VchannelInfo{ChannelName: "pchannel2_v2"}},
		{VchannelInfo: &datapb.VchannelInfo{ChannelName: "pchannel3_v1"}},
	}

	var scoreDelta map[int64]int
	nodeInfos := []*session.NodeInfo{
		session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}),
		session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}),
	}

	notFounChannels, plans, scoreDelta := assignChannelToWALLocatedFirstForNodeInfo(channels, nodeInfos)
	assert.Len(t, notFounChannels, 1)
	assert.Equal(t, notFounChannels[0].GetChannelName(), "pchannel3_v1")
	assert.Len(t, plans, 2)
	assert.Len(t, scoreDelta, 2)
	for _, plan := range plans {
		if plan.Channel.GetChannelName() == "pchannel_v1" {
			assert.Equal(t, plan.To, int64(1))
			assert.Equal(t, scoreDelta[1], 1)
		} else {
			assert.Equal(t, plan.To, int64(2))
			assert.Equal(t, scoreDelta[2], 1)
		}
	}
}
