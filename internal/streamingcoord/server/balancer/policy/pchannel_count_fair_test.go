package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func TestPChannelCountFair(t *testing.T) {
	policy := &pchannelCountFairPolicy{}
	assert.Equal(t, "pchannel_count_fair", policy.Name())
	expected, err := policy.Balance(balancer.CurrentLayout{
		View: newMockedView([]string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"}),
		IncomingChannels: []types.ChannelID{
			newRWChannelID("c8"),
			newRWChannelID("c9"),
			newRWChannelID("c10"),
		},
		AllNodesInfo: map[int64]types.StreamingNodeInfo{
			1: {ServerID: 1},
			2: {ServerID: 2},
			3: {ServerID: 3},
		},
		AssignedChannels: map[int64][]types.ChannelID{
			1: {},
			2: {
				{Name: "c1"},
				{Name: "c3"},
				{Name: "c4"},
			},
			3: {
				{Name: "c2"},
				{Name: "c5"},
				{Name: "c6"},
				{Name: "c7"},
			},
		},
		ChannelsToNodes: map[types.ChannelID]int64{
			newRWChannelID("c1"): 2,
			newRWChannelID("c3"): 2,
			newRWChannelID("c4"): 2,
			newRWChannelID("c2"): 3,
			newRWChannelID("c5"): 3,
			newRWChannelID("c6"): 3,
			newRWChannelID("c7"): 3,
		},
	})

	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.Equal(t, int64(2), expected.ChannelAssignment[newRWChannelID("c1")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newRWChannelID("c3")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newRWChannelID("c4")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newRWChannelID("c2")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newRWChannelID("c5")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newRWChannelID("c6")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newRWChannelID("c7")].ServerID)
	counts := countByServerID(expected)
	assert.Equal(t, 3, len(counts))
	for _, count := range counts {
		assert.GreaterOrEqual(t, count, 3)
		assert.LessOrEqual(t, count, 4)
	}
	assert.NoError(t, err)

	assert.Equal(t, "pchannel_count_fair", policy.Name())
	expected, err = policy.Balance(balancer.CurrentLayout{
		View: newMockedView([]string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"}),
		IncomingChannels: []types.ChannelID{
			newRWChannelID("c8"),
			newRWChannelID("c9"),
			newRWChannelID("c10"),
		},
		AllNodesInfo: map[int64]types.StreamingNodeInfo{
			1: {ServerID: 1},
			2: {ServerID: 2},
			3: {ServerID: 3},
		},
		AssignedChannels: map[int64][]types.ChannelID{
			1: {},
			2: {
				{Name: "c1"},
				{Name: "c4"},
			},
			3: {
				{Name: "c2"},
				{Name: "c3"},
				{Name: "c5"},
				{Name: "c6"},
				{Name: "c7"},
			},
		},
		ChannelsToNodes: map[types.ChannelID]int64{
			newRWChannelID("c1"): 2,
			newRWChannelID("c3"): 3,
			newRWChannelID("c4"): 2,
			newRWChannelID("c2"): 3,
			newRWChannelID("c5"): 3,
			newRWChannelID("c6"): 3,
			newRWChannelID("c7"): 3,
		},
	})

	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.Equal(t, int64(2), expected.ChannelAssignment[newRWChannelID("c1")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newRWChannelID("c4")].ServerID)
	counts = countByServerID(expected)
	assert.Equal(t, 3, len(counts))
	for _, count := range counts {
		assert.GreaterOrEqual(t, count, 3)
		assert.LessOrEqual(t, count, 4)
	}
	assert.NoError(t, err)

	assert.Equal(t, "pchannel_count_fair", policy.Name())
	expected, err = policy.Balance(balancer.CurrentLayout{
		View: newMockedView([]string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"}),
		IncomingChannels: []types.ChannelID{
			newRWChannelID("c10"),
		},
		AllNodesInfo: map[int64]types.StreamingNodeInfo{
			1: {ServerID: 1},
			2: {ServerID: 2},
			3: {ServerID: 3},
		},
		AssignedChannels: map[int64][]types.ChannelID{
			1: {
				{Name: "c1"},
				{Name: "c2"},
				{Name: "c3"},
			},
			2: {
				{Name: "c4"},
				{Name: "c5"},
				{Name: "c6"},
			},
			3: {
				{Name: "c7"},
				{Name: "c8"},
				{Name: "c9"},
			},
		},
		ChannelsToNodes: map[types.ChannelID]int64{
			newRWChannelID("c1"): 1,
			newRWChannelID("c2"): 1,
			newRWChannelID("c3"): 1,
			newRWChannelID("c4"): 2,
			newRWChannelID("c5"): 2,
			newRWChannelID("c6"): 2,
			newRWChannelID("c7"): 3,
			newRWChannelID("c8"): 3,
			newRWChannelID("c9"): 3,
		},
	})

	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.Equal(t, int64(1), expected.ChannelAssignment[newRWChannelID("c1")].ServerID)
	assert.Equal(t, int64(1), expected.ChannelAssignment[newRWChannelID("c2")].ServerID)
	assert.Equal(t, int64(1), expected.ChannelAssignment[newRWChannelID("c3")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newRWChannelID("c4")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newRWChannelID("c5")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newRWChannelID("c6")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newRWChannelID("c7")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newRWChannelID("c8")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newRWChannelID("c9")].ServerID)
	counts = countByServerID(expected)
	assert.Equal(t, 3, len(counts))
	for _, count := range counts {
		assert.GreaterOrEqual(t, count, 3)
		assert.LessOrEqual(t, count, 4)
	}
	assert.NoError(t, err)

	_, err = policy.Balance(balancer.CurrentLayout{})
	assert.Error(t, err)
}

func countByServerID(expected balancer.ExpectedLayout) map[int64]int {
	counts := make(map[int64]int)
	for _, node := range expected.ChannelAssignment {
		counts[node.ServerID]++
	}
	return counts
}

func newRWChannelID(name string) types.ChannelID {
	return types.ChannelID{
		Name:      name,
		ReplicaID: 0,
	}
}

func newMockedView(names []string) *channel.PChannelView {
	view := &channel.PChannelView{
		Channels: make(map[channel.ChannelID]*channel.PChannelMeta),
	}
	for _, name := range names {
		view.Channels[newRWChannelID(name)] = nil
	}
	return view
}
