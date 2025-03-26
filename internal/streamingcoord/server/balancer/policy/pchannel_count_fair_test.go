package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestPChannelCountFair(t *testing.T) {
	paramtable.Init()

	policy := &vchannelFairPolicy{}
	expected, err := policy.Balance(newLayout(map[string]int{
		"c1":  2,
		"c3":  2,
		"c4":  2,
		"c2":  3,
		"c5":  3,
		"c6":  3,
		"c7":  3,
		"c8":  -1,
		"c9":  -1,
		"c10": -1,
	}, make(map[string]map[string]int64), []int64{
		1, 2, 3,
	}))

	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.EqualValues(t, int64(2), expected.ChannelAssignment[newChannelID("c1")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c3")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c4")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c2")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c5")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c6")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c7")].ServerID)
	counts := countByServerID(expected)
	assert.Equal(t, 3, len(counts))
	for _, count := range counts {
		assert.GreaterOrEqual(t, count, 3)
		assert.LessOrEqual(t, count, 4)
	}
	assert.NoError(t, err)

	expected, err = policy.Balance(newLayout(map[string]int{
		"c1":  2,
		"c3":  3,
		"c4":  2,
		"c2":  3,
		"c5":  3,
		"c6":  3,
		"c7":  3,
		"c8":  -1,
		"c9":  -1,
		"c10": -1,
	}, make(map[string]map[string]int64), []int64{1, 2, 3}))

	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c1")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c4")].ServerID)
	counts = countByServerID(expected)
	assert.Equal(t, 3, len(counts))
	for _, count := range counts {
		assert.GreaterOrEqual(t, count, 3)
		assert.LessOrEqual(t, count, 4)
	}
	assert.NoError(t, err)

	expected, err = policy.Balance(newLayout(map[string]int{
		"c1":  1,
		"c2":  1,
		"c3":  1,
		"c4":  2,
		"c5":  2,
		"c6":  2,
		"c7":  3,
		"c8":  3,
		"c9":  3,
		"c10": -1,
	}, make(map[string]map[string]int64), []int64{1, 2, 3}))

	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.Equal(t, int64(1), expected.ChannelAssignment[newChannelID("c1")].ServerID)
	assert.Equal(t, int64(1), expected.ChannelAssignment[newChannelID("c2")].ServerID)
	assert.Equal(t, int64(1), expected.ChannelAssignment[newChannelID("c3")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c4")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c5")].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c6")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c7")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c8")].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c9")].ServerID)
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

func newChannelID(channel string) types.ChannelID {
	return types.ChannelID{
		Name: channel,
	}
}

// newLayout creates a new layout for test.
func newLayout(channels map[string]int, vchannels map[string]map[string]int64, serverID []int64) balancer.CurrentLayout {
	layout := balancer.CurrentLayout{
		Channels:        make(map[types.ChannelID]channel.PChannelStatsView),
		AllNodesInfo:    make(map[int64]types.StreamingNodeInfo),
		ChannelsToNodes: make(map[types.ChannelID]int64),
	}
	for _, id := range serverID {
		layout.AllNodesInfo[id] = types.StreamingNodeInfo{
			ServerID: id,
		}
	}
	for c, node := range channels {
		if vc, ok := vchannels[c]; !ok {
			layout.Channels[newChannelID(c)] = channel.PChannelStatsView{VChannels: make(map[string]int64)}
		} else {
			layout.Channels[newChannelID(c)] = channel.PChannelStatsView{VChannels: vc}
		}
		if node > 0 {
			layout.ChannelsToNodes[newChannelID(c)] = int64(node)
		}
	}
	return layout
}
