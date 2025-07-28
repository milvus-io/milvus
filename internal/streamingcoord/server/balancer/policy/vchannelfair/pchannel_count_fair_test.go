package vchannelfair

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestPChannelCountFair(t *testing.T) {
	paramtable.Init()

	policy := &policy{}
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
	assert.EqualValues(t, int64(2), expected.ChannelAssignment[newChannelID("c1")].Node.ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c3")].Node.ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c4")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c2")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c5")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c6")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c7")].Node.ServerID)
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
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c1")].Node.ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c4")].Node.ServerID)
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
	assert.Equal(t, int64(1), expected.ChannelAssignment[newChannelID("c1")].Node.ServerID)
	assert.Equal(t, int64(1), expected.ChannelAssignment[newChannelID("c2")].Node.ServerID)
	assert.Equal(t, int64(1), expected.ChannelAssignment[newChannelID("c3")].Node.ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c4")].Node.ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c5")].Node.ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c6")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c7")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c8")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c9")].Node.ServerID)
	counts = countByServerID(expected)
	assert.Equal(t, 3, len(counts))
	for _, count := range counts {
		assert.GreaterOrEqual(t, count, 3)
		assert.LessOrEqual(t, count, 4)
	}
	assert.NoError(t, err)

	expected, err = policy.Balance(newLayout(map[string]int{
		"c1": 1,
		"c2": 2,
		"c3": 3,
		"c4": 3,
		"c5": 3,
	}, make(map[string]map[string]int64), []int64{1, 2, 3}))
	assert.NoError(t, err)

	_, err = policy.Balance(balancer.CurrentLayout{})
	assert.Error(t, err)
}

func TestPChannelCountFairWithoutRebalance(t *testing.T) {
	paramtable.Init()
	b := &PolicyBuilder{}
	policy := b.Build()

	createNewLayout := func() balancer.CurrentLayout {
		return newLayout(map[string]int{
			"c1": 1,
			"c2": 2,
			"c3": 3,
			"c4": 3,
			"c5": 3,
			"c6": 3,
		}, make(map[string]map[string]int64), []int64{1, 2, 3})
	}

	// test allow rebalance
	layout := createNewLayout()
	expected, err := policy.Balance(layout)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(expected.ChannelAssignment))
	counts := countByServerID(expected)
	assert.Equal(t, 3, len(counts))
	for _, count := range counts {
		assert.Equal(t, count, 2)
	}

	// test allow rebalance = false
	layout = createNewLayout()
	layout.Config.AllowRebalance = false
	expected, err = policy.Balance(layout)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(expected.ChannelAssignment))
	assert.Equal(t, int64(1), expected.ChannelAssignment[newChannelID("c1")].Node.ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment[newChannelID("c2")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c3")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c4")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c5")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c6")].Node.ServerID)
	assert.Equal(t, 3, len(countByServerID(expected)))

	// test min rebalance interval threshold
	layout = createNewLayout()
	layout.Config.MinRebalanceIntervalThreshold = time.Hour
	layout.Stats[newChannelID("c3")] = channel.PChannelStatsView{
		LastAssignTimestamp: time.Now(),
		VChannels:           map[string]int64{},
	}
	layout.Stats[newChannelID("c4")] = channel.PChannelStatsView{
		LastAssignTimestamp: time.Now(),
		VChannels:           map[string]int64{},
	}
	layout.Stats[newChannelID("c5")] = channel.PChannelStatsView{
		LastAssignTimestamp: time.Now(),
		VChannels:           map[string]int64{},
	}
	expected, err = policy.Balance(layout)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(expected.ChannelAssignment))
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c3")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c4")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c5")].Node.ServerID)
	assert.NotEqual(t, int64(3), expected.ChannelAssignment[newChannelID("c6")].Node.ServerID)
	assert.Equal(t, 3, len(countByServerID(expected)))

	// test allow rebalance = false, but there's some channel is not assigned.
	layout = newLayout(map[string]int{
		"c1": -1,
		"c2": 3,
		"c3": -1,
		"c4": -1,
		"c5": 3,
		"c6": 3,
	}, make(map[string]map[string]int64), []int64{1, 2, 3})
	layout.Config.AllowRebalance = false

	expected, err = policy.Balance(layout)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(expected.ChannelAssignment))
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c2")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c5")].Node.ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment[newChannelID("c6")].Node.ServerID)
	assert.NotEqual(t, int64(3), expected.ChannelAssignment[newChannelID("c1")].Node.ServerID)
	assert.NotEqual(t, int64(3), expected.ChannelAssignment[newChannelID("c3")].Node.ServerID)
	assert.NotEqual(t, int64(3), expected.ChannelAssignment[newChannelID("c4")].Node.ServerID)
	assert.Equal(t, 3, len(countByServerID(expected)))
}

func countByServerID(expected balancer.ExpectedLayout) map[int64]int {
	counts := make(map[int64]int)
	for _, node := range expected.ChannelAssignment {
		counts[node.Node.ServerID]++
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
		Config: balancer.CommonBalancePolicyConfig{
			AllowRebalance:                     true,
			AllowRebalanceRecoveryLagThreshold: 1 * time.Second,
			MinRebalanceIntervalThreshold:      1 * time.Second,
		},
		Channels:           make(map[channel.ChannelID]types.PChannelInfo),
		Stats:              make(map[channel.ChannelID]channel.PChannelStatsView),
		AllNodesInfo:       make(map[int64]types.StreamingNodeStatus),
		ChannelsToNodes:    make(map[types.ChannelID]int64),
		ExpectedAccessMode: make(map[channel.ChannelID]types.AccessMode),
	}
	for _, id := range serverID {
		layout.AllNodesInfo[id] = types.StreamingNodeStatus{
			StreamingNodeInfo: types.StreamingNodeInfo{
				ServerID: id,
			},
		}
	}
	for c, node := range channels {
		if vc, ok := vchannels[c]; !ok {
			layout.Stats[newChannelID(c)] = channel.PChannelStatsView{VChannels: make(map[string]int64)}
		} else {
			layout.Stats[newChannelID(c)] = channel.PChannelStatsView{VChannels: vc}
		}
		if node > 0 {
			layout.ChannelsToNodes[newChannelID(c)] = int64(node)
		}
		layout.Channels[newChannelID(c)] = types.PChannelInfo{
			Name:       c,
			Term:       0,
			AccessMode: types.AccessModeRW,
		}
		layout.ExpectedAccessMode[newChannelID(c)] = types.AccessModeRW
	}
	return layout
}
