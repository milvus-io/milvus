package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func TestPChannelCountFair(t *testing.T) {
	policy := &pchannelCountFairPolicy{}
	assert.Equal(t, "pchannel_count_fair", policy.Name())
	expected, err := policy.Balance(balancer.CurrentLayout{
		IncomingChannels: []string{
			"c8",
			"c9",
			"c10",
		},
		AllNodesInfo: map[int64]types.StreamingNodeInfo{
			1: {ServerID: 1},
			2: {ServerID: 2},
			3: {ServerID: 3},
		},
		AssignedChannels: map[int64][]types.PChannelInfo{
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
		ChannelsToNodes: map[string]int64{
			"c1": 2,
			"c3": 2,
			"c4": 2,
			"c2": 3,
			"c5": 3,
			"c6": 3,
			"c7": 3,
		},
	})

	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.Equal(t, int64(2), expected.ChannelAssignment["c1"].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment["c3"].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment["c4"].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment["c2"].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment["c5"].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment["c6"].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment["c7"].ServerID)
	counts := countByServerID(expected)
	assert.Equal(t, 3, len(counts))
	for _, count := range counts {
		assert.GreaterOrEqual(t, count, 3)
		assert.LessOrEqual(t, count, 4)
	}
	assert.NoError(t, err)

	assert.Equal(t, "pchannel_count_fair", policy.Name())
	expected, err = policy.Balance(balancer.CurrentLayout{
		IncomingChannels: []string{
			"c8",
			"c9",
			"c10",
		},
		AllNodesInfo: map[int64]types.StreamingNodeInfo{
			1: {ServerID: 1},
			2: {ServerID: 2},
			3: {ServerID: 3},
		},
		AssignedChannels: map[int64][]types.PChannelInfo{
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
		ChannelsToNodes: map[string]int64{
			"c1": 2,
			"c3": 3,
			"c4": 2,
			"c2": 3,
			"c5": 3,
			"c6": 3,
			"c7": 3,
		},
	})

	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.Equal(t, int64(2), expected.ChannelAssignment["c1"].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment["c4"].ServerID)
	counts = countByServerID(expected)
	assert.Equal(t, 3, len(counts))
	for _, count := range counts {
		assert.GreaterOrEqual(t, count, 3)
		assert.LessOrEqual(t, count, 4)
	}
	assert.NoError(t, err)

	assert.Equal(t, "pchannel_count_fair", policy.Name())
	expected, err = policy.Balance(balancer.CurrentLayout{
		IncomingChannels: []string{
			"c10",
		},
		AllNodesInfo: map[int64]types.StreamingNodeInfo{
			1: {ServerID: 1},
			2: {ServerID: 2},
			3: {ServerID: 3},
		},
		AssignedChannels: map[int64][]types.PChannelInfo{
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
		ChannelsToNodes: map[string]int64{
			"c1": 1,
			"c2": 1,
			"c3": 1,
			"c4": 2,
			"c5": 2,
			"c6": 2,
			"c7": 3,
			"c8": 3,
			"c9": 3,
		},
	})

	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.Equal(t, int64(1), expected.ChannelAssignment["c1"].ServerID)
	assert.Equal(t, int64(1), expected.ChannelAssignment["c2"].ServerID)
	assert.Equal(t, int64(1), expected.ChannelAssignment["c3"].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment["c4"].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment["c5"].ServerID)
	assert.Equal(t, int64(2), expected.ChannelAssignment["c6"].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment["c7"].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment["c8"].ServerID)
	assert.Equal(t, int64(3), expected.ChannelAssignment["c9"].ServerID)
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
