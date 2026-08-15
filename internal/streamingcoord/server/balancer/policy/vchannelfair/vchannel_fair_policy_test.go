package vchannelfair

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestVChannelFairPolicy(t *testing.T) {
	paramtable.Init()

	policy := &policy{}
	assert.Equal(t, policy.Name(), "vchannelFair")
	_, err := policy.Balance(balancer.CurrentLayout{})
	assert.Error(t, err)
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
	}, map[string]map[string]int64{
		"c1": {
			"vc1": 1,
			"vc2": 2,
		},
		"c3": {
			"vc3": 1,
			"vc4": 2,
			"vc5": 3,
		},
		"c5": {
			"vc6": 4,
			"vc7": 5,
			"vc8": 6,
		},
	}, []int64{2, 3}))
	assert.NoError(t, err)
	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c3")].Node.ServerID, expected.ChannelAssignment[newChannelID("c1")].Node.ServerID)

	expected, err = policy.Balance(newLayout(map[string]int{
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
	}, map[string]map[string]int64{
		"c1": {
			"vc1": 1,
			"vc2": 2,
		},
		"c3": {
			"vc3": 1,
			"vc4": 2,
			"vc5": 3,
		},
		"c5": {
			"vc6": 4,
			"vc7": 5,
			"vc8": 6,
		},
	}, []int64{1, 2, 3}))
	assert.NoError(t, err)
	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c3")].Node.ServerID, expected.ChannelAssignment[newChannelID("c1")].Node.ServerID)
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c3")].Node.ServerID, expected.ChannelAssignment[newChannelID("c5")].Node.ServerID)
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c5")].Node.ServerID, expected.ChannelAssignment[newChannelID("c1")].Node.ServerID)

	expected, err = policy.Balance(newLayout(map[string]int{
		"c1":  -1,
		"c3":  -1,
		"c4":  -1,
		"c2":  -1,
		"c5":  -1,
		"c6":  -1,
		"c7":  -1,
		"c8":  -1,
		"c9":  -1,
		"c10": -1,
	}, map[string]map[string]int64{
		"c1": {
			"vc1": 1,
			"vc2": 2,
		},
		"c3": {
			"vc3": 1,
			"vc4": 2,
			"vc5": 3,
		},
		"c5": {
			"vc6": 4,
			"vc7": 5,
			"vc8": 6,
		},
	}, []int64{1, 2, 3}))
	assert.NoError(t, err)
	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c3")].Node.ServerID, expected.ChannelAssignment[newChannelID("c1")].Node.ServerID)
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c3")].Node.ServerID, expected.ChannelAssignment[newChannelID("c5")].Node.ServerID)
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c5")].Node.ServerID, expected.ChannelAssignment[newChannelID("c1")].Node.ServerID)
}

func TestVChannelFairPolicyRecoveryStorageCompatibility(t *testing.T) {
	paramtable.Init()
	policy := &policy{}

	layout := newLayout(map[string]int{
		"legacy": 1,
		"v2":     -1,
	}, nil, []int64{1, 2})
	layout.Config.AllowRebalance = false
	layout.AllNodesInfo[2] = types.StreamingNodeStatus{
		StreamingNodeInfo:      types.StreamingNodeInfo{ServerID: 2},
		RecoveryStorageVersion: types.RecoveryStorageVersionV2,
	}
	v2Channel := newChannelID("v2")
	v2Info := layout.Channels[v2Channel]
	v2Info.RequiredRecoveryStorageVersion = types.RecoveryStorageVersionV2
	layout.Channels[v2Channel] = v2Info

	expected, err := policy.Balance(layout)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), expected.ChannelAssignment[v2Channel].Node.ServerID)
	assert.Equal(t, types.RecoveryStorageVersionV2, expected.ChannelAssignment[v2Channel].Channel.RequiredRecoveryStorageVersion)
	assert.Equal(t, int64(1), expected.ChannelAssignment[newChannelID("legacy")].Node.ServerID)
	assert.Equal(t, types.RecoveryStorageVersionLegacy, expected.ChannelAssignment[newChannelID("legacy")].Channel.RequiredRecoveryStorageVersion)

	blockedLayout := newLayout(map[string]int{"v2": -1}, nil, []int64{1})
	blockedInfo := blockedLayout.Channels[v2Channel]
	blockedInfo.RequiredRecoveryStorageVersion = types.RecoveryStorageVersionV2
	blockedLayout.Channels[v2Channel] = blockedInfo
	expected, err = policy.Balance(blockedLayout)
	assert.NoError(t, err)
	assert.NotContains(t, expected.ChannelAssignment, v2Channel)
	assert.Equal(t, types.RecoveryStorageVersionV2, expected.BlockedChannels[v2Channel])

	incompatibleOwnerLayout := newLayout(map[string]int{"v2": 1}, nil, []int64{1})
	incompatibleOwnerInfo := incompatibleOwnerLayout.Channels[v2Channel]
	incompatibleOwnerInfo.RequiredRecoveryStorageVersion = types.RecoveryStorageVersionV2
	incompatibleOwnerLayout.Channels[v2Channel] = incompatibleOwnerInfo
	expected, err = policy.Balance(incompatibleOwnerLayout)
	assert.NoError(t, err)
	assert.NotContains(t, expected.ChannelAssignment, v2Channel)
	assert.Equal(t, types.RecoveryStorageVersionV2, expected.BlockedChannels[v2Channel])

	promoteLayout := newLayout(map[string]int{"legacy": 2}, nil, []int64{2})
	promoteLayout.Config.AllowRebalance = false
	promoteLayout.AllNodesInfo[2] = types.StreamingNodeStatus{
		StreamingNodeInfo:      types.StreamingNodeInfo{ServerID: 2},
		RecoveryStorageVersion: types.RecoveryStorageVersionV2,
	}
	expected, err = policy.Balance(promoteLayout)
	assert.NoError(t, err)
	assert.Equal(t, types.RecoveryStorageVersionV2, expected.ChannelAssignment[newChannelID("legacy")].Channel.RequiredRecoveryStorageVersion)
}
