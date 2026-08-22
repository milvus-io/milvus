package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestIncompatibleBlockedChannelIDs(t *testing.T) {
	incompatible := types.ChannelID{Name: "incompatible"}
	compatible := types.ChannelID{Name: "compatible"}
	unassigned := types.ChannelID{Name: "unassigned"}
	layout := CurrentLayout{
		AllNodesInfo: map[int64]types.StreamingNodeStatus{
			1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1}},
		},
	}
	expected := ExpectedLayout{
		BlockedChannels: map[types.ChannelID]types.RecoveryStorageVersion{
			incompatible: types.RecoveryStorageVersionV2,
			compatible:   types.RecoveryStorageVersionLegacy,
			unassigned:   types.RecoveryStorageVersionV2,
		},
	}
	assignments := map[types.ChannelID]types.PChannelInfoAssigned{
		incompatible: {
			Channel: types.PChannelInfo{Name: incompatible.Name, RequiredRecoveryStorageVersion: types.RecoveryStorageVersionV2},
			Node:    types.StreamingNodeInfo{ServerID: 1},
		},
		compatible: {
			Channel: types.PChannelInfo{Name: compatible.Name},
			Node:    types.StreamingNodeInfo{ServerID: 1},
		},
	}

	assert.ElementsMatch(t, []types.ChannelID{incompatible}, incompatibleBlockedChannelIDs(layout, expected, assignments))
}
