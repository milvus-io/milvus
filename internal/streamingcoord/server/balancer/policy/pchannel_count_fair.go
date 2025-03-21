package policy

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

var _ balancer.Policy = &pchannelCountFairPolicy{}

// pchannelCountFairPolicy is a policy to balance the load of log node by channel count.
// Make sure the channel count of each streaming node is equal or differ by 1.
type pchannelCountFairPolicy struct{}

func (p *pchannelCountFairPolicy) Name() string {
	return "pchannel_count_fair"
}

func (p *pchannelCountFairPolicy) Balance(currentLayout balancer.CurrentLayout) (expectedLayout balancer.ExpectedLayout, err error) {
	if currentLayout.TotalNodes() == 0 {
		return balancer.ExpectedLayout{}, errors.New("no available streaming node")
	}

	// Get the average and remaining channel count of all streaming node.
	avgChannelCount := currentLayout.TotalChannels() / currentLayout.TotalNodes()
	remainingChannelCount := currentLayout.TotalChannels() % currentLayout.TotalNodes()

	assignments := make(map[types.ChannelID]types.StreamingNodeInfo, currentLayout.TotalChannels())
	nodesChannelCount := make(map[int64]int, currentLayout.TotalNodes())
	needAssignChannel := currentLayout.IncomingChannels

	// keep the channel already on the node.
	for serverID, nodeInfo := range currentLayout.AllNodesInfo {
		nodesChannelCount[serverID] = 0
		for i, channelID := range currentLayout.AssignedChannels[serverID] {
			if i < avgChannelCount {
				assignments[channelID] = nodeInfo
				nodesChannelCount[serverID]++
			} else if i == avgChannelCount && remainingChannelCount > 0 {
				assignments[channelID] = nodeInfo
				nodesChannelCount[serverID]++
				remainingChannelCount--
			} else {
				needAssignChannel = append(needAssignChannel, channelID)
			}
		}
	}

	// assign the incoming node to the node with least channel count.
	for serverID, assignedChannelCount := range nodesChannelCount {
		assignCount := 0
		if assignedChannelCount < avgChannelCount {
			assignCount = avgChannelCount - assignedChannelCount
		} else if assignedChannelCount == avgChannelCount && remainingChannelCount > 0 {
			assignCount = 1
			remainingChannelCount--
		}
		for i := 0; i < assignCount; i++ {
			assignments[needAssignChannel[i]] = currentLayout.AllNodesInfo[serverID]
			nodesChannelCount[serverID]++
		}
		needAssignChannel = needAssignChannel[assignCount:]
	}

	return balancer.ExpectedLayout{
		ChannelAssignment: assignments,
	}, nil
}
