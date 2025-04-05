package vchannelfair

import (
	"math"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// newExpectedLayoutForVChannelFairPolicy creates a new expected layout for vchannel fair policy.
func newExpectedLayoutForVChannelFairPolicy(currentLayout balancer.CurrentLayout, cfg policyConfig) *expectedLayoutForVChannelFairPolicy {
	totalVChannel := currentLayout.TotalVChannels()
	// perfect average pchannel count per node.
	averagePChannelPerNode := float64(currentLayout.TotalChannels()) / float64(currentLayout.TotalNodes())
	// perfect average vchannel count per node.
	averageVChannelPerNode := float64(totalVChannel) / float64(currentLayout.TotalNodes())
	// current affinity of pchannel.
	affinity := newPChannelAffinity(currentLayout.Channels)

	// Create the node info for all
	nodes := make(map[int64]*streamingNodeInfo)
	for nodeID := range currentLayout.AllNodesInfo {
		nodes[nodeID] = &streamingNodeInfo{
			AssignedChannels: make(map[types.ChannelID]struct{}),
		}
	}
	layout := &expectedLayoutForVChannelFairPolicy{
		Config:                 cfg,
		PChannelAffinity:       affinity,
		CurrentLayout:          currentLayout,
		AveragePChannelPerNode: averagePChannelPerNode,
		AverageVChannelPerNode: averageVChannelPerNode,
		Assignments:            make(map[types.ChannelID]types.StreamingNodeInfo),
		Nodes:                  nodes,
	}
	layout.updateAllScore()
	return layout
}

// assignmentSnapshot is the assignment snapshot of the expected layout.
type assignmentSnapshot struct {
	Assignments           map[types.ChannelID]types.StreamingNodeInfo
	GlobalUnbalancedScore float64
}

// streamingNodeInfo is the streaming node info for vchannel fair policy.
type streamingNodeInfo struct {
	AssignedVChannelCount int
	UnbalancedScore       float64 // the number that indicates how unbalanced for current node.
	AssignedChannels      map[types.ChannelID]struct{}
}

// expectedLayoutForVChannelFairPolicy is the expected layout of streaming node and pChannel.
type expectedLayoutForVChannelFairPolicy struct {
	Config                 policyConfig
	CurrentLayout          balancer.CurrentLayout
	AveragePChannelPerNode float64
	AverageVChannelPerNode float64
	PChannelAffinity       *pchannelAffinity
	GlobalUnbalancedScore  float64                                     // the sum of unbalance score of all streamingnode, indicates how unbalanced the layout is, better if lower.
	Assignments            map[types.ChannelID]types.StreamingNodeInfo // current assignment of pchannel to streamingnode.
	Nodes                  map[int64]*streamingNodeInfo
}

// AssignmentSnapshot will return the assignment snapshot.
func (p *expectedLayoutForVChannelFairPolicy) AssignmentSnapshot() assignmentSnapshot {
	assignments := make(map[types.ChannelID]types.StreamingNodeInfo)
	for channelID, node := range p.Assignments {
		assignments[channelID] = node
	}
	return assignmentSnapshot{
		Assignments:           assignments,
		GlobalUnbalancedScore: p.GlobalUnbalancedScore,
	}
}

// TryAssignGlobalUnbalancedScore will try to assign the channel to the node and return the global unbalanced score.
func (p *expectedLayoutForVChannelFairPolicy) TryAssignGlobalUnbalancedScore(channelID types.ChannelID, serverID int64) float64 {
	p.Assign(channelID, serverID)
	score := p.GlobalUnbalancedScore
	p.Unassign(channelID)
	return score
}

// Assign will assign the channel to the node.
func (p *expectedLayoutForVChannelFairPolicy) Assign(channelID types.ChannelID, serverID int64) {
	if _, ok := p.Assignments[channelID]; ok {
		panic("channel already assigned")
	}
	stats, ok := p.CurrentLayout.Channels[channelID]
	if !ok {
		panic("stats not found")
	}
	node, ok := p.CurrentLayout.AllNodesInfo[serverID]
	if !ok {
		panic("node info not found")
	}

	// assign to the node that already has pchannel at highest priority.
	p.Assignments[channelID] = node
	p.Nodes[node.ServerID].AssignedChannels[channelID] = struct{}{}
	p.Nodes[node.ServerID].AssignedVChannelCount += len(stats.VChannels)
	p.updateNodeScore(node.ServerID)
}

// Unassign will unassign the channel from the node.
func (p *expectedLayoutForVChannelFairPolicy) Unassign(channelID types.ChannelID) {
	node, ok := p.Assignments[channelID]
	if !ok {
		panic("channel is not assigned")
	}
	delete(p.Assignments, channelID)
	delete(p.Nodes[node.ServerID].AssignedChannels, channelID)
	p.Nodes[node.ServerID].AssignedVChannelCount -= len(p.CurrentLayout.Channels[channelID].VChannels)
	p.updateNodeScore(node.ServerID)
}

// updateNodeScore will update the score for the node.
func (p *expectedLayoutForVChannelFairPolicy) updateNodeScore(serverID int64) {
	newUnbalancedScore := p.currentCost(p.Nodes[serverID])
	diff := newUnbalancedScore - p.Nodes[serverID].UnbalancedScore
	p.GlobalUnbalancedScore += diff
	p.Nodes[serverID].UnbalancedScore = newUnbalancedScore
}

// updateAllScore will update the score for all nodes and the global unbalanced score.
func (p *expectedLayoutForVChannelFairPolicy) updateAllScore() {
	// update the global unbalanced score.
	p.GlobalUnbalancedScore = 0
	for _, node := range p.Nodes {
		node.UnbalancedScore = p.currentCost(node)
		p.GlobalUnbalancedScore += node.UnbalancedScore
	}
}

// currentCost will calculate the cost of the channel on the node.
func (p *expectedLayoutForVChannelFairPolicy) currentCost(nodeInfo *streamingNodeInfo) float64 {
	cost := float64(0.0)
	if p.AveragePChannelPerNode != 0 {
		pDiff := (float64(len(nodeInfo.AssignedChannels)) - p.AveragePChannelPerNode) / p.AveragePChannelPerNode
		cost += p.Config.PChannelWeight * (pDiff * pDiff)
	}
	if p.AverageVChannelPerNode != 0 {
		vDiff := (float64(nodeInfo.AssignedVChannelCount) - p.AverageVChannelPerNode) / p.AverageVChannelPerNode
		cost += p.Config.VChannelWeight * (vDiff * vDiff)
	}
	assigned := lo.Keys(nodeInfo.AssignedChannels)
	for i := 0; i < len(assigned); i++ {
		for j := i + 1; j < len(assigned); j++ {
			cost += p.Config.AntiAffinityWeight * (1 - p.PChannelAffinity.MustGetAffinity(assigned[i], assigned[j]))
		}
	}
	return cost
}

// FindTheLeastUnbalanceScoreIncrementChannel will find the channel that increases the least score.
func (p *expectedLayoutForVChannelFairPolicy) FindTheLeastUnbalanceScoreIncrementChannel() types.ChannelID {
	var targetChannelID types.ChannelID
	minScore := math.MaxFloat64
	for channelID := range p.Assignments {
		serverID := p.Assignments[channelID].ServerID
		p.Unassign(channelID)
		currentScore := p.GlobalUnbalancedScore
		if currentScore < minScore {
			minScore = currentScore
			targetChannelID = channelID
		}
		p.Assign(channelID, serverID)
	}
	return targetChannelID
}
