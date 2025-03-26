package policy

import (
	"math"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	vchannelFairPolicyName = "vchannelFair"
)

var _ balancer.Policy = &vchannelFairPolicy{}

// vchannelFairPolicyBuilder is a builder to build vchannel fair policy.
type vchannelFairPolicyBuilder struct{}

// Name returns the name of the vchannel fair policy.
func (b *vchannelFairPolicyBuilder) Name() string {
	return vchannelFairPolicyName
}

// Build creates a new vchannel fair policy.
func (b *vchannelFairPolicyBuilder) Build() balancer.Policy {
	cfg := newVChannelFairPolicyConfig()
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	return &vchannelFairPolicy{
		cfg: cfg,
	}
}

// newVChannelFairPolicyConfig creates a new vchannel fair policy config.
func newVChannelFairPolicyConfig() vchannelFairPolicyConfig {
	params := paramtable.Get()
	return vchannelFairPolicyConfig{
		PChannelWeight:     params.StreamingCfg.WALBalancerPolicyVChannelFairPChannelWeight.GetAsFloat(),
		VChannelWeight:     params.StreamingCfg.WALBalancerPolicyVChannelFairVChannelWeight.GetAsFloat(),
		AntiAffinityWeight: params.StreamingCfg.WALBalancerPolicyVChannelFairAntiAffinityWeight.GetAsFloat(),
		RebalanceTolerance: params.StreamingCfg.WALBalancerPolicyVChannelFairRebalanceTolerance.GetAsFloat(),
		RebalanceMaxStep:   params.StreamingCfg.WALBalancerPolicyVChannelFairRebalanceMaxStep.GetAsInt(),
	}
}

type vchannelFairPolicyConfig struct {
	PChannelWeight     float64
	VChannelWeight     float64
	AntiAffinityWeight float64
	RebalanceTolerance float64
	RebalanceMaxStep   int
}

func (c vchannelFairPolicyConfig) Validate() error {
	if c.PChannelWeight < 0 || c.VChannelWeight < 0 || c.AntiAffinityWeight < 0 || c.RebalanceTolerance < 0 || c.RebalanceMaxStep < 0 {
		return errors.Errorf("invalid vchannel fair policy config, %+v", c)
	}
	return nil
}

// vchannelFairPolicy is a policy to balance the load of streaming node by vchannel count.
// It will try to make the vchannel count of each streaming node is closed to average as much as possible and
// the vchannel belong to same collection will be assigned to the different streaming node as much as possible.
type vchannelFairPolicy struct {
	log.Binder
	cfg vchannelFairPolicyConfig
}

func (p *vchannelFairPolicy) Name() string {
	return vchannelFairPolicyName
}

// Balance will balance the load of streaming node by vchannel count.
func (p *vchannelFairPolicy) Balance(currentLayout balancer.CurrentLayout) (balancer.ExpectedLayout, error) {
	if currentLayout.TotalNodes() == 0 {
		return balancer.ExpectedLayout{}, errors.New("no available streaming node")
	}

	p.updatePolicyConfiguration()
	expectedLayout := newExpectedLayoutForVChannelFairPolicy(currentLayout, p.cfg)

	// 1. Keep the current layout first to make the balance result more stable.
	newIncomingChannel := make(map[types.ChannelID]struct{}, len(currentLayout.Channels))
	for channelID := range currentLayout.Channels {
		if serverID, ok := currentLayout.ChannelsToNodes[channelID]; ok {
			expectedLayout.Assign(channelID, serverID)
			continue
		}
		newIncomingChannel[channelID] = struct{}{}
	}
	serverIDs := lo.Keys(currentLayout.AllNodesInfo)
	sort.Slice(serverIDs, func(i, j int) bool {
		return serverIDs[i] < serverIDs[j]
	})

	// 2. assign the new incoming channels at current layout based on lowest unbalance score.
	allChannelIDSortedByVChannels := currentLayout.GetAllPChannelsSortedByVChannelCountDesc()
	for _, channelID := range allChannelIDSortedByVChannels {
		// assign to the node that can achieve lowest cost.
		if _, ok := newIncomingChannel[channelID]; ok {
			var targetNodeID int64
			minScore := math.MaxFloat64
			for _, nodeID := range serverIDs {
				score := expectedLayout.TryAssignGlobalUnbalancedScore(channelID, nodeID)
				if score < minScore {
					minScore = score
					targetNodeID = nodeID
				}
			}
			if targetNodeID == 0 {
				panic("target node should never be zero")
			}
			expectedLayout.Assign(channelID, targetNodeID)
		}
	}

	// 3. Unassign some unbalanced channel to reassign it, try to make the layout more balanced
	snapshot := expectedLayout.AssignmentSnapshot()
	reassignChannelIDs := make([]types.ChannelID, 0, p.cfg.RebalanceMaxStep)
	for i := 0; i < p.cfg.RebalanceMaxStep; i++ {
		channelID := expectedLayout.FindTheLeastScoreIncreasingChannel()
		expectedLayout.Unassign(channelID)
		reassignChannelIDs = append(reassignChannelIDs, channelID)
	}

	greatestSnapshot := snapshot
	p.assignChannels(expectedLayout, reassignChannelIDs, &greatestSnapshot)
	if greatestSnapshot.GlobalUnbalancedScore < snapshot.GlobalUnbalancedScore-p.cfg.RebalanceTolerance {
		return balancer.ExpectedLayout{
			ChannelAssignment: greatestSnapshot.Assignments,
		}, nil
	}
	return balancer.ExpectedLayout{
		ChannelAssignment: snapshot.Assignments,
	}, nil
}

// updatePolicyConfiguration will update the policy configuration.
func (p *vchannelFairPolicy) updatePolicyConfiguration() {
	// try to fetch latest configuration.
	newCfg := newVChannelFairPolicyConfig()
	if err := newCfg.Validate(); err != nil {
		p.Logger().Warn("invalid new incoming vchannel fair policy config", zap.Any("new", newCfg))
	} else if p.cfg != newCfg {
		p.Logger().Info("vchannel fair policy config updated", zap.Any("old", p.cfg), zap.Any("new", newCfg))
		p.cfg = newCfg
	}
}

// assignChannels will assign the channels to the nodes.
func (p *vchannelFairPolicy) assignChannels(expectedLayout *expectedLayoutForVChannelFairPolicy, channelIDs []types.ChannelID, greatestSnapshot *AssignmentSnapshot) {
	if len(channelIDs) == 0 {
		if expectedLayout.GlobalUnbalancedScore < greatestSnapshot.GlobalUnbalancedScore {
			snapshot := expectedLayout.AssignmentSnapshot()
			greatestSnapshot.Assignments = snapshot.Assignments
			greatestSnapshot.GlobalUnbalancedScore = snapshot.GlobalUnbalancedScore
		}
		return
	}
	for nodeID := range expectedLayout.CurrentLayout.AllNodesInfo {
		channelID := channelIDs[0]
		expectedLayout.Assign(channelID, nodeID)
		p.assignChannels(expectedLayout, channelIDs[1:], greatestSnapshot)
		expectedLayout.Unassign(channelID)
	}
}

// newExpectedLayoutForVChannelFairPolicy creates a new expected layout for vchannel fair policy.
func newExpectedLayoutForVChannelFairPolicy(currentLayout balancer.CurrentLayout, cfg vchannelFairPolicyConfig) *expectedLayoutForVChannelFairPolicy {
	totalVChannel := currentLayout.TotalVChannels()
	// perfect average pchannel count per node.
	averagePChannelPerNode := float64(currentLayout.TotalChannels()) / float64(currentLayout.TotalNodes())
	// perfect average vchannel count per node.
	averageVChannelPerNode := float64(totalVChannel) / float64(currentLayout.TotalNodes())
	// perfect average vchannel count per node for each collection.
	averageVChannelOfCollectionPerNode := make(map[int64]float64)
	totalVChannelOfCollection := currentLayout.TotalVChannelsOfCollection()
	for collectionID, VChannelCount := range totalVChannelOfCollection {
		averageVChannelOfCollectionPerNode[collectionID] = float64(VChannelCount) / float64(currentLayout.TotalNodes())
	}

	// Create the node info for all
	nodes := make(map[int64]*streamingNodeInfoForVChannelFairPolicy)
	for nodeID := range currentLayout.AllNodesInfo {
		nodes[nodeID] = &streamingNodeInfoForVChannelFairPolicy{
			AssignedChannels: make(map[types.ChannelID]struct{}),
		}
	}
	layout := &expectedLayoutForVChannelFairPolicy{
		Config:                             cfg,
		PChannelAffinity:                   newPChannelAffinity(currentLayout.Channels),
		CurrentLayout:                      currentLayout,
		AveragePChannelPerNode:             averagePChannelPerNode,
		AverageVChannelPerNode:             averageVChannelPerNode,
		AverageVChannelOfCollectionPerNode: averageVChannelOfCollectionPerNode,
		Assignments:                        make(map[types.ChannelID]types.StreamingNodeInfo),
		Nodes:                              nodes,
	}
	return layout
}

type AssignmentSnapshot struct {
	Assignments           map[types.ChannelID]types.StreamingNodeInfo
	GlobalUnbalancedScore float64
}

// expectedLayoutForVChannelFairPolicy is the expected layout of streaming node and pChannel.
type expectedLayoutForVChannelFairPolicy struct {
	Config                             vchannelFairPolicyConfig
	PChannelAffinity                   *pchannelAffinity
	CurrentLayout                      balancer.CurrentLayout
	GlobalUnbalancedScore              float64 // the number that indicates how unbalanced the layout is, better if lower
	AveragePChannelPerNode             float64
	AverageVChannelPerNode             float64
	AverageVChannelOfCollectionPerNode map[int64]float64
	Assignments                        map[types.ChannelID]types.StreamingNodeInfo
	Nodes                              map[int64]*streamingNodeInfoForVChannelFairPolicy
}

// AssignmentSnapshot will return the assignment snapshot.
func (p *expectedLayoutForVChannelFairPolicy) AssignmentSnapshot() AssignmentSnapshot {
	assignments := make(map[types.ChannelID]types.StreamingNodeInfo)
	for channelID, node := range p.Assignments {
		assignments[channelID] = node
	}
	return AssignmentSnapshot{
		Assignments:           assignments,
		GlobalUnbalancedScore: p.GlobalUnbalancedScore,
	}
}

// currentCost will calculate the cost of the channel on the node.
func (p *expectedLayoutForVChannelFairPolicy) currentCost(nodeInfo *streamingNodeInfoForVChannelFairPolicy) float64 {
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
			cost += p.Config.AntiAffinityWeight * (1 - p.PChannelAffinity.GetAffinity(assigned[i], assigned[j]))
		}
	}
	return cost
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
	newUnbalancedScore := p.currentCost(p.Nodes[node.ServerID])
	diff := newUnbalancedScore - p.Nodes[node.ServerID].UnbalancedScore
	p.GlobalUnbalancedScore += diff
	p.Nodes[node.ServerID].UnbalancedScore = newUnbalancedScore
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
	newUnbalancedScore := p.currentCost(p.Nodes[node.ServerID])
	diff := newUnbalancedScore - p.Nodes[node.ServerID].UnbalancedScore
	p.GlobalUnbalancedScore += diff
	p.Nodes[node.ServerID].UnbalancedScore = newUnbalancedScore
}

// FindTheLeastScoreIncreasingChannel will find the channel that can increase the least score.
func (p *expectedLayoutForVChannelFairPolicy) FindTheLeastScoreIncreasingChannel() types.ChannelID {
	var targetChannelID types.ChannelID
	minScore := math.MaxFloat64
	channelIDs := lo.Keys(p.Assignments)
	for _, channelID := range channelIDs {
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

type streamingNodeInfoForVChannelFairPolicy struct {
	AssignedVChannelCount int
	UnbalancedScore       float64 // the number that indicates how unbalanced for current node.
	AssignedChannels      map[types.ChannelID]struct{}
}
