package vchannelfair

import (
	"math"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

var _ balancer.Policy = &policy{}

// policy is a policy to balance the load of streaming node by vchannel count.
// It will try to make the vchannel count of each streaming node is closed to average as much as possible and
// the vchannel belong to same collection will be assigned to the different streaming node as much as possible.
type policy struct {
	log.Binder
	cfg policyConfig
}

// Name returns the name of the policy.
func (p *policy) Name() string {
	return policyName
}

// Balance will balance the load of streaming node by vchannel count.
func (p *policy) Balance(currentLayout balancer.CurrentLayout) (balancer.ExpectedLayout, error) {
	if currentLayout.TotalNodes() == 0 {
		return balancer.ExpectedLayout{}, errors.New("no available streaming node")
	}

	// update policy configuration before balancing.
	p.updatePolicyConfiguration()

	// Create new expected layout to help balance the load.
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

	// 2. assign the new incoming channels at current layout based on lowest unbalance score.
	allChannelIDSortedByVChannels := currentLayout.GetAllPChannelsSortedByVChannelCountDesc()
	for _, channelID := range allChannelIDSortedByVChannels {
		// assign to the node that can achieve lowest cost.
		if _, ok := newIncomingChannel[channelID]; ok {
			var targetNodeID int64
			minScore := math.MaxFloat64
			for _, nodeID := range serverIDs {
				score := expectedLayout.TryAssignGlobalUnbalancedScore(channelID, nodeID)
				if score < minScore || (score == minScore && nodeID < targetNodeID) {
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

	// 3. Unassign some most unbalanced channel and reassign it, try to make the layout more balanced
	snapshot := expectedLayout.AssignmentSnapshot()
	reassignChannelIDs := make([]types.ChannelID, 0, p.cfg.RebalanceMaxStep)
	for i := 0; i < p.cfg.RebalanceMaxStep; i++ {
		channelID := expectedLayout.FindTheLeastUnbalanceScoreIncrementChannel()
		expectedLayout.Unassign(channelID)
		reassignChannelIDs = append(reassignChannelIDs, channelID)
	}

	// 4. Do a DFS to make a greatest snapshot.
	// The DFS will find the unbalance score minimized assignment based on current layout.
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
func (p *policy) updatePolicyConfiguration() {
	// try to fetch latest configuration.
	newCfg := newVChannelFairPolicyConfig()
	if err := newCfg.Validate(); err != nil {
		p.Logger().Warn("invalid new incoming vchannel fair policy config", zap.Any("new", newCfg))
	} else if p.cfg != newCfg {
		p.Logger().Info("vchannel fair policy config updated", zap.Any("old", p.cfg), zap.Any("new", newCfg))
		p.cfg = newCfg
	}
}

// assignChannels will do a recursive search, try to assign the channels to all nodes.
func (p *policy) assignChannels(expectedLayout *expectedLayoutForVChannelFairPolicy, channelIDs []types.ChannelID, greatestSnapshot *assignmentSnapshot) {
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
