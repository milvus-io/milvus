package balance

import (
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
)

func assignChannelToWALLocatedFirst(
	channels []*meta.DmChannel,
	nodeItems []*nodeItem,
) (notFoundChannels []*meta.DmChannel, plans []ChannelAssignPlan) {
	plans = make([]ChannelAssignPlan, 0)
	notFoundChannels = make([]*meta.DmChannel, 0)
	for _, c := range channels {
		nodeID := snmanager.StaticStreamingNodeManager.GetWALLocated(c.GetChannelName())
		// Check if nodeID is in the list of nodeItems
		found := false
		for _, item := range nodeItems {
			if item.nodeID == nodeID {
				item.AddCurrentScoreDelta(-1)
				plans = append(plans, ChannelAssignPlan{
					From:    -1,
					To:      item.nodeID,
					Channel: c,
				})
				found = true
				break
			}
		}
		if !found {
			notFoundChannels = append(notFoundChannels, c)
		}
	}
	return notFoundChannels, plans
}

func assignChannelToWALLocatedFirstForNodeInfo(
	channels []*meta.DmChannel,
	nodeItems []*session.NodeInfo,
) (notFoundChannels []*meta.DmChannel, plans []ChannelAssignPlan, scoreDelta map[int64]int) {
	plans = make([]ChannelAssignPlan, 0)
	notFoundChannels = make([]*meta.DmChannel, 0)
	scoreDelta = make(map[int64]int)
	for _, c := range channels {
		nodeID := snmanager.StaticStreamingNodeManager.GetWALLocated(c.GetChannelName())
		// Check if nodeID is in the list of nodeItems
		found := false
		for _, item := range nodeItems {
			if item.ID() == nodeID {
				plans = append(plans, ChannelAssignPlan{
					From:    -1,
					To:      item.ID(),
					Channel: c,
				})
				found = true
				scoreDelta[item.ID()] += 1
				break
			}
		}
		if !found {
			notFoundChannels = append(notFoundChannels, c)
		}
	}
	return notFoundChannels, plans, scoreDelta
}
