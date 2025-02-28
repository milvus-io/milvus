package balance

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

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
		// The nodeID may not be in the nodeItems when multi replica mode.
		// Only one replica can be assigned to the node that wal is located.
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

// filterSQNIfStreamingServiceEnabled filter out the non-sqn querynode.
func filterSQNIfStreamingServiceEnabled(nodes []int64) []int64 {
	if streamingutil.IsStreamingServiceEnabled() {
		sqns := snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDs()
		expectedSQNs := make([]int64, 0, len(nodes))
		unexpectedNodes := make([]int64, 0)
		for _, node := range nodes {
			if sqns.Contain(node) {
				expectedSQNs = append(expectedSQNs, node)
			} else {
				unexpectedNodes = append(unexpectedNodes, node)
			}
		}
		if len(unexpectedNodes) > 0 {
			log.Warn("unexpected streaming querynode found when enable streaming service", zap.Int64s("unexpectedNodes", unexpectedNodes))
		}
		return expectedSQNs
	}
	return nodes
}
