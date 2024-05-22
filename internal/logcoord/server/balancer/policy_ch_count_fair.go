package balancer

import "github.com/milvus-io/milvus/internal/util/logserviceutil/layout"

// NewChannelCountFairPolicy returns a policy to balance the load of log node by channel count.
func NewChannelCountFairPolicy() Policy {
	return &channelCountFairPolicy{}
}

// channelCountFairPolicy is a policy to balance the load of log node by channel count.
// Make sure the channel count of each log node is close to each other.
type channelCountFairPolicy struct{}

func (p *channelCountFairPolicy) Balance(b *BalanceOPBuilder) map[string][]BalanceOP {
	// Can not do any operation.
	if len(b.GetLayout().Nodes) == 0 {
		return nil
	}

	// Find the inconsistency in layout.
	needAssign, expired := b.GetLayout().FindInconsistency()

	// remove operation must be executed before assign operation.
	for _, r := range expired {
		b.AddRemoveChannelOP(r)
	}

	for _, channel := range needAssign {
		// select minium channel count log node to assign.
		serverID, _ := b.GetLayout().FindMinChannelCountNode()
		b.AddAssignChannelOP(serverID, channel)
	}

	// balance the final channel distribution.
	for {
		maxServerID, maxCount := b.GetLayout().FindMaxChannelCountNode()
		minServerID, minCount := b.GetLayout().FindMinChannelCountNode()
		if maxCount-minCount <= 1 || maxCount == 0 {
			break
		}
		// Assign a channel from the node of max to node of min
		moved := false
		for channelName, channel := range b.GetLayout().Nodes[maxServerID].Channels {
			// nothing to do if channel is already applied with operation.
			if b.OPExist(channelName) {
				continue
			}
			// Remove from old node and assign to new node.
			b.AddRemoveChannelOP(layout.Relation{
				Channel:  channel,
				ServerID: maxServerID,
			})
			pChannel := b.GetLayout().Channels[channelName]
			b.AddAssignChannelOP(minServerID, pChannel)
			moved = true
			break
		}
		if !moved {
			break
		}
	}
	return b.Build()
}
