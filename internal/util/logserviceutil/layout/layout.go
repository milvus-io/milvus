package layout

import (
	"math"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/logcoord/server/channel"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// NewLayout create a new layout.
func NewLayout(Channels map[string]channel.PhysicalChannel, Nodes map[int64]*NodeStatus) *Layout {
	return &Layout{
		Version:  0,
		Nodes:    Nodes,
		Channels: Channels,
	}
}

// Relation is the relation between log node and channel.
type Relation struct {
	Channel  *logpb.PChannelInfo
	ServerID int64
}

// Layout is the full topology of log node and pChannel.
// Layout is not concurrent safe.
type Layout struct {
	Version  int64
	Nodes    map[int64]*NodeStatus              // serverID -> node
	Channels map[string]channel.PhysicalChannel // channelName -> Channel info
}

// Clone clone a layout.
func (layout *Layout) Clone() *Layout {
	cloned := &Layout{
		Version:  layout.Version,
		Nodes:    make(map[int64]*NodeStatus, len(layout.Nodes)),
		Channels: make(map[string]channel.PhysicalChannel, len(layout.Channels)),
	}
	for serverID, node := range layout.Nodes {
		cloned.Nodes[serverID] = node.Clone()
	}
	for name, channel := range layout.Channels {
		cloned.Channels[name] = channel
	}
	return cloned
}

// Clone clone the node status.
func (layout *Layout) CloneNodes() map[int64]*NodeStatus {
	nodes := make(map[int64]*NodeStatus, len(layout.Nodes))
	for serverID, node := range layout.Nodes {
		nodes[serverID] = node.Clone()
	}
	return nodes
}

// Cluster modification operations, called by cluster modification.
//
// AddChannel add a channel to layout.
func (layout *Layout) AddChannel(pChannel channel.PhysicalChannel) {
	if layout.Channels == nil {
		layout.Channels = make(map[string]channel.PhysicalChannel, 5)
	}
	layout.Channels[pChannel.Name()] = pChannel
}

// RemoveChannel remove a channel from layout.
func (layout *Layout) DeleteChannel(name string) {
	delete(layout.Channels, name)
}

// UpdateNodeStatus update node status in layout to latest.
func (layout *Layout) UpdateNodeStatus(statuses map[int64]*NodeStatus) {
	// Remove node not exist.
	for serverID := range layout.Nodes {
		if _, ok := statuses[serverID]; !ok {
			delete(layout.Nodes, serverID)
		}
	}

	// update node or add node.
	// TODO: distributed data race,
	// the time order of assign and remove rpc can be executed concurrently with update node status.
	for serverID, status := range statuses {
		if status.Error != nil {
			if errors.Is(status.Error, ErrFreeze) {
				// Node freeze, remove the node from layout.
				delete(layout.Nodes, serverID)
			}
			// skip error node.
			continue
		}
		// overwrite node status.
		if layout.Nodes == nil {
			layout.Nodes = make(map[int64]*NodeStatus, 5)
		}

		// Add new node
		if _, ok := layout.Nodes[serverID]; !ok {
			layout.Nodes[serverID] = status
		} else {
			// or update node.
			layout.Nodes[serverID].Update(status)
		}
	}
}

// Assignment operations, called by balancer.
//
// AssignChannel assign a channel to log node.
func (layout *Layout) AssignChannelToNode(serverID int64, channel *logpb.PChannelInfo) {
	if _, ok := layout.Nodes[serverID]; ok {
		if layout.Nodes[serverID].Channels == nil {
			layout.Nodes[serverID].Channels = make(map[string]*logpb.PChannelInfo, 5)
		}
		layout.Nodes[serverID].Channels[channel.Name] = channel
		layout.Version++
	}
}

// RemoveChannel remove a channel from log node.
func (layout *Layout) RemoveChannelFromNode(serverID int64, channel string) {
	if _, ok := layout.Nodes[serverID]; ok {
		delete(layout.Nodes[serverID].Channels, channel)
		layout.Version++
	}
}

// FindMinChannelCountNode find the log node with minimum channel count.
func (layout *Layout) FindMinChannelCountNode() (int64, int) {
	serverID := int64(-1)
	channelCount := math.MaxInt
	for _, node := range layout.Nodes {
		if len(node.Channels) < channelCount {
			serverID = node.ServerID
			channelCount = len(node.Channels)
		}
	}
	return serverID, channelCount
}

// FindMaxChannelCountNode find the log node with maximum channel count.
func (layout *Layout) FindMaxChannelCountNode() (int64, int) {
	serverID := int64(-1)
	channelCount := math.MinInt
	for _, node := range layout.Nodes {
		if len(node.Channels) > channelCount {
			serverID = node.ServerID
			channelCount = len(node.Channels)
		}
	}
	return serverID, channelCount
}

// FixInconsistency find the inconsistency in current layout.
// Inconsistency may be caused by:
// 1. log node failure.
// 2. log coord failure and recovering.
// 3. some rpc network failure.
// There is a consistency constraints:
// 1. a channel must in meta be assigned
// 2. a channel must be assigned on one and only one log node.
// It return the channel need to be assigned and extra expired relation need to be removed.
func (layout *Layout) FindInconsistency() ([]channel.PhysicalChannel, []Relation) {
	// nodes    map[int64]*NodeStatus    // serverID -> node
	// channels map[string]logpb.PChannelInfo // channelName -> Channel info

	// build channel to node map.
	channelNodeMap := make(map[string]map[int64]*logpb.PChannelInfo, len(layout.Channels))
	for serverID, nodeStatus := range layout.Nodes {
		for channel, channelInfo := range nodeStatus.Channels {
			if _, ok := channelNodeMap[channel]; !ok {
				channelNodeMap[channel] = make(map[int64]*logpb.PChannelInfo)
			}
			channelNodeMap[channel][serverID] = channelInfo
		}
	}

	// find all expired running channel
	expiredChannel := make([]Relation, 0)
	for pChannelName, nodeMap := range channelNodeMap {
		for serverID, pChannelInfoInNode := range nodeMap {
			// two cases need to expired a running channel:
			// 1. channel not found in meta (channel is deleted), or it's unused (no vchannel on it, or channel is removed from meta but PhysicalChannel object is not released).
			// 2. channel in meta's term is greater than channel in node (channel is reassigned to another node)
			// Should be removed from these log node.
			// TODO: we should check if channel is used, for vchannel info not sync to logservice.
			// So we couldn't make sure a pchannel is in used.
			if channelInfoInMeta, ok := layout.Channels[pChannelName]; !ok || channelInfoInMeta.IsDrop() || channelInfoInMeta.Term() > pChannelInfoInNode.Term {
				if ok {
					log.Info("channel term in meta is newer than log node", zap.Any("channel", pChannelInfoInNode), zap.Any("termInMeta", channelInfoInMeta.Term()), zap.Int64("serverID", serverID))
				} else {
					log.Info("channel not found in meta", zap.Any("channel", pChannelInfoInNode), zap.Int64("serverID", serverID))
				}
				expiredChannel = append(expiredChannel, Relation{
					Channel:  pChannelInfoInNode,
					ServerID: serverID,
				})
				// Temporarily remove channel from node map to find unassigned channel in next step.
				delete(nodeMap, serverID)
			}
		}
	}

	// find all unassigned channel
	unassignedChannel := make([]channel.PhysicalChannel, 0)
	for name, channel := range layout.Channels {
		// TODO: we should check if channel is used, for vchannel info not sync to logservice.
		// So we couldn't make sure a pchannel is in used.
		// skip unused channel.

		// check if channel is assigned.
		if nodeMap, ok := channelNodeMap[name]; !ok || len(nodeMap) == 0 {
			// channel not assigned
			unassignedChannel = append(unassignedChannel, channel)
		}
	}
	return unassignedChannel, expiredChannel
}
