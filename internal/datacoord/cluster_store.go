// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datacoord

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
)

// ClusterStore is the interface stores DataNodes information
type ClusterStore interface {
	GetNodes() []*NodeInfo
	SetNode(nodeID UniqueID, node *NodeInfo)
	DeleteNode(nodeID UniqueID)
	GetNode(nodeID UniqueID) *NodeInfo
	SetClient(nodeID UniqueID, client types.DataNode)
	SetWatched(nodeID UniqueID, channelsName []string)
}

// NodeInfo wrapper struct for storing DataNode information
// and related controlling struct
type NodeInfo struct {
	Info    *datapb.DataNodeInfo
	eventCh chan *NodeEvent
	client  types.DataNode
	ctx     context.Context
	cancel  context.CancelFunc
}

// eventChBuffer magic number for channel buffer size in NodeInfo
const eventChBuffer = 1024

// NodeEvent data node event struct
type NodeEvent struct {
	Type NodeEventType
	Req  interface{}
}

// NewNodeInfo helper function to create a NodeInfo from provided datapb.DataNodeInfo
func NewNodeInfo(ctx context.Context, info *datapb.DataNodeInfo) *NodeInfo {
	ctx, cancel := context.WithCancel(ctx)
	return &NodeInfo{
		Info:    info,
		eventCh: make(chan *NodeEvent, eventChBuffer),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// ShadowClone shadow clones a NodeInfo
// note that info, eventCh, etc is not created again
func (n *NodeInfo) ShadowClone(opts ...NodeOpt) *NodeInfo {
	cloned := &NodeInfo{
		Info:    n.Info,
		eventCh: n.eventCh,
		client:  n.client,
		ctx:     n.ctx,
		cancel:  n.cancel,
	}
	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

// Clone "deep" clone a NodeInfo
// note that ONLY `info` field is deep copied
// parameter opts is applied in sequence to clone NodeInfo
func (n *NodeInfo) Clone(opts ...NodeOpt) *NodeInfo {
	info := proto.Clone(n.Info).(*datapb.DataNodeInfo)
	cloned := &NodeInfo{
		Info:    info,
		eventCh: n.eventCh,
		client:  n.client,
		ctx:     n.ctx,
		cancel:  n.cancel,
	}
	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

// GetEventChannel returns event channel
func (n *NodeInfo) GetEventChannel() chan *NodeEvent {
	return n.eventCh
}

// GetClient returns client
func (n *NodeInfo) GetClient() types.DataNode {
	return n.client
}

// Dispose stops the data node client and calls cancel
func (n *NodeInfo) Dispose() {
	defer n.cancel()
	if n.client != nil {
		n.client.Stop()
	}
}

// make sure NodesInfo implements ClusterStore
var _ ClusterStore = (*NodesInfo)(nil)

// NodesInfo wraps a map UniqueID -> NodeInfo
// implements ClusterStore interface
// not lock related field is required so all operations shall be protected outside
type NodesInfo struct {
	nodes map[UniqueID]*NodeInfo
}

// NewNodesInfo helper function creates a NodesInfo
func NewNodesInfo() *NodesInfo {
	c := &NodesInfo{
		nodes: make(map[UniqueID]*NodeInfo),
	}
	return c
}

// GetNodes returns nodes list in NodesInfo
func (c *NodesInfo) GetNodes() []*NodeInfo {
	nodes := make([]*NodeInfo, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// SetNode sets a NodeInfo with provided UniqueID
func (c *NodesInfo) SetNode(nodeID UniqueID, node *NodeInfo) {
	c.nodes[nodeID] = node
	metrics.DataCoordDataNodeList.WithLabelValues("online").Inc()
	metrics.DataCoordDataNodeList.WithLabelValues("offline").Dec()
}

// DeleteNode deletes a NodeInfo with provided UniqueID
func (c *NodesInfo) DeleteNode(nodeID UniqueID) {
	delete(c.nodes, nodeID)
	metrics.DataCoordDataNodeList.WithLabelValues("online").Dec()
	metrics.DataCoordDataNodeList.WithLabelValues("offline").Inc()
}

// GetNode get NodeInfo binding to the specified UniqueID
// returns nil if no Info is found
func (c *NodesInfo) GetNode(nodeID UniqueID) *NodeInfo {
	node, ok := c.nodes[nodeID]
	if !ok {
		return nil
	}
	return node
}

// SetClient set DataNode client to specified UniqueID
// do nothing if no Info is found
func (c *NodesInfo) SetClient(nodeID UniqueID, client types.DataNode) {
	if node, ok := c.nodes[nodeID]; ok {
		c.nodes[nodeID] = node.ShadowClone(SetClient(client))
	}
}

// SetWatched set specified channels watch state from Uncomplete to Complete
// do nothing if no Info is found
func (c *NodesInfo) SetWatched(nodeID UniqueID, channelsName []string) {
	if node, ok := c.nodes[nodeID]; ok {
		c.nodes[nodeID] = node.Clone(SetWatched(channelsName))
	}
}

// NodeOpt helper functions updating NodeInfo properties
type NodeOpt func(n *NodeInfo)

// SetWatched returns a NodeOpt updating specified channels watch states from Uncomplete to Complete
func SetWatched(channelsName []string) NodeOpt {
	return func(n *NodeInfo) {
		channelsMap := make(map[string]struct{})
		for _, channelName := range channelsName {
			channelsMap[channelName] = struct{}{}
		}
		for _, ch := range n.Info.Channels {
			_, ok := channelsMap[ch.GetName()]
			if !ok {
				continue
			}
			if ch.State == datapb.ChannelWatchState_Uncomplete {
				ch.State = datapb.ChannelWatchState_Complete
			}
		}
	}
}

// SetClient returns NodeOpt update DataNode client
func SetClient(client types.DataNode) NodeOpt {
	return func(n *NodeInfo) {
		n.client = client
	}
}

// AddChannels returns NodeOpt adding specified channels to assigned list
func AddChannels(channels []*datapb.ChannelStatus) NodeOpt {
	return func(n *NodeInfo) {
		n.Info.Channels = append(n.Info.Channels, channels...)
	}
}

// SetChannels returns NodeOpt updating assigned channels
func SetChannels(channels []*datapb.ChannelStatus) NodeOpt {
	return func(n *NodeInfo) {
		n.Info.Channels = channels
	}
}
