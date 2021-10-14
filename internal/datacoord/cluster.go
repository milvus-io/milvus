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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
)

// Cluster provides interfaces to interact with datanode cluster
type Cluster struct {
	sessionManager *SessionManager
	channelManager *ChannelManager
}

// NewCluster create new cluster
func NewCluster(sessionManager *SessionManager, channelManager *ChannelManager) *Cluster {
	c := &Cluster{
		sessionManager: sessionManager,
		channelManager: channelManager,
	}

	return c
}

// Startup init the cluster
func (c *Cluster) Startup(nodes []*NodeInfo) error {
	for _, node := range nodes {
		c.sessionManager.AddSession(node)
	}
	currs := make([]int64, 0, len(nodes))
	for _, node := range nodes {
		currs = append(currs, node.NodeID)
	}
	return c.channelManager.Startup(currs)
}

// Register registers a new node in cluster
func (c *Cluster) Register(node *NodeInfo) error {
	c.sessionManager.AddSession(node)
	return c.channelManager.AddNode(node.NodeID)
}

// UnRegister removes a node from cluster
func (c *Cluster) UnRegister(node *NodeInfo) error {
	c.sessionManager.DeleteSession(node)
	return c.channelManager.DeleteNode(node.NodeID)
}

// Watch try to add a channel in datanode cluster
func (c *Cluster) Watch(ch string, collectionID UniqueID) error {
	return c.channelManager.Watch(&channel{name: ch, collectionID: collectionID})
}

// Flush sends flush requests to corresponding datanodes according to channels that segments belong to
func (c *Cluster) Flush(ctx context.Context, segments []*datapb.SegmentInfo) {
	channels := c.channelManager.GetChannels()
	nodeSegments := make(map[int64][]int64)
	channelNodes := make(map[string]int64)
	// channel -> node
	for _, c := range channels {
		for _, ch := range c.Channels {
			channelNodes[ch.name] = c.NodeID
		}
	}
	// find node on which segment exists
	for _, segment := range segments {
		nodeID, ok := channelNodes[segment.GetInsertChannel()]
		if !ok {
			log.Warn("channel is not allocated to any node", zap.String("channel", segment.GetInsertChannel()))
			continue
		}
		nodeSegments[nodeID] = append(nodeSegments[nodeID], segment.GetID())
	}

	for nodeID, segments := range nodeSegments {
		req := &datapb.FlushSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Flush,
				SourceID: Params.NodeID,
			},
			SegmentIDs: segments,
		}
		c.sessionManager.Flush(ctx, nodeID, req)
	}
}

// GetSessions returns all sessions
func (c *Cluster) GetSessions() []*Session {
	return c.sessionManager.GetSessions()
}

// Close releases resources opened in Cluster
func (c *Cluster) Close() {
	c.sessionManager.Close()
}
