// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

// Cluster provides interfaces to interact with datanode cluster
type Cluster struct {
	sessionManager *SessionManager
	channelManager *ChannelManager
}

// NewCluster creates a new cluster
func NewCluster(sessionManager *SessionManager, channelManager *ChannelManager) *Cluster {
	c := &Cluster{
		sessionManager: sessionManager,
		channelManager: channelManager,
	}

	return c
}

// Startup inits the cluster with the given data nodes.
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
	err := c.channelManager.AddNode(node.NodeID)
	if err == nil {
		metrics.DataCoordNumDataNodes.WithLabelValues().Inc()
	}
	return err
}

// UnRegister removes a node from cluster
func (c *Cluster) UnRegister(node *NodeInfo) error {
	c.sessionManager.DeleteSession(node)
	err := c.channelManager.DeleteNode(node.NodeID)
	if err == nil {
		metrics.DataCoordNumDataNodes.WithLabelValues().Dec()
	}
	return c.channelManager.DeleteNode(node.NodeID)
}

// Watch tries to add a channel in datanode cluster
func (c *Cluster) Watch(ch string, collectionID UniqueID) error {
	return c.channelManager.Watch(&channel{Name: ch, CollectionID: collectionID})
}

// Flush sends flush requests to corresponding datanodes according to channels that segments belong to
func (c *Cluster) Flush(ctx context.Context, segments []*datapb.SegmentInfo, markSegments []*datapb.SegmentInfo) {
	channels := c.channelManager.GetChannels()
	nodeSegments := make(map[int64][]int64)
	nodeMarks := make(map[int64][]int64)
	channelNodes := make(map[string]int64)
	targetNodes := make(map[int64]struct{})
	// channel -> node
	for _, c := range channels {
		for _, ch := range c.Channels {
			channelNodes[ch.Name] = c.NodeID
		}
	}
	// collectionID shall be the same in single Flush call
	var collectionID int64
	// find node on which segment exists
	for _, segment := range segments {
		collectionID = segment.CollectionID
		nodeID, ok := channelNodes[segment.GetInsertChannel()]
		if !ok {
			log.Warn("channel is not allocated to any node", zap.String("channel", segment.GetInsertChannel()))
			continue
		}
		nodeSegments[nodeID] = append(nodeSegments[nodeID], segment.GetID())
		targetNodes[nodeID] = struct{}{}
	}
	for _, segment := range markSegments {
		collectionID = segment.CollectionID
		nodeID, ok := channelNodes[segment.GetInsertChannel()]
		if !ok {
			log.Warn("channel is not allocated to any node", zap.String("channel", segment.GetInsertChannel()))
			continue
		}
		nodeMarks[nodeID] = append(nodeMarks[nodeID], segment.GetID())
		targetNodes[nodeID] = struct{}{}
	}

	for nodeID := range targetNodes {
		segments := nodeSegments[nodeID]
		marks := nodeMarks[nodeID]
		if len(segments)+len(marks) == 0 { // no segment for this node
			continue
		}
		req := &datapb.FlushSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Flush,
				SourceID: Params.DataCoordCfg.NodeID,
			},
			CollectionID:   collectionID,
			SegmentIDs:     segments,
			MarkSegmentIDs: marks,
		}
		log.Info("Plan to flush", zap.Int64("node_id", nodeID), zap.Int64s("segments", segments), zap.Int64s("marks", marks))
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
