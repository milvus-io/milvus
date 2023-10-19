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
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
func (c *Cluster) Startup(ctx context.Context, nodes []*NodeInfo) error {
	for _, node := range nodes {
		c.sessionManager.AddSession(node)
	}
	currs := make([]int64, 0, len(nodes))
	for _, node := range nodes {
		currs = append(currs, node.NodeID)
	}
	return c.channelManager.Startup(ctx, currs)
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

// Watch tries to add a channel in datanode cluster
func (c *Cluster) Watch(ctx context.Context, ch string, collectionID UniqueID) error {
	return c.channelManager.Watch(ctx, &channel{Name: ch, CollectionID: collectionID})
}

// Flush sends flush requests to dataNodes specified
// which also according to channels where segments are assigned to.
func (c *Cluster) Flush(ctx context.Context, nodeID int64, channel string,
	segments []*datapb.SegmentInfo,
) error {
	if !c.channelManager.Match(nodeID, channel) {
		log.Warn("node is not matched with channel",
			zap.String("channel", channel),
			zap.Int64("nodeID", nodeID),
		)
		return fmt.Errorf("channel %s is not watched on node %d", channel, nodeID)
	}

	ch := c.channelManager.getChannelByNodeAndName(nodeID, channel)

	getSegmentID := func(segment *datapb.SegmentInfo, _ int) int64 {
		return segment.GetID()
	}

	req := &datapb.FlushSegmentsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Flush),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
			commonpbutil.WithTargetID(nodeID),
		),
		CollectionID: ch.CollectionID,
		SegmentIDs:   lo.Map(segments, getSegmentID),
		ChannelName:  channel,
	}

	c.sessionManager.Flush(ctx, nodeID, req)
	return nil
}

func (c *Cluster) FlushChannels(ctx context.Context, nodeID int64, flushTs Timestamp, channels []string) error {
	if len(channels) == 0 {
		return nil
	}

	for _, channel := range channels {
		if !c.channelManager.Match(nodeID, channel) {
			return fmt.Errorf("channel %s is not watched on node %d", channel, nodeID)
		}
	}

	req := &datapb.FlushChannelsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
			commonpbutil.WithTargetID(nodeID),
		),
		FlushTs:  flushTs,
		Channels: channels,
	}

	return c.sessionManager.FlushChannels(ctx, nodeID, req)
}

// Import sends import requests to DataNodes whose ID==nodeID.
func (c *Cluster) Import(ctx context.Context, nodeID int64, it *datapb.ImportTaskRequest) {
	c.sessionManager.Import(ctx, nodeID, it)
}

// GetSessions returns all sessions
func (c *Cluster) GetSessions() []*Session {
	return c.sessionManager.GetSessions()
}

// Close releases resources opened in Cluster
func (c *Cluster) Close() {
	c.sessionManager.Close()
	c.channelManager.Close()
}
