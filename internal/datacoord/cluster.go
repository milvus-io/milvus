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
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// Cluster provides interfaces to interact with datanode cluster
//
//go:generate mockery --name=Cluster --structname=MockCluster --output=./  --filename=mock_cluster.go --with-expecter --inpackage
type Cluster interface {
	Startup(ctx context.Context, nodes []*session.NodeInfo) error
	Register(node *session.NodeInfo) error
	UnRegister(node *session.NodeInfo) error
	Watch(ctx context.Context, ch RWChannel) error
	Flush(ctx context.Context, nodeID int64, channel string, segments []*datapb.SegmentInfo) error
	FlushChannels(ctx context.Context, nodeID int64, flushTs Timestamp, channels []string) error
	PreImport(nodeID int64, in *datapb.PreImportRequest) error
	ImportV2(nodeID int64, in *datapb.ImportRequest) error
	QueryPreImport(nodeID int64, in *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error)
	QueryImport(nodeID int64, in *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error)
	DropImport(nodeID int64, in *datapb.DropImportRequest) error
	QuerySlots() map[int64]int64
	GetSessions() []*session.Session
	Close()
}

var _ Cluster = (*ClusterImpl)(nil)

type ClusterImpl struct {
	sessionManager session.DataNodeManager
	channelManager ChannelManager
}

// NewClusterImpl creates a new cluster
func NewClusterImpl(sessionManager session.DataNodeManager, channelManager ChannelManager) *ClusterImpl {
	c := &ClusterImpl{
		sessionManager: sessionManager,
		channelManager: channelManager,
	}

	return c
}

// Startup inits the cluster with the given data nodes.
func (c *ClusterImpl) Startup(ctx context.Context, nodes []*session.NodeInfo) error {
	for _, node := range nodes {
		c.sessionManager.AddSession(node)
	}

	var (
		legacyNodes []int64
		allNodes    []int64
	)

	lo.ForEach(nodes, func(info *session.NodeInfo, _ int) {
		if info.IsLegacy {
			legacyNodes = append(legacyNodes, info.NodeID)
		}
		allNodes = append(allNodes, info.NodeID)
	})
	return c.channelManager.Startup(ctx, legacyNodes, allNodes)
}

// Register registers a new node in cluster
func (c *ClusterImpl) Register(node *session.NodeInfo) error {
	c.sessionManager.AddSession(node)
	return c.channelManager.AddNode(node.NodeID)
}

// UnRegister removes a node from cluster
func (c *ClusterImpl) UnRegister(node *session.NodeInfo) error {
	c.sessionManager.DeleteSession(node)
	return c.channelManager.DeleteNode(node.NodeID)
}

// Watch tries to add a channel in datanode cluster
func (c *ClusterImpl) Watch(ctx context.Context, ch RWChannel) error {
	return c.channelManager.Watch(ctx, ch)
}

// Flush sends async FlushSegments requests to dataNodes
// which also according to channels where segments are assigned to.
func (c *ClusterImpl) Flush(ctx context.Context, nodeID int64, channel string, segments []*datapb.SegmentInfo) error {
	ch, founded := c.channelManager.GetChannel(nodeID, channel)
	if !founded {
		log.Warn("node is not matched with channel",
			zap.String("channel", channel),
			zap.Int64("nodeID", nodeID),
		)
		return fmt.Errorf("channel %s is not watched on node %d", channel, nodeID)
	}

	getSegmentID := func(segment *datapb.SegmentInfo, _ int) int64 {
		return segment.GetID()
	}

	req := &datapb.FlushSegmentsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Flush),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
			commonpbutil.WithTargetID(nodeID),
		),
		CollectionID: ch.GetCollectionID(),
		SegmentIDs:   lo.Map(segments, getSegmentID),
		ChannelName:  channel,
	}

	c.sessionManager.Flush(ctx, nodeID, req)
	return nil
}

func (c *ClusterImpl) FlushChannels(ctx context.Context, nodeID int64, flushTs Timestamp, channels []string) error {
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

func (c *ClusterImpl) PreImport(nodeID int64, in *datapb.PreImportRequest) error {
	return c.sessionManager.PreImport(nodeID, in)
}

func (c *ClusterImpl) ImportV2(nodeID int64, in *datapb.ImportRequest) error {
	return c.sessionManager.ImportV2(nodeID, in)
}

func (c *ClusterImpl) QueryPreImport(nodeID int64, in *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error) {
	return c.sessionManager.QueryPreImport(nodeID, in)
}

func (c *ClusterImpl) QueryImport(nodeID int64, in *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error) {
	return c.sessionManager.QueryImport(nodeID, in)
}

func (c *ClusterImpl) DropImport(nodeID int64, in *datapb.DropImportRequest) error {
	return c.sessionManager.DropImport(nodeID, in)
}

func (c *ClusterImpl) QuerySlots() map[int64]int64 {
	nodeIDs := c.sessionManager.GetSessionIDs()
	nodeSlots := make(map[int64]int64)
	mu := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	for _, nodeID := range nodeIDs {
		wg.Add(1)
		go func(nodeID int64) {
			defer wg.Done()
			resp, err := c.sessionManager.QuerySlot(nodeID)
			if err != nil {
				log.Ctx(context.TODO()).Warn("query slot failed", zap.Int64("nodeID", nodeID), zap.Error(err))
				return
			}
			mu.Lock()
			defer mu.Unlock()
			nodeSlots[nodeID] = resp.GetNumSlots()
		}(nodeID)
	}
	wg.Wait()
	log.Ctx(context.TODO()).Debug("query slot done", zap.Any("nodeSlots", nodeSlots))
	return nodeSlots
}

// GetSessions returns all sessions
func (c *ClusterImpl) GetSessions() []*session.Session {
	return c.sessionManager.GetSessions()
}

// Close releases resources opened in Cluster
func (c *ClusterImpl) Close() {
	c.sessionManager.Close()
	c.channelManager.Close()
}
