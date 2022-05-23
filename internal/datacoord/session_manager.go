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
	"time"

	grpcdatanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"go.uber.org/zap"
)

const (
	flushTimeout = 5 * time.Second
	// TODO: evaluate and update import timeout.
	importTimeout    = 3 * time.Hour
	reCollectTimeout = 5 * time.Second
)

// SessionManager provides the grpc interfaces of cluster
type SessionManager struct {
	sessions struct {
		sync.RWMutex
		data map[int64]*Session
	}
	sessionCreator dataNodeCreatorFunc
}

// SessionOpt provides a way to set params in SessionManager
type SessionOpt func(c *SessionManager)

func withSessionCreator(creator dataNodeCreatorFunc) SessionOpt {
	return func(c *SessionManager) { c.sessionCreator = creator }
}

func defaultSessionCreator() dataNodeCreatorFunc {
	return func(ctx context.Context, addr string) (types.DataNode, error) {
		return grpcdatanodeclient.NewClient(ctx, addr)
	}
}

// NewSessionManager creates a new SessionManager
func NewSessionManager(options ...SessionOpt) *SessionManager {
	m := &SessionManager{
		sessions: struct {
			sync.RWMutex
			data map[int64]*Session
		}{data: make(map[int64]*Session)},
		sessionCreator: defaultSessionCreator(),
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}

// AddSession creates a new session
func (c *SessionManager) AddSession(node *NodeInfo) {
	c.sessions.Lock()
	defer c.sessions.Unlock()

	session := NewSession(node, c.sessionCreator)
	c.sessions.data[node.NodeID] = session
}

// DeleteSession removes the node session
func (c *SessionManager) DeleteSession(node *NodeInfo) {
	c.sessions.Lock()
	defer c.sessions.Unlock()

	if session, ok := c.sessions.data[node.NodeID]; ok {
		session.Dispose()
		delete(c.sessions.data, node.NodeID)
	}
}

// GetSessions gets all node sessions
func (c *SessionManager) GetSessions() []*Session {
	c.sessions.RLock()
	defer c.sessions.RUnlock()

	ret := make([]*Session, 0, len(c.sessions.data))
	for _, s := range c.sessions.data {
		ret = append(ret, s)
	}
	return ret
}

// Flush is a grpc interface. It will send req to nodeID asynchronously
func (c *SessionManager) Flush(ctx context.Context, nodeID int64, req *datapb.FlushSegmentsRequest) {
	go c.execFlush(ctx, nodeID, req)
}

func (c *SessionManager) execFlush(ctx context.Context, nodeID int64, req *datapb.FlushSegmentsRequest) {
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Warn("failed to get dataNode client", zap.Int64("dataNode ID", nodeID), zap.Error(err))
		return
	}
	ctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()

	resp, err := cli.FlushSegments(ctx, req)
	if err := VerifyResponse(resp, err); err != nil {
		log.Error("flush call (perhaps partially) failed", zap.Int64("dataNode ID", nodeID), zap.Error(err))
	} else {
		log.Info("flush call succeeded", zap.Int64("dataNode ID", nodeID))
	}
}

// Compaction is a grpc interface. It will send request to DataNode with provided `nodeID` asynchronously.
func (c *SessionManager) Compaction(nodeID int64, plan *datapb.CompactionPlan) {
	go c.execCompaction(nodeID, plan)
}

func (c *SessionManager) execCompaction(nodeID int64, plan *datapb.CompactionPlan) {
	ctx, cancel := context.WithTimeout(context.Background(), compactionTimeout)
	defer cancel()
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Warn("failed to get client", zap.Int64("nodeID", nodeID), zap.Error(err))
		return
	}

	resp, err := cli.Compaction(ctx, plan)
	if err := VerifyResponse(resp, err); err != nil {
		log.Warn("failed to execute compaction", zap.Int64("node", nodeID), zap.Error(err), zap.Int64("planID", plan.GetPlanID()))
		return
	}

	log.Info("success to execute compaction", zap.Int64("node", nodeID), zap.Any("planID", plan.GetPlanID()))
}

// Import is a grpc interface. It will send request to DataNode with provided `nodeID` asynchronously.
func (c *SessionManager) Import(ctx context.Context, nodeID int64, itr *datapb.ImportTaskRequest) {
	go c.execImport(ctx, nodeID, itr)
}

// execImport gets the corresponding DataNode with its ID and calls its Import method.
func (c *SessionManager) execImport(ctx context.Context, nodeID int64, itr *datapb.ImportTaskRequest) {
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Warn("failed to get client for import", zap.Int64("nodeID", nodeID), zap.Error(err))
		return
	}
	ctx, cancel := context.WithTimeout(ctx, importTimeout)
	defer cancel()
	resp, err := cli.Import(ctx, itr)
	if err := VerifyResponse(resp, err); err != nil {
		log.Warn("failed to import", zap.Int64("node", nodeID), zap.Error(err))
		return
	}

	log.Info("success to import", zap.Int64("node", nodeID), zap.Any("import task", itr))
}

// ReCollectSegmentStats collects segment stats info from DataNodes, after DataCoord reboots.
func (c *SessionManager) ReCollectSegmentStats(ctx context.Context, nodeID int64) {
	go c.execReCollectSegmentStats(ctx, nodeID)
}

func (c *SessionManager) execReCollectSegmentStats(ctx context.Context, nodeID int64) {
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Warn("failed to get dataNode client", zap.Int64("DataNode ID", nodeID), zap.Error(err))
		return
	}
	ctx, cancel := context.WithTimeout(ctx, reCollectTimeout)
	defer cancel()
	resp, err := cli.ResendSegmentStats(ctx, &datapb.ResendSegmentStatsRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_ResendSegmentStats,
			SourceID: Params.DataCoordCfg.GetNodeID(),
		},
	})
	if err := VerifyResponse(resp, err); err != nil {
		log.Error("re-collect segment stats call failed",
			zap.Int64("DataNode ID", nodeID), zap.Error(err))
	} else {
		log.Info("re-collect segment stats call succeeded",
			zap.Int64("DataNode ID", nodeID),
			zap.Int64s("segment stat collected", resp.GetSegResent()))
	}
}

func (c *SessionManager) getClient(ctx context.Context, nodeID int64) (types.DataNode, error) {
	c.sessions.RLock()
	session, ok := c.sessions.data[nodeID]
	c.sessions.RUnlock()

	if !ok {
		return nil, fmt.Errorf("can not find session of node %d", nodeID)
	}

	return session.GetOrCreateClient(ctx)
}

// Close release sessions
func (c *SessionManager) Close() {
	c.sessions.Lock()
	defer c.sessions.Unlock()

	for _, s := range c.sessions.data {
		s.Dispose()
	}
	c.sessions.data = nil
}
