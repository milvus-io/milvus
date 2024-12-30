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

package session

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	grpcdatanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	flushTimeout      = 15 * time.Second
	importTaskTimeout = 10 * time.Second
	querySlotTimeout  = 10 * time.Second
)

// DataNodeManager is the interface for datanode session manager.
type DataNodeManager interface {
	AddSession(node *NodeInfo)
	DeleteSession(node *NodeInfo)
	GetSessionIDs() []int64
	GetSessions() []*Session
	GetSession(int64) (*Session, bool)

	Flush(ctx context.Context, nodeID int64, req *datapb.FlushSegmentsRequest)
	FlushChannels(ctx context.Context, nodeID int64, req *datapb.FlushChannelsRequest) error
	Compaction(ctx context.Context, nodeID int64, plan *datapb.CompactionPlan) error
	SyncSegments(ctx context.Context, nodeID int64, req *datapb.SyncSegmentsRequest) error
	GetCompactionPlanResult(nodeID int64, planID int64) (*datapb.CompactionPlanResult, error)
	GetCompactionPlansResults() (map[int64]*typeutil.Pair[int64, *datapb.CompactionPlanResult], error)
	NotifyChannelOperation(ctx context.Context, nodeID int64, req *datapb.ChannelOperationsRequest) error
	CheckChannelOperationProgress(ctx context.Context, nodeID int64, info *datapb.ChannelWatchInfo) (*datapb.ChannelOperationProgressResponse, error)
	PreImport(nodeID int64, in *datapb.PreImportRequest) error
	ImportV2(nodeID int64, in *datapb.ImportRequest) error
	QueryPreImport(nodeID int64, in *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error)
	QueryImport(nodeID int64, in *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error)
	DropImport(nodeID int64, in *datapb.DropImportRequest) error
	CheckHealth(ctx context.Context) error
	QuerySlot(nodeID int64) (*datapb.QuerySlotResponse, error)
	DropCompactionPlan(nodeID int64, req *datapb.DropCompactionPlanRequest) error
	Close()
}

var _ DataNodeManager = (*DataNodeManagerImpl)(nil)

// DataNodeManagerImpl provides the grpc interfaces of cluster
type DataNodeManagerImpl struct {
	sessions struct {
		lock.RWMutex
		data map[int64]*Session
	}
	sessionCreator DataNodeCreatorFunc
}

// SessionOpt provides a way to set params in SessionManagerImpl
type SessionOpt func(c *DataNodeManagerImpl)

func WithDataNodeCreator(creator DataNodeCreatorFunc) SessionOpt {
	return func(c *DataNodeManagerImpl) { c.sessionCreator = creator }
}

func defaultSessionCreator() DataNodeCreatorFunc {
	return func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
		return grpcdatanodeclient.NewClient(ctx, addr, nodeID)
	}
}

// NewDataNodeManagerImpl creates a new NewDataNodeManagerImpl
func NewDataNodeManagerImpl(options ...SessionOpt) *DataNodeManagerImpl {
	m := &DataNodeManagerImpl{
		sessions: struct {
			lock.RWMutex
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
func (c *DataNodeManagerImpl) AddSession(node *NodeInfo) {
	c.sessions.Lock()
	defer c.sessions.Unlock()

	session := NewSession(node, c.sessionCreator)
	c.sessions.data[node.NodeID] = session
	metrics.DataCoordNumDataNodes.WithLabelValues().Set(float64(len(c.sessions.data)))
}

// GetSession return a Session related to nodeID
func (c *DataNodeManagerImpl) GetSession(nodeID int64) (*Session, bool) {
	c.sessions.RLock()
	defer c.sessions.RUnlock()
	s, ok := c.sessions.data[nodeID]
	return s, ok
}

// DeleteSession removes the node session
func (c *DataNodeManagerImpl) DeleteSession(node *NodeInfo) {
	c.sessions.Lock()
	defer c.sessions.Unlock()

	if session, ok := c.sessions.data[node.NodeID]; ok {
		session.Dispose()
		delete(c.sessions.data, node.NodeID)
	}
	metrics.DataCoordNumDataNodes.WithLabelValues().Set(float64(len(c.sessions.data)))
}

// GetSessionIDs returns IDs of all live DataNodes.
func (c *DataNodeManagerImpl) GetSessionIDs() []int64 {
	c.sessions.RLock()
	defer c.sessions.RUnlock()

	ret := make([]int64, 0, len(c.sessions.data))
	for id := range c.sessions.data {
		ret = append(ret, id)
	}
	return ret
}

// GetSessions gets all node sessions
func (c *DataNodeManagerImpl) GetSessions() []*Session {
	c.sessions.RLock()
	defer c.sessions.RUnlock()

	ret := make([]*Session, 0, len(c.sessions.data))
	for _, s := range c.sessions.data {
		ret = append(ret, s)
	}
	return ret
}

func (c *DataNodeManagerImpl) getClient(ctx context.Context, nodeID int64) (types.DataNodeClient, error) {
	c.sessions.RLock()
	session, ok := c.sessions.data[nodeID]
	c.sessions.RUnlock()

	if !ok {
		return nil, merr.WrapErrNodeNotFound(nodeID, "can not find session")
	}

	return session.GetOrCreateClient(ctx)
}

// Flush is a grpc interface. It will send req to nodeID asynchronously
func (c *DataNodeManagerImpl) Flush(ctx context.Context, nodeID int64, req *datapb.FlushSegmentsRequest) {
	go c.execFlush(ctx, nodeID, req)
}

func (c *DataNodeManagerImpl) execFlush(ctx context.Context, nodeID int64, req *datapb.FlushSegmentsRequest) {
	log := log.Ctx(ctx).With(zap.Int64("nodeID", nodeID), zap.String("channel", req.GetChannelName()))
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Warn("failed to get dataNode client", zap.Error(err))
		return
	}
	ctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()

	resp, err := cli.FlushSegments(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Error("flush call (perhaps partially) failed", zap.Error(err))
	} else {
		log.Info("flush call succeeded")
	}
}

// Compaction is a grpc interface. It will send request to DataNode with provided `nodeID` synchronously.
func (c *DataNodeManagerImpl) Compaction(ctx context.Context, nodeID int64, plan *datapb.CompactionPlan) error {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().DataCoordCfg.CompactionRPCTimeout.GetAsDuration(time.Second))
	defer cancel()
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Warn("failed to get client", zap.Int64("nodeID", nodeID), zap.Error(err))
		return err
	}

	resp, err := cli.CompactionV2(ctx, plan)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to execute compaction", zap.Int64("node", nodeID), zap.Error(err), zap.Int64("planID", plan.GetPlanID()))
		return err
	}

	log.Info("success to execute compaction", zap.Int64("node", nodeID), zap.Int64("planID", plan.GetPlanID()))
	return nil
}

// SyncSegments is a grpc interface. It will send request to DataNode with provided `nodeID` synchronously.
func (c *DataNodeManagerImpl) SyncSegments(ctx context.Context, nodeID int64, req *datapb.SyncSegmentsRequest) error {
	log := log.With(
		zap.Int64("nodeID", nodeID),
		zap.Int64("planID", req.GetPlanID()),
	)
	childCtx, cancel := context.WithTimeout(context.Background(), paramtable.Get().DataCoordCfg.CompactionRPCTimeout.GetAsDuration(time.Second))
	cli, err := c.getClient(childCtx, nodeID)
	cancel()
	if err != nil {
		log.Warn("failed to get client", zap.Error(err))
		return err
	}

	err = retry.Do(ctx, func() error {
		// doesn't set timeout
		resp, err := cli.SyncSegments(ctx, req)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			log.Warn("failed to sync segments", zap.Error(err))
			return err
		}
		return nil
	})
	if err != nil {
		log.Warn("failed to sync segments after retry", zap.Error(err))
		return err
	}

	log.Info("success to sync segments")
	return nil
}

// GetCompactionPlansResults returns map[planID]*pair[nodeID, *CompactionPlanResults]
func (c *DataNodeManagerImpl) GetCompactionPlansResults() (map[int64]*typeutil.Pair[int64, *datapb.CompactionPlanResult], error) {
	ctx := context.Background()
	errorGroup, ctx := errgroup.WithContext(ctx)

	plans := typeutil.NewConcurrentMap[int64, *typeutil.Pair[int64, *datapb.CompactionPlanResult]]()
	c.sessions.RLock()
	for nodeID, s := range c.sessions.data {
		nodeID, s := nodeID, s // https://golang.org/doc/faq#closures_and_goroutines
		errorGroup.Go(func() error {
			cli, err := s.GetOrCreateClient(ctx)
			if err != nil {
				log.Info("Cannot Create Client", zap.Int64("NodeID", nodeID))
				return err
			}
			ctx, cancel := context.WithTimeout(ctx, paramtable.Get().DataCoordCfg.CompactionRPCTimeout.GetAsDuration(time.Second))
			defer cancel()
			resp, err := cli.GetCompactionState(ctx, &datapb.CompactionStateRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_GetSystemConfigs),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
			})

			if err != nil || resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				log.Info("Get State failed", zap.Error(err))
				return err
			}

			for _, rst := range resp.GetResults() {
				binlog.CompressCompactionBinlogs(rst.GetSegments())
				nodeRst := typeutil.NewPair(nodeID, rst)
				plans.Insert(rst.PlanID, &nodeRst)
			}
			return nil
		})
	}
	c.sessions.RUnlock()

	// wait for all request done
	if err := errorGroup.Wait(); err != nil {
		return nil, err
	}

	rst := make(map[int64]*typeutil.Pair[int64, *datapb.CompactionPlanResult])
	plans.Range(func(planID int64, result *typeutil.Pair[int64, *datapb.CompactionPlanResult]) bool {
		rst[planID] = result
		return true
	})

	return rst, nil
}

func (c *DataNodeManagerImpl) GetCompactionPlanResult(nodeID int64, planID int64) (*datapb.CompactionPlanResult, error) {
	ctx := context.Background()
	c.sessions.RLock()
	s, ok := c.sessions.data[nodeID]
	if !ok {
		c.sessions.RUnlock()
		return nil, merr.WrapErrNodeNotFound(nodeID)
	}
	c.sessions.RUnlock()
	cli, err := s.GetOrCreateClient(ctx)
	if err != nil {
		log.Info("Cannot Create Client", zap.Int64("NodeID", nodeID))
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), paramtable.Get().DataCoordCfg.CompactionRPCTimeout.GetAsDuration(time.Second))
	defer cancel()
	resp, err2 := cli.GetCompactionState(ctx, &datapb.CompactionStateRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		PlanID: planID,
	})

	if err2 != nil {
		return nil, err2
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Info("GetCompactionState state is not", zap.Error(err))
		return nil, fmt.Errorf("GetCopmactionState failed")
	}
	var result *datapb.CompactionPlanResult
	for _, rst := range resp.GetResults() {
		if rst.GetPlanID() != planID {
			continue
		}
		binlog.CompressCompactionBinlogs(rst.GetSegments())
		result = rst
		break
	}

	return result, nil
}

func (c *DataNodeManagerImpl) FlushChannels(ctx context.Context, nodeID int64, req *datapb.FlushChannelsRequest) error {
	log := log.Ctx(ctx).With(zap.Int64("nodeID", nodeID),
		zap.Time("flushTs", tsoutil.PhysicalTime(req.GetFlushTs())),
		zap.Strings("channels", req.GetChannels()))
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Warn("failed to get client", zap.Error(err))
		return err
	}

	log.Info("SessionManagerImpl.FlushChannels start")
	resp, err := cli.FlushChannels(ctx, req)
	err = merr.CheckRPCCall(resp, err)
	if err != nil {
		log.Warn("SessionManagerImpl.FlushChannels failed", zap.Error(err))
		return err
	}
	log.Info("SessionManagerImpl.FlushChannels successfully")
	return nil
}

func (c *DataNodeManagerImpl) NotifyChannelOperation(ctx context.Context, nodeID int64, req *datapb.ChannelOperationsRequest) error {
	log := log.Ctx(ctx).With(zap.Int64("nodeID", nodeID))
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Info("failed to get dataNode client", zap.Error(err))
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().DataCoordCfg.ChannelOperationRPCTimeout.GetAsDuration(time.Second))
	defer cancel()
	resp, err := cli.NotifyChannelOperation(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("Notify channel operations failed", zap.Error(err))
		return err
	}
	return nil
}

func (c *DataNodeManagerImpl) CheckChannelOperationProgress(ctx context.Context, nodeID int64, info *datapb.ChannelWatchInfo) (*datapb.ChannelOperationProgressResponse, error) {
	log := log.With(
		zap.Int64("nodeID", nodeID),
		zap.String("channel", info.GetVchan().GetChannelName()),
		zap.String("operation", info.GetState().String()),
	)
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Info("failed to get dataNode client", zap.Error(err))
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().DataCoordCfg.ChannelOperationRPCTimeout.GetAsDuration(time.Second))
	defer cancel()
	resp, err := cli.CheckChannelOperationProgress(ctx, info)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("Check channel operation failed", zap.Error(err))
		return nil, err
	}

	return resp, nil
}

func (c *DataNodeManagerImpl) PreImport(nodeID int64, in *datapb.PreImportRequest) error {
	log := log.With(
		zap.Int64("nodeID", nodeID),
		zap.Int64("jobID", in.GetJobID()),
		zap.Int64("taskID", in.GetTaskID()),
		zap.Int64("collectionID", in.GetCollectionID()),
		zap.Int64s("partitionIDs", in.GetPartitionIDs()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), importTaskTimeout)
	defer cancel()
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Info("failed to get client", zap.Error(err))
		return err
	}
	status, err := cli.PreImport(ctx, in)
	return merr.CheckRPCCall(status, err)
}

func (c *DataNodeManagerImpl) ImportV2(nodeID int64, in *datapb.ImportRequest) error {
	log := log.With(
		zap.Int64("nodeID", nodeID),
		zap.Int64("jobID", in.GetJobID()),
		zap.Int64("taskID", in.GetTaskID()),
		zap.Int64("collectionID", in.GetCollectionID()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), importTaskTimeout)
	defer cancel()
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Info("failed to get client", zap.Error(err))
		return err
	}
	status, err := cli.ImportV2(ctx, in)
	return merr.CheckRPCCall(status, err)
}

func (c *DataNodeManagerImpl) QueryPreImport(nodeID int64, in *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error) {
	log := log.With(
		zap.Int64("nodeID", nodeID),
		zap.Int64("jobID", in.GetJobID()),
		zap.Int64("taskID", in.GetTaskID()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), importTaskTimeout)
	defer cancel()
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Info("failed to get client", zap.Error(err))
		return nil, err
	}
	resp, err := cli.QueryPreImport(ctx, in)
	if err = merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *DataNodeManagerImpl) QueryImport(nodeID int64, in *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error) {
	log := log.With(
		zap.Int64("nodeID", nodeID),
		zap.Int64("jobID", in.GetJobID()),
		zap.Int64("taskID", in.GetTaskID()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), importTaskTimeout)
	defer cancel()
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Info("failed to get client", zap.Error(err))
		return nil, err
	}
	resp, err := cli.QueryImport(ctx, in)
	if err = merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *DataNodeManagerImpl) DropImport(nodeID int64, in *datapb.DropImportRequest) error {
	log := log.With(
		zap.Int64("nodeID", nodeID),
		zap.Int64("jobID", in.GetJobID()),
		zap.Int64("taskID", in.GetTaskID()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), importTaskTimeout)
	defer cancel()
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Info("failed to get client", zap.Error(err))
		return err
	}
	status, err := cli.DropImport(ctx, in)
	return merr.CheckRPCCall(status, err)
}

func (c *DataNodeManagerImpl) CheckHealth(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)

	ids := c.GetSessionIDs()
	for _, nodeID := range ids {
		nodeID := nodeID
		group.Go(func() error {
			cli, err := c.getClient(ctx, nodeID)
			if err != nil {
				return fmt.Errorf("failed to get DataNode %d: %v", nodeID, err)
			}

			sta, err := cli.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
			if err != nil {
				return err
			}
			err = merr.AnalyzeState("DataNode", nodeID, sta)
			return err
		})
	}

	return group.Wait()
}

func (c *DataNodeManagerImpl) QuerySlot(nodeID int64) (*datapb.QuerySlotResponse, error) {
	log := log.With(zap.Int64("nodeID", nodeID))
	ctx, cancel := context.WithTimeout(context.Background(), querySlotTimeout)
	defer cancel()
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Info("failed to get client", zap.Error(err))
		return nil, err
	}
	resp, err := cli.QuerySlot(ctx, &datapb.QuerySlotRequest{})
	if err = merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *DataNodeManagerImpl) DropCompactionPlan(nodeID int64, req *datapb.DropCompactionPlanRequest) error {
	log := log.With(
		zap.Int64("nodeID", nodeID),
		zap.Int64("planID", req.GetPlanID()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), paramtable.Get().DataCoordCfg.CompactionRPCTimeout.GetAsDuration(time.Second))
	defer cancel()
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			log.Info("node not found, skip dropping compaction plan")
			return nil
		}
		log.Warn("failed to get client", zap.Error(err))
		return err
	}

	err = retry.Do(context.Background(), func() error {
		ctx, cancel := context.WithTimeout(context.Background(), paramtable.Get().DataCoordCfg.CompactionRPCTimeout.GetAsDuration(time.Second))
		defer cancel()

		resp, err := cli.DropCompactionPlan(ctx, req)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			log.Warn("failed to drop compaction plan", zap.Error(err))
			return err
		}
		return nil
	})
	if err != nil {
		log.Warn("failed to drop compaction plan after retry", zap.Error(err))
		return err
	}

	log.Info("success to drop compaction plan")
	return nil
}

// Close release sessions
func (c *DataNodeManagerImpl) Close() {
	c.sessions.Lock()
	defer c.sessions.Unlock()

	for _, s := range c.sessions.data {
		s.Dispose()
	}
	c.sessions.data = nil
}
