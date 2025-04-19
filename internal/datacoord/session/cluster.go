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
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/task"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// TODO: sheep, sperate a node manager
type Cluster interface {
	AddNode(nodeID int64, address string) error
	RemoveNode(nodeID int64)

	// QuerySlot
	QuerySlot() map[typeutil.UniqueID]*WorkerSlots

	// Compaction
	CreateCompaction(nodeID int64, in *datapb.CompactionPlan) error
	QueryCompaction(nodeID int64, in *datapb.CompactionStateRequest) (*datapb.CompactionPlanResult, error)
	DropCompaction(nodeID int64, in *datapb.DropCompactionPlanRequest) error

	// Import
	CreatePreImport(nodeID int64, in *datapb.PreImportRequest) error
	CreateImport(nodeID int64, in *datapb.ImportRequest) error
	QueryPreImport(nodeID int64, in *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error)
	QueryImport(nodeID int64, in *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error)
	DropImport(nodeID int64, in *datapb.DropImportRequest) error

	// Index
	CreateIndex(nodeID int64, in *workerpb.CreateJobRequest) error
	QueryIndex(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.IndexJobResults, error)
	DropIndex(nodeID int64, in *workerpb.DropJobsRequest) error

	// Stats
	CreateStats(nodeID int64, in *workerpb.CreateStatsRequest) error
	QueryStats(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.StatsResults, error)
	DropStats(nodeID int64, in *workerpb.DropJobsRequest) error

	// Analyze
	CreateAnalyze(nodeID int64, in *workerpb.AnalyzeRequest) error
	QueryAnalyze(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.AnalyzeResults, error)
	DropAnalyze(nodeID int64, in *workerpb.DropJobsRequest) error
}

var _ Cluster = (*cluster)(nil)

type cluster struct {
	mu          lock.RWMutex
	nodeClients map[int64]types.DataNodeClient
	nodeCreator DataNodeCreatorFunc
}

func NewCluster(nodeCreator DataNodeCreatorFunc) Cluster {
	c := &cluster{
		nodeClients: make(map[int64]types.DataNodeClient),
		nodeCreator: nodeCreator,
	}
	return c
}

func (c *cluster) AddNode(nodeID int64, address string) error {
	log := log.Ctx(context.Background()).With(zap.Int64("nodeID", nodeID), zap.String("address", address))
	log.Info("adding node...")
	nodeClient, err := c.nodeCreator(context.Background(), address, nodeID)
	if err != nil {
		log.Error("create client fail", zap.Error(err))
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodeClients[nodeID] = nodeClient
	numNodes := len(c.nodeClients)
	metrics.IndexNodeNum.WithLabelValues().Set(float64(numNodes))
	metrics.DataCoordNumDataNodes.WithLabelValues().Set(float64(numNodes))
	log.Info("node added", zap.Int("numNodes", numNodes))
	return nil
}

func (c *cluster) RemoveNode(nodeID int64) {
	log := log.Ctx(context.Background()).With(zap.Int64("nodeID", nodeID))
	log.Info("removing node...")
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.nodeClients[nodeID]; ok {
		if err := client.Close(); err != nil {
			log.Warn("failed to close client", zap.Error(err))
		}
		delete(c.nodeClients, nodeID)
		numNodes := len(c.nodeClients)
		metrics.IndexNodeNum.WithLabelValues().Set(float64(numNodes))
		metrics.DataCoordNumDataNodes.WithLabelValues().Set(float64(numNodes))
		log.Info("node removed", zap.Int("numNodes", numNodes))
	}
}

func (c *cluster) getClientIDs() []int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return lo.Keys(c.nodeClients)
}

func (c *cluster) getClient(nodeID int64) (types.DataNodeClient, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	client, ok := c.nodeClients[nodeID]
	if !ok {
		return nil, merr.WrapErrNodeNotFound(nodeID)
	}
	return client, nil
}

func (c *cluster) createTask(nodeID int64, in proto.Message, properties task.Properties) error {
	timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cli, err := c.getClient(nodeID)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get client", zap.Error(err))
		return err
	}

	payload, err := proto.Marshal(in)
	if err != nil {
		log.Ctx(ctx).Warn("marshal request failed", zap.Error(err))
		return err
	}

	status, err := cli.CreateTask(ctx, &workerpb.CreateTaskRequest{
		Payload:    payload,
		Properties: properties,
	})
	return merr.CheckRPCCall(status, err)
}

func (c *cluster) queryTask(nodeID int64, in proto.Message, properties task.Properties) (*workerpb.QueryTaskResponse, error) {
	timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cli, err := c.getClient(nodeID)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get client", zap.Error(err))
		return nil, err
	}

	payload, err := proto.Marshal(in)
	if err != nil {
		log.Ctx(ctx).Warn("marshal request failed", zap.Error(err))
		return nil, err
	}

	resp, err := cli.QueryTask(ctx, &workerpb.QueryTaskRequest{
		Payload:    payload,
		Properties: properties,
	})
	if err = merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *cluster) dropTask(nodeID int64, in proto.Message, properties task.Properties) error {
	timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cli, err := c.getClient(nodeID)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get client", zap.Error(err))
		return err
	}

	payload, err := proto.Marshal(in)
	if err != nil {
		log.Ctx(ctx).Warn("marshal request failed", zap.Error(err))
		return err
	}

	status, err := cli.DropTask(ctx, &workerpb.DropTaskRequest{
		Payload:    payload,
		Properties: properties,
	})
	return merr.CheckRPCCall(status, err)
}

func (c *cluster) QuerySlot() map[typeutil.UniqueID]*WorkerSlots {
	var (
		mu        = &sync.Mutex{}
		wg        = &sync.WaitGroup{}
		nodeSlots = make(map[int64]*WorkerSlots)
	)
	properties := task.NewProperties()
	properties.AppendType(task.QuerySlot)
	for _, nodeID := range c.getClientIDs() {
		wg.Add(1)
		nodeID := nodeID
		go func() {
			defer wg.Done()
			resp, err := c.queryTask(nodeID, &workerpb.GetJobStatsRequest{}, properties)
			if err = merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
				log.Ctx(context.TODO()).Warn("failed to get node slot", zap.Int64("nodeID", nodeID), zap.Error(err))
				return
			}
			result := &workerpb.GetJobStatsResponse{}
			err = proto.Unmarshal(resp.GetPayload(), result)
			if err != nil {
				log.Ctx(context.TODO()).Warn("failed to unmarshal result", zap.Int64("nodeID", nodeID), zap.Error(err))
				return
			}
			mu.Lock()
			defer mu.Unlock()
			nodeSlots[nodeID] = &WorkerSlots{
				NodeID:         nodeID,
				TotalSlots:     result.GetTotalSlots(),
				AvailableSlots: result.GetAvailableSlots(),
			}
		}()
	}
	wg.Wait()
	log.Ctx(context.TODO()).Debug("query slot done", zap.Any("nodeSlots", nodeSlots))
	return nodeSlots
}

func (c *cluster) CreateCompaction(nodeID int64, in *datapb.CompactionPlan) error {
	properties := task.NewProperties()
	properties.AppendType(task.Compaction)
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) QueryCompaction(nodeID int64, in *datapb.CompactionStateRequest) (*datapb.CompactionPlanResult, error) {
	properties := task.NewProperties()
	properties.AppendType(task.Compaction)
	resp, err := c.queryTask(nodeID, in, properties)
	if err != nil {
		return nil, err
	}
	result := &datapb.CompactionStateResponse{}
	err = proto.Unmarshal(resp.GetPayload(), result)
	if err != nil {
		return nil, err
	}
	var ret *datapb.CompactionPlanResult
	// TODO: sheep, wrap marshal and unmarshal function in common package
	for _, rst := range result.GetResults() {
		if rst.GetPlanID() != in.GetPlanID() {
			continue
		}
		err = binlog.CompressCompactionBinlogs(rst.GetSegments())
		if err != nil {
			return nil, err
		}
		ret = rst
		break
	}
	return ret, err
}

func (c *cluster) DropCompaction(nodeID int64, in *datapb.DropCompactionPlanRequest) error {
	properties := task.NewProperties()
	properties.AppendType(task.Compaction)
	return c.dropTask(nodeID, in, properties)
}

func (c *cluster) CreatePreImport(nodeID int64, in *datapb.PreImportRequest) error {
	properties := task.NewProperties()
	properties.AppendType(task.PreImport)
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) CreateImport(nodeID int64, in *datapb.ImportRequest) error {
	properties := task.NewProperties()
	properties.AppendType(task.Import)
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) QueryPreImport(nodeID int64, in *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error) {
	properties := task.NewProperties()
	properties.AppendType(task.PreImport)
	resp, err := c.queryTask(nodeID, in, properties)
	if err != nil {
		return nil, err
	}
	result := &datapb.QueryPreImportResponse{}
	err = proto.Unmarshal(resp.GetPayload(), result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *cluster) QueryImport(nodeID int64, in *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error) {
	properties := task.NewProperties()
	properties.AppendType(task.Import)
	resp, err := c.queryTask(nodeID, in, properties)
	if err != nil {
		return nil, err
	}
	result := &datapb.QueryImportResponse{}
	err = proto.Unmarshal(resp.GetPayload(), result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *cluster) DropImport(nodeID int64, in *datapb.DropImportRequest) error {
	properties := task.NewProperties()
	properties.AppendType(task.Import)
	return c.dropTask(nodeID, in, properties)
}

func (c *cluster) CreateIndex(nodeID int64, in *workerpb.CreateJobRequest) error {
	properties := task.NewProperties()
	properties.AppendType(task.Index)
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) QueryIndex(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.IndexJobResults, error) {
	properties := task.NewProperties()
	properties.AppendType(task.Index)
	resp, err := c.queryTask(nodeID, in, properties)
	if err != nil {
		return nil, err
	}
	result := &workerpb.QueryJobsV2Response{}
	err = proto.Unmarshal(resp.GetPayload(), result)
	if err != nil {
		return nil, err
	}
	return result.GetIndexJobResults(), nil
}

func (c *cluster) DropIndex(nodeID int64, in *workerpb.DropJobsRequest) error {
	properties := task.NewProperties()
	properties.AppendType(task.Index)
	return c.dropTask(nodeID, in, properties)
}

func (c *cluster) CreateStats(nodeID int64, in *workerpb.CreateStatsRequest) error {
	properties := task.NewProperties()
	properties.AppendType(task.Stats)
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) QueryStats(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.StatsResults, error) {
	properties := task.NewProperties()
	properties.AppendType(task.Stats)
	resp, err := c.queryTask(nodeID, in, properties)
	if err != nil {
		return nil, err
	}
	result := &workerpb.QueryJobsV2Response{}
	err = proto.Unmarshal(resp.GetPayload(), result)
	if err != nil {
		return nil, err
	}
	return result.GetStatsJobResults(), nil
}

func (c *cluster) DropStats(nodeID int64, in *workerpb.DropJobsRequest) error {
	properties := task.NewProperties()
	properties.AppendType(task.Stats)
	return c.dropTask(nodeID, in, properties)
}

func (c *cluster) CreateAnalyze(nodeID int64, in *workerpb.AnalyzeRequest) error {
	properties := task.NewProperties()
	properties.AppendType(task.Analyze)
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) QueryAnalyze(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.AnalyzeResults, error) {
	properties := task.NewProperties()
	properties.AppendType(task.Analyze)
	resp, err := c.queryTask(nodeID, in, properties)
	if err != nil {
		return nil, err
	}
	result := &workerpb.QueryJobsV2Response{}
	err = proto.Unmarshal(resp.GetPayload(), result)
	if err != nil {
		return nil, err
	}
	return result.GetAnalyzeJobResults(), nil
}

func (c *cluster) DropAnalyze(nodeID int64, in *workerpb.DropJobsRequest) error {
	properties := task.NewProperties()
	properties.AppendType(task.Analyze)
	return c.dropTask(nodeID, in, properties)
}
