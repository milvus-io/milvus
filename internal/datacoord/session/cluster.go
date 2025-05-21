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

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// WorkerSlots represents the slot information for a worker node
type WorkerSlots struct {
	NodeID         int64
	AvailableSlots int64
}

// Cluster defines the interface for tasks
type Cluster interface {
	// QuerySlot returns the slot information for all worker nodes
	QuerySlot() map[int64]*WorkerSlots

	// CreateCompaction creates a new compaction task on the specified node
	CreateCompaction(nodeID int64, in *datapb.CompactionPlan) error
	// QueryCompaction queries the status of a compaction task
	QueryCompaction(nodeID int64, in *datapb.CompactionStateRequest) (*datapb.CompactionPlanResult, error)
	// DropCompaction drops a compaction task
	DropCompaction(nodeID int64, planID int64) error

	// CreatePreImport creates a pre-import task
	CreatePreImport(nodeID int64, in *datapb.PreImportRequest, taskSlot int64) error
	// CreateImport creates an import task
	CreateImport(nodeID int64, in *datapb.ImportRequest, taskSlot int64) error
	// QueryPreImport queries the status of a pre-import task
	QueryPreImport(nodeID int64, in *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error)
	// QueryImport queries the status of an import task
	QueryImport(nodeID int64, in *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error)
	// DropImport drops an import task
	DropImport(nodeID int64, taskID int64) error

	// CreateIndex creates an index building task
	CreateIndex(nodeID int64, in *workerpb.CreateJobRequest) error
	// QueryIndex queries the status of index building tasks
	QueryIndex(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.IndexJobResults, error)
	// DropIndex drops an index building task
	DropIndex(nodeID int64, taskID int64) error

	// CreateStats creates a statistics collection task
	CreateStats(nodeID int64, in *workerpb.CreateStatsRequest) error
	// QueryStats queries the status of statistics collection tasks
	QueryStats(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.StatsResults, error)
	// DropStats drops a statistics collection task
	DropStats(nodeID int64, taskID int64) error

	// CreateAnalyze creates an analysis task
	CreateAnalyze(nodeID int64, in *workerpb.AnalyzeRequest) error
	// QueryAnalyze queries the status of analysis tasks
	QueryAnalyze(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.AnalyzeResults, error)
	// DropAnalyze drops an analysis task
	DropAnalyze(nodeID int64, taskID int64) error
}

var _ Cluster = (*cluster)(nil)

// cluster implements the Cluster interface
type cluster struct {
	nm NodeManager
}

// NewCluster creates a new instance of cluster
func NewCluster(nm NodeManager) Cluster {
	c := &cluster{
		nm: nm,
	}
	return c
}

func (c *cluster) createTask(nodeID int64, in proto.Message, properties taskcommon.Properties) error {
	timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cli, err := c.nm.GetClient(nodeID)
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

func (c *cluster) queryTask(nodeID int64, properties taskcommon.Properties) (*workerpb.QueryTaskResponse, error) {
	timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cli, err := c.nm.GetClient(nodeID)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get client", zap.Error(err))
		return nil, err
	}

	resp, err := cli.QueryTask(ctx, &workerpb.QueryTaskRequest{
		Properties: properties,
	})
	if err = merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *cluster) dropTask(nodeID int64, properties taskcommon.Properties) error {
	timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cli, err := c.nm.GetClient(nodeID)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get client", zap.Error(err))
		return err
	}

	status, err := cli.DropTask(ctx, &workerpb.DropTaskRequest{
		Properties: properties,
	})
	return merr.CheckRPCCall(status, err)
}

func (c *cluster) QuerySlot() map[int64]*WorkerSlots {
	var (
		mu                 = &sync.Mutex{}
		wg                 = &sync.WaitGroup{}
		availableNodeSlots = make(map[int64]*WorkerSlots)
	)
	for _, nodeID := range c.nm.GetClientIDs() {
		wg.Add(1)
		nodeID := nodeID
		go func() {
			defer wg.Done()
			timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			cli, err := c.nm.GetClient(nodeID)
			if err != nil {
				log.Ctx(ctx).Warn("failed to get client", zap.Error(err))
				return
			}
			resp, err := cli.QuerySlot(ctx, &datapb.QuerySlotRequest{})
			if err = merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
				log.Ctx(ctx).Warn("failed to get node slot", zap.Int64("nodeID", nodeID), zap.Error(err))
				return
			}
			mu.Lock()
			defer mu.Unlock()
			availableNodeSlots[nodeID] = &WorkerSlots{
				NodeID:         nodeID,
				AvailableSlots: resp.GetAvailableSlots(),
			}
		}()
	}
	wg.Wait()
	log.Ctx(context.TODO()).Debug("query slot done", zap.Any("nodeSlots", availableNodeSlots))
	return availableNodeSlots
}

func (c *cluster) CreateCompaction(nodeID int64, in *datapb.CompactionPlan) error {
	properties := taskcommon.NewProperties(nil)
	properties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	properties.AppendTaskID(in.GetPlanID())
	properties.AppendType(taskcommon.Compaction)
	properties.AppendTaskSlot(in.GetSlotUsage())
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) QueryCompaction(nodeID int64, in *datapb.CompactionStateRequest) (*datapb.CompactionPlanResult, error) {
	reqProperties := taskcommon.NewProperties(nil)
	reqProperties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	reqProperties.AppendTaskID(in.GetPlanID())
	reqProperties.AppendType(taskcommon.Compaction)
	resp, err := c.queryTask(nodeID, reqProperties)
	if err != nil {
		return nil, err
	}

	resProperties := taskcommon.NewProperties(resp.GetProperties())
	state, err := resProperties.GetTaskState()
	if err != nil {
		return nil, err
	}
	switch state {
	case taskcommon.None, taskcommon.Init, taskcommon.InProgress, taskcommon.Retry:
		return &datapb.CompactionPlanResult{State: taskcommon.ToCompactionState(state)}, nil
	case taskcommon.Finished, taskcommon.Failed:
		result := &datapb.CompactionStateResponse{}
		err = proto.Unmarshal(resp.GetPayload(), result)
		if err != nil {
			return nil, err
		}
		var ret *datapb.CompactionPlanResult
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
	default:
		panic("should not happen")
	}
}

func (c *cluster) DropCompaction(nodeID int64, planID int64) error {
	properties := taskcommon.NewProperties(nil)
	properties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	properties.AppendTaskID(planID)
	properties.AppendType(taskcommon.Compaction)
	return c.dropTask(nodeID, properties)
}

func (c *cluster) CreatePreImport(nodeID int64, in *datapb.PreImportRequest, taskSlot int64) error {
	// TODO: sheep, use taskSlot in request
	properties := taskcommon.NewProperties(nil)
	properties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	properties.AppendTaskID(in.GetTaskID())
	properties.AppendType(taskcommon.PreImport)
	properties.AppendTaskSlot(taskSlot)
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) CreateImport(nodeID int64, in *datapb.ImportRequest, taskSlot int64) error {
	properties := taskcommon.NewProperties(nil)
	properties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	properties.AppendTaskID(in.GetTaskID())
	properties.AppendType(taskcommon.Import)
	properties.AppendTaskSlot(taskSlot)
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) QueryPreImport(nodeID int64, in *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error) {
	repProperties := taskcommon.NewProperties(nil)
	repProperties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	repProperties.AppendTaskID(in.GetTaskID())
	repProperties.AppendType(taskcommon.PreImport)
	resp, err := c.queryTask(nodeID, repProperties)
	if err != nil {
		return nil, err
	}

	resProperties := taskcommon.NewProperties(resp.GetProperties())
	state, err := resProperties.GetTaskState()
	if err != nil {
		return nil, err
	}
	reason := resProperties.GetTaskReason()
	switch state {
	case taskcommon.None, taskcommon.Init, taskcommon.InProgress, taskcommon.Retry:
		return &datapb.QueryPreImportResponse{State: taskcommon.ToImportState(state), Reason: reason}, nil
	case taskcommon.Finished, taskcommon.Failed:
		result := &datapb.QueryPreImportResponse{}
		err = proto.Unmarshal(resp.GetPayload(), result)
		if err != nil {
			return nil, err
		}
		return result, nil
	default:
		panic("should not happen")
	}
}

func (c *cluster) QueryImport(nodeID int64, in *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error) {
	reqProperties := taskcommon.NewProperties(nil)
	reqProperties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	reqProperties.AppendTaskID(in.GetTaskID())
	reqProperties.AppendType(taskcommon.Import)
	resp, err := c.queryTask(nodeID, reqProperties)
	if err != nil {
		return nil, err
	}

	resProperties := taskcommon.NewProperties(resp.GetProperties())
	state, err := resProperties.GetTaskState()
	if err != nil {
		return nil, err
	}
	reason := resProperties.GetTaskReason()
	switch state {
	case taskcommon.None, taskcommon.Init, taskcommon.InProgress, taskcommon.Retry:
		return &datapb.QueryImportResponse{State: taskcommon.ToImportState(state), Reason: reason}, nil
	case taskcommon.Finished, taskcommon.Failed:
		result := &datapb.QueryImportResponse{}
		err = proto.Unmarshal(resp.GetPayload(), result)
		if err != nil {
			return nil, err
		}
		return result, nil
	default:
		panic("should not happen")
	}
}

func (c *cluster) DropImport(nodeID int64, taskID int64) error {
	properties := taskcommon.NewProperties(nil)
	properties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	properties.AppendTaskID(taskID)
	properties.AppendType(taskcommon.Import)
	return c.dropTask(nodeID, properties)
}

func (c *cluster) CreateIndex(nodeID int64, in *workerpb.CreateJobRequest) error {
	properties := taskcommon.NewProperties(nil)
	properties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	properties.AppendTaskID(in.GetBuildID())
	properties.AppendType(taskcommon.Index)
	properties.AppendTaskSlot(in.GetTaskSlot())
	properties.AppendNumRows(in.GetNumRows())
	properties.AppendTaskVersion(in.GetIndexVersion())
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) QueryIndex(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.IndexJobResults, error) {
	reqProperties := taskcommon.NewProperties(nil)
	reqProperties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	reqProperties.AppendTaskID(in.GetTaskIDs()[0])
	reqProperties.AppendType(taskcommon.Index)
	resp, err := c.queryTask(nodeID, reqProperties)
	if err != nil {
		return nil, err
	}

	resProperties := taskcommon.NewProperties(resp.GetProperties())
	state, err := resProperties.GetTaskState()
	if err != nil {
		return nil, err
	}
	reason := resProperties.GetTaskReason()
	switch state {
	case taskcommon.None, taskcommon.Init, taskcommon.InProgress, taskcommon.Retry:
		return &workerpb.IndexJobResults{
			Results: []*workerpb.IndexTaskInfo{
				{
					BuildID:    in.GetTaskIDs()[0],
					State:      commonpb.IndexState(state),
					FailReason: reason,
				},
			},
		}, nil
	case taskcommon.Finished, taskcommon.Failed:
		result := &workerpb.QueryJobsV2Response{}
		err = proto.Unmarshal(resp.GetPayload(), result)
		if err != nil {
			return nil, err
		}
		return result.GetIndexJobResults(), nil
	default:
		panic("should not happen")
	}
}

func (c *cluster) DropIndex(nodeID int64, taskID int64) error {
	properties := taskcommon.NewProperties(nil)
	properties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	properties.AppendTaskID(taskID)
	properties.AppendType(taskcommon.Index)
	return c.dropTask(nodeID, properties)
}

func (c *cluster) CreateStats(nodeID int64, in *workerpb.CreateStatsRequest) error {
	properties := taskcommon.NewProperties(nil)
	properties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	properties.AppendTaskID(in.GetTaskID())
	properties.AppendType(taskcommon.Stats)
	properties.AppendSubType(in.GetSubJobType().String())
	properties.AppendTaskSlot(in.GetTaskSlot())
	properties.AppendNumRows(in.GetNumRows())
	properties.AppendTaskVersion(in.GetTaskVersion())
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) QueryStats(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.StatsResults, error) {
	reqProperties := taskcommon.NewProperties(nil)
	reqProperties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	reqProperties.AppendTaskID(in.GetTaskIDs()[0])
	reqProperties.AppendType(taskcommon.Stats)
	resp, err := c.queryTask(nodeID, reqProperties)
	if err != nil {
		return nil, err
	}

	resProperties := taskcommon.NewProperties(resp.GetProperties())
	state, err := resProperties.GetTaskState()
	if err != nil {
		return nil, err
	}
	reason := resProperties.GetTaskReason()
	switch state {
	case taskcommon.None, taskcommon.Init, taskcommon.InProgress, taskcommon.Retry:
		return &workerpb.StatsResults{
			Results: []*workerpb.StatsResult{
				{
					TaskID:     in.GetTaskIDs()[0],
					State:      state,
					FailReason: reason,
				},
			},
		}, nil
	case taskcommon.Finished, taskcommon.Failed:
		result := &workerpb.QueryJobsV2Response{}
		err = proto.Unmarshal(resp.GetPayload(), result)
		if err != nil {
			return nil, err
		}
		return result.GetStatsJobResults(), nil
	default:
		panic("should not happen")
	}
}

func (c *cluster) DropStats(nodeID int64, taskID int64) error {
	properties := taskcommon.NewProperties(nil)
	properties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	properties.AppendTaskID(taskID)
	properties.AppendType(taskcommon.Stats)
	return c.dropTask(nodeID, properties)
}

func (c *cluster) CreateAnalyze(nodeID int64, in *workerpb.AnalyzeRequest) error {
	properties := taskcommon.NewProperties(nil)
	properties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	properties.AppendTaskID(in.GetTaskID())
	properties.AppendType(taskcommon.Analyze)
	properties.AppendTaskSlot(in.GetTaskSlot())
	properties.AppendTaskVersion(in.GetVersion())
	return c.createTask(nodeID, in, properties)
}

func (c *cluster) QueryAnalyze(nodeID int64, in *workerpb.QueryJobsRequest) (*workerpb.AnalyzeResults, error) {
	reqProperties := taskcommon.NewProperties(nil)
	reqProperties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	reqProperties.AppendTaskID(in.GetTaskIDs()[0])
	reqProperties.AppendType(taskcommon.Analyze)
	resp, err := c.queryTask(nodeID, reqProperties)
	if err != nil {
		return nil, err
	}

	resProperties := taskcommon.NewProperties(resp.GetProperties())
	state, err := resProperties.GetTaskState()
	if err != nil {
		return nil, err
	}
	reason := resProperties.GetTaskReason()
	switch state {
	case taskcommon.None, taskcommon.Init, taskcommon.InProgress, taskcommon.Retry:
		return &workerpb.AnalyzeResults{
			Results: []*workerpb.AnalyzeResult{
				{
					TaskID:     in.GetTaskIDs()[0],
					State:      state,
					FailReason: reason,
				},
			},
		}, nil
	case taskcommon.Finished, taskcommon.Failed:
		result := &workerpb.QueryJobsV2Response{}
		err = proto.Unmarshal(resp.GetPayload(), result)
		if err != nil {
			return nil, err
		}
		return result.GetAnalyzeJobResults(), nil
	default:
		panic("should not happen")
	}
}

func (c *cluster) DropAnalyze(nodeID int64, taskID int64) error {
	properties := taskcommon.NewProperties(nil)
	properties.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	properties.AppendTaskID(taskID)
	properties.AppendType(taskcommon.Analyze)
	return c.dropTask(nodeID, properties)
}
