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
	"time"

	"go.uber.org/zap"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type statsTask struct {
	*indexpb.StatsTask

	taskSlot int64

	times *taskcommon.Times

	meta                *meta
	handler             Handler
	allocator           allocator.Allocator
	compactionInspector CompactionInspector
	ievm                IndexEngineVersionManager
}

var _ globalTask.Task = (*statsTask)(nil)

func newStatsTask(t *indexpb.StatsTask,
	taskSlot int64,
	mt *meta,
	inspector CompactionInspector,
	handler Handler,
	allocator allocator.Allocator,
	ievm IndexEngineVersionManager,
) *statsTask {
	return &statsTask{
		StatsTask:           t,
		taskSlot:            taskSlot,
		times:               taskcommon.NewTimes(),
		meta:                mt,
		handler:             handler,
		allocator:           allocator,
		compactionInspector: inspector,
		ievm:                ievm,
	}
}

func (st *statsTask) GetTaskID() int64 {
	return st.TaskID
}

func (st *statsTask) GetTaskType() taskcommon.Type {
	return taskcommon.Stats
}

func (st *statsTask) GetTaskState() taskcommon.State {
	return st.GetState()
}

func (st *statsTask) GetTaskSlot() int64 {
	return st.taskSlot
}

func (st *statsTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	st.times.SetTaskTime(timeType, time)
}

func (st *statsTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(st.times)
}

func (st *statsTask) SetState(state indexpb.JobState, failReason string) {
	st.State = state
	st.FailReason = failReason
}

func (st *statsTask) UpdateStateWithMeta(state indexpb.JobState, failReason string) error {
	if err := st.meta.statsTaskMeta.UpdateTaskState(st.GetTaskID(), state, failReason); err != nil {
		log.Warn("update stats task state failed", zap.Int64("taskID", st.GetTaskID()),
			zap.String("state", state.String()), zap.String("failReason", failReason),
			zap.Error(err))
		return err
	}
	st.SetState(state, failReason)
	return nil
}

func (st *statsTask) UpdateTaskVersion(nodeID int64) error {
	if err := st.meta.statsTaskMeta.UpdateVersion(st.GetTaskID(), nodeID); err != nil {
		return err
	}
	st.Version++
	st.NodeID = nodeID
	return nil
}

func (st *statsTask) resetTask(ctx context.Context, reason string) {
	// reset isCompacting
	st.meta.SetSegmentsCompacting(ctx, []UniqueID{st.GetSegmentID()}, false)
	st.meta.SetSegmentStating(st.GetSegmentID(), false)

	// reset state to init
	st.UpdateStateWithMeta(indexpb.JobState_JobStateInit, reason)
}

func (st *statsTask) dropAndResetTaskOnWorker(ctx context.Context, cluster session.Cluster, reason string) {
	if err := st.tryDropTaskOnWorker(cluster); err != nil {
		return
	}
	st.resetTask(ctx, reason)
}

func (st *statsTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx, cancel := context.WithTimeout(context.Background(), Params.DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second))
	defer cancel()

	log := log.Ctx(ctx).With(
		zap.Int64("taskID", st.GetTaskID()),
		zap.Int64("segmentID", st.GetSegmentID()),
		zap.Int64("targetSegmentID", st.GetTargetSegmentID()),
		zap.String("subJobType", st.GetSubJobType().String()),
	)

	// Check segment compaction state
	if exist, canCompact := st.meta.CheckAndSetSegmentsCompacting(ctx, []UniqueID{st.GetSegmentID()}); !exist || !canCompact {
		log.Warn("segment is not exist or is compacting, skip stats and remove stats task",
			zap.Bool("exist", exist), zap.Bool("canCompact", canCompact))

		if err := st.meta.statsTaskMeta.DropStatsTask(ctx, st.GetTaskID()); err != nil {
			log.Warn("remove stats task failed, will retry later", zap.Error(err))
			return
		}
		st.SetState(indexpb.JobState_JobStateNone, "segment is not exist or is compacting")
		return
	}

	// Check if segment is part of L0 compaction
	if !st.compactionInspector.checkAndSetSegmentStating(st.GetInsertChannel(), st.GetSegmentID()) {
		log.Warn("segment is contained by L0 compaction, skipping stats task")
		// Reset compaction flag
		st.meta.SetSegmentsCompacting(ctx, []UniqueID{st.GetSegmentID()}, false)
		return
	}

	var err error
	defer func() {
		if err != nil {
			// reset compaction flag and stating flag
			st.resetTask(ctx, err.Error())
		}
	}()

	// Handle empty segment case
	segment := st.meta.GetHealthySegment(ctx, st.GetSegmentID())
	if segment == nil {
		log.Warn("segment is not healthy, skipping stats task")
		if err := st.meta.statsTaskMeta.DropStatsTask(ctx, st.GetTaskID()); err != nil {
			log.Warn("remove stats task failed, will retry later", zap.Error(err))
			return
		}
		st.SetState(indexpb.JobState_JobStateNone, "segment is not healthy")
		return
	}

	if segment.GetNumOfRows() == 0 {
		if err := st.handleEmptySegment(ctx); err != nil {
			log.Warn("failed to handle empty segment", zap.Error(err))
		}
		return
	}

	// Update task version
	if err := st.UpdateTaskVersion(nodeID); err != nil {
		log.Warn("failed to update stats task version", zap.Error(err))
		return
	}

	// Prepare request
	req, err := st.prepareJobRequest(ctx, segment)
	if err != nil {
		log.Warn("failed to prepare stats request", zap.Error(err))
		return
	}

	// Use defer for cleanup on error
	defer func() {
		if err != nil {
			st.tryDropTaskOnWorker(cluster)
		}
	}()
	// Execute task creation
	if err = cluster.CreateStats(nodeID, req); err != nil {
		log.Warn("failed to create stats task on worker", zap.Error(err))
		return
	}
	log.Info("assign stats task to worker successfully", zap.Int64("taskID", st.GetTaskID()))

	if err = st.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); err != nil {
		log.Warn("failed to update stats task state to InProgress", zap.Error(err))
		return
	}

	log.Info("stats task update state to InProgress successfully", zap.Int64("task version", st.GetVersion()))
}

func (st *statsTask) QueryTaskOnWorker(cluster session.Cluster) {
	ctx := context.TODO()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", st.GetTaskID()),
		zap.Int64("segmentID", st.GetSegmentID()),
		zap.Int64("nodeID", st.NodeID),
	)

	// Query task status
	results, err := cluster.QueryStats(st.NodeID, &workerpb.QueryJobsRequest{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []int64{st.GetTaskID()},
	})
	if err != nil {
		log.Warn("query stats task result failed", zap.Error(err))
		st.dropAndResetTaskOnWorker(ctx, cluster, err.Error())
		return
	}

	// Process query results
	for _, result := range results.GetResults() {
		if result.GetTaskID() != st.GetTaskID() {
			continue
		}

		state := result.GetState()
		// Handle different task states
		switch state {
		case indexpb.JobState_JobStateFinished:
			if err := st.SetJobInfo(ctx, result); err != nil {
				return
			}
			st.UpdateStateWithMeta(state, result.GetFailReason())
		case indexpb.JobState_JobStateRetry, indexpb.JobState_JobStateNone:
			st.dropAndResetTaskOnWorker(ctx, cluster, result.GetFailReason())
		case indexpb.JobState_JobStateFailed:
			st.UpdateStateWithMeta(state, result.GetFailReason())
		}
		// Otherwise (inProgress or unissued/init), keep InProgress state
		return
	}

	log.Warn("task not found in results")
	st.resetTask(ctx, "task not found in results")
}

func (st *statsTask) tryDropTaskOnWorker(cluster session.Cluster) error {
	log := log.Ctx(context.TODO()).With(
		zap.Int64("taskID", st.GetTaskID()),
		zap.Int64("segmentID", st.GetSegmentID()),
		zap.Int64("nodeID", st.NodeID),
	)

	err := cluster.DropStats(st.NodeID, st.GetTaskID())
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		log.Warn("failed to drop stats task on worker", zap.Error(err))
		return err
	}

	log.Info("stats task dropped successfully")
	return nil
}

func (st *statsTask) DropTaskOnWorker(cluster session.Cluster) {
	st.tryDropTaskOnWorker(cluster)
}

// Helper for empty segment handling
func (st *statsTask) handleEmptySegment(ctx context.Context) error {
	result := &workerpb.StatsResult{
		TaskID:       st.GetTaskID(),
		State:        st.GetState(),
		FailReason:   st.GetFailReason(),
		CollectionID: st.GetCollectionID(),
		PartitionID:  st.GetPartitionID(),
		SegmentID:    st.GetSegmentID(),
		Channel:      st.GetInsertChannel(),
		NumRows:      0,
	}

	if err := st.SetJobInfo(ctx, result); err != nil {
		return err
	}

	if err := st.UpdateStateWithMeta(indexpb.JobState_JobStateFinished, "segment num row is zero"); err != nil {
		return err
	}

	return nil
}

// Prepare the stats request
func (st *statsTask) prepareJobRequest(ctx context.Context, segment *SegmentInfo) (*workerpb.CreateStatsRequest, error) {
	collInfo, err := st.handler.GetCollection(ctx, segment.GetCollectionID())
	if err != nil || collInfo == nil {
		return nil, fmt.Errorf("failed to get collection info: %w", err)
	}

	collTtl, err := getCollectionTTL(collInfo.Properties)
	if err != nil {
		return nil, fmt.Errorf("failed to get collection TTL: %w", err)
	}

	// Calculate binlog allocation
	binlogNum := (segment.getSegmentSize()/Params.DataNodeCfg.BinLogMaxSize.GetAsInt64() + 1) *
		int64(len(collInfo.Schema.GetFields())) *
		paramtable.Get().DataCoordCfg.CompactionPreAllocateIDExpansionFactor.GetAsInt64()

	// Allocate IDs
	start, end, err := st.allocator.AllocN(binlogNum + int64(len(collInfo.Schema.GetFunctions())) + 1)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate log IDs: %w", err)
	}

	// Create the request
	req := &workerpb.CreateStatsRequest{
		ClusterID:       Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskID:          st.GetTaskID(),
		CollectionID:    segment.GetCollectionID(),
		PartitionID:     segment.GetPartitionID(),
		InsertChannel:   segment.GetInsertChannel(),
		SegmentID:       segment.GetID(),
		StorageConfig:   createStorageConfig(),
		Schema:          collInfo.Schema,
		SubJobType:      st.GetSubJobType(),
		TargetSegmentID: st.GetTargetSegmentID(),
		InsertLogs:      segment.GetBinlogs(),
		DeltaLogs:       segment.GetDeltalogs(),
		StartLogID:      start,
		EndLogID:        end,
		NumRows:         segment.GetNumOfRows(),
		CollectionTtl:   collTtl.Nanoseconds(),
		CurrentTs:       tsoutil.GetCurrentTime(),
		// update version after check
		TaskVersion:               st.GetVersion(),
		BinlogMaxSize:             Params.DataNodeCfg.BinLogMaxSize.GetAsUint64(),
		EnableJsonKeyStats:        Params.CommonCfg.EnabledJSONKeyStats.GetAsBool(),
		JsonKeyStatsTantivyMemory: Params.DataCoordCfg.JSONKeyStatsMemoryBudgetInTantivy.GetAsInt64(),
		JsonKeyStatsDataFormat:    1,
		EnableJsonKeyStatsInSort:  Params.DataCoordCfg.EnabledJSONKeyStatsInSort.GetAsBool(),
		TaskSlot:                  st.taskSlot,
		StorageVersion:            segment.StorageVersion,
		CurrentScalarIndexVersion: st.ievm.GetCurrentScalarIndexEngineVersion(),
	}

	return req, nil
}

func (st *statsTask) SetJobInfo(ctx context.Context, result *workerpb.StatsResult) error {
	switch st.GetSubJobType() {
	case indexpb.StatsSubJob_Sort:
		// first update segment, failed state cannot generate new segment
		metricMutation, err := st.meta.SaveStatsResultSegment(st.GetSegmentID(), result)
		if err != nil {
			log.Ctx(ctx).Warn("save sort stats result failed", zap.Int64("taskID", st.GetTaskID()),
				zap.Int64("segmentID", st.GetSegmentID()), zap.Error(err))
			return err
		}
		metricMutation.commit()

		select {
		case getBuildIndexChSingleton() <- result.GetSegmentID():
		default:
		}
	case indexpb.StatsSubJob_TextIndexJob:
		err := st.meta.UpdateSegment(st.GetSegmentID(), SetTextIndexLogs(result.GetTextStatsLogs()))
		if err != nil {
			log.Ctx(ctx).Warn("save text index stats result failed", zap.Int64("taskID", st.GetTaskID()),
				zap.Int64("segmentID", st.GetSegmentID()), zap.Error(err))
			return err
		}
	case indexpb.StatsSubJob_BM25Job:
		// bm25 logs are generated during with segment flush.
	}

	log.Ctx(ctx).Info("SetJobInfo for stats task success", zap.Int64("taskID", st.GetTaskID()),
		zap.Int64("oldSegmentID", st.GetSegmentID()), zap.Int64("targetSegmentID", st.GetTargetSegmentID()),
		zap.String("subJobType", st.GetSubJobType().String()), zap.String("state", st.GetState().String()))
	return nil
}
