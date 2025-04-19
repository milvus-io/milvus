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
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/task"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type analyzeTask struct {
	*indexpb.AnalyzeTask

	queueTime time.Time
	startTime time.Time
	endTime   time.Time

	meta *meta
}

var _ globalTask.Task = (*analyzeTask)(nil)

func newAnalyzeTask(task *indexpb.AnalyzeTask, meta *meta) *analyzeTask {
	return &analyzeTask{
		AnalyzeTask: task,
		meta:        meta,
	}
}

func (at *analyzeTask) GetTaskID() int64 {
	return at.TaskID
}

func (at *analyzeTask) GetNodeID() int64 {
	return at.NodeID
}

func (at *analyzeTask) SetQueueTime(t time.Time) {
	at.queueTime = t
}

func (at *analyzeTask) GetQueueTime() time.Time {
	return at.queueTime
}

func (at *analyzeTask) SetStartTime(t time.Time) {
	at.startTime = t
}

func (at *analyzeTask) GetStartTime() time.Time {
	return at.startTime
}

func (at *analyzeTask) SetEndTime(t time.Time) {
	at.endTime = t
}

func (at *analyzeTask) GetEndTime() time.Time {
	return at.endTime
}

func (at *analyzeTask) GetTaskType() task.Type {
	return task.Analyze
}

func (at *analyzeTask) GetTaskState() task.State {
	return task.State(at.State)
}

func (at *analyzeTask) GetTaskSlot() int64 {
	return Params.DataCoordCfg.AnalyzeTaskSlotUsage.GetAsInt64()
}

func (at *analyzeTask) SetState(state indexpb.JobState, failReason string) {
	at.State = state
	at.FailReason = failReason
}

func (at *analyzeTask) UpdateStateWithMeta(state indexpb.JobState, failReason string) error {
	if err := at.meta.analyzeMeta.UpdateState(at.GetTaskID(), state, failReason); err != nil {
		return err
	}
	at.SetState(state, failReason)
	return nil
}

func (at *analyzeTask) UpdateVersion(nodeID int64) error {
	if err := at.meta.analyzeMeta.UpdateVersion(at.GetTaskID(), nodeID); err != nil {
		return err
	}
	at.Version++
	at.NodeID = nodeID
	return nil
}

func (at *analyzeTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	log := log.Ctx(context.TODO()).With(zap.Int64("taskID", at.GetTaskID()))

	// Check if task still exists in meta
	task := at.meta.analyzeMeta.GetTask(at.GetTaskID())
	if task == nil {
		log.Info("analyze task has not exist in meta table, remove task")
		at.SetState(indexpb.JobState_JobStateNone, "analyze task has not exist in meta table")
		return
	}

	// Update task version
	if err := at.UpdateVersion(nodeID); err != nil {
		log.Warn("failed to update task version", zap.Error(err))
		return
	}
	req := &workerpb.AnalyzeRequest{
		ClusterID:     Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskID:        at.GetTaskID(),
		CollectionID:  task.CollectionID,
		PartitionID:   task.PartitionID,
		FieldID:       task.FieldID,
		FieldName:     task.FieldName,
		FieldType:     task.FieldType,
		Dim:           task.Dim,
		SegmentStats:  make(map[int64]*indexpb.SegmentStats),
		Version:       task.Version + 1,
		StorageConfig: createStorageConfig(),
	}

	var err error
	defer func() {
		if err != nil {
			log.Warn("assign analyze task to worker failed, try drop task on worker", zap.Error(err))
			at.DropTaskOnWorker(cluster)
		}
	}()

	err = cluster.CreateAnalyze(nodeID, req)
	if err != nil {
		log.Warn("assign analyze task to worker failed", zap.Error(err))
		return
	}

	log.Info("analyze task assigned successfully")
	if err = at.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); err != nil {
		log.Warn("failed to update task state to inProgress", zap.Error(err))
	}
	log.Info("update task state to inProgress successfully")
}

func (at *analyzeTask) QueryTaskOnWorker(cluster session.Cluster) {
	log := log.Ctx(context.TODO()).With(
		zap.Int64("taskID", at.GetTaskID()),
		zap.Int64("nodeID", at.NodeID),
	)

	result, err := cluster.QueryAnalyze(at.NodeID, &workerpb.QueryJobsRequest{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []int64{at.GetTaskID()},
	})
	if err != nil {
		log.Warn("query analyze task result from worker failed", zap.Error(err))
		if errors.Is(err, merr.ErrNodeNotFound) {
			// try to set task to init
			at.UpdateStateWithMeta(indexpb.JobState_JobStateInit, err.Error())
		}
		return
	}

	// Process query results
	at.processQueryResults(result.GetResults())
}

// Process query results helper
func (at *analyzeTask) processQueryResults(results []*workerpb.AnalyzeResult) {
	log := log.Ctx(context.TODO()).With(zap.Int64("taskID", at.GetTaskID()))
	for _, result := range results {
		if result.GetTaskID() != at.GetTaskID() {
			continue
		}

		state := result.GetState()
		log.Debug("query analyze task result success",
			zap.String("state", state.String()),
			zap.String("failReason", result.GetFailReason()))

		// Handle different task states
		switch state {
		case indexpb.JobState_JobStateFinished, indexpb.JobState_JobStateFailed, indexpb.JobState_JobStateRetry:
			// Save the result
			if err := at.meta.analyzeMeta.FinishTask(at.GetTaskID(), result); err != nil {
				log.Warn("failed to finish analyze task", zap.Error(err))
			} else {
				at.SetState(state, result.GetFailReason())
			}
		case indexpb.JobState_JobStateNone:
			at.UpdateStateWithMeta(indexpb.JobState_JobStateRetry, "analyze task state is none in info response")
		}
		// Otherwise (inProgress or unissued/init), keep current state
		return
	}

	// Task not found in results, try to set task to init
	log.Warn("query analyze task info failed, worker does not have task info")
	at.UpdateStateWithMeta(indexpb.JobState_JobStateInit, "analyze result is not in info response")
}

func (at *analyzeTask) DropTaskOnWorker(cluster session.Cluster) {
	log := log.Ctx(context.TODO()).With(
		zap.Int64("taskID", at.GetTaskID()),
		zap.Int64("nodeID", at.NodeID),
	)

	if err := cluster.DropAnalyze(at.NodeID, &workerpb.DropJobsRequest{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []int64{at.GetTaskID()},
	}); err != nil {
		log.Warn("failed to drop analyze task on worker", zap.Error(err))
		return
	}

	log.Info("dropped analyze task on worker successfully")
}
