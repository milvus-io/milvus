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
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newCompactionTaskStats(task *datapb.CompactionTask) *metricsinfo.CompactionTask {
	return &metricsinfo.CompactionTask{
		PlanID:       task.PlanID,
		CollectionID: task.CollectionID,
		Type:         task.Type.String(),
		State:        task.State.String(),
		FailReason:   task.FailReason,
		StartTime:    typeutil.TimestampToString(uint64(task.StartTime) * 1000),
		EndTime:      typeutil.TimestampToString(uint64(task.EndTime) * 1000),
		TotalRows:    task.TotalRows,
		InputSegments: lo.Map(task.InputSegments, func(t int64, i int) string {
			return strconv.FormatInt(t, 10)
		}),
		ResultSegments: lo.Map(task.ResultSegments, func(t int64, i int) string {
			return strconv.FormatInt(t, 10)
		}),
		NodeID: task.NodeID,
	}
}

type compactionTaskMeta struct {
	sync.RWMutex
	ctx     context.Context
	catalog metastore.DataCoordCatalog
	// currently only clustering compaction task is stored in persist meta
	compactionTasks map[int64]map[int64]*datapb.CompactionTask // triggerID -> planID
	taskStats       *expirable.LRU[UniqueID, *metricsinfo.CompactionTask]
}

func newCompactionTaskMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*compactionTaskMeta, error) {
	csm := &compactionTaskMeta{
		RWMutex:         sync.RWMutex{},
		ctx:             ctx,
		catalog:         catalog,
		compactionTasks: make(map[int64]map[int64]*datapb.CompactionTask, 0),
		taskStats:       expirable.NewLRU[UniqueID, *metricsinfo.CompactionTask](512, nil, time.Minute*15),
	}
	if err := csm.reloadFromKV(); err != nil {
		return nil, err
	}
	return csm, nil
}

func (csm *compactionTaskMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("compactionTaskMeta-reloadFromKV")
	compactionTasks, err := csm.catalog.ListCompactionTask(csm.ctx)
	if err != nil {
		return err
	}
	for _, task := range compactionTasks {
		// Compatibility handling: for milvus ≤v2.4, since compaction task has no PreAllocatedSegmentIDs field,
		// here we just mark the task as failed and wait for the compaction trigger to generate a new one.
		//
		// NOTE:
		// - Only compaction tasks that require pre-allocated segment IDs should be marked
		//   as failed when PreAllocatedSegmentIDs is nil.
		// - Level0DeleteCompaction tasks never use PreAllocatedSegmentIDs and must be ignored here,
		//   otherwise unfinished L0 delete compaction tasks created before upgrade will be
		//   incorrectly marked as failed on reload.
		if !isCompactionTaskFinished(task) &&
			task.PreAllocatedSegmentIDs == nil &&
			task.GetType() != datapb.CompactionType_Level0DeleteCompaction {
			mlog.Warn(csm.ctx, "PreAllocatedSegmentIDs is nil, mark the task as failed",
				mlog.FieldTaskID(task.GetPlanID()),
				mlog.String("type", task.GetType().String()),
				mlog.String("originalState", task.State.String()),
			)
			task.State = datapb.CompactionTaskState_failed
			task.FailReason = fmt.Sprintf("PreAllocatedSegmentIDs is nil, taskID: %v", task.GetPlanID())
		}
		csm.saveCompactionTaskMemory(task)
	}
	mlog.Info(csm.ctx, "DataCoord compactionTaskMeta reloadFromKV done", mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

// GetCompactionTasks returns clustering compaction tasks from local cache
func (csm *compactionTaskMeta) GetCompactionTasks() map[int64][]*datapb.CompactionTask {
	csm.RLock()
	defer csm.RUnlock()
	res := make(map[int64][]*datapb.CompactionTask, 0)
	for triggerID, tasks := range csm.compactionTasks {
		triggerTasks := make([]*datapb.CompactionTask, 0)
		for _, task := range tasks {
			triggerTasks = append(triggerTasks, proto.Clone(task).(*datapb.CompactionTask))
		}
		res[triggerID] = triggerTasks
	}
	return res
}

func (csm *compactionTaskMeta) GetCompactionTasksByCollection(collectionID int64) map[int64][]*datapb.CompactionTask {
	csm.RLock()
	defer csm.RUnlock()
	res := make(map[int64][]*datapb.CompactionTask, 0)
	for _, tasks := range csm.compactionTasks {
		for _, task := range tasks {
			if task.CollectionID == collectionID {
				_, exist := res[task.TriggerID]
				if !exist {
					res[task.TriggerID] = make([]*datapb.CompactionTask, 0)
				}
				res[task.TriggerID] = append(res[task.TriggerID], proto.Clone(task).(*datapb.CompactionTask))
			}
		}
	}
	return res
}

func (csm *compactionTaskMeta) GetCompactionTasksByTriggerID(triggerID int64) []*datapb.CompactionTask {
	csm.RLock()
	defer csm.RUnlock()
	res := make([]*datapb.CompactionTask, 0)
	tasks, triggerIDExist := csm.compactionTasks[triggerID]
	if triggerIDExist {
		for _, task := range tasks {
			res = append(res, proto.Clone(task).(*datapb.CompactionTask))
		}
	}
	return res
}

// compactionTaskDigest is the part of a compaction task a caller reads to
// follow its progress. Copied out of compaction meta without cloning the task,
// which carries the full collection schema.
type compactionTaskDigest struct {
	PlanID        int64
	Type          datapb.CompactionType
	State         datapb.CompactionTaskState
	InputSegments []int64
	StartTime     int64
	EndTime       int64
}

// GetCompactionTaskDigestsByTriggerID returns the digests of every task of a
// trigger. Unlike GetCompactionTasksByTriggerID it deep-copies nothing but the
// input id lists: a stored task is replaced on every save, never changed in
// place, so its scalar fields are read as they are under the lock.
func (csm *compactionTaskMeta) GetCompactionTaskDigestsByTriggerID(triggerID int64) []compactionTaskDigest {
	csm.RLock()
	defer csm.RUnlock()
	tasks := csm.compactionTasks[triggerID]
	res := make([]compactionTaskDigest, 0, len(tasks))
	for _, task := range tasks {
		res = append(res, compactionTaskDigest{
			PlanID:        task.GetPlanID(),
			Type:          task.GetType(),
			State:         task.GetState(),
			InputSegments: slices.Clone(task.GetInputSegments()),
			StartTime:     task.GetStartTime(),
			EndTime:       task.GetEndTime(),
		})
	}
	return res
}

func (csm *compactionTaskMeta) SaveCompactionTask(ctx context.Context, task *datapb.CompactionTask) error {
	csm.Lock()
	defer csm.Unlock()
	if err := csm.catalog.SaveCompactionTask(ctx, task); err != nil {
		mlog.Error(ctx, "meta update: update compaction task fail", mlog.Err(err))
		return err
	}
	csm.saveCompactionTaskMemory(task)
	return nil
}

func (csm *compactionTaskMeta) saveCompactionTaskMemory(task *datapb.CompactionTask) {
	_, triggerIDExist := csm.compactionTasks[task.TriggerID]
	if !triggerIDExist {
		csm.compactionTasks[task.TriggerID] = make(map[int64]*datapb.CompactionTask, 0)
	}
	csm.compactionTasks[task.TriggerID][task.PlanID] = task
	csm.taskStats.Add(task.PlanID, newCompactionTaskStats(task))
}

func (csm *compactionTaskMeta) DropCompactionTask(ctx context.Context, task *datapb.CompactionTask) error {
	csm.Lock()
	defer csm.Unlock()
	if err := csm.catalog.DropCompactionTask(ctx, task); err != nil {
		mlog.Error(ctx, "meta update: drop compaction task fail", mlog.Int64("triggerID", task.TriggerID), mlog.Int64("planID", task.PlanID), mlog.FieldCollectionID(task.CollectionID), mlog.Err(err))
		return err
	}
	_, triggerIDExist := csm.compactionTasks[task.TriggerID]
	if triggerIDExist {
		delete(csm.compactionTasks[task.TriggerID], task.PlanID)
	}
	if len(csm.compactionTasks[task.TriggerID]) == 0 {
		delete(csm.compactionTasks, task.TriggerID)
	}
	return nil
}

func (csm *compactionTaskMeta) TaskStatsJSON() string {
	tasks := csm.taskStats.Values()
	ret, err := json.Marshal(tasks)
	if err != nil {
		return ""
	}
	return string(ret)
}
