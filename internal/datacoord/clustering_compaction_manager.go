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
	"path"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ClusteringCompactionManager struct {
	ctx               context.Context
	meta              *meta
	allocator         allocator
	compactionHandler compactionPlanContext
	scheduler         Scheduler
	analyzeScheduler  *taskScheduler
	handler           Handler
	quit              chan struct{}
	wg                sync.WaitGroup
}

const TaskMaxRetryTimes int32 = 3

type ClusteringCompactionSummary struct {
	state         commonpb.CompactionState
	executingCnt  int
	pipeliningCnt int
	completedCnt  int
	failedCnt     int
	timeoutCnt    int
	initCnt       int
	analyzingCnt  int
	analyzedCnt   int
	indexingCnt   int
	indexedCnt    int
	cleanedCnt    int
}

func newClusteringCompactionManager(
	ctx context.Context,
	meta *meta,
	allocator allocator,
	compactionHandler compactionPlanContext,
	analyzeScheduler *taskScheduler,
	handler Handler,
) *ClusteringCompactionManager {
	return &ClusteringCompactionManager{
		ctx:               ctx,
		meta:              meta,
		allocator:         allocator,
		compactionHandler: compactionHandler,
		analyzeScheduler:  analyzeScheduler,
		handler:           handler,
	}
}

func (t *ClusteringCompactionManager) start() {
	triggers := t.meta.GetClusteringCompactionTasks()
	for _, tasks := range triggers {
		for _, task := range tasks {
			if task.State != datapb.CompactionTaskState_indexed && task.State == datapb.CompactionTaskState_cleaned {
				// set segments state compacting
				for _, segment := range task.InputSegments {
					t.meta.SetSegmentCompacting(segment, true)
				}
			}
		}
	}
	t.quit = make(chan struct{})
	t.wg.Add(2)
	go t.startJobCheckLoop()
	go t.startGCLoop()
}

func (t *ClusteringCompactionManager) stop() {
	close(t.quit)
	t.wg.Wait()
}

func (t *ClusteringCompactionManager) submit(tasks []*datapb.CompactionTask) error {
	log.Info("Insert clustering compaction tasks", zap.Int64("triggerID", tasks[0].TriggerID), zap.Int64("collectionID", tasks[0].CollectionID), zap.Int("task_num", len(tasks)))
	currentID, _, err := t.allocator.allocN(int64(2 * len(tasks)))
	if err != nil {
		return err
	}
	for _, task := range tasks {
		task.PlanID = currentID
		currentID++
		task.AnalyzeTaskID = currentID
		currentID++
		err := t.saveTask(task)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *ClusteringCompactionManager) getByTriggerId(triggerID int64) []*datapb.CompactionTask {
	return t.meta.GetClusteringCompactionTasksByTriggerID(triggerID)
}

func (t *ClusteringCompactionManager) startJobCheckLoop() {
	defer logutil.LogPanic()
	defer t.wg.Done()
	ticker := time.NewTicker(paramtable.Get().DataCoordCfg.ClusteringCompactionStateCheckInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-t.quit:
			log.Info("clustering compaction loop exit")
			return
		case <-ticker.C:
			err := t.processAllTasks()
			if err != nil {
				log.Warn("unable to triggerClusteringCompaction", zap.Error(err))
			}
			ticker.Reset(paramtable.Get().DataCoordCfg.ClusteringCompactionStateCheckInterval.GetAsDuration(time.Second))
		}
	}
}

func (t *ClusteringCompactionManager) startGCLoop() {
	defer logutil.LogPanic()
	defer t.wg.Done()
	ticker := time.NewTicker(paramtable.Get().DataCoordCfg.ClusteringCompactionGCInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-t.quit:
			log.Info("clustering compaction gc loop exit")
			return
		case <-ticker.C:
			err := t.gc()
			if err != nil {
				log.Warn("fail to gc", zap.Error(err))
			}
			ticker.Reset(paramtable.Get().DataCoordCfg.ClusteringCompactionGCInterval.GetAsDuration(time.Second))
		}
	}
}

func (t *ClusteringCompactionManager) gcPartitionStatsInfo(info *datapb.PartitionStatsInfo) error {
	removePaths := make([]string, 0)
	partitionStatsPath := path.Join(t.meta.chunkManager.RootPath(), common.PartitionStatsPath,
		metautil.JoinIDPath(info.CollectionID, info.PartitionID),
		info.GetVChannel(), strconv.FormatInt(info.GetVersion(), 10))
	removePaths = append(removePaths, partitionStatsPath)
	analyzeT := t.meta.analyzeMeta.GetTask(info.GetAnalyzeTaskID())
	if analyzeT != nil {
		centroidsFilePath := path.Join(t.meta.chunkManager.RootPath(), common.AnalyzeStatsPath,
			metautil.JoinIDPath(analyzeT.GetTaskID(), analyzeT.GetVersion(), analyzeT.GetCollectionID(),
				analyzeT.GetPartitionID(), analyzeT.GetFieldID()),
			"centroids",
		)
		removePaths = append(removePaths, centroidsFilePath)
		for _, segID := range info.GetSegmentIDs() {
			segmentOffsetMappingFilePath := path.Join(t.meta.chunkManager.RootPath(), common.AnalyzeStatsPath,
				metautil.JoinIDPath(analyzeT.GetTaskID(), analyzeT.GetVersion(), analyzeT.GetCollectionID(),
					analyzeT.GetPartitionID(), analyzeT.GetFieldID(), segID),
				"offset_mapping",
			)
			removePaths = append(removePaths, segmentOffsetMappingFilePath)
		}
	}

	log.Debug("remove clustering compaction stats files",
		zap.Int64("collectionID", info.GetCollectionID()),
		zap.Int64("partitionID", info.GetPartitionID()),
		zap.String("vChannel", info.GetVChannel()),
		zap.Int64("planID", info.GetVersion()),
		zap.Strings("removePaths", removePaths))
	err := t.meta.chunkManager.MultiRemove(t.ctx, removePaths)
	if err != nil {
		log.Warn("remove clustering compaction stats files failed", zap.Error(err))
		return err
	}

	// first clean analyze task
	if err = t.meta.analyzeMeta.DropAnalyzeTask(info.GetAnalyzeTaskID()); err != nil {
		log.Warn("remove analyze task failed", zap.Int64("analyzeTaskID", info.GetAnalyzeTaskID()), zap.Error(err))
		return err
	}

	// finally clean up the partition stats info, and make sure the analysis task is cleaned up
	err = t.meta.DropPartitionStatsInfo(info)
	log.Debug("drop partition stats meta",
		zap.Int64("collectionID", info.GetCollectionID()),
		zap.Int64("partitionID", info.GetPartitionID()),
		zap.String("vChannel", info.GetVChannel()),
		zap.Int64("planID", info.GetVersion()))
	if err != nil {
		return err
	}
	return nil
}

func (t *ClusteringCompactionManager) gc() error {
	log.Debug("start gc clustering compaction related meta and files")
	// gc clustering compaction tasks
	triggers := t.meta.GetClusteringCompactionTasks()
	checkTaskFunc := func(task *datapb.CompactionTask) {
		// indexed is the final state of a clustering compaction task
		if task.State == datapb.CompactionTaskState_indexed || task.State == datapb.CompactionTaskState_cleaned {
			if time.Since(tsoutil.PhysicalTime(task.StartTime)) > Params.DataCoordCfg.ClusteringCompactionDropTolerance.GetAsDuration(time.Second) {
				// skip handle this error, try best to delete meta
				err := t.dropTask(task)
				if err != nil {
					log.Warn("fail to drop task", zap.Int64("taskPlanID", task.PlanID), zap.Error(err))
				}
			}
		}
	}
	for _, tasks := range triggers {
		for _, task := range tasks {
			checkTaskFunc(task)
		}
	}
	// gc partition stats
	channelPartitionStatsInfos := make(map[string][]*datapb.PartitionStatsInfo)
	unusedPartStats := make([]*datapb.PartitionStatsInfo, 0)
	for _, partitionStatsInfo := range t.meta.partitionStatsInfos {
		collInfo := t.meta.GetCollection(partitionStatsInfo.GetCollectionID())
		if collInfo == nil {
			unusedPartStats = append(unusedPartStats, partitionStatsInfo)
			continue
		}
		channel := fmt.Sprintf("%d/%d/%s", partitionStatsInfo.CollectionID, partitionStatsInfo.PartitionID, partitionStatsInfo.VChannel)
		if _, ok := channelPartitionStatsInfos[channel]; !ok {
			channelPartitionStatsInfos[channel] = make([]*datapb.PartitionStatsInfo, 0)
		}
		channelPartitionStatsInfos[channel] = append(channelPartitionStatsInfos[channel], partitionStatsInfo)
	}
	log.Debug("channels with PartitionStats meta", zap.Int("len", len(channelPartitionStatsInfos)))

	for _, info := range unusedPartStats {
		log.Debug("collection has been dropped, remove partition stats",
			zap.Int64("collID", info.GetCollectionID()))
		if err := t.gcPartitionStatsInfo(info); err != nil {
			return err
		}
	}

	for channel, infos := range channelPartitionStatsInfos {
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Version > infos[j].Version
		})
		log.Debug("PartitionStats in channel", zap.String("channel", channel), zap.Int("len", len(infos)))
		if len(infos) > 2 {
			for i := 2; i < len(infos); i++ {
				info := infos[i]
				if err := t.gcPartitionStatsInfo(info); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (t *ClusteringCompactionManager) processAllTasks() error {
	triggers := t.meta.GetClusteringCompactionTasks()
	for triggerID, tasks := range triggers {
		log := log.With(zap.Int64("TriggerId", triggerID), zap.Int64("collectionID", tasks[0].CollectionID))
		for _, task := range tasks {
			log := log.With(zap.Int64("PlanID", task.PlanID))
			stateBefore := task.GetState().String()
			err := t.processTask(task)
			if err != nil {
				log.Warn("fail in process task", zap.Error(err))
				if merr.IsRetryableErr(err) && task.RetryTimes < TaskMaxRetryTimes {
					// retry in next loop
					task.RetryTimes = task.RetryTimes + 1
				} else {
					log.Error("task fail with unretryable reason or meet max retry times", zap.Error(err))
					task.State = datapb.CompactionTaskState_failed
					task.FailReason = err.Error()
				}
			}
			// task state update, refresh retry times count
			if task.State.String() != stateBefore {
				task.RetryTimes = 0
			}
			log.Debug("process task", zap.String("stateBefore", stateBefore), zap.String("stateAfter", task.State.String()))
			t.saveTask(task)
		}
		t.summaryCompactionTaskState(tasks)
	}
	return nil
}

func (t *ClusteringCompactionManager) processTask(task *datapb.CompactionTask) error {
	if task.State == datapb.CompactionTaskState_indexed || task.State == datapb.CompactionTaskState_cleaned {
		return nil
	}
	coll, err := t.handler.GetCollection(t.ctx, task.GetCollectionID())
	if err != nil {
		log.Warn("fail to get collection", zap.Int64("collectionID", task.GetCollectionID()), zap.Error(err))
		return merr.WrapErrClusteringCompactionGetCollectionFail(task.GetCollectionID(), err)
	}
	if coll == nil {
		log.Warn("collection not found, it may be dropped, stop clustering compaction task", zap.Int64("collectionID", task.GetCollectionID()))
		return merr.WrapErrCollectionNotFound(task.GetCollectionID())
	}

	switch task.State {
	case datapb.CompactionTaskState_init:
		return t.processInitTask(task)
	case datapb.CompactionTaskState_compact_pipelining, datapb.CompactionTaskState_compact_executing:
		return t.processExecutingTask(task)
	case datapb.CompactionTaskState_compact_completed:
		return t.processCompactedTask(task)
	case datapb.CompactionTaskState_analyzing:
		return t.processAnalyzingTask(task)
	case datapb.CompactionTaskState_analyzed:
		return t.processAnalyzedTask(task)
	case datapb.CompactionTaskState_indexing:
		return t.processIndexingTask(task)
	case datapb.CompactionTaskState_timeout:
		return t.processFailedOrTimeoutTask(task)
	case datapb.CompactionTaskState_failed:
		return t.processFailedOrTimeoutTask(task)
	case datapb.CompactionTaskState_indexed:
		// indexed is the final state of a clustering compaction task
	case datapb.CompactionTaskState_cleaned:
		// task must be cleaned properly if task fail or timeout
	}
	return nil
}

func (t *ClusteringCompactionManager) processInitTask(task *datapb.CompactionTask) error {
	log := log.With(zap.Int64("triggerID", task.TriggerID), zap.Int64("collectionID", task.GetCollectionID()), zap.Int64("planID", task.GetPlanID()))
	var operators []UpdateOperator
	for _, segID := range task.InputSegments {
		operators = append(operators, UpdateSegmentLevelOperator(segID, datapb.SegmentLevel_L2))
	}
	err := t.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		log.Warn("fail to set segment level to L2", zap.Error(err))
		return err
	}

	if typeutil.IsVectorType(task.GetClusteringKeyField().DataType) {
		err := t.submitToAnalyze(task)
		if err != nil {
			log.Warn("fail to submit analyze task", zap.Error(err))
			return merr.WrapErrClusteringCompactionSubmitTaskFail("analyze", err)
		}
	} else {
		err := t.submitToCompact(task)
		if err != nil {
			log.Warn("fail to submit compaction task", zap.Error(err))
			return merr.WrapErrClusteringCompactionSubmitTaskFail("compact", err)
		}
	}
	return nil
}

func (t *ClusteringCompactionManager) processExecutingTask(task *datapb.CompactionTask) error {
	compactionTask := t.compactionHandler.getCompaction(task.GetPlanID())
	if compactionTask == nil {
		// if one compaction task is lost, mark it as failed, and the clustering compaction will be marked failed as well
		log.Warn("compaction task lost", zap.Int64("planID", task.GetPlanID()))
		// trigger retry
		oldPlanID := task.GetPlanID()
		// todo: whether needs allocate a new planID
		task.State = datapb.CompactionTaskState_compact_pipelining
		return merr.WrapErrClusteringCompactionCompactionTaskLost(oldPlanID)
	}
	log.Info("compaction task state", zap.Int64("planID", compactionTask.plan.PlanID), zap.Int32("state", int32(compactionTask.state)))
	task.State = compactionTaskStateV2(compactionTask.state)
	if task.State == datapb.CompactionTaskState_compact_completed {
		return t.processCompactedTask(task)
	}
	return nil
}

func (t *ClusteringCompactionManager) processCompactedTask(task *datapb.CompactionTask) error {
	if len(task.ResultSegments) == 0 {
		compactionTask := t.compactionHandler.getCompaction(task.GetPlanID())
		if compactionTask == nil {
			// if one compaction task is lost, mark it as failed, and the clustering compaction will be marked failed as well
			log.Warn("compaction task lost", zap.Int64("planID", task.GetPlanID()))
			// trigger retry
			oldPlanID := task.GetPlanID()
			// todo: whether needs allocate a new planID
			task.State = datapb.CompactionTaskState_compact_pipelining
			return merr.WrapErrClusteringCompactionCompactionTaskLost(oldPlanID)
		}
		segmentIDs := make([]int64, 0)
		for _, seg := range compactionTask.result.Segments {
			segmentIDs = append(segmentIDs, seg.GetSegmentID())
		}
		task.ResultSegments = segmentIDs
	}

	return t.processIndexingTask(task)
}

func (t *ClusteringCompactionManager) processIndexingTask(task *datapb.CompactionTask) error {
	// wait for segment indexed
	collectionIndexes := t.meta.indexMeta.GetIndexesForCollection(task.GetCollectionID(), "")
	indexed := func() bool {
		for _, collectionIndex := range collectionIndexes {
			for _, segmentID := range task.ResultSegments {
				segmentIndexState := t.meta.indexMeta.GetSegmentIndexState(task.GetCollectionID(), segmentID, collectionIndex.IndexID)
				if segmentIndexState.GetState() != commonpb.IndexState_Finished {
					return false
				}
			}
		}
		return true
	}()
	log.Debug("check compaction result segments index states", zap.Bool("indexed", indexed), zap.Int64("planID", task.GetPlanID()), zap.Int64s("segments", task.ResultSegments))
	if indexed {
		t.processIndexedTask(task)
	} else {
		task.State = datapb.CompactionTaskState_indexing
	}
	return nil
}

// indexed is the final state of a clustering compaction task
// one task should only run this once
func (t *ClusteringCompactionManager) processIndexedTask(task *datapb.CompactionTask) error {
	err := t.meta.SavePartitionStatsInfo(&datapb.PartitionStatsInfo{
		CollectionID: task.GetCollectionID(),
		PartitionID:  task.GetPartitionID(),
		VChannel:     task.GetChannel(),
		Version:      task.GetPlanID(),
		SegmentIDs:   task.GetResultSegments(),
	})
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("SavePartitionStatsInfo", err)
	}
	var operators []UpdateOperator
	for _, segID := range task.GetResultSegments() {
		operators = append(operators, UpdateSegmentPartitionStatsVersionOperator(segID, task.GetPlanID()))
	}
	err = t.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentPartitionStatsVersion", err)
	}

	task.State = datapb.CompactionTaskState_indexed
	ts, err := t.allocator.allocTimestamp(t.ctx)
	if err != nil {
		return err
	}
	task.EndTime = ts
	elapse := tsoutil.PhysicalTime(ts).UnixMilli() - tsoutil.PhysicalTime(task.StartTime).UnixMilli()
	log.Info("clustering compaction task elapse", zap.Int64("triggerID", task.GetTriggerID()), zap.Int64("collectionID", task.GetCollectionID()), zap.Int64("planID", task.GetPlanID()), zap.Int64("elapse", elapse))
	metrics.DataCoordCompactionLatency.
		WithLabelValues(fmt.Sprint(typeutil.IsVectorType(task.GetClusteringKeyField().DataType)), datapb.CompactionType_ClusteringCompaction.String()).
		Observe(float64(elapse))
	return nil
}

func (t *ClusteringCompactionManager) processAnalyzingTask(task *datapb.CompactionTask) error {
	analyzeTask := t.meta.analyzeMeta.GetTask(task.GetAnalyzeTaskID())
	log.Info("check analyze task state", zap.Int64("id", task.GetAnalyzeTaskID()), zap.String("state", analyzeTask.State.String()))
	switch analyzeTask.State {
	case indexpb.JobState_JobStateFinished:
		if analyzeTask.GetCentroidsFile() == "" {
			// fake finished vector clustering is not supported in opensource
			return merr.WrapErrClusteringCompactionNotSupportVector()
		} else {
			task.State = datapb.CompactionTaskState_analyzed
			task.AnalyzeVersionID = analyzeTask.GetVersion()
			t.processAnalyzedTask(task)
		}
	case indexpb.JobState_JobStateFailed:
		log.Warn("analyze task fail", zap.Int64("analyzeID", task.GetAnalyzeTaskID()))
		// todo rethinking all the error flow
		return errors.New(analyzeTask.FailReason)
	default:
	}
	return nil
}

func (t *ClusteringCompactionManager) processAnalyzedTask(task *datapb.CompactionTask) error {
	return t.submitToCompact(task)
}

func (t *ClusteringCompactionManager) processFailedOrTimeoutTask(task *datapb.CompactionTask) error {
	log.Info("clean fail or timeout task", zap.Int64("triggerID", task.GetTriggerID()), zap.Int64("planID", task.GetPlanID()))
	// revert segment level
	var operators []UpdateOperator
	for _, segID := range task.InputSegments {
		operators = append(operators, RevertSegmentLevelOperator(segID))
		operators = append(operators, RevertSegmentPartitionStatsVersionOperator(segID))
	}
	err := t.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		log.Warn("UpdateSegmentsInfo fail", zap.Error(err))
	}

	// drop partition stats if uploaded
	partitionStatsInfo := &datapb.PartitionStatsInfo{
		CollectionID: task.GetCollectionID(),
		PartitionID:  task.GetPartitionID(),
		VChannel:     task.GetChannel(),
		Version:      task.GetPlanID(),
		SegmentIDs:   task.GetResultSegments(),
	}
	err = t.gcPartitionStatsInfo(partitionStatsInfo)
	if err != nil {
		log.Warn("gcPartitionStatsInfo fail", zap.Error(err))
	}

	// set segment compacting
	for _, segID := range task.InputSegments {
		t.meta.SetSegmentCompacting(segID, false)
	}
	task.State = datapb.CompactionTaskState_cleaned
	return nil
}

func (t *ClusteringCompactionManager) submitToAnalyze(task *datapb.CompactionTask) error {
	newAnalyzeTask := &indexpb.AnalyzeTask{
		CollectionID: task.GetCollectionID(),
		PartitionID:  task.GetPartitionID(),
		FieldID:      task.GetClusteringKeyField().FieldID,
		FieldName:    task.GetClusteringKeyField().Name,
		FieldType:    task.GetClusteringKeyField().DataType,
		SegmentIDs:   task.GetInputSegments(),
		TaskID:       task.GetAnalyzeTaskID(), // analyze id is pre allocated
		State:        indexpb.JobState_JobStateInit,
	}
	err := t.meta.analyzeMeta.AddAnalyzeTask(newAnalyzeTask)
	if err != nil {
		log.Warn("failed to create analyze task", zap.Int64("planID", task.GetPlanID()), zap.Error(err))
		return err
	}
	t.analyzeScheduler.enqueue(&analyzeTask{
		taskID: task.GetAnalyzeTaskID(),
		taskInfo: &indexpb.AnalyzeResult{
			TaskID: task.GetAnalyzeTaskID(),
			State:  indexpb.JobState_JobStateInit,
		},
	})
	task.State = datapb.CompactionTaskState_analyzing
	log.Info("submit analyze task", zap.Int64("planID", task.GetPlanID()), zap.Int64("triggerID", task.GetTriggerID()), zap.Int64("collectionID", task.GetCollectionID()), zap.Int64("id", task.GetAnalyzeTaskID()))
	return nil
}

func (t *ClusteringCompactionManager) submitToCompact(task *datapb.CompactionTask) error {
	trigger := &compactionSignal{
		id:           task.TriggerID,
		collectionID: task.CollectionID,
		partitionID:  task.PartitionID,
	}

	segments := make([]*SegmentInfo, 0)
	for _, segmentID := range task.InputSegments {
		segments = append(segments, t.meta.GetSegment(segmentID))
	}
	compactionPlan := segmentsToPlan(segments, datapb.CompactionType_ClusteringCompaction, &compactTime{collectionTTL: time.Duration(task.CollectionTtl)})
	compactionPlan.PlanID = task.GetPlanID()
	compactionPlan.StartTime = task.GetStartTime()
	compactionPlan.TimeoutInSeconds = Params.DataCoordCfg.ClusteringCompactionTimeoutInSeconds.GetAsInt32()
	compactionPlan.Timetravel = task.GetTimetravel()
	compactionPlan.ClusteringKeyId = task.GetClusteringKeyField().FieldID
	compactionPlan.MaxSegmentRows = task.GetMaxSegmentRows()
	compactionPlan.PreferSegmentRows = task.GetPreferSegmentRows()
	compactionPlan.AnalyzeResultPath = path.Join(metautil.JoinIDPath(task.AnalyzeTaskID, task.AnalyzeVersionID))
	err := t.compactionHandler.execCompactionPlan(trigger, compactionPlan)
	if err != nil {
		log.Warn("failed to execute compaction task", zap.Int64("planID", task.GetPlanID()), zap.Error(err))
		return err
	}
	task.State = datapb.CompactionTaskState_compact_pipelining
	log.Info("send compaction task to execute", zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.Int64("collectionID", task.GetCollectionID()),
		zap.Int64("partitionID", task.GetPartitionID()),
		zap.Int64s("inputSegments", task.InputSegments))
	return nil
}

func (t *ClusteringCompactionManager) summaryCompactionTaskState(compactionTasks []*datapb.CompactionTask) ClusteringCompactionSummary {
	var state commonpb.CompactionState
	var executingCnt, pipeliningCnt, completedCnt, failedCnt, timeoutCnt, initCnt, analyzingCnt, analyzedCnt, indexingCnt, indexedCnt, cleanedCnt int
	for _, task := range compactionTasks {
		if task == nil {
			continue
		}
		switch task.State {
		case datapb.CompactionTaskState_compact_executing:
			executingCnt++
		case datapb.CompactionTaskState_compact_pipelining:
			pipeliningCnt++
		case datapb.CompactionTaskState_compact_completed:
			completedCnt++
		case datapb.CompactionTaskState_failed:
			failedCnt++
		case datapb.CompactionTaskState_timeout:
			timeoutCnt++
		case datapb.CompactionTaskState_init:
			initCnt++
		case datapb.CompactionTaskState_analyzing:
			analyzingCnt++
		case datapb.CompactionTaskState_analyzed:
			analyzedCnt++
		case datapb.CompactionTaskState_indexing:
			indexingCnt++
		case datapb.CompactionTaskState_indexed:
			indexedCnt++
		case datapb.CompactionTaskState_cleaned:
			cleanedCnt++
		default:
		}
	}

	// fail and timeout task must be cleaned first before mark the job complete
	if executingCnt+pipeliningCnt+completedCnt+initCnt+analyzingCnt+analyzedCnt+indexingCnt+failedCnt+timeoutCnt != 0 {
		state = commonpb.CompactionState_Executing
	} else {
		state = commonpb.CompactionState_Completed
	}

	log.Debug("compaction states",
		zap.Int64("triggerID", compactionTasks[0].TriggerID),
		zap.String("state", state.String()),
		zap.Int("executingCnt", executingCnt),
		zap.Int("pipeliningCnt", pipeliningCnt),
		zap.Int("completedCnt", completedCnt),
		zap.Int("failedCnt", failedCnt),
		zap.Int("timeoutCnt", timeoutCnt),
		zap.Int("initCnt", initCnt),
		zap.Int("analyzingCnt", analyzingCnt),
		zap.Int("analyzedCnt", analyzedCnt),
		zap.Int("indexingCnt", indexingCnt),
		zap.Int("indexedCnt", indexedCnt),
		zap.Int("cleanedCnt", cleanedCnt))
	return ClusteringCompactionSummary{
		state:         state,
		executingCnt:  executingCnt,
		pipeliningCnt: pipeliningCnt,
		completedCnt:  completedCnt,
		failedCnt:     failedCnt,
		timeoutCnt:    timeoutCnt,
		initCnt:       initCnt,
		analyzingCnt:  analyzingCnt,
		analyzedCnt:   analyzedCnt,
		indexingCnt:   indexingCnt,
		indexedCnt:    indexedCnt,
		cleanedCnt:    cleanedCnt,
	}
}

// getClusteringCompactingJobs get clustering compaction info by collection id
func (t *ClusteringCompactionManager) collectionIsClusteringCompacting(collectionID UniqueID) (bool, int64) {
	triggers := t.meta.GetClusteringCompactionTasksByCollection(collectionID)
	if len(triggers) == 0 {
		return false, 0
	}
	var latestTriggerID int64 = 0
	for triggerID := range triggers {
		if latestTriggerID > triggerID {
			latestTriggerID = triggerID
		}
	}
	tasks := triggers[latestTriggerID]
	if len(tasks) > 0 {
		summary := t.summaryCompactionTaskState(tasks)
		return summary.state == commonpb.CompactionState_Executing, tasks[0].TriggerID
	}
	return false, 0
}

func (t *ClusteringCompactionManager) fillClusteringCompactionTask(task *datapb.CompactionTask, segments []*SegmentInfo) {
	var totalRows int64
	segmentIDs := make([]int64, 0)
	for _, s := range segments {
		totalRows += s.GetNumOfRows()
		segmentIDs = append(segmentIDs, s.GetID())
	}
	task.TotalRows = totalRows
	task.InputSegments = segmentIDs
	clusteringMaxSegmentSize := paramtable.Get().DataCoordCfg.ClusteringCompactionMaxSegmentSize.GetAsSize()
	clusteringPreferSegmentSize := paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSize.GetAsSize()
	segmentMaxSize := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	task.MaxSegmentRows = segments[0].MaxRowNum * clusteringMaxSegmentSize / segmentMaxSize
	task.PreferSegmentRows = segments[0].MaxRowNum * clusteringPreferSegmentSize / segmentMaxSize
}

// dropTask drop clustering compaction task in meta
func (t *ClusteringCompactionManager) dropTask(task *datapb.CompactionTask) error {
	return t.meta.DropClusteringCompactionTask(task)
}

// saveTask update clustering compaction task in meta
func (t *ClusteringCompactionManager) saveTask(task *datapb.CompactionTask) error {
	return t.meta.SaveClusteringCompactionTask(task)
}

func triggerCompactionPolicy(ctx context.Context, meta *meta, collectionID int64, partitionID int64, channel string, segments []*SegmentInfo) (bool, error) {
	log := log.With(zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID))
	partitionStatsInfos := meta.ListPartitionStatsInfos(collectionID, partitionID, channel)
	sort.Slice(partitionStatsInfos, func(i, j int) bool {
		return partitionStatsInfos[i].Version > partitionStatsInfos[j].Version
	})

	if len(partitionStatsInfos) == 0 {
		var newDataSize int64 = 0
		for _, seg := range segments {
			newDataSize += seg.getSegmentSize()
		}
		if newDataSize > Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.GetAsSize() {
			log.Info("New data is larger than threshold, do compaction", zap.Int64("newDataSize", newDataSize))
			return true, nil
		}
		log.Info("No partition stats and no enough new data, skip compaction")
		return false, nil
	}

	partitionStats := partitionStatsInfos[0]
	version := partitionStats.Version
	pTime, _ := tsoutil.ParseTS(uint64(version))
	if time.Since(pTime) < Params.DataCoordCfg.ClusteringCompactionMinInterval.GetAsDuration(time.Second) {
		log.Info("Too short time before last clustering compaction, skip compaction")
		return false, nil
	}
	if time.Since(pTime) > Params.DataCoordCfg.ClusteringCompactionMaxInterval.GetAsDuration(time.Second) {
		log.Info("It is a long time after last clustering compaction, do compaction")
		return true, nil
	}

	var compactedSegmentSize int64 = 0
	var uncompactedSegmentSize int64 = 0
	for _, seg := range segments {
		if lo.Contains(partitionStats.SegmentIDs, seg.ID) {
			compactedSegmentSize += seg.getSegmentSize()
		} else {
			uncompactedSegmentSize += seg.getSegmentSize()
		}
	}

	// ratio based
	//ratio := float64(uncompactedSegmentSize) / float64(compactedSegmentSize)
	//if ratio > Params.DataCoordCfg.ClusteringCompactionNewDataRatioThreshold.GetAsFloat() {
	//	log.Info("New data is larger than threshold, do compaction", zap.Float64("ratio", ratio))
	//	return true, nil
	//}
	//log.Info("New data is smaller than threshold, skip compaction", zap.Float64("ratio", ratio))
	//return false, nil

	// size based
	if uncompactedSegmentSize > Params.DataCoordCfg.ClusteringCompactionNewDataSizeThreshold.GetAsSize() {
		log.Info("New data is larger than threshold, do compaction", zap.Int64("newDataSize", uncompactedSegmentSize))
		return true, nil
	}
	log.Info("New data is smaller than threshold, skip compaction", zap.Int64("newDataSize", uncompactedSegmentSize))
	return false, nil
}

func compactionTaskStateV2(state compactionTaskState) datapb.CompactionTaskState {
	switch state {
	case pipelining:
		return datapb.CompactionTaskState_compact_pipelining
	case executing:
		return datapb.CompactionTaskState_compact_executing
	case completed:
		return datapb.CompactionTaskState_compact_completed
	case timeout:
		return datapb.CompactionTaskState_timeout
	case failed:
		return datapb.CompactionTaskState_failed
	default:
		return datapb.CompactionTaskState_unknown
	}
}
