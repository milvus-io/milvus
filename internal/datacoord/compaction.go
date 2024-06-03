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
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// TODO this num should be determined by resources of datanode, for now, we set to a fixed value for simple
// TODO we should split compaction into different priorities, small compaction helps to merge segment, large compaction helps to handle delta and expiration of large segments
const (
	tsTimeout         = uint64(1)
	taskMaxRetryTimes = int32(3)
)

//go:generate mockery --name=compactionPlanContext --structname=MockCompactionPlanContext --output=./  --filename=mock_compaction_plan_context.go --with-expecter --inpackage
type compactionPlanContext interface {
	start()
	stop()
	// enqueueCompaction enqueue compaction task and return immediately
	enqueueCompaction(task CompactionTask) error
	// getCompaction return compaction task. If planId does not exist, return nil.
	getCompaction(planID int64) CompactionTask
	// updateCompaction set the compaction state to timeout or completed
	updateCompaction(ts Timestamp) error
	// isFull return true if the task pool is full
	isFull() bool
	// get compaction tasks by signal id
	getCompactionTasksBySignalID(signalID int64) []CompactionTask
	collectionIsClusteringCompacting(collectionID int64) (bool, int64)
	removeTasksByChannel(channel string)
}

var (
	errChannelNotWatched = errors.New("channel is not watched")
	errChannelInBuffer   = errors.New("channel is in buffer")
)

var _ compactionPlanContext = (*compactionPlanHandler)(nil)

type compactionPlanHandler struct {
	mu    lock.RWMutex
	plans map[int64]CompactionTask // planID -> task

	meta             CompactionMeta
	allocator        allocator
	chManager        ChannelManager
	scheduler        Scheduler
	sessions         SessionManager
	analyzeScheduler *taskScheduler

	stopCh   chan struct{}
	stopOnce sync.Once
	stopWg   sync.WaitGroup

	compactionResults map[int64]*typeutil.Pair[int64, *datapb.CompactionPlanResult]
}

func newCompactionPlanHandler(cluster Cluster, sessions SessionManager, cm ChannelManager, meta CompactionMeta, allocator allocator, analyzeScheduler *taskScheduler,
) *compactionPlanHandler {
	return &compactionPlanHandler{
		plans:            make(map[int64]CompactionTask),
		chManager:        cm,
		meta:             meta,
		sessions:         sessions,
		allocator:        allocator,
		scheduler:        NewCompactionScheduler(cluster),
		analyzeScheduler: analyzeScheduler,
	}
}

func (c *compactionPlanHandler) checkResult() {
	// deal results
	ts, err := c.GetCurrentTS()
	if err != nil {
		log.Warn("fail to check result", zap.Error(err))
		return
	}
	err = c.updateCompaction(ts)
	if err != nil {
		log.Warn("fail to update compaction", zap.Error(err))
		return
	}
}

func (c *compactionPlanHandler) GetCurrentTS() (Timestamp, error) {
	interval := Params.DataCoordCfg.CompactionRPCTimeout.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), interval)
	defer cancel()
	ts, err := c.allocator.allocTimestamp(ctx)
	if err != nil {
		log.Warn("unable to alloc timestamp", zap.Error(err))
		return 0, err
	}
	return ts, nil
}

func (c *compactionPlanHandler) schedule() {
	// schedule queuing tasks
	tasks := c.scheduler.Schedule()
	if len(tasks) > 0 {
		c.notifyTasks(tasks)
		c.scheduler.LogStatus()
	}
}

func (c *compactionPlanHandler) start() {
	interval := Params.DataCoordCfg.CompactionCheckIntervalInSeconds.GetAsDuration(time.Second)
	c.stopCh = make(chan struct{})
	c.stopWg.Add(3)

	triggers := c.meta.GetClusteringCompactionTasks()
	for _, tasks := range triggers {
		for _, task := range tasks {
			if task.State != datapb.CompactionTaskState_indexed && task.State != datapb.CompactionTaskState_cleaned {
				// set segments state compacting
				for _, segment := range task.InputSegments {
					c.meta.SetSegmentCompacting(segment, true)
				}
				c.plans[task.PlanID] = &clusteringCompactionTask{
					CompactionTask: task,
				}
			}
		}
	}

	go func() {
		defer c.stopWg.Done()
		checkResultTicker := time.NewTicker(interval)
		log.Info("Compaction handler check result loop start", zap.Any("check result interval", interval))
		defer checkResultTicker.Stop()
		for {
			select {
			case <-c.stopCh:
				log.Info("compaction handler check result loop quit")
				return
			case <-checkResultTicker.C:
				c.checkResult()
			}
		}
	}()

	// separate check results and schedule goroutine so that check results doesn't
	// influence the schedule
	go func() {
		defer c.stopWg.Done()
		scheduleTicker := time.NewTicker(2 * time.Second)
		defer scheduleTicker.Stop()
		log.Info("compaction handler start schedule")
		for {
			select {
			case <-c.stopCh:
				log.Info("Compaction handler quit schedule")
				return

			case <-scheduleTicker.C:
				c.schedule()
			}
		}
	}()

	go func() {
		defer c.stopWg.Done()
		cleanTicker := time.NewTicker(30 * time.Minute)
		defer cleanTicker.Stop()
		for {
			select {
			case <-c.stopCh:
				log.Info("Compaction handler quit clean")
				return
			case <-cleanTicker.C:
				c.Clean()
				c.gcPartitionStats()
			}
		}
	}()
}

func (c *compactionPlanHandler) Clean() {
	current := tsoutil.GetCurrentTime()
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, task := range c.plans {
		switch task.GetType() {
		case datapb.CompactionType_ClusteringCompaction:
			if task.GetState() != datapb.CompactionTaskState_cleaned || task.GetState() != datapb.CompactionTaskState_indexed {
				continue
			}
		default:
			if task.GetState() == datapb.CompactionTaskState_executing || task.GetState() == datapb.CompactionTaskState_pipelining {
				continue
			}
		}
		// after timeout + 1h, the plan will be cleaned
		if isTimeout(current, task.GetStartTime(), task.GetTimeoutInSeconds()+60*60) {
			delete(c.plans, id)
		}
	}
}

func (c *compactionPlanHandler) stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	c.stopWg.Wait()
}

func (c *compactionPlanHandler) removeTasksByChannel(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, task := range c.plans {
		if task.GetChannel() == channel {
			log.Info("Compaction handler removing tasks by channel",
				zap.String("channel", channel),
				zap.Int64("planID", task.GetPlanID()),
				zap.Int64("node", task.GetNodeID()),
			)
			c.scheduler.Finish(task.GetNodeID(), task)
			delete(c.plans, id)
		}
	}
}

func (c *compactionPlanHandler) updateTask(planID int64, opts ...compactionTaskOpt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if plan, ok := c.plans[planID]; ok {
		c.plans[planID] = plan.ShadowClone(opts...)
	}
}

func (c *compactionPlanHandler) enqueueCompaction(task CompactionTask) error {
	log := log.With(zap.Int64("planID", task.GetPlanID()), zap.Int64("collectionID", task.GetCollectionID()), zap.String("type", task.GetType().String()))
	c.setSegmentsCompacting(task, true)

	_, span := otel.Tracer(typeutil.DataCoordRole).Start(context.Background(), fmt.Sprintf("Compaction-%s", task.GetType()))
	task.SetSpan(span)

	c.mu.Lock()
	c.plans[task.GetPlanID()] = task
	c.mu.Unlock()
	log.Info("Compaction plan enqueue")
	return nil
}

func (c *compactionPlanHandler) notifyTasks(tasks []CompactionTask) {
	for _, task := range tasks {
		// avoid closure capture iteration variable
		innerTask := task
		plan, err := innerTask.BuildCompactionRequest(c)
		if err != nil {
			c.updateTask(innerTask.GetPlanID(), setState(datapb.CompactionTaskState_failed), endSpan())
			c.scheduler.Finish(innerTask.GetNodeID(), innerTask)
			log.Warn("failed to refresh task",
				zap.Int64("plan", task.GetPlanID()),
				zap.Error(err))
			continue
		}
		getOrCreateIOPool().Submit(func() (any, error) {
			ctx := tracer.SetupSpan(context.Background(), innerTask.GetSpan())
			log := log.Ctx(ctx).With(zap.Int64("planID", plan.GetPlanID()), zap.Int64("nodeID", innerTask.GetNodeID()))
			log.Info("Notify compaction task to DataNode")
			ts, err := c.allocator.allocTimestamp(ctx)
			if err != nil {
				log.Warn("Alloc start time for CompactionPlan failed", zap.Error(err))
				// update plan ts to TIMEOUT ts
				c.updateTask(plan.PlanID, setState(datapb.CompactionTaskState_executing), setStartTime(tsTimeout))
				return nil, err
			}
			c.updateTask(plan.PlanID, setStartTime(ts))

			err = c.sessions.Compaction(ctx, innerTask.GetNodeID(), plan)

			c.updateTask(plan.PlanID, setState(datapb.CompactionTaskState_executing))
			if err != nil {
				log.Warn("Failed to notify compaction tasks to DataNode", zap.Error(err))
				return nil, err
			}
			log.Info("Compaction start")
			return nil, nil
		})
	}
}

func (c *compactionPlanHandler) setSegmentsCompacting(task CompactionTask, compacting bool) {
	for _, segmentID := range task.GetInputSegments() {
		c.meta.SetSegmentCompacting(segmentID, compacting)
	}
}

func (c *compactionPlanHandler) handleL0CompactionResult(plan *datapb.CompactionPlan, result *datapb.CompactionPlanResult) error {
	var operators []UpdateOperator
	for _, seg := range result.GetSegments() {
		operators = append(operators, AddBinlogsOperator(seg.GetSegmentID(), nil, nil, seg.GetDeltalogs()))
	}

	levelZeroSegments := lo.Filter(plan.GetSegmentBinlogs(), func(b *datapb.CompactionSegmentBinlogs, _ int) bool {
		return b.GetLevel() == datapb.SegmentLevel_L0
	})

	for _, seg := range levelZeroSegments {
		operators = append(operators, UpdateStatusOperator(seg.GetSegmentID(), commonpb.SegmentState_Dropped), UpdateCompactedOperator(seg.GetSegmentID()))
	}

	log.Info("meta update: update segments info for level zero compaction",
		zap.Int64("planID", plan.GetPlanID()),
	)
	return c.meta.UpdateSegmentsInfo(operators...)
}

func (c *compactionPlanHandler) handleMergeCompactionResult(plan *datapb.CompactionPlan, result *datapb.CompactionPlanResult) error {
	log := log.With(zap.Int64("planID", plan.GetPlanID()), zap.String("type", plan.GetType().String()))
	if plan.GetType() == datapb.CompactionType_ClusteringCompaction {
		if len(result.GetSegments()) == 0 {
			// should never happen
			log.Warn("illegal compaction results")
			return fmt.Errorf("Illegal compaction results: %v", result)
		}
	} else {
		if len(result.GetSegments()) == 0 || len(result.GetSegments()) > 1 {
			// should never happen
			log.Warn("illegal compaction results")
			return fmt.Errorf("Illegal compaction results: %v", result)
		}
	}

	newSegmentInfo := c.meta.GetHealthySegment(result.GetSegments()[0].SegmentID)
	if newSegmentInfo != nil {
		log.Info("meta has already been changed, skip meta change and retry sync segments")
	} else {
		// Also prepare metric updates.
		_, metricMutation, err := c.meta.CompleteCompactionMutation(plan, result)
		if err != nil {
			return err
		}
		// Apply metrics after successful meta update.
		metricMutation.commit()
	}

	log.Info("handleCompactionResult: success to handle compaction result")
	return nil
}

// getCompaction return compaction task. If planId does not exist, return nil.
func (c *compactionPlanHandler) getCompaction(planID int64) CompactionTask {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.plans[planID]
}

func (c *compactionPlanHandler) updateCompaction(ts Timestamp) error {
	// Get executing executingTasks before GetCompactionState from DataNode to prevent false failure,
	//  for DC might add new task while GetCompactionState.
	tasks := c.getTasks()
	planStates, err := c.sessions.GetCompactionPlansResults()
	if err != nil {
		// if there is a data node alive but we failed to get info,
		log.Warn("failed to get compaction plans from all nodes", zap.Error(err))
		return err
	}
	c.compactionResults = planStates
	cachedPlans := []int64{}

	// TODO reduce the lock range
	c.mu.Lock()
	for _, task := range tasks {
		planID := task.GetPlanID()
		cachedPlans = append(cachedPlans, planID)
		switch task.GetType() {
		case datapb.CompactionType_ClusteringCompaction:
			clusterTask := task.(*clusteringCompactionTask)
			log := log.With(zap.Int64("PlanID", task.GetPlanID()))
			stateBefore := clusterTask.GetState().String()
			err := clusterTask.ProcessTask(c)
			if err != nil {
				log.Warn("fail in process task", zap.Error(err))
				if merr.IsRetryableErr(err) && clusterTask.RetryTimes < taskMaxRetryTimes {
					// retry in next loop
					clusterTask.RetryTimes = clusterTask.RetryTimes + 1
				} else {
					log.Error("task fail with unretryable reason or meet max retry times", zap.Error(err))
					clusterTask.State = datapb.CompactionTaskState_failed
					clusterTask.FailReason = err.Error()
				}
			}
			// task state update, refresh retry times count
			if clusterTask.State.String() != stateBefore {
				clusterTask.RetryTimes = 0
			}
			log.Debug("process task", zap.String("stateBefore", stateBefore), zap.String("stateAfter", clusterTask.State.String()))
			c.meta.SaveClusteringCompactionTask(clusterTask.CompactionTask)
		default:
			log := log.With(
				zap.Int64("planID", task.GetPlanID()),
				zap.Int64("nodeID", task.GetNodeID()),
				zap.String("channel", task.GetChannel()))
			err := task.ProcessTask(c)
			if err != nil {
				log.Warn("fail in process task", zap.Error(err))
				return err
			}
		}
	}
	c.mu.Unlock()

	// Compaction plans in DN but not in DC are unknown plans, need to notify DN to clear it.
	// No locks needed, because no changes in DC memeory
	completedPlans := lo.PickBy(planStates, func(planID int64, planState *typeutil.Pair[int64, *datapb.CompactionPlanResult]) bool {
		return planState.B.GetState() == commonpb.CompactionState_Completed
	})

	unknownPlansInWorker, _ := lo.Difference(lo.Keys(completedPlans), cachedPlans)
	for _, planID := range unknownPlansInWorker {
		if nodeUnkonwnPlan, ok := completedPlans[planID]; ok {
			nodeID, plan := nodeUnkonwnPlan.A, nodeUnkonwnPlan.B
			log := log.With(zap.Int64("planID", planID), zap.Int64("nodeID", nodeID), zap.String("channel", plan.GetChannel()))
			// TODO @xiaocai2333: drop compaction plan on datanode
			log.Info("drop unknown plan with node")
		}
	}

	return nil
}

func isTimeout(now Timestamp, start Timestamp, timeout int32) bool {
	startTime, _ := tsoutil.ParseTS(start)
	ts, _ := tsoutil.ParseTS(now)
	return int32(ts.Sub(startTime).Seconds()) >= timeout
}

// isFull return true if the task pool is full
func (c *compactionPlanHandler) isFull() bool {
	return c.scheduler.GetTaskCount() >= Params.DataCoordCfg.CompactionMaxParallelTasks.GetAsInt()
}

func (c *compactionPlanHandler) getTasks() []CompactionTask {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tasks := make([]CompactionTask, 0, len(c.plans))
	for _, plan := range c.plans {
		tasks = append(tasks, plan)
	}
	return tasks
}

// get compaction tasks by signal id; if signalID == 0 return all tasks
func (c *compactionPlanHandler) getCompactionTasksBySignalID(signalID int64) []CompactionTask {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var tasks []CompactionTask
	for _, t := range c.plans {
		if signalID == 0 {
			tasks = append(tasks, t)
			continue
		}
		if t.GetTriggerID() != signalID {
			continue
		}
		tasks = append(tasks, t)
	}
	return tasks
}

func (c *compactionPlanHandler) gcPartitionStatsInfo(info *datapb.PartitionStatsInfo) error {
	removePaths := make([]string, 0)
	meta := c.meta.(*meta)
	partitionStatsPath := path.Join(meta.chunkManager.RootPath(), common.PartitionStatsPath,
		metautil.JoinIDPath(info.CollectionID, info.PartitionID),
		info.GetVChannel(), strconv.FormatInt(info.GetVersion(), 10))
	removePaths = append(removePaths, partitionStatsPath)
	analyzeT := meta.analyzeMeta.GetTask(info.GetAnalyzeTaskID())
	if analyzeT != nil {
		centroidsFilePath := path.Join(meta.chunkManager.RootPath(), common.AnalyzeStatsPath,
			metautil.JoinIDPath(analyzeT.GetTaskID(), analyzeT.GetVersion(), analyzeT.GetCollectionID(),
				analyzeT.GetPartitionID(), analyzeT.GetFieldID()),
			"centroids",
		)
		removePaths = append(removePaths, centroidsFilePath)
		for _, segID := range info.GetSegmentIDs() {
			segmentOffsetMappingFilePath := path.Join(meta.chunkManager.RootPath(), common.AnalyzeStatsPath,
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
	err := meta.chunkManager.MultiRemove(context.Background(), removePaths)
	if err != nil {
		log.Warn("remove clustering compaction stats files failed", zap.Error(err))
		return err
	}

	// first clean analyze task
	if err = meta.analyzeMeta.DropAnalyzeTask(info.GetAnalyzeTaskID()); err != nil {
		log.Warn("remove analyze task failed", zap.Int64("analyzeTaskID", info.GetAnalyzeTaskID()), zap.Error(err))
		return err
	}

	// finally clean up the partition stats info, and make sure the analysis task is cleaned up
	err = meta.partitionStatsMeta.DropPartitionStatsInfo(info)
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

func (c *compactionPlanHandler) gcPartitionStats() error {
	log.Debug("start gc clustering compaction related meta and files")
	// gc clustering compaction tasks
	triggers := c.meta.GetClusteringCompactionTasks()
	checkTaskFunc := func(task *datapb.CompactionTask) {
		// indexed is the final state of a clustering compaction task
		if task.State == datapb.CompactionTaskState_indexed || task.State == datapb.CompactionTaskState_cleaned {
			if time.Since(tsoutil.PhysicalTime(task.StartTime)) > Params.DataCoordCfg.ClusteringCompactionDropTolerance.GetAsDuration(time.Second) {
				// skip handle this error, try best to delete meta
				err := c.meta.DropClusteringCompactionTask(task)
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
	infos := c.meta.(*meta).partitionStatsMeta.ListAllPartitionStatsInfos()
	for _, info := range infos {
		collInfo := c.meta.(*meta).GetCollection(info.GetCollectionID())
		if collInfo == nil {
			unusedPartStats = append(unusedPartStats, info)
			continue
		}
		channel := fmt.Sprintf("%d/%d/%s", info.CollectionID, info.PartitionID, info.VChannel)
		if _, ok := channelPartitionStatsInfos[channel]; !ok {
			channelPartitionStatsInfos[channel] = make([]*datapb.PartitionStatsInfo, 0)
		}
		channelPartitionStatsInfos[channel] = append(channelPartitionStatsInfos[channel], info)
	}
	log.Debug("channels with PartitionStats meta", zap.Int("len", len(channelPartitionStatsInfos)))

	for _, info := range unusedPartStats {
		log.Debug("collection has been dropped, remove partition stats",
			zap.Int64("collID", info.GetCollectionID()))
		if err := c.gcPartitionStatsInfo(info); err != nil {
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
				if err := c.gcPartitionStatsInfo(info); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *compactionPlanHandler) collectionIsClusteringCompacting(collectionID UniqueID) (bool, int64) {
	triggers := c.meta.GetClusteringCompactionTasksByCollection(collectionID)
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
		cTasks := make([]CompactionTask, 0)
		for _, task := range tasks {
			cTasks = append(cTasks, &clusteringCompactionTask{
				CompactionTask: task,
			})
		}
		summary := summaryCompactionState(cTasks)
		return summary.state == commonpb.CompactionState_Executing, tasks[0].TriggerID
	}
	return false, 0
}

type CompactionTriggerSummary struct {
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

func summaryCompactionState(compactionTasks []CompactionTask) CompactionTriggerSummary {
	var state commonpb.CompactionState
	var executingCnt, pipeliningCnt, completedCnt, failedCnt, timeoutCnt, initCnt, analyzingCnt, analyzedCnt, indexingCnt, indexedCnt, cleanedCnt int
	for _, task := range compactionTasks {
		if task == nil {
			continue
		}
		switch task.GetState() {
		case datapb.CompactionTaskState_executing:
			executingCnt++
		case datapb.CompactionTaskState_pipelining:
			pipeliningCnt++
		case datapb.CompactionTaskState_completed:
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
		//zap.Int64("triggerID", compactionTasks[0].GetTriggerID()),
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
	return CompactionTriggerSummary{
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

// 0.5*min(8, NumCPU/2)
func calculateParallel() int {
	// TODO after node memory management enabled, use this config as hard limit
	return Params.DataCoordCfg.CompactionWorkerParalleTasks.GetAsInt()
	//cores := hardware.GetCPUNum()
	//if cores < 16 {
	//return 4
	//}
	//return cores / 2
}

var (
	ioPool         *conc.Pool[any]
	ioPoolInitOnce sync.Once
)

func initIOPool() {
	capacity := Params.DataNodeCfg.IOConcurrency.GetAsInt()
	if capacity > 32 {
		capacity = 32
	}
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool = conc.NewPool[any](capacity)
}

func getOrCreateIOPool() *conc.Pool[any] {
	ioPoolInitOnce.Do(initIOPool)
	return ioPool
}
