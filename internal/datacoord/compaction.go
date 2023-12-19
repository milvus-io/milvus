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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

// TODO this num should be determined by resources of datanode, for now, we set to a fixed value for simple
// TODO we should split compaction into different priorities, small compaction helps to merge segment, large compaction helps to handle delta and expiration of large segments
const (
	tsTimeout = uint64(1)
)

type compactionPlanContext interface {
	start()
	stop()
	// execCompactionPlan start to execute plan and return immediately
	execCompactionPlan(signal *compactionSignal, plan *datapb.CompactionPlan) error
	// getCompaction return compaction task. If planId does not exist, return nil.
	getCompaction(planID int64) *compactionTask
	// updateCompaction set the compaction state to timeout or completed
	updateCompaction(ts Timestamp) error
	// isFull return true if the task pool is full
	isFull() bool
	// get compaction tasks by signal id
	getCompactionTasksBySignalID(signalID int64) []*compactionTask
	removeTasksByChannel(channel string)
}

type compactionTaskState int8

const (
	executing compactionTaskState = iota + 1
	pipelining
	completed
	failed
	timeout
)

var (
	errChannelNotWatched = errors.New("channel is not watched")
	errChannelInBuffer   = errors.New("channel is in buffer")
)

type CompactionMeta interface {
	SelectSegments(selector SegmentInfoSelector) []*SegmentInfo
	GetHealthySegment(segID UniqueID) *SegmentInfo
	UpdateSegmentsInfo(operators ...UpdateOperator) error
	SetSegmentCompacting(segmentID int64, compacting bool)

	PrepareCompleteCompactionMutation(plan *datapb.CompactionPlan, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *SegmentInfo, *segMetricMutation, error)
	alterMetaStoreAfterCompaction(segmentCompactTo *SegmentInfo, segmentsCompactFrom []*SegmentInfo) error
}

var _ CompactionMeta = (*meta)(nil)

type compactionTask struct {
	triggerInfo *compactionSignal
	plan        *datapb.CompactionPlan
	state       compactionTaskState
	dataNodeID  int64
	result      *datapb.CompactionPlanResult
}

func (t *compactionTask) shadowClone(opts ...compactionTaskOpt) *compactionTask {
	task := &compactionTask{
		triggerInfo: t.triggerInfo,
		plan:        t.plan,
		state:       t.state,
		dataNodeID:  t.dataNodeID,
	}
	for _, opt := range opts {
		opt(task)
	}
	return task
}

var _ compactionPlanContext = (*compactionPlanHandler)(nil)

type compactionPlanHandler struct {
	mu    sync.RWMutex
	plans map[int64]*compactionTask // planID -> task

	meta      CompactionMeta
	allocator allocator
	chManager *ChannelManager
	scheduler Scheduler
	sessions  SessionManager

	stopCh   chan struct{}
	stopOnce sync.Once
	stopWg   sync.WaitGroup
}

func newCompactionPlanHandler(sessions SessionManager, cm *ChannelManager, meta CompactionMeta, allocator allocator,
) *compactionPlanHandler {
	return &compactionPlanHandler{
		plans:     make(map[int64]*compactionTask),
		chManager: cm,
		meta:      meta,
		sessions:  sessions,
		allocator: allocator,
		scheduler: NewCompactionScheduler(),
	}
}

func (c *compactionPlanHandler) checkResult() {
	// deal results
	interval := Params.DataCoordCfg.CompactionRPCTimeout.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), interval)
	defer cancel()
	ts, err := c.allocator.allocTimestamp(ctx)
	if err != nil {
		log.Warn("unable to alloc timestamp", zap.Error(err))
	}
	_ = c.updateCompaction(ts)
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
	c.stopWg.Add(2)

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

	// saperate check results and schedule goroutine so that check results doesn't
	// influence the schedule
	go func() {
		defer c.stopWg.Done()
		scheduleTicker := time.NewTicker(200 * time.Millisecond)
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
		if task.triggerInfo.channel == channel {
			log.Info("Compaction handler removing tasks by channel",
				zap.String("channel", channel),
				zap.Int64("planID", task.plan.GetPlanID()),
				zap.Int64("node", task.dataNodeID),
			)
			c.scheduler.Finish(task.dataNodeID, task.plan.PlanID)
			delete(c.plans, id)
		}
	}
}

func (c *compactionPlanHandler) updateTask(planID int64, opts ...compactionTaskOpt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if plan, ok := c.plans[planID]; ok {
		c.plans[planID] = plan.shadowClone(opts...)
	}
}

func (c *compactionPlanHandler) enqueuePlan(signal *compactionSignal, plan *datapb.CompactionPlan) error {
	nodeID, err := c.chManager.FindWatcher(plan.GetChannel())
	if err != nil {
		log.Error("failed to find watcher", zap.Int64("planID", plan.GetPlanID()), zap.Error(err))
		return err
	}

	log := log.With(zap.Int64("planID", plan.GetPlanID()), zap.Int64("nodeID", nodeID))
	c.setSegmentsCompacting(plan, true)

	task := &compactionTask{
		triggerInfo: signal,
		plan:        plan,
		state:       pipelining,
		dataNodeID:  nodeID,
	}
	c.mu.Lock()
	c.plans[plan.PlanID] = task
	c.mu.Unlock()

	c.scheduler.Submit(task)
	log.Info("Compaction plan submited")
	return nil
}

func (c *compactionPlanHandler) RefreshPlan(task *compactionTask) {
	plan := task.plan
	log := log.With(zap.Int64("taskID", task.triggerInfo.id), zap.Int64("planID", plan.GetPlanID()))
	if plan.GetType() == datapb.CompactionType_Level0DeleteCompaction {
		sealedSegments := c.meta.SelectSegments(func(info *SegmentInfo) bool {
			return info.GetCollectionID() == task.triggerInfo.collectionID &&
				(task.triggerInfo.partitionID == -1 || info.GetPartitionID() == task.triggerInfo.partitionID) &&
				info.GetInsertChannel() == plan.GetChannel() &&
				isFlushState(info.GetState()) &&
				!info.isCompacting &&
				!info.GetIsImporting() &&
				info.GetLevel() != datapb.SegmentLevel_L0 &&
				info.GetDmlPosition().GetTimestamp() < task.triggerInfo.pos.GetTimestamp()
		})

		sealedSegBinlogs := lo.Map(sealedSegments, func(info *SegmentInfo, _ int) *datapb.CompactionSegmentBinlogs {
			return &datapb.CompactionSegmentBinlogs{
				SegmentID: info.GetID(),
				Level:     datapb.SegmentLevel_L1,
			}
		})

		plan.SegmentBinlogs = append(plan.SegmentBinlogs, sealedSegBinlogs...)
		log.Info("Compaction handler refreshed level zero compaction plan", zap.Any("target segments", sealedSegBinlogs))
		return
	}

	if plan.GetType() == datapb.CompactionType_MixCompaction {
		for _, seg := range plan.GetSegmentBinlogs() {
			if info := c.meta.GetHealthySegment(seg.GetSegmentID()); info != nil {
				seg.Deltalogs = info.GetDeltalogs()
			}
		}
		log.Info("Compaction handler refreshed mix compaction plan")
		return
	}
}

func (c *compactionPlanHandler) notifyTasks(tasks []*compactionTask) {
	for _, task := range tasks {
		// avoid closure capture iteration variable
		innerTask := task
		c.RefreshPlan(innerTask)
		getOrCreateIOPool().Submit(func() (any, error) {
			plan := innerTask.plan
			log := log.With(zap.Int64("planID", plan.GetPlanID()), zap.Int64("nodeID", innerTask.dataNodeID))
			log.Info("Notify compaction task to DataNode")
			ts, err := c.allocator.allocTimestamp(context.TODO())
			if err != nil {
				log.Warn("Alloc start time for CompactionPlan failed", zap.Error(err))
				// update plan ts to TIMEOUT ts
				c.updateTask(plan.PlanID, setState(executing), setStartTime(tsTimeout))
				return nil, err
			}
			c.updateTask(plan.PlanID, setStartTime(ts))
			err = c.sessions.Compaction(innerTask.dataNodeID, plan)
			c.updateTask(plan.PlanID, setState(executing))
			if err != nil {
				log.Warn("Failed to notify compaction tasks to DataNode", zap.Error(err))
				return nil, err
			}
			log.Info("Compaction start")
			return nil, nil
		})
	}
}

// execCompactionPlan start to execute plan and return immediately
func (c *compactionPlanHandler) execCompactionPlan(signal *compactionSignal, plan *datapb.CompactionPlan) error {
	return c.enqueuePlan(signal, plan)
}

func (c *compactionPlanHandler) setSegmentsCompacting(plan *datapb.CompactionPlan, compacting bool) {
	for _, segmentBinlogs := range plan.GetSegmentBinlogs() {
		c.meta.SetSegmentCompacting(segmentBinlogs.GetSegmentID(), compacting)
	}
}

// complete a compaction task
// not threadsafe, only can be used internally
func (c *compactionPlanHandler) completeCompaction(result *datapb.CompactionPlanResult) error {
	planID := result.PlanID
	if _, ok := c.plans[planID]; !ok {
		return fmt.Errorf("plan %d is not found", planID)
	}

	if c.plans[planID].state != executing {
		return fmt.Errorf("plan %d's state is %v", planID, c.plans[planID].state)
	}

	plan := c.plans[planID].plan
	nodeID := c.plans[planID].dataNodeID
	defer c.scheduler.Finish(nodeID, plan.PlanID)
	switch plan.GetType() {
	case datapb.CompactionType_MergeCompaction, datapb.CompactionType_MixCompaction:
		if err := c.handleMergeCompactionResult(plan, result); err != nil {
			return err
		}
	case datapb.CompactionType_Level0DeleteCompaction:
		if err := c.handleL0CompactionResult(plan, result); err != nil {
			return err
		}
	default:
		return errors.New("unknown compaction type")
	}
	c.plans[planID] = c.plans[planID].shadowClone(setState(completed), setResult(result))
	// TODO: when to clean task list
	UpdateCompactionSegmentSizeMetrics(result.GetSegments())
	return nil
}

func (c *compactionPlanHandler) handleL0CompactionResult(plan *datapb.CompactionPlan, result *datapb.CompactionPlanResult) error {
	var operators []UpdateOperator
	for _, seg := range result.GetSegments() {
		operators = append(operators, UpdateBinlogsOperator(seg.GetSegmentID(), nil, nil, seg.GetDeltalogs()))
	}

	levelZeroSegments := lo.Filter(plan.GetSegmentBinlogs(), func(b *datapb.CompactionSegmentBinlogs, _ int) bool {
		return b.GetLevel() == datapb.SegmentLevel_L0
	})

	for _, seg := range levelZeroSegments {
		operators = append(operators, UpdateStatusOperator(seg.SegmentID, commonpb.SegmentState_Dropped))
	}

	log.Info("meta update: update segments info for level zero compaction",
		zap.Int64("planID", plan.GetPlanID()),
	)
	return c.meta.UpdateSegmentsInfo(operators...)
}

func (c *compactionPlanHandler) handleMergeCompactionResult(plan *datapb.CompactionPlan, result *datapb.CompactionPlanResult) error {
	log := log.With(zap.Int64("planID", plan.GetPlanID()))
	if len(result.GetSegments()) == 0 || len(result.GetSegments()) > 1 {
		// should never happen
		log.Warn("illegal compaction results")
		return fmt.Errorf("Illegal compaction results: %v", result)
	}

	// Merge compaction has one and only one segment
	newSegmentInfo := c.meta.GetHealthySegment(result.GetSegments()[0].SegmentID)
	if newSegmentInfo != nil {
		log.Info("meta has already been changed, skip meta change and retry sync segments")
	} else {
		// Also prepare metric updates.
		modSegments, newSegment, metricMutation, err := c.meta.PrepareCompleteCompactionMutation(plan, result)
		if err != nil {
			return err
		}

		if err := c.meta.alterMetaStoreAfterCompaction(newSegment, modSegments); err != nil {
			log.Warn("fail to alert meta store", zap.Error(err))
			return err
		}

		// Apply metrics after successful meta update.
		metricMutation.commit()

		newSegmentInfo = newSegment
	}

	nodeID := c.plans[plan.GetPlanID()].dataNodeID
	req := &datapb.SyncSegmentsRequest{
		PlanID:        plan.PlanID,
		CompactedTo:   newSegmentInfo.GetID(),
		CompactedFrom: newSegmentInfo.GetCompactionFrom(),
		NumOfRows:     newSegmentInfo.GetNumOfRows(),
		StatsLogs:     newSegmentInfo.GetStatslogs(),
		ChannelName:   plan.GetChannel(),
		PartitionId:   newSegmentInfo.GetPartitionID(),
		CollectionId:  newSegmentInfo.GetCollectionID(),
	}

	log.Info("handleCompactionResult: syncing segments with node", zap.Int64("nodeID", nodeID))
	if err := c.sessions.SyncSegments(nodeID, req); err != nil {
		log.Warn("handleCompactionResult: fail to sync segments with node",
			zap.Int64("nodeID", nodeID), zap.Error(err))
		return err
	}

	log.Info("handleCompactionResult: success to handle merge compaction result")
	return nil
}

// getCompaction return compaction task. If planId does not exist, return nil.
func (c *compactionPlanHandler) getCompaction(planID int64) *compactionTask {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.plans[planID]
}

// expireCompaction set the compaction state to expired
func (c *compactionPlanHandler) updateCompaction(ts Timestamp) error {
	// Get executing executingTasks before GetCompactionState from DataNode to prevent false failure,
	//  for DC might add new task while GetCompactionState.
	executingTasks := c.getTasksByState(executing)
	timeoutTasks := c.getTasksByState(timeout)
	planStates := c.sessions.GetCompactionPlansResults()

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range executingTasks {
		planResult, ok := planStates[task.plan.PlanID]
		state := planResult.GetState()
		planID := task.plan.PlanID
		// check whether the state of CompactionPlan is working
		if ok {
			if state == commonpb.CompactionState_Completed {
				log.Info("complete compaction", zap.Int64("planID", planID), zap.Int64("nodeID", task.dataNodeID))
				err := c.completeCompaction(planResult)
				if err != nil {
					log.Warn("fail to complete compaction", zap.Int64("planID", planID), zap.Int64("nodeID", task.dataNodeID), zap.Error(err))
				}
				continue
			}
			// check wether the CompactionPlan is timeout
			if state == commonpb.CompactionState_Executing && !c.isTimeout(ts, task.plan.GetStartTime(), task.plan.GetTimeoutInSeconds()) {
				continue
			}
			log.Warn("compaction timeout",
				zap.Int64("planID", task.plan.PlanID),
				zap.Int64("nodeID", task.dataNodeID),
				zap.Uint64("startTime", task.plan.GetStartTime()),
				zap.Uint64("now", ts),
			)
			c.plans[planID] = c.plans[planID].shadowClone(setState(timeout))
			continue
		}

		log.Info("compaction failed", zap.Int64("planID", task.plan.PlanID), zap.Int64("nodeID", task.dataNodeID))
		c.plans[planID] = c.plans[planID].shadowClone(setState(failed))
		c.setSegmentsCompacting(task.plan, false)
		c.scheduler.Finish(task.dataNodeID, task.plan.PlanID)
	}

	// Timeout tasks will be timeout and failed in DataNode
	// need to wait for DataNode reporting failure and
	// clean the status.
	for _, task := range timeoutTasks {
		stateResult, ok := planStates[task.plan.PlanID]
		planID := task.plan.PlanID

		if !ok {
			log.Info("compaction failed for timeout", zap.Int64("planID", task.plan.PlanID), zap.Int64("nodeID", task.dataNodeID))
			c.plans[planID] = c.plans[planID].shadowClone(setState(failed))
			c.setSegmentsCompacting(task.plan, false)
			c.scheduler.Finish(task.dataNodeID, task.plan.PlanID)
		}

		// DataNode will check if plan's are timeout but not as sensitive as DataCoord,
		// just wait another round.
		if ok && stateResult.GetState() == commonpb.CompactionState_Executing {
			log.Info("compaction timeout in DataCoord yet DataNode is still running",
				zap.Int64("planID", planID),
				zap.Int64("nodeID", task.dataNodeID))
			continue
		}
	}
	return nil
}

func (c *compactionPlanHandler) isTimeout(now Timestamp, start Timestamp, timeout int32) bool {
	startTime, _ := tsoutil.ParseTS(start)
	ts, _ := tsoutil.ParseTS(now)
	return int32(ts.Sub(startTime).Seconds()) >= timeout
}

// isFull return true if the task pool is full
func (c *compactionPlanHandler) isFull() bool {
	return c.scheduler.GetTaskCount() >= Params.DataCoordCfg.CompactionMaxParallelTasks.GetAsInt()
}

func (c *compactionPlanHandler) getTasksByState(state compactionTaskState) []*compactionTask {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tasks := make([]*compactionTask, 0, len(c.plans))
	for _, plan := range c.plans {
		if plan.state == state {
			tasks = append(tasks, plan)
		}
	}
	return tasks
}

// get compaction tasks by signal id; if signalID == 0 return all tasks
func (c *compactionPlanHandler) getCompactionTasksBySignalID(signalID int64) []*compactionTask {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var tasks []*compactionTask
	for _, t := range c.plans {
		if signalID == 0 {
			tasks = append(tasks, t)
			continue
		}
		if t.triggerInfo.id != signalID {
			continue
		}
		tasks = append(tasks, t)
	}
	return tasks
}

type compactionTaskOpt func(task *compactionTask)

func setState(state compactionTaskState) compactionTaskOpt {
	return func(task *compactionTask) {
		task.state = state
	}
}

func setStartTime(startTime uint64) compactionTaskOpt {
	return func(task *compactionTask) {
		task.plan.StartTime = startTime
	}
}

func setResult(result *datapb.CompactionPlanResult) compactionTaskOpt {
	return func(task *compactionTask) {
		task.result = result
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
