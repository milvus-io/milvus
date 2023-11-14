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
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	plans     map[int64]*compactionTask // planID -> task
	sessions  *SessionManager
	meta      *meta
	chManager *ChannelManager
	mu        sync.RWMutex
	allocator allocator
	quit      chan struct{}
	wg        sync.WaitGroup
	scheduler *scheduler
}

type scheduler struct {
	taskNumber    *atomic.Int32
	queuingTasks  []*compactionTask
	parallelTasks map[int64][]*compactionTask
	mu            sync.RWMutex
}

func newScheduler() *scheduler {
	return &scheduler{
		taskNumber:    atomic.NewInt32(0),
		queuingTasks:  make([]*compactionTask, 0),
		parallelTasks: make(map[int64][]*compactionTask),
	}
}

// schedule pick 1 or 0 tasks for 1 node
func (s *scheduler) schedule() []*compactionTask {
	nodeTasks := make(map[int64][]*compactionTask) // nodeID

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, task := range s.queuingTasks {
		if _, ok := nodeTasks[task.dataNodeID]; !ok {
			nodeTasks[task.dataNodeID] = make([]*compactionTask, 0)
		}

		nodeTasks[task.dataNodeID] = append(nodeTasks[task.dataNodeID], task)
	}

	executable := make(map[int64]*compactionTask)

	pickPriorPolicy := func(tasks []*compactionTask, exclusiveChannels []string, executing []string) *compactionTask {
		for _, task := range tasks {
			if lo.Contains(exclusiveChannels, task.plan.GetChannel()) {
				continue
			}

			if task.plan.GetType() == datapb.CompactionType_Level0DeleteCompaction {
				// Channel of LevelZeroCompaction task with no executing compactions
				if !lo.Contains(executing, task.plan.GetChannel()) {
					return task
				}

				// Don't schedule any tasks for channel with LevelZeroCompaction task
				// when there're executing compactions
				exclusiveChannels = append(exclusiveChannels, task.plan.GetChannel())
				continue
			}

			return task
		}

		return nil
	}

	// pick 1 or 0 task for 1 node
	for node, tasks := range nodeTasks {
		parallel := s.parallelTasks[node]
		if len(parallel) >= calculateParallel() {
			log.Info("Compaction parallel in DataNode reaches the limit", zap.Int64("nodeID", node), zap.Int("parallel", len(parallel)))
			continue
		}

		var (
			executing         = typeutil.NewSet[string]()
			channelsExecPrior = typeutil.NewSet[string]()
		)
		for _, t := range parallel {
			executing.Insert(t.plan.GetChannel())
			if t.plan.GetType() == datapb.CompactionType_Level0DeleteCompaction {
				channelsExecPrior.Insert(t.plan.GetChannel())
			}
		}

		picked := pickPriorPolicy(tasks, channelsExecPrior.Collect(), executing.Collect())
		if picked != nil {
			executable[node] = picked
		}
	}

	var pickPlans []int64
	for node, task := range executable {
		pickPlans = append(pickPlans, task.plan.PlanID)
		if _, ok := s.parallelTasks[node]; !ok {
			s.parallelTasks[node] = []*compactionTask{task}
		} else {
			s.parallelTasks[node] = append(s.parallelTasks[node], task)
		}
	}

	s.queuingTasks = lo.Filter(s.queuingTasks, func(t *compactionTask, _ int) bool {
		return !lo.Contains(pickPlans, t.plan.PlanID)
	})

	// clean parallelTasks with nodes of no running tasks
	for node, tasks := range s.parallelTasks {
		if len(tasks) == 0 {
			delete(s.parallelTasks, node)
		}
	}

	return lo.Values(executable)
}

func (s *scheduler) finish(nodeID, planID UniqueID) {
	s.mu.Lock()
	if parallel, ok := s.parallelTasks[nodeID]; ok {
		tasks := lo.Filter(parallel, func(t *compactionTask, _ int) bool {
			return t.plan.PlanID != planID
		})
		s.parallelTasks[nodeID] = tasks
		s.taskNumber.Dec()
	}
	s.mu.Unlock()

	log.Info("Compaction finished", zap.Int64("planID", planID), zap.Int64("nodeID", nodeID))
	s.logStatus()
}

func (s *scheduler) logStatus() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	waiting := lo.Map(s.queuingTasks, func(t *compactionTask, _ int) int64 {
		return t.plan.PlanID
	})

	var executing []int64
	for _, tasks := range s.parallelTasks {
		executing = append(executing, lo.Map(tasks, func(t *compactionTask, _ int) int64 {
			return t.plan.PlanID
		})...)
	}

	if len(waiting) > 0 || len(executing) > 0 {
		log.Info("Compaction scheduler status", zap.Int64s("waiting", waiting), zap.Int64s("executing", executing))
	}
}

func (s *scheduler) submit(tasks ...*compactionTask) {
	s.mu.Lock()
	s.queuingTasks = append(s.queuingTasks, tasks...)
	s.mu.Unlock()

	s.taskNumber.Add(int32(len(tasks)))
	s.logStatus()
}

func (s *scheduler) getExecutingTaskNum() int {
	return int(s.taskNumber.Load())
}

func newCompactionPlanHandler(sessions *SessionManager, cm *ChannelManager, meta *meta, allocator allocator,
) *compactionPlanHandler {
	return &compactionPlanHandler{
		plans:     make(map[int64]*compactionTask),
		chManager: cm,
		meta:      meta,
		sessions:  sessions,
		allocator: allocator,
		scheduler: newScheduler(),
	}
}

func (c *compactionPlanHandler) start() {
	interval := Params.DataCoordCfg.CompactionCheckIntervalInSeconds.GetAsDuration(time.Second)
	c.quit = make(chan struct{})
	c.wg.Add(1)

	go func() {
		defer c.wg.Done()
		checkResultTicker := time.NewTicker(interval)
		scheduleTicker := time.NewTicker(200 * time.Millisecond)
		log.Info("compaction handler start", zap.Any("check result interval", interval))
		defer checkResultTicker.Stop()
		defer scheduleTicker.Stop()
		for {
			select {
			case <-c.quit:
				log.Info("compaction handler quit")
				return
			case <-checkResultTicker.C:
				// deal results
				cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				ts, err := c.allocator.allocTimestamp(cctx)
				if err != nil {
					log.Warn("unable to alloc timestamp", zap.Error(err))
					cancel()
					continue
				}
				cancel()
				_ = c.updateCompaction(ts)

			case <-scheduleTicker.C:
				// schedule queuing tasks
				tasks := c.scheduler.schedule()
				c.notifyTasks(tasks)

				if len(tasks) > 0 {
					c.scheduler.logStatus()
				}
			}
		}
	}()
}

func (c *compactionPlanHandler) stop() {
	close(c.quit)
	c.wg.Wait()
}

func (c *compactionPlanHandler) updateTask(planID int64, opts ...compactionTaskOpt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.plans[planID] = c.plans[planID].shadowClone(opts...)
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

	c.scheduler.submit(task)
	log.Info("Compaction plan submited")
	return nil
}

func (c *compactionPlanHandler) notifyTasks(tasks []*compactionTask) {
	for _, task := range tasks {
		// avoid closure capture iteration variable
		innerTask := task
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
	defer c.scheduler.finish(nodeID, plan.PlanID)
	switch plan.GetType() {
	case datapb.CompactionType_MergeCompaction, datapb.CompactionType_MixCompaction:
		if err := c.handleMergeCompactionResult(plan, result); err != nil {
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

func (c *compactionPlanHandler) handleMergeCompactionResult(plan *datapb.CompactionPlan, result *datapb.CompactionPlanResult) error {
	// Also prepare metric updates.
	_, modSegments, newSegment, metricMutation, err := c.meta.PrepareCompleteCompactionMutation(plan, result)
	if err != nil {
		return err
	}
	log := log.With(zap.Int64("planID", plan.GetPlanID()))

	if err := c.meta.alterMetaStoreAfterCompaction(newSegment, modSegments); err != nil {
		log.Warn("fail to alert meta store", zap.Error(err))
		return err
	}

	nodeID := c.plans[plan.GetPlanID()].dataNodeID
	req := &datapb.SyncSegmentsRequest{
		PlanID:        plan.PlanID,
		CompactedTo:   newSegment.GetID(),
		CompactedFrom: newSegment.GetCompactionFrom(),
		NumOfRows:     newSegment.GetNumOfRows(),
		StatsLogs:     newSegment.GetStatslogs(),
		ChannelName:   plan.GetChannel(),
		PartitionId:   newSegment.GetPartitionID(),
		CollectionId:  newSegment.GetCollectionID(),
	}

	log.Info("handleCompactionResult: syncing segments with node", zap.Int64("nodeID", nodeID))
	if err := c.sessions.SyncSegments(nodeID, req); err != nil {
		log.Warn("handleCompactionResult: fail to sync segments with node, reverting metastore",
			zap.Int64("nodeID", nodeID), zap.Error(err))
		return err
	}
	// Apply metrics after successful meta update.
	metricMutation.commit()

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
		c.scheduler.finish(task.dataNodeID, task.plan.PlanID)
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
			c.scheduler.finish(task.dataNodeID, task.plan.PlanID)
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
	return c.scheduler.getExecutingTaskNum() >= Params.DataCoordCfg.CompactionMaxParallelTasks.GetAsInt()
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
