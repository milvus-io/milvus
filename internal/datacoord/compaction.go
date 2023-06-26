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
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
)

// TODO this num should be determined by resources of datanode, for now, we set to a fixed value for simple
// TODO we should split compaction into different priorities, small compaction helps to merge segment, large compaction helps to handle delta and expiration of large segments
const (
	maxParallelCompactionTaskNum = 100
	rpcCompactionTimeout         = 10 * time.Second
	tsTimeout                    = uint64(1)
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
	result      *datapb.CompactionResult
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
	plans            map[int64]*compactionTask // planID -> task
	sessions         *SessionManager
	meta             *meta
	chManager        *ChannelManager
	mu               sync.RWMutex
	executingTaskNum int
	allocator        allocator
	quit             chan struct{}
	wg               sync.WaitGroup
	flushCh          chan UniqueID
	//segRefer         *SegmentReferenceManager
	parallelCh map[int64]chan struct{}
}

func newCompactionPlanHandler(sessions *SessionManager, cm *ChannelManager, meta *meta,
	allocator allocator, flush chan UniqueID) *compactionPlanHandler {
	return &compactionPlanHandler{
		plans:     make(map[int64]*compactionTask),
		chManager: cm,
		meta:      meta,
		sessions:  sessions,
		allocator: allocator,
		flushCh:   flush,
		//segRefer:   segRefer,
		parallelCh: make(map[int64]chan struct{}),
	}
}

func (c *compactionPlanHandler) start() {
	interval := Params.DataCoordCfg.CompactionCheckIntervalInSeconds.GetAsDuration(time.Second)
	c.quit = make(chan struct{})
	c.wg.Add(1)

	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.quit:
				log.Info("compaction handler quit")
				return
			case <-ticker.C:
				cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				ts, err := c.allocator.allocTimestamp(cctx)
				if err != nil {
					log.Warn("unable to alloc timestamp", zap.Error(err))
					cancel()
					continue
				}
				cancel()
				_ = c.updateCompaction(ts)
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

// execCompactionPlan start to execute plan and return immediately
func (c *compactionPlanHandler) execCompactionPlan(signal *compactionSignal, plan *datapb.CompactionPlan) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeID, err := c.chManager.FindWatcher(plan.GetChannel())
	if err != nil {
		log.Error("failed to find watcher",
			zap.Int64("plan ID", plan.GetPlanID()),
			zap.Error(err))
		return err
	}

	c.setSegmentsCompacting(plan, true)

	task := &compactionTask{
		triggerInfo: signal,
		plan:        plan,
		state:       pipelining,
		dataNodeID:  nodeID,
	}
	c.plans[plan.PlanID] = task
	c.executingTaskNum++

	go func() {
		log.Info("acquire queue", zap.Int64("nodeID", nodeID), zap.Int64("planID", plan.GetPlanID()))
		c.acquireQueue(nodeID)

		ts, err := c.allocator.allocTimestamp(context.TODO())
		if err != nil {
			log.Warn("Alloc start time for CompactionPlan failed", zap.Int64("planID", plan.GetPlanID()))
			// update plan ts to TIMEOUT ts
			c.updateTask(plan.PlanID, setState(executing), setStartTime(tsTimeout))
			return
		}
		c.updateTask(plan.PlanID, setStartTime(ts))
		err = c.sessions.Compaction(nodeID, plan)
		c.updateTask(plan.PlanID, setState(executing))
		if err != nil {
			log.Warn("try to Compaction but DataNode rejected",
				zap.Int64("targetNodeID", nodeID),
				zap.Int64("planID", plan.GetPlanID()),
			)
			// do nothing here, prevent double release, see issue#21014
			// release queue will be done in `updateCompaction`
			return
		}
		log.Info("start compaction", zap.Int64("nodeID", nodeID), zap.Int64("planID", plan.GetPlanID()))
	}()
	return nil
}

func (c *compactionPlanHandler) setSegmentsCompacting(plan *datapb.CompactionPlan, compacting bool) {
	for _, segmentBinlogs := range plan.GetSegmentBinlogs() {
		c.meta.SetSegmentCompacting(segmentBinlogs.GetSegmentID(), compacting)
	}
}

// complete a compaction task
// not threadsafe, only can be used internally
func (c *compactionPlanHandler) completeCompaction(result *datapb.CompactionResult) error {
	planID := result.PlanID
	if _, ok := c.plans[planID]; !ok {
		return fmt.Errorf("plan %d is not found", planID)
	}

	if c.plans[planID].state != executing {
		return fmt.Errorf("plan %d's state is %v", planID, c.plans[planID].state)
	}

	plan := c.plans[planID].plan
	switch plan.GetType() {
	case datapb.CompactionType_MergeCompaction, datapb.CompactionType_MixCompaction:
		if err := c.handleMergeCompactionResult(plan, result); err != nil {
			return err
		}
	default:
		return errors.New("unknown compaction type")
	}
	c.plans[planID] = c.plans[planID].shadowClone(setState(completed), setResult(result))
	c.executingTaskNum--
	if c.plans[planID].plan.GetType() == datapb.CompactionType_MergeCompaction ||
		c.plans[planID].plan.GetType() == datapb.CompactionType_MixCompaction {
		c.flushCh <- result.GetSegmentID()
	}
	// TODO: when to clean task list

	nodeID := c.plans[planID].dataNodeID
	c.releaseQueue(nodeID)

	metrics.DataCoordCompactedSegmentSize.WithLabelValues().Observe(float64(getCompactedSegmentSize(result)))
	return nil
}

func (c *compactionPlanHandler) handleMergeCompactionResult(plan *datapb.CompactionPlan, result *datapb.CompactionResult) error {
	// Also prepare metric updates.
	_, modSegments, newSegment, metricMutation, err := c.meta.PrepareCompleteCompactionMutation(plan.GetSegmentBinlogs(), result)
	if err != nil {
		return err
	}
	log := log.With(zap.Int64("planID", plan.GetPlanID()))

	if err := c.meta.alterMetaStoreAfterCompaction(newSegment, modSegments); err != nil {
		log.Warn("fail to alert meta store", zap.Error(err))
		return err
	}

	var nodeID = c.plans[plan.GetPlanID()].dataNodeID
	req := &datapb.SyncSegmentsRequest{
		PlanID:        plan.PlanID,
		CompactedTo:   newSegment.GetID(),
		CompactedFrom: newSegment.GetCompactionFrom(),
		NumOfRows:     newSegment.GetNumOfRows(),
		StatsLogs:     newSegment.GetStatslogs(),
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
	planStates := c.sessions.GetCompactionState()

	c.mu.Lock()
	defer c.mu.Unlock()

	tasks := c.getExecutingCompactions()
	for _, task := range tasks {
		stateResult, ok := planStates[task.plan.PlanID]
		state := stateResult.GetState()
		planID := task.plan.PlanID
		// check whether the state of CompactionPlan is working
		if ok {
			if state == commonpb.CompactionState_Completed {
				log.Info("compaction completed", zap.Int64("planID", planID), zap.Int64("nodeID", task.dataNodeID))
				err := c.completeCompaction(stateResult.GetResult())
				if err != nil {
					log.Warn("fail to complete compaction", zap.Int64("planID", planID), zap.Int64("nodeID", task.dataNodeID), zap.Error(err))
				}
				continue
			}
			// check wether the CompactionPlan is timeout
			if state == commonpb.CompactionState_Executing && !c.isTimeout(ts, task.plan.GetStartTime(), task.plan.GetTimeoutInSeconds()) {
				continue
			}
			log.Info("compaction timeout",
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
		c.executingTaskNum--
		c.releaseQueue(task.dataNodeID)
	}

	return nil
}

func (c *compactionPlanHandler) isTimeout(now Timestamp, start Timestamp, timeout int32) bool {
	startTime, _ := tsoutil.ParseTS(start)
	ts, _ := tsoutil.ParseTS(now)
	return int32(ts.Sub(startTime).Seconds()) >= timeout
}

func (c *compactionPlanHandler) acquireQueue(nodeID int64) {
	c.mu.Lock()
	_, ok := c.parallelCh[nodeID]
	if !ok {
		c.parallelCh[nodeID] = make(chan struct{}, calculateParallel())
	}
	c.mu.Unlock()

	c.mu.RLock()
	ch := c.parallelCh[nodeID]
	c.mu.RUnlock()
	ch <- struct{}{}
}

func (c *compactionPlanHandler) releaseQueue(nodeID int64) {
	log.Info("try to release queue", zap.Int64("nodeID", nodeID))
	ch, ok := c.parallelCh[nodeID]
	if !ok {
		return
	}
	<-ch
}

// isFull return true if the task pool is full
func (c *compactionPlanHandler) isFull() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.executingTaskNum >= maxParallelCompactionTaskNum
}

func (c *compactionPlanHandler) getExecutingCompactions() []*compactionTask {
	tasks := make([]*compactionTask, 0, len(c.plans))
	for _, plan := range c.plans {
		if plan.state == executing {
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

func setResult(result *datapb.CompactionResult) compactionTaskOpt {
	return func(task *compactionTask) {
		task.result = result
	}
}

// 0.5*min(8, NumCPU/2)
func calculateParallel() int {
	return 2
	//cores := runtime.NumCPU()
	//if cores < 16 {
	//return 4
	//}
	//return cores / 2
}
