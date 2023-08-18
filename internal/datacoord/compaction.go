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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
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
	// getCompaction return compaction task. If planID does not exist, return nil.
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
	sessions  *SessionManager
	meta      *meta
	chManager *ChannelManager
	allocator allocator

	plansGuard sync.RWMutex
	plans      map[int64]*compactionTask // planID -> task

	executingTaskNum *atomic.Int64
	parallelCh       *typeutil.ConcurrentMap[int64, chan struct{}] // nodeID -> queue
	flushCh          chan UniqueID

	stopSig    chan struct{}
	stopOnce   sync.Once
	stopWaiter sync.WaitGroup
}

func newCompactionPlanHandler(sessions *SessionManager, cm *ChannelManager, meta *meta,
	allocator allocator, flush chan UniqueID) *compactionPlanHandler {
	return &compactionPlanHandler{
		plans:            make(map[int64]*compactionTask),
		chManager:        cm,
		meta:             meta,
		sessions:         sessions,
		allocator:        allocator,
		flushCh:          flush,
		parallelCh:       typeutil.NewConcurrentMap[int64, chan struct{}](),
		stopSig:          make(chan struct{}),
		executingTaskNum: atomic.NewInt64(0),
	}
}

func (c *compactionPlanHandler) start() {
	interval := Params.DataCoordCfg.CompactionCheckIntervalInSeconds.GetAsDuration(time.Second)
	c.stopWaiter.Add(1)

	go func() {
		defer c.stopWaiter.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopSig:
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
				c.updateCompaction(ts)
			}
		}
	}()
}

func (c *compactionPlanHandler) stop() {
	c.stopOnce.Do(func() {
		close(c.stopSig)
		c.stopWaiter.Wait()
	})
}

// unsafe inner func
func (c *compactionPlanHandler) updateTask(planID int64, opts ...compactionTaskOpt) {
	c.plans[planID] = c.plans[planID].shadowClone(opts...)
}

// unsafe inner func
func (c *compactionPlanHandler) addTask(planID int64, task *compactionTask) {
	c.plans[planID] = task
	c.executingTaskNum.Inc()
}

// unsafe inner func
func (c *compactionPlanHandler) finishTask(planID int64, opts ...compactionTaskOpt) {
	c.updateTask(planID, opts...)
	c.executingTaskNum.Dec()
}

// execCompactionPlan start to execute plan and return immediately
func (c *compactionPlanHandler) execCompactionPlan(signal *compactionSignal, plan *datapb.CompactionPlan) error {
	planID := plan.GetPlanID()
	log := log.With(
		zap.String("channel", plan.GetChannel()),
		zap.Int64("planID", planID))

	nodeID, err := c.chManager.FindWatcher(plan.GetChannel())
	if err != nil {
		log.Error("failed to find watcher", zap.Error(err))
		return err
	}

	log = log.With(zap.Int64("nodeID", nodeID))

	c.setSegmentsCompacting(plan, true)
	task := &compactionTask{
		triggerInfo: signal,
		plan:        plan,
		state:       pipelining,
		dataNodeID:  nodeID,
	}
	c.plansGuard.Lock()
	c.addTask(planID, task)
	c.plansGuard.Unlock()

	go func() {
		c.plansGuard.Lock()
		defer c.plansGuard.Unlock()
		log.Info("acquire queue")
		c.acquireQueue(nodeID)

		ts, err := c.allocator.allocTimestamp(context.TODO())
		if err != nil {
			log.Warn("failed to alloc start time for compaction")
			// update plan ts to TIMEOUT ts
			c.updateTask(planID, setState(executing), setStartTime(tsTimeout))
			return
		}
		c.updateTask(planID, setStartTime(ts))
		err = c.sessions.Compaction(nodeID, plan)
		c.updateTask(planID, setState(executing))
		if err != nil {
			log.Warn("try to call Compaction but DataNode rejected")
			// do nothing here, prevent double release, see issue#21014
			// release queue will be done in `updateCompaction`
			return
		}
		log.Info("start compaction")
	}()
	return nil
}

func (c *compactionPlanHandler) setSegmentsCompacting(plan *datapb.CompactionPlan, compacting bool) {
	for _, segmentBinlogs := range plan.GetSegmentBinlogs() {
		c.meta.SetSegmentCompacting(segmentBinlogs.GetSegmentID(), compacting)
	}
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
	c.plansGuard.RLock()
	defer c.plansGuard.RUnlock()

	return c.plans[planID]
}

// unsave inner func
func (c *compactionPlanHandler) completeCompaction(ts Timestamp, cachedTask *compactionTask, latestResult *datapb.CompactionStateResult) error {
	var (
		resultState = latestResult.GetState()
		result      = latestResult.GetResult()
		planID      = latestResult.GetPlanID()
	)

	log := log.With(zap.Int64("planID", planID), zap.Int64("nodeID", cachedTask.dataNodeID))

	if cachedTask.state == executing {
		if resultState == commonpb.CompactionState_Completed {
			log.Info("try to complete compaction")
			if err := c.handleMergeCompactionResult(cachedTask.plan, result); err != nil {
				log.Warn("failed to complete compaction")
				c.finishTask(planID, setState(failed))
				c.releaseQueue(cachedTask.dataNodeID)
				return err
			}
			c.finishTask(planID, setState(completed))
			c.releaseQueue(cachedTask.dataNodeID)
			// All compaction now should be flushed.
			c.flushCh <- result.GetSegmentID()

			metrics.DataCoordCompactedSegmentSize.WithLabelValues().Observe(float64(getCompactedSegmentSize(result)))
			return nil
		}

		if resultState == commonpb.CompactionState_Executing {
			if c.isTimeout(ts, cachedTask.plan.GetStartTime(), cachedTask.plan.GetTimeoutInSeconds()) {
				log.Warn("compaction timeout", zap.Uint64("startTime", cachedTask.plan.GetStartTime()), zap.Uint64("now", ts))
				c.finishTask(planID, setState(timeout))
				c.releaseQueue(cachedTask.dataNodeID)
			}
			return nil
		}

		log.Warn("unknown results state", zap.String("state", resultState.String()))
		return nil
	}

	log.Warn("compaction plan already finished", zap.Any("finished state", cachedTask.state))
	return nil
}

// updateCompaction updates the compaction task state
func (c *compactionPlanHandler) updateCompaction(ts Timestamp) error {
	// Get executing cachedTasks before GetCompactionState from DataNode to prevent false failure,
	//  for DC might add new task while GetCompactionState.
	cachedTasks := c.getExecutingCompactions()
	latestPlanStates := c.sessions.GetCompactionState()

	c.plansGuard.Lock()
	defer c.plansGuard.Unlock()
	for _, cachedTask := range cachedTasks {
		var (
			planID = cachedTask.plan.PlanID
			nodeID = cachedTask.dataNodeID
		)
		log := log.With(zap.Int64("planID", planID), zap.Int64("nodeID", nodeID))

		// plan not in the latestPlanStates from DataNode, means the comapction has failed in DN
		latestResult, ok := latestPlanStates[planID]
		if ok {
			err := c.completeCompaction(ts, cachedTask, latestResult)
			if err != nil {
				log.Warn("fail to finish compaction", zap.Error(err))
			}
			// continues to deal other tasks
			continue
		}

		log.Warn("compaction failed")
		c.finishTask(planID, setState(failed))
		c.releaseQueue(nodeID)
		c.setSegmentsCompacting(cachedTask.plan, false)
	}

	return nil
}

func (c *compactionPlanHandler) isTimeout(now Timestamp, start Timestamp, timeout int32) bool {
	startTime, _ := tsoutil.ParseTS(start)
	ts, _ := tsoutil.ParseTS(now)
	return int32(ts.Sub(startTime).Seconds()) >= timeout
}

func (c *compactionPlanHandler) acquireQueue(nodeID int64) {
	ch, _ := c.parallelCh.GetOrInsert(nodeID, make(chan struct{}, calculateParallel()))

	log.Info("try to acquire queue", zap.Int64("nodeID", nodeID), zap.Int("qsize before acquire", len(ch)))
	ch <- struct{}{}
}

func (c *compactionPlanHandler) releaseQueue(nodeID int64) {
	if ch, ok := c.parallelCh.Get(nodeID); ok {
		log.Info("try to release queue", zap.Int64("nodeID", nodeID), zap.Int("qsize before release", len(ch)))
		<-ch
	}
}

// isFull return true if the task pool is full
func (c *compactionPlanHandler) isFull() bool {
	return c.executingTaskNum.Load() >= Params.DataCoordCfg.CompactionMaxParallelTasks.GetAsInt64()
}

func (c *compactionPlanHandler) getExecutingCompactions() []*compactionTask {
	c.plansGuard.RLock()
	defer c.plansGuard.RUnlock()
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
	c.plansGuard.RLock()
	defer c.plansGuard.RUnlock()

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

func calculateParallel() int {
	// TODO after node memory management enabled, use this config as hard limit
	return Params.DataCoordCfg.CompactionWorkerParalleTasks.GetAsInt()
}
