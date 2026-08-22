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
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// maxConcurrentCleanups bounds the cleanup fan-out. Cleanup is metadata work
// that may first wait out a worker callback, so it needs a ceiling, but it is
// also on the path that frees input segments, so the ceiling is not tight.
const maxConcurrentCleanups = 16

// maxCompactionTaskExecutionDuration is how long each type is expected to take.
// It drives the "this task is running slow" warning only -- an unreachable
// worker abandons the attempt at the first unanswered query -- so the values
// are per-type: what counts as slow depends on the work.
var maxCompactionTaskExecutionDuration = map[datapb.CompactionType]time.Duration{
	datapb.CompactionType_MixCompaction:               30 * time.Minute,
	datapb.CompactionType_Level0DeleteCompaction:      30 * time.Minute,
	datapb.CompactionType_ClusteringCompaction:        60 * time.Minute,
	datapb.CompactionType_SortCompaction:              20 * time.Minute,
	datapb.CompactionType_BumpSchemaVersionCompaction: 30 * time.Minute,
}

type CompactionInspector interface {
	// start launches every background loop unconditionally. The inspector never
	// consults dataCoord.enableCompaction -- that switch gates the producers of
	// new work, not execution; see start() on compactionInspector.
	start()
	stop()
	// enqueueCompaction start to enqueue compaction task and return immediately
	enqueueCompaction(task *datapb.CompactionTask) error
	// isFull return true if the task pool is full
	isFull() bool
	// get compaction tasks by signal id
	getCompactionTasksNumByTriggerID(triggerID int64) int
	getCompactionInfo(ctx context.Context, triggerID int64) *compactionInfo
	removeTasksByChannel(channel string)
	getCompactionTasksNum(filters ...compactionTaskFilter) int
}

var _ CompactionInspector = (*compactionInspector)(nil)

type compactionInfo struct {
	state        commonpb.CompactionState
	executingCnt int
	completedCnt int
	failedCnt    int
	timeoutCnt   int
	mergeInfos   map[int64]*milvuspb.CompactionMergeInfo
}

type compactionInspector struct {
	// ctx is the process-lifetime context handed to CreateServer. Stop() never
	// cancels it -- it cancels serverLoopCtx, a child -- so during a graceful
	// stop the in-flight debt writes (the replacement record, the cleaned
	// state) still complete under it; only the process being torn down aborts
	// them, and recovery redoes what was lost. Every outgoing call is
	// additionally bounded below this layer: etcdKV wraps each op in its
	// request timeout, worker RPCs in dataCoord.requestTimeoutSeconds, and the
	// allocator in its own bound.
	ctx context.Context

	queueTasks *CompactionQueue

	executingGuard lock.RWMutex
	executingTasks map[int64]CompactionTask // planID -> task

	cleaningGuard lock.RWMutex
	cleaningTasks map[int64]CompactionTask // planID -> task
	// cleaningInFlight, also under cleaningGuard, holds the planIDs whose
	// cleanup goroutine has not finished. Membership is the cleanup slot: it
	// keeps a slow cleanup from being re-dispatched every schedule round, and
	// its size bounds the fan-out at maxConcurrentCleanups.
	cleaningInFlight map[int64]struct{}

	meta      CompactionMeta
	allocator allocator.Allocator
	handler   Handler
	cluster   session.Cluster
	scheduler task.GlobalScheduler
	ievm      IndexEngineVersionManager

	stopCh   chan struct{}
	stopOnce sync.Once
	stopWg   sync.WaitGroup
}

func (c *compactionInspector) getCompactionInfo(ctx context.Context, triggerID int64) *compactionInfo {
	tasks := c.meta.GetCompactionTasksByTriggerID(ctx, triggerID)
	return summaryCompactionState(ctx, triggerID, tasks)
}

func summaryCompactionState(ctx context.Context, triggerID int64, tasks []*datapb.CompactionTask) *compactionInfo {
	ret := &compactionInfo{}
	var executingCnt, pipeliningCnt, completedCnt, failedCnt, timeoutCnt, analyzingCnt, indexingCnt, cleanedCnt, metaSavedCnt, stats, awaitingReplanCnt int
	mergeInfos := make(map[int64]*milvuspb.CompactionMergeInfo)

	for _, task := range tasks {
		if task == nil {
			continue
		}
		// A task still owed a rebuild stands for work that is owed, not work
		// that is over: this trigger keeps its ID across the rebuild, so
		// reporting the failure now would tell the caller the compaction had
		// settled while cleanup is about to retry it.
		if isRetrying(task) {
			awaitingReplanCnt++
			mergeInfos[task.GetPlanID()] = getCompactionMergeInfo(task)
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
		case datapb.CompactionTaskState_analyzing:
			analyzingCnt++
		case datapb.CompactionTaskState_indexing:
			indexingCnt++
		case datapb.CompactionTaskState_cleaned:
			cleanedCnt++
		case datapb.CompactionTaskState_meta_saved:
			metaSavedCnt++
		case datapb.CompactionTaskState_statistic:
			stats++
		default:
		}
		mergeInfos[task.GetPlanID()] = getCompactionMergeInfo(task)
	}

	ret.executingCnt = executingCnt + pipeliningCnt + analyzingCnt + indexingCnt + metaSavedCnt + stats + awaitingReplanCnt
	ret.completedCnt = completedCnt
	ret.timeoutCnt = timeoutCnt
	ret.failedCnt = failedCnt
	ret.mergeInfos = mergeInfos

	if ret.executingCnt != 0 {
		ret.state = commonpb.CompactionState_Executing
	} else {
		ret.state = commonpb.CompactionState_Completed
	}

	mlog.Info(ctx, "compaction states",
		mlog.Int64("triggerID", triggerID),
		mlog.String("state", ret.state.String()),
		mlog.Int("executingCnt", executingCnt),
		mlog.Int("pipeliningCnt", pipeliningCnt),
		mlog.Int("completedCnt", completedCnt),
		mlog.Int("failedCnt", failedCnt),
		mlog.Int("analyzingCnt", analyzingCnt),
		mlog.Int("indexingCnt", indexingCnt),
		mlog.Int("timeoutCnt", timeoutCnt),
		mlog.Int("cleanedCnt", cleanedCnt),
		mlog.Int("metaSavedCnt", metaSavedCnt),
		mlog.Int("awaitingReplanCnt", awaitingReplanCnt))
	return ret
}

func (c *compactionInspector) getCompactionTasksNumByTriggerID(triggerID int64) int {
	cnt := 0
	c.queueTasks.ForEach(func(ct CompactionTask) {
		if ct.GetTask().GetTriggerID() == triggerID {
			cnt += 1
		}
	})
	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
		if t.GetTask().GetTriggerID() == triggerID {
			cnt += 1
		}
	}
	c.executingGuard.RUnlock()
	return cnt
}

func newCompactionInspector(ctx context.Context, meta CompactionMeta,
	allocator allocator.Allocator, handler Handler, cluster session.Cluster, scheduler task.GlobalScheduler, ievm IndexEngineVersionManager,
) *compactionInspector {
	capacity := paramtable.Get().DataCoordCfg.CompactionTaskQueueCapacity.GetAsInt()
	return &compactionInspector{
		ctx:              ctx,
		queueTasks:       NewCompactionQueue(capacity, getPrioritizer()),
		meta:             meta,
		allocator:        allocator,
		stopCh:           make(chan struct{}),
		executingTasks:   make(map[int64]CompactionTask),
		cleaningTasks:    make(map[int64]CompactionTask),
		cleaningInFlight: make(map[int64]struct{}),
		handler:          handler,
		cluster:          cluster,
		scheduler:        scheduler,
		ievm:             ievm,
	}
}

func (c *compactionInspector) checkSchedule() {
	c.checkCompaction()
	c.cleanFailedTasks()
	c.reconcileReplacements()
	c.schedule()
}

func (c *compactionInspector) schedule() []CompactionTask {
	selected := make([]CompactionTask, 0)

	// Sync before the empty-queue early return, so a configuration change made
	// while the queue happens to be empty is still adopted. The cost on an
	// empty queue is one lock acquisition and one string compare -- the
	// re-prioritize loop iterates zero times.
	c.queueTasks.SyncPrioritizer(getPrioritizerName())

	if c.queueTasks.Len() == 0 {
		return selected
	}

	l0ChannelExcludes := typeutil.NewSet[string]()
	mixChannelExcludes := typeutil.NewSet[string]()
	clusterChannelExcludes := typeutil.NewSet[string]()
	mixLabelExcludes := typeutil.NewSet[string]()
	clusterLabelExcludes := typeutil.NewSet[string]()

	exclude := func(t CompactionTask) {
		switch t.GetTask().GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			l0ChannelExcludes.Insert(t.GetTask().GetChannel())
		case datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction, datapb.CompactionType_BumpSchemaVersionCompaction:
			mixChannelExcludes.Insert(t.GetTask().GetChannel())
			mixLabelExcludes.Insert(t.GetLabel())
		case datapb.CompactionType_ClusteringCompaction:
			clusterChannelExcludes.Insert(t.GetTask().GetChannel())
			clusterLabelExcludes.Insert(t.GetLabel())
		}
	}

	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
		exclude(t)
	}
	c.executingGuard.RUnlock()

	// A task awaiting cleanup has left executingTasks but still owns its input
	// segments until cleanup releases them, and a worker callback for it may
	// still be submitting results. Keep excluding its channel and label, or a
	// same-label task could start while the old one is still finishing.
	c.cleaningGuard.RLock()
	for _, t := range c.cleaningTasks {
		exclude(t)
	}
	c.cleaningGuard.RUnlock()

	excluded := make([]CompactionTask, 0)
	defer func() {
		// Add back the excluded tasks. Enqueue always accepts, so a task that
		// was popped for an exclusion decision can always go home.
		for _, t := range excluded {
			c.queueTasks.Enqueue(t)
		}
	}()

	// The schedule loop will stop if either:
	// 1. no more task to schedule (the task queue is empty)
	// 2. no available slots
	for {
		t, err := c.queueTasks.Dequeue()
		if err != nil {
			break // 1. no more task to schedule
		}

		switch t.GetTask().GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			if mixChannelExcludes.Contain(t.GetTask().GetChannel()) ||
				clusterChannelExcludes.Contain(t.GetTask().GetChannel()) {
				excluded = append(excluded, t)
				continue
			}
			l0ChannelExcludes.Insert(t.GetTask().GetChannel())
			selected = append(selected, t)
		case datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction, datapb.CompactionType_BumpSchemaVersionCompaction:
			// BumpSchemaVersionCompaction shares the same exclusion rules as Mix/Sort:
			// - Channel-level mutual exclusion with L0 (L0 may write delta logs to any segment on the channel)
			// - Label-level exclusion registered for Clustering awareness
			if l0ChannelExcludes.Contain(t.GetTask().GetChannel()) {
				excluded = append(excluded, t)
				continue
			}
			mixChannelExcludes.Insert(t.GetTask().GetChannel())
			mixLabelExcludes.Insert(t.GetLabel())
			selected = append(selected, t)
		case datapb.CompactionType_ClusteringCompaction:
			if l0ChannelExcludes.Contain(t.GetTask().GetChannel()) ||
				mixLabelExcludes.Contain(t.GetLabel()) ||
				clusterLabelExcludes.Contain(t.GetLabel()) {
				excluded = append(excluded, t)
				continue
			}
			clusterChannelExcludes.Insert(t.GetTask().GetChannel())
			clusterLabelExcludes.Insert(t.GetLabel())
			selected = append(selected, t)
		}

		// Read the node this task is counted under before Enqueue: dispatch is
		// asynchronous, so the scheduler can assign one the moment the task is
		// handed over, and the increment below has to name the same bucket the
		// pending decrement and the eventual executing decrement do.
		metricNode := t.GetTask().GetNodeID()
		c.executingGuard.Lock()
		c.executingTasks[t.GetTask().GetPlanID()] = t
		c.scheduler.Enqueue(t)
		mlog.Info(c.ctx, "compaction task enqueued",
			mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.String("type", t.GetTask().GetType().String()),
			mlog.String("channel", t.GetTask().GetChannel()),
			mlog.String("label", t.GetLabel()),
			mlog.Int64s("inputSegments", t.GetTask().GetInputSegments()),
		)
		c.executingGuard.Unlock()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(metricNode), t.GetTask().GetType().String(), metrics.Pending).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(metricNode), t.GetTask().GetType().String(), metrics.Executing).Inc()
	}
	return selected
}

// start launches both background loops unconditionally: the inspector is a
// pure executor and never consults dataCoord.enableCompaction. That switch
// lives at every producer of brand-new work -- the triggers are not started
// and ManualCompaction is rejected while it is off -- so whatever reaches
// queueTasks is either inherited debt (recovered tasks, replans of them) or
// work correctness requires regardless of the switch (an import's sort
// compaction, without which the imported segments never leave IsInvisible).
// Freezing any of those would hold their input segments' compacting locks
// until restart: canTriggerSortCompaction requires !isCompacting, so an input
// whose task never finishes can never be re-sorted, and a sort input that is
// never re-sorted never leaves the growing query path.
func (c *compactionInspector) start() {
	c.stopWg.Add(2)
	go c.loopSchedule()
	go c.loopClean()
}

// loadMeta rebuilds in-memory state from the persisted compaction tasks. It
// runs on the DataCoord startup path and fails it on any metastore write it
// cannot complete: the only writes here erase records this recovery has judged
// unusable, and continuing past a failed erase would leave DataCoord running
// as though it had cleaned up state that is in fact still persisted. A
// metastore that cannot take these writes cannot serve compaction anyway, so
// failing to start is the honest outcome -- the next attempt re-runs recovery
// from the same records.
func (c *compactionInspector) loadMeta() error {
	// TODO: make it compatible to all types of compaction with persist meta
	triggers := c.meta.GetCompactionTasks(c.ctx)

	// Inputs still owed a cleanup. A live task and a task awaiting cleanup can
	// never share inputs while DataCoord is up -- admission makes inputs
	// exclusive, and a terminated task keeps holding them until cleanup hands
	// them back. One case produces such a pair: a crash after buildReplacement
	// wrote the replacement but before the attempt it replaces was cleaned.
	// Admitting the replacement then would let the old attempt's cleanup
	// release inputs the new one is already compacting, so a third task could
	// be planned on top of it.
	owedCleanup := typeutil.NewUniqueSet()
	for _, tasks := range triggers {
		for _, task := range tasks {
			if needsCleanup(task.GetState()) {
				owedCleanup.Insert(task.GetInputSegments()...)
			}
		}
	}

	for _, tasks := range triggers {
		for _, task := range tasks {
			if err := c.recoverTask(task, owedCleanup); err != nil {
				return err
			}
		}
	}
	return nil
}

// eraseAtLoad drops a record recovery has decided can never run. A failure here
// fails startup on purpose: every caller has already concluded the record must
// go, and carrying on against a metastore that cannot be written would hide the
// broken store until the first compaction needed it.
func (c *compactionInspector) eraseAtLoad(ctx context.Context, task *datapb.CompactionTask, reason string) error {
	if err := c.meta.DropCompactionTask(ctx, task); err != nil {
		return merr.Wrapf(err, "failed to erase %s compaction task %d", reason, task.GetPlanID())
	}
	return nil
}

// recoverTask decides what one persisted record becomes on startup: forgotten,
// queued for cleanup, deferred to its predecessor's cleanup, erased, or handed
// back to the queue or the scheduler.
func (c *compactionInspector) recoverTask(task *datapb.CompactionTask, owedCleanup typeutil.UniqueSet) error {
	log := mlog.With(
		mlog.Int64("planID", task.GetPlanID()),
		mlog.String("type", task.GetType().String()),
		mlog.String("state", task.GetState().String()))

	if isCompactionTaskCleaned(task) {
		log.Info(c.ctx, "compactionInspector loadMeta abandon compactionTask")
		return nil
	}

	t, err := c.buildCompactTask(task)
	if err != nil {
		log.Info(c.ctx, "compactionInspector loadMeta build compactionTask failed, try to clean it", mlog.Err(err))
		// Erase the unusable record. Nothing holds these inputs -- admission
		// never ran.
		return c.eraseAtLoad(c.ctx, task, "unbuildable")
	}

	// Terminal tasks are queued for cleanup before admission is even considered.
	// Admission rejects a task whose inputs a snapshot protects -- which is
	// exactly the state that terminated a sort task in the first place -- and
	// dropping the task there would leave its inputs locked, so nothing would
	// ever re-sort them.
	if needsCleanup(task.GetState()) {
		// Hand the task to the same asynchronous cleanup the runtime path uses,
		// rather than cleaning it here: loadMeta runs before DataCoord reports
		// ready, and the backlog is unbounded -- terminal tasks are only ever
		// GC'd after they reach cleaned, so a cluster that ran with compaction
		// disabled accumulates them indefinitely. Blocking readiness on that
		// backlog would hold the whole coordinator behind metastore writes that
		// the retry loop can just as well make afterwards. Cleanup also gets the
		// channel and label exclusion it would not have had here.
		// isCompacting is intentionally process-local, so recovery has to
		// reconstruct the claim before producers are started. Merely putting the
		// task in cleaningTasks only excludes dispatch: a trigger can still plan
		// and admit the same inputs, after which this old task's
		// resetSegmentCompacting would release the new owner's claim. Terminal
		// cleanup bypasses normal admission (a snapshot may be why it
		// terminated), so reserve the surviving inputs directly.
		c.meta.SetSegmentsCompacting(c.ctx, task.GetInputSegments(), true)
		c.cleaningGuard.Lock()
		c.cleaningTasks[task.GetPlanID()] = t
		c.cleaningGuard.Unlock()
		log.Info(c.ctx, "compactionInspector loadMeta queued recovered terminal task for cleanup")
		return nil
	}

	// The replacement half of the pair described above. Leave the record exactly
	// as it is: its predecessor's cleanup finds it by its input set and reuses it
	// instead of writing a fresh one, and the reconciler admits it once cleanup
	// hands the inputs back. Submitting it here would race that cleanup for the
	// inputs, and erasing it would only pay two metastore writes to rebuild what
	// already exists.
	if lo.SomeBy(task.GetInputSegments(), func(segmentID int64) bool { return owedCleanup.Contain(segmentID) }) {
		log.Info(c.ctx, "compactionInspector loadMeta deferring a replan whose predecessor is not cleaned yet")
		return nil
	}

	if err := c.admitCompactTask(task); err != nil {
		if task.GetState() == datapb.CompactionTaskState_pipelining {
			// A pipelining record with no runtime owner is the reconciler's
			// business, for any RetryTimes. Snapshot protection and an
			// in-flight input claim are transient -- the reconciler retries
			// admission every round -- and a decisive rejection (inputs gone)
			// is erased by that same reconciler. Deferring keeps recovery and
			// steady-state on one path, and never lets a trigger settle while
			// the record still exists.
			log.Info(c.ctx, "compactionInspector loadMeta deferred a blocked or rejected task to the reconciler", mlog.Err(err))
			return nil
		}
		// Executing records cannot be deferred: the reconciler drives only
		// pipelining records. End the attempt exactly as an unanswered worker
		// round would; cleanup rebuilds the work under a fresh plan ID, and
		// the reconciler admits or erases the replacement.
		if saveErr := t.(replannableTask).updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error())); saveErr != nil {
			return saveErr
		}
		c.meta.SetSegmentsCompacting(c.ctx, task.GetInputSegments(), true)
		c.cleaningGuard.Lock()
		c.cleaningTasks[task.GetPlanID()] = t
		c.cleaningGuard.Unlock()
		return nil
	}

	if !t.NeedReAssignNodeID() {
		c.restoreTask(t)
		log.Info(c.ctx, "compactionInspector loadMeta restoreTask",
			mlog.Int64("triggerID", t.GetTask().GetTriggerID()),
			mlog.FieldCollectionID(t.GetTask().GetCollectionID()))
		return nil
	}

	// Recovery never declines a durable task. It used to be turned away when the
	// queue was full and, unless it was a replan, erased for it -- destroying
	// durable work over process-local, transient state.
	c.submitTask(t)
	log.Info(c.ctx, "compactionInspector loadMeta submitTask",
		mlog.Int64("triggerID", t.GetTask().GetTriggerID()),
		mlog.FieldCollectionID(t.GetTask().GetCollectionID()))
	return nil
}

// releaseWorkerResources hands back everything a dead attempt still holds on
// workers: the DataNode executor entry for its plan, and -- for a clustering
// attempt that failed while analyzing -- the analyze job that would otherwise
// keep a worker slot with nothing left to claim its result, while the replan is
// already starting a second analysis.
//
// Strictly best-effort, and deliberately so: each release is attempted once
// from here, and a failure is logged and forgotten. There is no retry, no
// pending-release bookkeeping, and no special case for the paths that skip it
// entirely -- a record dropped at recovery because it could not be admitted, or
// a drop that outran its own create RPC and found nothing to remove. Every one
// of those is bounded by the DataNode executor's TTL sweep (see
// terminalTaskTTL), which reclaims an uncollected terminal entry without
// depending on anything outside that process. This path only makes the common
// case reclaim sooner.
//
// This is not the only sender, and is not meant to be. The global scheduler
// drops the plan too when a task reaches a terminal state under its own
// dispatch (schedule() on a create that terminated immediately, check() on a
// query that ended the attempt), which for a replanned attempt is the ordinary
// path and happens before cleanup ever runs. So a plan may be dropped more than
// once. That is harmless by construction rather than by luck: the DataNode's
// RemoveTask is idempotent, a drop for an executing plan is remembered and
// applied at completion, and a drop for an entry already reclaimed is a no-op.
// Deduplicating the two would mean tracking cross-component state whose only
// payoff is saving an RPC that costs nothing when it lands twice.
//
// It runs on its own goroutine, tracked by stopWg. Both releases are RPCs
// bounded only by dataCoord.requestTimeoutSeconds, and the analyze abort waits
// on that job's scheduler lock as well; running either inline would pin the
// caller's cleanup slot -- and with it the channel and label exclusion -- for
// that whole duration under any unresponsive worker. Concurrency is unbounded
// on purpose: each goroutine holds at most one RPC at a time, gRPC multiplexes
// them per node, and the count is capped by how many tasks reach cleanup.
func (c *compactionInspector) releaseWorkerResources(task *datapb.CompactionTask) {
	analyzeTaskID := task.GetAnalyzeTaskID()
	dropPlan := c.cluster != nil && task.GetNodeID() > 0
	abortAnalyze := c.scheduler != nil && analyzeTaskID > 0
	if !dropPlan && !abortAnalyze {
		return
	}
	c.stopWg.Add(1)
	go func() {
		defer c.stopWg.Done()
		if dropPlan {
			if err := c.cluster.DropCompaction(task.GetNodeID(), task.GetPlanID()); err != nil {
				mlog.Warn(c.ctx, "failed to drop compaction plan on worker",
					mlog.Int64("planID", task.GetPlanID()),
					mlog.FieldNodeID(task.GetNodeID()),
					mlog.Err(err))
			}
		}
		if abortAnalyze {
			// Idempotent: aborting an unknown or already-finished analyze task
			// is a no-op, which is the common case -- a clustering compaction
			// cannot complete before its analysis does.
			c.scheduler.AbortAndRemoveTask(analyzeTaskID)
		}
	}()
}

// buildReplacement rebuilds an abandoned attempt's work as a NEW task: same
// triggerID, same input segments, fresh planID and fresh pre-allocated output
// segment range.
//
// Why a new planID rather than re-dispatching the old one. When an attempt ends
// without its outcome established, the worker may still be executing it. Two
// executions of the same plan collide on every artifact whose name is derived
// from the plan: the clustering partition-stats object, keyed by planID alone,
// and the text index, which lands on fixed names under a base path derived from
// the pre-allocated output segment ID. Those are overwritten in place, and
// neither manifest versioning nor GC can undo it. Give the retry its own IDs and
// the collision is impossible by construction, no matter what the old worker is
// still doing -- whatever it writes is unreferenced, and GC reclaims it.
//
// Why the triggerID is preserved. It is the ID ManualCompaction handed back to
// the client, and GetCompactionState answers by looking up tasks under it,
// reporting Completed as soon as none of them is still executing. A replacement
// filed under a new triggerID would make the caller's poll report success while
// the work had only just been re-queued. TriggerID is the work item; planID is
// one attempt at it.
//
// Best effort by design. Every failure here leaves the abandoned predecessor
// intact, so the next cleanup round can try again. Queue capacity is deliberately
// not one of those failures: it is transient admission state, and the reconciler
// retries admission of the durable record after the predecessor has handed off
// its inputs.
func (c *compactionInspector) buildReplacement(t CompactionTask) *datapb.CompactionTask {
	old := t.GetTask()
	log := mlog.With(
		mlog.Int64("planID", old.GetPlanID()),
		mlog.Int64("triggerID", old.GetTriggerID()),
		mlog.String("type", old.GetType().String()))

	// Whether a rebuild is owed at all was decided by the caller
	// (isRetrying); a nil return here means only that it could
	// not be built right now, which is why the caller retries the round rather
	// than cleaning without one.

	// Everything needed to make the replacement durable is done here, before
	// the caller cleans the old attempt: past that point an allocation or store
	// failure has no way to give the work back. Queue admission is intentionally
	// deferred until after cleanup. A full queue can otherwise form a cycle with
	// cleaningTasks' channel/label exclusion: the queued task cannot run while
	// this predecessor is cleaning, and this predecessor cannot clean while the
	// queued task occupies the only slot. The reconciler retries admission of
	// the durable record once capacity returns.

	planID, err := c.allocator.AllocID(c.ctx)
	if err != nil {
		log.Warn(c.ctx, "failed to allocate planID for compaction replan", mlog.Err(err))
		return nil
	}

	// Reserve the same number of output segment IDs the trigger sized this task
	// for, without reproducing any of the per-type sizing rules: the count is the
	// only thing that matters, and the old range already carries it.
	newProto := proto.Clone(old).(*datapb.CompactionTask)
	if r := old.GetPreAllocatedSegmentIDs(); r != nil && r.GetEnd() > r.GetBegin() {
		begin, end, err := c.allocator.AllocN(r.GetEnd() - r.GetBegin())
		if err != nil {
			log.Warn(c.ctx, "failed to allocate output segment IDs for compaction replan", mlog.Err(err))
			return nil
		}
		newProto.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: begin, End: end}
	}

	newProto.PlanID = planID
	newProto.State = datapb.CompactionTaskState_pipelining
	newProto.NodeID = NullNodeID
	newProto.RetryTimes = old.GetRetryTimes() + 1
	newProto.FailReason = ""
	newProto.EndTime = 0
	// Results belong to the attempt that produced them. Carrying them over would
	// let the new attempt's completion path adopt segments the old worker wrote
	// under the old IDs.
	newProto.ResultSegments = nil
	newProto.TmpSegments = nil
	// A clustering retry gets its own analyze task and re-runs the analysis.
	//
	// Carrying the old analyze task over would save a k-means pass, but it makes
	// the old record's cleanup conditional on this function succeeding: cleanup
	// has to be told to leave the analyze record alone BEFORE the replacement is
	// known to exist, and this function has four ways to return without creating
	// one -- including losing the race for the input segments, which is a normal
	// outcome. Every one of those would strand the analyze record and its files
	// with nothing left to reference them. Re-running the analysis costs a rare
	// clustering retry some time; getting the ownership handoff wrong leaks
	// forever, and only on the paths hardest to test.
	if old.GetAnalyzeTaskID() > 0 {
		analyzeTaskID, err := c.allocator.AllocID(c.ctx)
		if err != nil {
			log.Warn(c.ctx, "failed to allocate analyze task ID for compaction replan", mlog.Err(err))
			return nil
		}
		newProto.AnalyzeTaskID = analyzeTaskID
		// Clear the result of the previous analysis so the state machine runs it
		// again: CreateTaskOnWorker only calls doAnalyze while AnalyzeVersion is 0.
		newProto.AnalyzeVersion = 0
	}

	taskCreateTS, err := c.allocator.AllocTimestamp(c.ctx)
	if err != nil {
		log.Warn(c.ctx, "failed to allocate create timestamp for compaction replan", mlog.Err(err))
		return nil
	}
	newProto.CreateTs = taskCreateTS
	newProto.StartTime = tsoutil.PhysicalTime(taskCreateTS).Unix()

	// Persist before the caller cleans, so the debt is durable at every instant:
	// first the old record carries it, then both do, then only the new one. A
	// crash in that window is recovered on restart: loadMeta defers the pair,
	// cleanup finds this record by its input set and reuses it, and the
	// reconciler admits it once the clean hands the inputs back. What is
	// left after the clean -- claiming the inputs and pushing onto the queue --
	// touches no store and so cannot lose it.
	if err := c.meta.SaveCompactionTask(c.ctx, newProto); err != nil {
		log.Warn(c.ctx, "failed to persist the compaction replan", mlog.Err(err),
			mlog.Int64("newPlanID", planID))
		return nil
	}

	// Name both ends of the handover. A replan changes the plan ID, so without
	// the pair a reader following one piece of work across ten failed attempts
	// has no way to link the attempts to each other -- the log says a new plan
	// appeared, never which one it replaced or why.
	log.Info(c.ctx, "built a compaction replan under a fresh plan ID",
		mlog.Int64("oldPlanID", old.GetPlanID()),
		mlog.Int64("newPlanID", planID),
		mlog.Int("attempt", int(newProto.GetRetryTimes())),
		mlog.String("reason", old.GetFailReason()))

	return newProto
}

// replacementForCleanup returns the durable replacement for a predecessor that
// owes a rebuild, creating and persisting one only if none exists yet. An
// existing record is found by the predecessor semantics copied by a replan.
// Input overlap alone is insufficient: unrelated compaction types and triggers
// may legitimately contend for one segment while owing different work. Reusing
// only an exact successor is what keeps a clean that fails and retries from
// persisting one fresh record per round without stealing another trigger's debt.
func (c *compactionInspector) replacementForCleanup(t CompactionTask) *datapb.CompactionTask {
	predecessor := t.GetTask()

	var existing *datapb.CompactionTask
	c.meta.GetCompactionTaskMeta().Range(func(task *datapb.CompactionTask) bool {
		if compactionTaskIsDirectReplacement(predecessor, task) {
			existing = proto.Clone(task).(*datapb.CompactionTask)
			return false
		}
		return true
	})
	if existing != nil {
		return existing
	}

	return c.buildReplacement(t)
}

func compactionInputSetsEqual(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	leftSet := typeutil.NewUniqueSet(left...)
	rightSet := typeutil.NewUniqueSet(right...)
	return leftSet.Len() == len(left) && rightSet.Len() == len(right) &&
		leftSet.Len() == rightSet.Len() && leftSet.Contain(right...)
}

func compactionTaskIsDirectReplacement(predecessor, candidate *datapb.CompactionTask) bool {
	return candidate.GetState() == datapb.CompactionTaskState_pipelining &&
		candidate.GetTriggerID() == predecessor.GetTriggerID() &&
		candidate.GetType() == predecessor.GetType() &&
		candidate.GetRetryTimes() == predecessor.GetRetryTimes()+1 &&
		candidate.GetCollectionID() == predecessor.GetCollectionID() &&
		candidate.GetPartitionID() == predecessor.GetPartitionID() &&
		candidate.GetChannel() == predecessor.GetChannel() &&
		compactionInputSetsEqual(candidate.GetInputSegments(), predecessor.GetInputSegments())
}

// reconcileReplacements drives every unowned pipelining record -- a durable
// replan, or an original attempt recovery deferred because its admission was
// blocked -- toward one of its two terminal ownership states: queued, or
// erased. It runs once per schedule round and re-derives everything from
// persisted meta, so there is no runtime state to hand over and none to lose:
// a record it cannot finish this round is found again on the next one, and
// after a restart the same sweep resumes from the same persisted facts.
//
// The sweep is scoped to pipelining records no runtime structure holds. The
// writers that leave such a record behind are buildReplacement (a replan,
// persisted before its predecessor is cleaned) and loadMeta deferring a
// blocked admission; enqueueCompaction claims, persists, and queues in one
// call, and a reconciler racing an in-flight enqueueCompaction is turned back
// by the claim conflict and retries once the record is queued.
func (c *compactionInspector) reconcileReplacements() {
	// Snapshot which plans a runtime structure already owns. Only this function
	// submits replan records, and it runs on the schedule loop alone, so the
	// snapshot cannot go stale in the direction that would double-queue one.
	known := typeutil.NewUniqueSet()
	c.queueTasks.ForEach(func(t CompactionTask) {
		known.Insert(t.GetTask().GetPlanID())
	})
	c.executingGuard.RLock()
	for planID := range c.executingTasks {
		known.Insert(planID)
	}
	c.executingGuard.RUnlock()
	c.cleaningGuard.RLock()
	for planID := range c.cleaningTasks {
		known.Insert(planID)
	}
	c.cleaningGuard.RUnlock()

	// Collect the unowned pipelining records first and bail out before deriving
	// anything else. Having one is the exceptional case -- a replan whose
	// rebuild has not been queued yet, or a record recovery deferred because
	// its admission was blocked -- so the steady-state cost of this round is a
	// single walk that allocates nothing.
	taskMeta := c.meta.GetCompactionTaskMeta()
	var candidates []*datapb.CompactionTask
	taskMeta.Range(func(task *datapb.CompactionTask) bool {
		if task.GetState() == datapb.CompactionTaskState_pipelining &&
			!known.Contain(task.GetPlanID()) {
			candidates = append(candidates, proto.Clone(task).(*datapb.CompactionTask))
		}
		return true
	})
	if len(candidates) == 0 {
		return
	}

	// Now derive who holds which inputs. cleanupHeld inputs belong to a
	// terminated record that has not finished cleanup -- its clean releases them
	// last, so until then its replacement must not try admission. A live overlap
	// is deliberately not used to erase a replacement: overlap proves contention,
	// not equivalence of compaction type, semantics, or the complete input set.
	cleanupHeld := typeutil.NewUniqueSet()
	taskMeta.Range(func(task *datapb.CompactionTask) bool {
		if needsCleanup(task.GetState()) {
			cleanupHeld.Insert(task.GetInputSegments()...)
		}
		return true
	})

	for _, replacement := range candidates {
		c.driveReplacement(replacement, cleanupHeld)
	}
}

// driveReplacement gives one unowned pipelining record its attempt for this
// round. Every exit that is neither "queued" nor "erased" leaves the record
// untouched, to be re-derived and retried on the next round.
func (c *compactionInspector) driveReplacement(replacement *datapb.CompactionTask, cleanupHeld typeutil.UniqueSet) {
	log := mlog.With(
		mlog.Int64("newPlanID", replacement.GetPlanID()),
		mlog.Int64("triggerID", replacement.GetTriggerID()),
		mlog.String("type", replacement.GetType().String()))

	// The predecessor still owns the inputs: it has not finished cleanup, and
	// its clean releases them last. Trying admission now can only conflict.
	if lo.SomeBy(replacement.GetInputSegments(), func(segmentID int64) bool { return cleanupHeld.Contain(segmentID) }) {
		return
	}

	// Avoid taking and immediately releasing the segment claim while the queue
	// is already full.
	if c.isFull() {
		return
	}

	// buildReplacement already wrote this record, so the path below deliberately
	// does not go through enqueueCompaction: re-persisting it would add a
	// metastore write that can fail after the work debt has nowhere else to live.
	t, err := c.createCompactTask(replacement)
	if err != nil {
		// A conflict here, with no winner visible in meta above, is not
		// decisive: it is either the predecessor's clean between its durable
		// cleaned-write and its in-memory flag release, or a concurrent
		// enqueueCompaction that claimed the inputs but has not persisted its
		// record yet. Snapshot protection is transient for the same reason: it
		// deliberately pauses compaction without satisfying it. Erasing either
		// record would lose the work whenever the competing claim disappears or
		// the snapshot is released.
		if errors.Is(err, merr.ErrCompactionPlanConflict) || errors.Is(err, merr.ErrCompactionBlocked) {
			log.RatedInfo(c.ctx, rate.Limit(1.0/60), "compaction replan is temporarily blocked, retrying next round", mlog.Err(err))
			return
		}
		// The rest are decisive: the inputs are gone, and the
		// record has nothing left to do. A failed erase is retried next round.
		log.Info(c.ctx, "compaction replan not admitted, erasing its record", mlog.Err(err))
		if dropErr := c.meta.DropCompactionTask(c.ctx, replacement); dropErr != nil {
			log.Warn(c.ctx, "failed to erase the inadmissible compaction replan, retrying next round", mlog.Err(dropErr))
		}
		return
	}
	c.submitTask(t)
	// queueTasks now owns the persisted record.
	log.Info(c.ctx, "unowned compaction record admitted by the reconciler",
		mlog.Int32("retryTimes", replacement.GetRetryTimes()))
}

func (c *compactionInspector) loopSchedule() {
	interval := paramtable.Get().DataCoordCfg.CompactionScheduleInterval.GetAsDuration(time.Millisecond)
	mlog.Info(c.ctx, "compactionInspector start loop schedule", mlog.Duration("schedule interval", interval))
	defer c.stopWg.Done()

	scheduleTicker := time.NewTicker(interval)
	defer scheduleTicker.Stop()
	for {
		select {
		case <-c.stopCh:
			mlog.Info(c.ctx, "compactionInspector quit loop schedule")
			return
		// The process-lifetime context: never canceled by a graceful stop
		// (that path closes stopCh), only by the process being torn down.
		case <-c.ctx.Done():
			mlog.Info(c.ctx, "compactionInspector quit loop schedule, context done")
			return
		case <-scheduleTicker.C:
			c.checkSchedule()
		}
	}
}

func (c *compactionInspector) loopClean() {
	interval := Params.DataCoordCfg.CompactionGCIntervalInSeconds.GetAsDuration(time.Second)
	mlog.Info(c.ctx, "compactionInspector start clean check loop", mlog.Duration("gcInterval", interval))
	defer c.stopWg.Done()
	cleanTicker := time.NewTicker(interval)
	defer cleanTicker.Stop()
	for {
		select {
		case <-c.stopCh:
			mlog.Info(c.ctx, "Compaction inspector quit loopClean")
			return
		case <-c.ctx.Done():
			mlog.Info(c.ctx, "Compaction inspector quit loopClean, context done")
			return
		case <-cleanTicker.C:
			c.Clean()
		}
	}
}

func (c *compactionInspector) Clean() {
	c.cleanCompactionTaskMeta()
	c.cleanPartitionStats()
}

func (c *compactionInspector) cleanCompactionTaskMeta() {
	// gc clustering compaction tasks
	triggers := c.meta.GetCompactionTasks(c.ctx)
	for _, tasks := range triggers {
		for _, task := range tasks {
			if task.State == datapb.CompactionTaskState_cleaned {
				duration := time.Since(time.Unix(task.StartTime, 0)).Seconds()
				if duration > Params.DataCoordCfg.CompactionDropToleranceInSeconds.GetAsDuration(time.Second).Seconds() {
					// try best to delete meta
					err := c.meta.DropCompactionTask(c.ctx, task)
					mlog.Debug(c.ctx, "drop compaction task meta", mlog.Int64("planID", task.PlanID))
					if err != nil {
						mlog.Warn(c.ctx, "fail to drop task", mlog.Int64("planID", task.PlanID), mlog.Err(err))
					}
				}
			}
		}
	}
}

func (c *compactionInspector) cleanPartitionStats() {
	mlog.Debug(c.ctx, "start gc partitionStats meta and files")
	// gc partition stats
	channelPartitionStatsInfos := make(map[string][]*datapb.PartitionStatsInfo)
	unusedPartStats := make([]*datapb.PartitionStatsInfo, 0)
	if c.meta.GetPartitionStatsMeta() == nil {
		return
	}
	infos := c.meta.GetPartitionStatsMeta().ListAllPartitionStatsInfos()
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
	mlog.Debug(c.ctx, "channels with PartitionStats meta", mlog.Int("len", len(channelPartitionStatsInfos)))

	for _, info := range unusedPartStats {
		mlog.Debug(c.ctx, "collection has been dropped, remove partition stats",
			mlog.Int64("collID", info.GetCollectionID()))
		if err := c.meta.CleanPartitionStatsInfo(c.ctx, info); err != nil {
			mlog.Warn(c.ctx, "gcPartitionStatsInfo fail", mlog.Err(err))
			continue
		}
	}

	for channel, infos := range channelPartitionStatsInfos {
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Version > infos[j].Version
		})
		mlog.Debug(c.ctx, "PartitionStats in channel", mlog.String("channel", channel), mlog.Int("len", len(infos)))
		if len(infos) > 2 {
			for i := 2; i < len(infos); i++ {
				info := infos[i]
				if err := c.meta.CleanPartitionStatsInfo(c.ctx, info); err != nil {
					mlog.Warn(c.ctx, "gcPartitionStatsInfo fail", mlog.Err(err))
					continue
				}
			}
		}
	}
}

func (c *compactionInspector) stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	// The reconciler runs only on the schedule loop and cleanup runs only on
	// tracked goroutines, so this waits for every in-flight round to settle.
	// Replans that are merely waiting need no draining: their records are
	// already durable, and the next start re-derives them from meta exactly as
	// the next round would have.
	c.stopWg.Wait()
}

func (c *compactionInspector) removeTasksByChannel(channel string) {
	mlog.Info(c.ctx, "removing tasks by channel", mlog.String("channel", channel))
	c.queueTasks.RemoveAll(func(task CompactionTask) bool {
		if task.GetTask().GetChannel() == channel {
			mlog.Info(c.ctx, "Compaction inspector removing tasks by channel",
				mlog.String("channel", channel),
				mlog.Int64("planID", task.GetTask().GetPlanID()),
				mlog.FieldNodeID(task.GetTask().GetNodeID()),
			)
			metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(task.GetTask().GetNodeID()), task.GetTask().GetType().String(), metrics.Pending).Dec()
			return true
		}
		return false
	})

	c.executingGuard.Lock()
	for id, task := range c.executingTasks {
		if task.GetTask().GetChannel() != channel {
			continue
		}
		mlog.Info(c.ctx, "Compaction inspector removing tasks by channel",
			mlog.String("channel", channel),
			mlog.Int64("planID", task.GetTask().GetPlanID()),
			mlog.FieldNodeID(task.GetTask().GetNodeID()),
		)
		delete(c.executingTasks, id)
		metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(task.GetTask().GetNodeID()), task.GetTask().GetType().String(), metrics.Executing).Dec()
	}
	c.executingGuard.Unlock()
}

// submitTask publishes a task the caller has already persisted. It cannot fail:
// the queue accepts unconditionally, because the alternative is a durable record
// that nothing in memory drives.
func (c *compactionInspector) submitTask(t CompactionTask) {
	c.queueTasks.Enqueue(t)
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(t.GetTask().GetNodeID()), t.GetTask().GetType().String(), metrics.Pending).Inc()
}

// restoreTask used to restore Task from etcd
func (c *compactionInspector) restoreTask(t CompactionTask) {
	// A restored task already owns its worker, and this PR never re-dispatches
	// one, so this label is the same one its completion decrements.
	metricNode := t.GetTask().GetNodeID()
	c.executingGuard.Lock()
	c.executingTasks[t.GetTask().GetPlanID()] = t
	c.scheduler.Enqueue(t)
	c.executingGuard.Unlock()
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(metricNode), t.GetTask().GetType().String(), metrics.Executing).Inc()
}

// getCompactionTask return compaction
func (c *compactionInspector) getCompactionTask(planID int64) CompactionTask {
	var t CompactionTask = nil
	c.queueTasks.ForEach(func(task CompactionTask) {
		if task.GetTask().GetPlanID() == planID {
			t = task
		}
	})
	if t != nil {
		return t
	}

	c.executingGuard.RLock()
	defer c.executingGuard.RUnlock()
	t = c.executingTasks[planID]
	return t
}

func (c *compactionInspector) enqueueCompaction(task *datapb.CompactionTask) error {
	log := mlog.With(mlog.Int64("planID", task.GetPlanID()), mlog.Int64("triggerID", task.GetTriggerID()), mlog.FieldCollectionID(task.GetCollectionID()), mlog.String("type", task.GetType().String()))
	t, err := c.createCompactTask(task)
	if err != nil {
		// Conflict is normal
		if errors.Is(err, merr.ErrCompactionPlanConflict) {
			log.RatedInfo(c.ctx, rate.Limit(1.0/60), "Failed to create compaction task, compaction plan conflict", mlog.Err(err))
		} else {
			log.Warn(c.ctx, "Failed to create compaction task, unable to create compaction task", mlog.Err(err))
		}
		return err
	}

	// Check capacity before persisting, and treat the answer as advice. The
	// trigger manager and import checker enqueue concurrently, so a few
	// producers can pass this check together and push the queue slightly past
	// the limit -- which is fine. The limit exists to stop unbounded growth of
	// persisted compaction tasks, each of which holds its inputs compacting;
	// being exact about it would mean making the queue refuse a task that is
	// already durable, and a refused durable task has no runtime owner until
	// the next restart. Overshoot by a handful beats that trade every time.
	if c.queueTasks.IsFull() {
		c.meta.SetSegmentsCompacting(c.ctx, t.GetTask().GetInputSegments(), false)
		log.RatedInfo(c.ctx, rate.Limit(1.0/60), "compaction task queue is full, not enqueuing")
		return merr.WrapErrServiceQuotaExceeded("compaction task queue is full")
	}

	taskCreateTS, err := c.allocator.AllocTimestamp(c.ctx)
	if err != nil {
		c.meta.SetSegmentsCompacting(c.ctx, t.GetTask().GetInputSegments(), false)
		log.Warn(c.ctx, "Failed to enqueue compaction task, unable to allocate task create timestamp", mlog.Err(err))
		return err
	}
	startTime := tsoutil.PhysicalTime(taskCreateTS).Unix()
	t.SetTask(t.ShadowClone(setStartTime(startTime), setCreateTs(taskCreateTS)))
	err = t.SaveTaskMeta()
	if err != nil {
		c.meta.SetSegmentsCompacting(c.ctx, t.GetTask().GetInputSegments(), false)
		log.Warn(c.ctx, "Failed to enqueue compaction task, unable to save task meta", mlog.Err(err))
		return err
	}
	c.queueTasks.Enqueue(t)
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(t.GetTask().GetNodeID()), t.GetTask().GetType().String(), metrics.Pending).Inc()
	log.Info(c.ctx, "Compaction plan submitted")
	return nil
}

// buildCompactTask wraps a persisted task in its behavior, and nothing more. It
// is deliberately separate from admission: a terminal task recovered at startup
// still owes its inputs their segment lock, and must be reachable even when it
// could no longer be admitted.
func (c *compactionInspector) buildCompactTask(t *datapb.CompactionTask) (CompactionTask, error) {
	switch t.GetType() {
	case datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction:
		return newMixCompactionTask(c.ctx, t, c.allocator, c.meta, c.ievm), nil
	case datapb.CompactionType_Level0DeleteCompaction:
		return newL0CompactionTask(c.ctx, t, c.allocator, c.meta), nil
	case datapb.CompactionType_ClusteringCompaction:
		return newClusteringCompactionTask(c.ctx, t, c.allocator, c.meta, c.handler, c.scheduler, c.ievm), nil
	case datapb.CompactionType_BumpSchemaVersionCompaction:
		return newBumpSchemaVersionTask(c.ctx, t, c.allocator, c.meta, c.ievm), nil
	default:
		return nil, merr.WrapErrIllegalCompactionPlan("illegal compaction type")
	}
}

// admitCompactTask decides whether a task may execute and claims its inputs.
// Only tasks that are going to run go through it.
func (c *compactionInspector) admitCompactTask(t *datapb.CompactionTask) error {
	// Revalidate input and snapshot state at admission so a protection change
	// after planning cannot enter the task queue unchecked.
	if err := c.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t); err != nil {
		return err
	}
	// A durable replan reserves its inputs from the moment it is persisted until
	// admission claims them: before admission it has not claimed them yet and its
	// predecessor has released them, so without this check an unrelated task
	// could claim them first, get them compacted away, and leave the replan
	// nothing to admit. The replan itself skips the check -- its predecessors'
	// admissions already made replacements' inputs disjoint, and a residual
	// overlap is still caught by the claim below.
	if t.GetRetryTimes() == 0 && c.inputsReservedByReplacement(t.GetInputSegments()) {
		return merr.WrapErrCompactionPlanConflict("segments reserved by a pending replan")
	}
	exist, succeed := c.meta.CheckAndSetSegmentsCompacting(c.ctx, t.GetInputSegments())
	if !exist {
		return merr.WrapErrIllegalCompactionPlan("segment not exist")
	}
	if !succeed {
		return merr.WrapErrCompactionPlanConflict("segment is compacting")
	}
	return nil
}

// inputsReservedByReplacement reports whether any durable replan record that is
// still awaiting admission names one of these segments as its input. The
// reservation is the durable record itself: it disappears once the replan is
// dispatched (state leaves pipelining) or erased, so no release step exists.
func (c *compactionInspector) inputsReservedByReplacement(inputs []int64) bool {
	reserved := typeutil.NewUniqueSet()
	c.meta.GetCompactionTaskMeta().Range(func(task *datapb.CompactionTask) bool {
		if task.GetState() == datapb.CompactionTaskState_pipelining && task.GetRetryTimes() > 0 {
			reserved.Insert(task.GetInputSegments()...)
		}
		return true
	})
	if reserved.Len() == 0 {
		return false
	}
	return lo.SomeBy(inputs, func(segmentID int64) bool { return reserved.Contain(segmentID) })
}

func (c *compactionInspector) createCompactTask(t *datapb.CompactionTask) (CompactionTask, error) {
	task, err := c.buildCompactTask(t)
	if err != nil {
		return nil, err
	}
	if err := c.admitCompactTask(t); err != nil {
		return nil, err
	}
	return task, nil
}

// checkCompaction retrieves executing tasks and calls each task's Process() method
// to evaluate its state and progress through the state machine.
// Completed tasks are removed from executingTasks.
// Tasks that fail or timeout are moved from executingTasks to cleaningTasks,
// where task-specific clean logic is performed asynchronously.
func (c *compactionInspector) checkCompaction() {
	// Get executing executingTasks before GetCompactionState from DataNode to prevent false failure,
	//  for DC might add new task while GetCompactionState.

	type finishedCompactionTask struct {
		task         CompactionTask
		needsCleanup bool
	}
	var finishedTasks []finishedCompactionTask
	// Snapshot under the read lock, process after releasing it. Each
	// scheduler.Update below waits for that task's in-flight worker callback,
	// bounded by dataCoord.requestTimeoutSeconds -- holding executingGuard
	// across those waits would block every writer (removeTasksByChannel,
	// restoreTask) and, through Go's writer-preference, every later reader for
	// up to 30s per hung node. The map itself is only mutated under the write
	// lock, so the snapshot is a consistent point-in-time view; a task removed
	// concurrently is simply processed one extra time, which the terminal-state
	// guards make a no-op.
	c.executingGuard.RLock()
	executingSnapshot := make([]CompactionTask, 0, len(c.executingTasks))
	for _, t := range c.executingTasks {
		executingSnapshot = append(executingSnapshot, t)
	}
	c.executingGuard.RUnlock()

	for _, t := range executingSnapshot {
		c.checkDelay(t)
		// Decide cleanup from immutable snapshots taken around Process, never from
		// a later re-read. Between this loop and the cleaningTasks insert below the
		// lock is released, and a scheduler callback that fails to probe its
		// worker historically rewrote a terminal state back to pipelining (the
		// terminal-state guards now stop that, but the snapshot keeps this
		// decision independent of any later write). A task removed
		// from executingTasks without entering cleaningTasks is never cleaned, so
		// its input segments stay isCompacting until DataCoord restarts.
		// The scheduler owns this task's state; borrow it under its per-task
		// lock so Process cannot interleave with a worker callback. This waits
		// for an in-flight callback, bounded by dataCoord.requestTimeoutSeconds.
		planID := t.GetTask().GetPlanID()
		var stateBeforeProcess, stateAfterProcess datapb.CompactionTaskState
		var finished bool
		c.scheduler.Update(planID, func() {
			stateBeforeProcess = t.GetTask().GetState()
			finished = t.Process()
			stateAfterProcess = t.GetTask().GetState()
		})
		needsCleanup := needsCleanup(stateBeforeProcess) ||
			needsCleanup(stateAfterProcess)
		if finished || needsCleanup {
			finishedTasks = append(finishedTasks, finishedCompactionTask{
				task:         t,
				needsCleanup: needsCleanup,
			})
		}
	}

	// delete all finished
	c.executingGuard.Lock()
	for _, finishedTask := range finishedTasks {
		t := finishedTask.task
		delete(c.executingTasks, t.GetTask().GetPlanID())
		mlog.Info(c.ctx, "compaction task finished",
			mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.String("type", t.GetTask().GetType().String()),
			mlog.String("state", t.GetTask().GetState().String()),
			mlog.String("channel", t.GetTask().GetChannel()),
			mlog.String("label", t.GetLabel()),
			mlog.FieldNodeID(t.GetTask().GetNodeID()),
			mlog.Int64s("inputSegments", t.GetTask().GetInputSegments()),
			mlog.String("reason", t.GetTask().GetFailReason()),
		)
		metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(t.GetTask().GetNodeID()), t.GetTask().GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(t.GetTask().GetNodeID()), t.GetTask().GetType().String(), metrics.Done).Inc()
	}
	c.executingGuard.Unlock()

	// insert task need to clean
	c.cleaningGuard.Lock()
	for _, finishedTask := range finishedTasks {
		t := finishedTask.task
		if finishedTask.needsCleanup {
			mlog.Info(c.ctx, "task need to clean",
				mlog.FieldCollectionID(t.GetTask().GetCollectionID()),
				mlog.Int64("planID", t.GetTask().GetPlanID()),
				mlog.String("state", t.GetTask().GetState().String()))
			c.cleaningTasks[t.GetTask().GetPlanID()] = t
		}
	}
	c.cleaningGuard.Unlock()
}

// cleanFailedTasks performs task define Clean logic
// while compactionInspector.Clean is to do garbage collection for cleaned tasks
func (c *compactionInspector) cleanFailedTasks() {
	c.cleaningGuard.RLock()
	tasks := lo.Values(c.cleaningTasks)
	c.cleaningGuard.RUnlock()

	for _, t := range tasks {
		planID := t.GetTask().GetPlanID()
		select {
		case <-c.stopCh:
			return
		default:
		}
		// Cleanup persists metadata and may have to wait out an in-flight worker
		// callback, so it must not run inline: the caller is the single
		// checkSchedule goroutine that also drives checkCompaction and schedule
		// for every other compaction task. One slow cleanup would stall them all.
		//
		// Membership in cleaningInFlight IS the cleanup slot, taken under the
		// guard before spawning: the same check bounds the fan-out and keeps a
		// task from being dispatched twice while its previous attempt still
		// runs, and a dispatch that finds no slot spawns nothing -- there is no
		// goroutine parked on a semaphore to drain at shutdown.
		c.cleaningGuard.Lock()
		_, busy := c.cleaningInFlight[planID]
		full := len(c.cleaningInFlight) >= maxConcurrentCleanups
		if !busy && !full {
			c.cleaningInFlight[planID] = struct{}{}
		}
		c.cleaningGuard.Unlock()
		if busy || full {
			continue
		}
		// In production this runs on the loopSchedule goroutine, which holds a
		// stopWg count of its own, so Add never races the Wait inside stop().
		c.stopWg.Add(1)
		go func(t CompactionTask, planID int64) {
			defer c.stopWg.Done()
			defer func() {
				c.cleaningGuard.Lock()
				delete(c.cleaningInFlight, planID)
				c.cleaningGuard.Unlock()
			}()
			// Build and persist the replacement BEFORE cleaning, and abandon
			// the whole round if that fails. Clean() overwrites the state with
			// cleaned, which is the record's last claim that the work is owed;
			// unless the replacement is already durable by then, a failure
			// afterwards leaves the trigger with nothing under it, reporting
			// Completed for work that never ran. Leaving the task retrying
			// instead costs one round and the next one tries again.
			if isRetrying(t.GetTask()) {
				if c.replacementForCleanup(t) == nil {
					return
				}
			}

			// Finalize drops the task from dispatch first, so no further plan can
			// be handed to a worker, then runs cleanup under the same per-task
			// lock as the callbacks -- waiting for any in-flight one to drain.
			cleaned := false
			c.scheduler.Finalize(planID, func() { cleaned = t.Clean() })
			if !cleaned {
				// The predecessor still owns the inputs and still says the work
				// is owed. The durable replacement stays untouched: the next
				// cleanup round finds it by its input set and reuses it, and the
				// reconciler leaves it alone for as long as the predecessor
				// holds the inputs. Erasing and rebuilding here instead has an
				// unrecoverable double-failure window: if the erase fails, the
				// next round persists a second replacement over the first.
				return
			}
			// Release ownership before anything slow: deleting from cleaningTasks
			// lifts the channel/label exclusion, and returning frees the cleanup
			// slot. Cleanup is complete at this point -- the releases below are
			// worker housekeeping and must not pin cleanup resources on an RPC.
			c.cleaningGuard.Lock()
			delete(c.cleaningTasks, planID)
			c.cleaningGuard.Unlock()
			// Finalize took the task out of dispatch, so no further scheduler
			// round can reach this task. A round that already observed the
			// terminal state may have sent its own drop before that; see
			// releaseWorkerResources on why a second one is harmless.
			c.releaseWorkerResources(t.GetTask())
			// The replacement is NOT queued here: the reconciler on the schedule
			// loop finds the durable record -- now that its predecessor no
			// longer holds the inputs -- and admits it within a round. One
			// driver, one code path, whether the clean happened in this process
			// or before a restart.
		}(t, planID)
	}
}

// isFull reports whether the queue can take no more work. Capacity 0 means
// unbounded -- the same reading CompactionQueue.Enqueue uses, and the two must
// not disagree: a stricter isFull would reject every new compaction while
// Enqueue happily accepted the recovered ones.
func (c *compactionInspector) isFull() bool {
	return c.queueTasks.IsFull()
}

func (c *compactionInspector) checkDelay(t CompactionTask) {
	maxExecDuration := maxCompactionTaskExecutionDuration[t.GetTask().GetType()]
	startTime := time.Unix(t.GetTask().GetStartTime(), 0)
	execDuration := time.Since(startTime)
	if execDuration >= maxExecDuration {
		mlog.RatedWarn(c.ctx, rate.Limit(1.0/60), "compaction task is delay",
			mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.String("type", t.GetTask().GetType().String()),
			mlog.String("state", t.GetTask().GetState().String()),
			mlog.FieldVChannel(t.GetTask().GetChannel()),
			mlog.FieldNodeID(t.GetTask().GetNodeID()),
			mlog.Time("startTime", startTime),
			mlog.Duration("execDuration", execDuration))
	}
}

func (c *compactionInspector) getCompactionTasksNum(filters ...compactionTaskFilter) int {
	cnt := 0
	isMatch := func(task CompactionTask) bool {
		for _, f := range filters {
			if !f(task) {
				return false
			}
		}
		return true
	}
	c.queueTasks.ForEach(func(task CompactionTask) {
		if isMatch(task) {
			cnt += 1
		}
	})
	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
		if isMatch(t) {
			cnt += 1
		}
	}
	c.executingGuard.RUnlock()
	return cnt
}

type compactionTaskFilter func(task CompactionTask) bool

func CollectionIDCompactionTaskFilter(collectionID int64) compactionTaskFilter {
	return func(task CompactionTask) bool {
		return task.GetTask().GetCollectionID() == collectionID
	}
}

func L0CompactionCompactionTaskFilter() compactionTaskFilter {
	return func(task CompactionTask) bool {
		return task.GetTask().GetType() == datapb.CompactionType_Level0DeleteCompaction
	}
}
