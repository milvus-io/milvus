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

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type CompactionTask interface {
	task.Task
	// Process performs the task's state machine
	//
	// Returns:
	//   - <bool>:  whether the task state machine ends.
	//
	// Notes:
	//
	//	`end` doesn't mean the task completed, its state may be completed or failed or timeout.
	Process() bool
	// Clean performs clean logic for a fail/timeout task
	Clean() bool
	BuildCompactionRequest() (*datapb.CompactionPlan, error)

	SetTask(*datapb.CompactionTask)
	GetTask() *datapb.CompactionTask
}

// replannableTask is the slice of a compaction task abandonAttempt needs. All
// four implementations live in this package, so the unexported saver is
// reachable here.
type replannableTask interface {
	GetTask() *datapb.CompactionTask
	updateAndSaveTaskMeta(opts ...compactionTaskOpt) error
}

// cleanFinisher is the slice of a compaction task finishClean needs. All four
// implementations live in this package, so the unexported methods are
// reachable here.
type cleanFinisher interface {
	updateAndSaveTaskMeta(opts ...compactionTaskOpt) error
	resetSegmentCompacting()
}

// finishClean writes the cleaned state and then releases the input claims. The
// release must stay the LAST step: resetSegmentCompacting must run exactly
// once per task, and releasing before the durable cleaned-write would let a
// second task claim inputs this one still owes.
func finishClean(ctx context.Context, t cleanFinisher, logPrefix string) error {
	if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned)); err != nil {
		mlog.Warn(ctx, logPrefix+" failed to updateAndSaveTaskMeta", mlog.Err(err))
		return err
	}
	t.resetSegmentCompacting()
	mlog.Info(ctx, logPrefix+" clean done")
	return nil
}

// dispatchCompactionPlan persists the worker assignment, sends the create RPC,
// and handles the two outcomes worth distinguishing: a duplicate means an
// earlier attempt was accepted (keep the assignment; the query path takes
// over), anything else abandons the attempt for a replan under a fresh
// planID. Persisting before the RPC closes the accepted-request→failed-save
// window; LastStateStartTime is stamped here so the execution budget measures
// this attempt.
func dispatchCompactionPlan(ctx context.Context, t replannableTask, nodeID int64, cluster session.Cluster, plan *datapb.CompactionPlan, logPrefix string) error {
	originNodeID := t.GetTask().GetNodeID()
	if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing),
		setNodeID(nodeID), setLastStateStartTime(time.Now().Unix())); err != nil {
		if ctx.Err() == nil {
			mlog.Fatal(ctx, logPrefix+" failed to persist assignment; terminating process",
				mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.FieldNodeID(nodeID), mlog.Err(err))
		}
		mlog.Warn(ctx, logPrefix+" failed to persist assignment, not sending plan",
			mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.FieldNodeID(nodeID), mlog.Err(err))
		return err
	}
	moveExecutingCompactionMetric(t.GetTask().GetType(), originNodeID, nodeID)

	if err := cluster.CreateCompaction(nodeID, plan, t.GetTask().GetCollectionID()); err != nil {
		if errors.Is(err, merr.ErrDuplicatedCompactionTask) {
			mlog.Warn(ctx, logPrefix+" plan already running on worker, keeping assignment",
				mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.FieldNodeID(nodeID), mlog.Err(err))
			return nil
		}
		// An explicit refusal or an outcome we cannot read at all is handled
		// the same way: abandon this attempt and rebuild the work under a
		// fresh planID, since two executions under different planIDs can never
		// collide on plan-derived artifact names.
		mlog.Warn(ctx, logPrefix+" create failed, abandoning attempt for replan",
			mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.FieldNodeID(nodeID), mlog.Err(err))
		abandonAttempt(ctx, t, fmt.Sprintf("create failed: %v", err))
		return nil
	}
	mlog.Info(ctx, logPrefix+" notified DataNode")
	return nil
}

func dropCompactionTaskOnWorker(ctx context.Context, task *datapb.CompactionTask, cluster session.Cluster, logPrefix string) {
	if task.GetNodeID() <= 0 {
		return
	}
	if err := cluster.DropCompaction(task.GetNodeID(), task.GetPlanID()); err != nil {
		mlog.Warn(ctx, logPrefix+" unable to drop compaction plan",
			mlog.Int64("planID", task.GetPlanID()), mlog.FieldNodeID(task.GetNodeID()), mlog.Err(err))
	}
}

// abandonAttempt gives up on an attempt whose outcome could not be established.
// setAttemptEnded decides what that means for the work -- a rebuild under a
// fresh planID, or the trigger's final failure -- so this only has to persist
// the decision along with why the attempt ended.
//
// A failed save is not an error worth propagating: the state stays executing and
// the next round asks again, which is the same thing this call was trying to
// arrange.
func abandonAttempt(ctx context.Context, t replannableTask, reason string) {
	if err := t.updateAndSaveTaskMeta(
		setAttemptEnded(),
		setFailReason(reason),
	); err != nil {
		mlog.Warn(ctx, "failed to persist abandoned compaction attempt",
			mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.String("reason", reason),
			mlog.Err(err))
	}
}

// The two predicates below are derived from one another so the containment
// stays explicit: every state that owes cleanup is terminal, and cleanup is the
// only thing that may follow a terminal state.
//
//	terminal (completed|failed|timeout|retrying|cleaned)
//	  ⊃ needsCleanup (minus cleaned: already cleaned, nothing owed)
//
// Whether cleanup also owes a rebuild is a separate question: only the
// persisted retrying state carries that decision.

// isTerminalState reports whether a state ends the task's
// execution. Only cleanup may follow one.
func isTerminalState(state datapb.CompactionTaskState) bool {
	return state == datapb.CompactionTaskState_completed ||
		state == datapb.CompactionTaskState_failed ||
		state == datapb.CompactionTaskState_timeout ||
		state == datapb.CompactionTaskState_retrying ||
		state == datapb.CompactionTaskState_cleaned
}

// workerCompactionFailReason normalizes the reason a worker reported with a
// failed plan. Old DataNodes send none -- CompactionPlanResult.reason did not
// exist -- so keep a fixed stand-in rather than persisting an empty string that
// reads as "no failure recorded".
func workerCompactionFailReason(reason string) string {
	if reason == "" {
		return "compaction failed in datanode"
	}
	return reason
}

// compactionTaskMetaWriter is the persistence surface every compaction task
// implementation already has. It exists so updateAndSaveCompactionTaskMeta can
// be written once instead of four times.
type compactionTaskMetaWriter interface {
	GetTask() *datapb.CompactionTask
	saveTaskMeta(task *datapb.CompactionTask) error
	SetTask(task *datapb.CompactionTask)
}

// updateAndSaveCompactionTaskMeta applies opts, stamps the end time if the
// result is the first terminal state this record reaches, and persists it.
//
// The terminal check runs after applying opts so EndTime records the first
// transition into a terminal state. An existing EndTime is never overwritten.
func updateAndSaveCompactionTaskMeta(t compactionTaskMetaWriter, opts ...compactionTaskOpt) error {
	// Edit a clone, never the shared record: concurrent readers hold the
	// current pointer lock-free, so the new state must be published atomically
	// via SetTask after it is fully built and persisted.
	task := proto.Clone(t.GetTask()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(task)
	}
	if isTerminalState(task.GetState()) && task.GetEndTime() == 0 {
		task.EndTime = time.Now().Unix()
	}
	if err := t.saveTaskMeta(task); err != nil {
		return err
	}
	t.SetTask(task)
	return nil
}

// alreadyCleaned reports whether Clean has nothing left to do. It makes Clean
// idempotent across retries -- and guards any future second caller: doClean's
// resetSegmentCompacting must never run twice, or it could unlock inputs another
// compaction has legitimately re-acquired. Unlike isCompactionTaskCleaned this
// asks only about the cleaned state, never about unknown.
func alreadyCleaned(t CompactionTask) bool {
	return t.GetTask().GetState() == datapb.CompactionTaskState_cleaned
}

// needsCleanup reports whether a task still owes its inputs the
// cleanup pass -- every terminal state except cleaned, which already ran it.
func needsCleanup(state datapb.CompactionTaskState) bool {
	return isTerminalState(state) &&
		state != datapb.CompactionTaskState_cleaned
}

// attemptsExhausted reports whether this attempt has used up the
// rebuilds dataCoord.compaction.maxAttempts allows, i.e. whether cleanup will
// stop retrying the work under this trigger and leave it to the periodic one.
//
// RetryTimes is 0 on the original attempt and old+1 on every replacement, so
// attempt number = RetryTimes+1. It is trustworthy because the replan is its
// only writer.
//
// setAttemptEnded is the only reader: it spends the cap once when the attempt
// ends and writes retrying or failed. buildReplacement deliberately does not
// re-check the cap -- it only increments RetryTimes on a record that already
// carries retrying -- so "the summary still calls it running" and "cleanup
// will actually rebuild it" cannot disagree.
func attemptsExhausted(task *datapb.CompactionTask) bool {
	maxAttempts := Params.DataCoordCfg.CompactionMaxAttempts.GetAsInt()
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	return int(task.GetRetryTimes())+1 >= maxAttempts
}

type compactionTaskOpt func(task *datapb.CompactionTask)

func cloneCompactionTask(task *datapb.CompactionTask, opts ...compactionTaskOpt) *datapb.CompactionTask {
	cloned := proto.Clone(task).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

func cloneCompactionTaskAsMetaSaved(task *datapb.CompactionTask, resultSegments []*SegmentInfo) *datapb.CompactionTask {
	opts := []compactionTaskOpt{setState(datapb.CompactionTaskState_meta_saved)}
	switch task.GetType() {
	case datapb.CompactionType_MixCompaction,
		datapb.CompactionType_SortCompaction,
		datapb.CompactionType_BumpSchemaVersionCompaction:
		resultSegmentIDs := make([]int64, 0, len(resultSegments))
		for _, segment := range resultSegments {
			resultSegmentIDs = append(resultSegmentIDs, segment.GetID())
		}
		opts = append(opts, setResultSegments(resultSegmentIDs))
	}
	return cloneCompactionTask(task, opts...)
}

func commitsTaskWithSegmentAdoption(task *datapb.CompactionTask, result *datapb.CompactionPlanResult) bool {
	// Only identity swaps belong here: once their inputs are Dropped, replaying
	// from the old task record is no longer possible. Clustering keeps its
	// outputs invisible and owned through TmpSegments, while an in-place schema
	// bump is replayable through its manifest CAS, so both retain their existing
	// state transitions.
	if task.GetType() == datapb.CompactionType_MixCompaction || task.GetType() == datapb.CompactionType_SortCompaction {
		return true
	}
	return task.GetType() == datapb.CompactionType_BumpSchemaVersionCompaction &&
		len(task.GetInputSegments()) == 1 && len(result.GetSegments()) == 1 &&
		task.GetInputSegments()[0] != result.GetSegments()[0].GetSegmentID()
}

func compactionTaskLabel(task *datapb.CompactionTask) string {
	return fmt.Sprintf("%d-%s", task.GetPartitionID(), task.GetChannel())
}

func setNodeID(nodeID int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.NodeID = nodeID
	}
}

func setFailReason(reason string) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.FailReason = reason
	}
}

func setEndTime(endTime int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.EndTime = endTime
	}
}

func setResultSegments(segments []int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.ResultSegments = segments
	}
}

func setTmpSegments(segments []int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.TmpSegments = segments
	}
}

// compactionMetricNode is the node label for DataCoordCompactionTaskNum.
//
// A task with no owner carries NodeID 0 before it is ever dispatched and
// NullNodeID once an assignment is taken away; both mean "unassigned", and
// collapsing them is what lets a count incremented under one be decremented
// under the other.
func compactionMetricNode(nodeID int64) string {
	if nodeID <= 0 {
		return fmt.Sprintf("%d", NullNodeID)
	}
	return fmt.Sprintf("%d", nodeID)
}

// moveExecutingCompactionMetric moves one task's executing count from the node
// it is currently counted under to the one it has just been assigned to.
//
// The gauge is keyed by node, so every write of NodeID has to move the count
// with it. Without this the count incremented at admission -- when the task has
// no owner yet, because dispatch is asynchronous -- is decremented at
// completion under the worker's real ID, so the unassigned bucket rises without
// bound while every worker's bucket falls negative.
func moveExecutingCompactionMetric(compactionType datapb.CompactionType, from, to int64) {
	fromLabel, toLabel := compactionMetricNode(from), compactionMetricNode(to)
	if fromLabel == toLabel {
		return
	}
	metrics.DataCoordCompactionTaskNum.WithLabelValues(fromLabel, compactionType.String(), metrics.Executing).Dec()
	metrics.DataCoordCompactionTaskNum.WithLabelValues(toLabel, compactionType.String(), metrics.Executing).Inc()
}

// setAttemptEnded records that this attempt ended without succeeding. Which
// state that is IS the decision about what happens next, and it is made here,
// once, from the record itself: retrying while the attempt cap still leaves a
// rebuild, failed once it does not.
//
// Deciding at the moment of failure rather than re-deriving it later is what
// keeps the answer stable. dataCoord.compaction.maxAttempts is refreshable, so
// a cap lowered mid-flight must not retroactively turn a task that is already
// queued for a rebuild into a settled failure -- or the trigger would report an
// outcome cleanup is about to contradict.
func setAttemptEnded() compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		if attemptsExhausted(task) {
			task.State = datapb.CompactionTaskState_failed
			return
		}
		task.State = datapb.CompactionTaskState_retrying
	}
}

func setState(state datapb.CompactionTaskState) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.State = state
	}
}

func setStartTime(startTime int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.StartTime = startTime
	}
}

func setCreateTs(createTS uint64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.CreateTs = createTS
	}
}

func setLastStateStartTime(lastStateStartTime int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.LastStateStartTime = lastStateStartTime
	}
}

func setAnalyzeTaskID(id int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.AnalyzeTaskID = id
	}
}

func setAnalyzeVersion(version int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.AnalyzeVersion = version
	}
}
