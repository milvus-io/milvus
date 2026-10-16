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

package compaction

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// nullNodeID means that no node runs the task.
const nullNodeID = -1

const (
	// commitTimeout bounds one CommitCompaction call.
	commitTimeout = 10 * time.Second
	// commitBackoffInitial is the wait after the first unavailable commit.
	commitBackoffInitial = time.Second
	// commitBackoffMax is the longest wait between two commit attempts.
	commitBackoffMax = 60 * time.Second
)

// IDAllocator hands out global IDs.
type IDAllocator interface {
	AllocN(n int64) (begin int64, end int64, err error)
}

// Committer is the manifest manager as the compaction uses it.
type Committer interface {
	Current(ctx context.Context, t manifest.Target) (*pkindexpb.Manifest, error)
	CommitCompaction(ctx context.Context, collectionID int64, vchannel string, c manifest.Compaction) (*pkindexpb.Manifest, error)
}

// Task is one compaction in the generic task scheduler. The scheduler calls
// CreateTaskOnWorker, QueryTaskOnWorker and DropTaskOnWorker; the task
// commits the result itself and tells onTerminal when it finished or failed.
// onTerminal runs inside QueryTaskOnWorker, under the per-task read lock of
// the scheduler: it must not call a scheduler method for this task and must
// not block.
type Task struct {
	plan
	ctx        context.Context
	taskID     int64
	runID      int64
	cfg        Config
	alloc      IDAllocator
	committer  Committer
	onTerminal func(*Task)
	now        func() time.Time
	times      *taskcommon.Times

	mu        sync.Mutex
	state     taskcommon.State
	nodeID    int64
	reason    string
	idBegin   int64
	idEnd     int64
	outputs   []*pkindexpb.SSTRef
	processed int64

	// nextCommitAt is the earliest time of the next commit attempt;
	// commitBackoff is the wait that follows the next unavailable commit.
	nextCommitAt  time.Time
	commitBackoff time.Duration
}

// newTask returns a task in Init with no node. See Task for the rules of
// onTerminal.
func newTask(ctx context.Context, p plan, taskID, runID int64, cfg Config, alloc IDAllocator, committer Committer, onTerminal func(*Task)) *Task {
	return &Task{
		plan: p, ctx: ctx, taskID: taskID, runID: runID, cfg: cfg, alloc: alloc, committer: committer,
		onTerminal: onTerminal, now: time.Now, times: taskcommon.NewTimes(), state: taskcommon.Init, nodeID: nullNodeID,
	}
}

func (t *Task) GetTaskID() int64             { return t.taskID }
func (t *Task) GetTaskType() taskcommon.Type { return taskcommon.PKIndexCompaction }
func (t *Task) GetTaskSlot() int64           { return t.cfg.TaskSlot }
func (t *Task) GetTaskVersion() int64        { return 0 }

func (t *Task) GetTaskState() taskcommon.State {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

func (t *Task) SetTaskTime(tt taskcommon.TimeType, tm time.Time) { t.times.SetTaskTime(tt, tm) }
func (t *Task) GetTaskTime(tt taskcommon.TimeType) time.Time     { return tt.GetTaskTime(t.times) }

// outputRange returns the output ID range of the current attempt, or 0, 0
// before the first dispatch.
func (t *Task) outputRange() (int64, int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.idBegin, t.idEnd
}

func (t *Task) setOutputRange(begin, end int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.idBegin, t.idEnd = begin, end
}

func (t *Task) getNodeID() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nodeID
}

// progress returns the number of input entries the node has processed.
func (t *Task) progress() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.processed
}

func (t *Task) logFields(fields ...mlog.Field) []mlog.Field {
	return append([]mlog.Field{
		mlog.FieldTaskID(t.taskID), mlog.FieldCollectionID(t.target.CollectionID), mlog.FieldVChannel(t.target.VChannel),
		mlog.String("rule", t.rule.String()), mlog.Int("inputs", len(t.inputs)), mlog.Int64("inputBytes", t.inputBytes),
	}, fields...)
}

// CreateTaskOnWorker sends the task to nodeID with a fresh output ID range. A
// failure leaves the task in Init and the scheduler tries again later. A
// retry never reuses an ID range: the node of an earlier attempt may still
// write objects under the old IDs.
func (t *Task) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	n := outputIDCount(t.inputBytes, t.cfg.TargetFileSize)
	begin, end, err := t.alloc.AllocN(n)
	if err != nil {
		mlog.Warn(t.ctx, "pkindex compaction cannot allocate output ids", t.logFields(mlog.Err(err))...)
		return
	}
	req := &pkindexpb.CompactionRequest{
		TaskId: t.taskID, CollectionId: t.target.CollectionID, Vchannel: t.target.VChannel, SstBasePath: t.sstBasePath,
		Inputs: t.inputs, RunId: t.runID, OutputIdBegin: begin, OutputIdEnd: end,
		TargetFileSize: t.cfg.TargetFileSize, DropTombstones: t.dropTombstones,
	}
	if err := cluster.CreatePKIndexCompaction(nodeID, req, t.cfg.TaskSlot); err != nil {
		mlog.Warn(t.ctx, "pkindex compaction cannot be created on the node", t.logFields(mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	t.mu.Lock()
	t.state, t.nodeID, t.idBegin, t.idEnd = taskcommon.InProgress, nodeID, begin, end
	t.nextCommitAt, t.commitBackoff = time.Time{}, 0
	t.mu.Unlock()
	mlog.Info(t.ctx, "pkindex compaction dispatched", t.logFields(mlog.FieldNodeID(nodeID), mlog.Int64("outputIdBegin", begin), mlog.Int64("outputIdEnd", end))...)
}

// QueryTaskOnWorker reads the state of the task from its node. A finished
// task is committed here; a commit that may succeed later keeps the task in
// flight. A lost node, a task the node does not know, or a failure that
// another attempt may clear puts the task back to Init with no node, and the
// scheduler dispatches it again.
func (t *Task) QueryTaskOnWorker(cluster session.Cluster) {
	nodeID := t.getNodeID()
	result, state, reason, err := cluster.QueryPKIndexCompaction(nodeID, t.taskID)
	if err != nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			mlog.Warn(t.ctx, "pkindex compaction node is gone, will dispatch again", t.logFields(mlog.FieldNodeID(nodeID), mlog.Err(err))...)
			t.reset(cluster)
			return
		}
		mlog.Warn(t.ctx, "pkindex compaction query failed, will query again", t.logFields(mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	switch state {
	case taskcommon.None:
		mlog.Warn(t.ctx, "pkindex compaction is unknown to the node, will dispatch again", t.logFields(mlog.FieldNodeID(nodeID))...)
		t.reset(cluster)
	case taskcommon.Init, taskcommon.InProgress, taskcommon.Retry:
		t.mu.Lock()
		t.processed = result.GetEntriesProcessed()
		t.mu.Unlock()
	case taskcommon.Failed:
		if result.GetRetriable() {
			mlog.Warn(t.ctx, "pkindex compaction failed on the node, will dispatch again", t.logFields(mlog.FieldNodeID(nodeID), mlog.String("reason", reason))...)
			t.reset(cluster)
			return
		}
		t.fail(reason)
	case taskcommon.Finished:
		t.mu.Lock()
		t.processed = result.GetEntriesProcessed()
		wait := t.now().Before(t.nextCommitAt)
		t.mu.Unlock()
		if wait {
			return
		}
		t.commit(cluster, result.GetOutputs())
	}
}

// DropTaskOnWorker removes the task from its node.
func (t *Task) DropTaskOnWorker(cluster session.Cluster) {
	nodeID := t.getNodeID()
	if nodeID == nullNodeID {
		return
	}
	if err := cluster.DropPKIndexCompaction(nodeID, t.taskID); err != nil {
		mlog.Warn(t.ctx, "pkindex compaction cannot be dropped on the node", t.logFields(mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	t.mu.Lock()
	t.nodeID = nullNodeID
	t.mu.Unlock()
}

// reset drops the attempt on its node, best effort, and puts the task back
// to Init so that the scheduler dispatches it with a new ID range.
func (t *Task) reset(cluster session.Cluster) {
	t.DropTaskOnWorker(cluster)
	t.mu.Lock()
	t.state, t.nodeID = taskcommon.Init, nullNodeID
	t.mu.Unlock()
}

// stale reports whether an output does not belong to the current attempt.
// The task ID is the same across attempts, so a node may still hold the
// result of an earlier one.
func (t *Task) stale(outputs []*pkindexpb.SSTRef) bool {
	begin, end := t.outputRange()
	for _, o := range outputs {
		if o.GetRunId() != t.runID || o.GetId() < begin || o.GetId() >= end {
			return true
		}
	}
	return false
}

// commit validates the result of the node and commits it. It runs on the
// check pool of the scheduler, whose round waits for every task, so it must
// be bounded: the call has a deadline, and an unavailable commit backs off.
func (t *Task) commit(cluster session.Cluster, outputs []*pkindexpb.SSTRef) {
	if t.stale(outputs) {
		mlog.Warn(t.ctx, "pkindex compaction result is not of the current attempt, will dispatch again", t.logFields(mlog.Int("outputs", len(outputs)))...)
		t.reset(cluster)
		return
	}
	if len(outputs) == 0 && !t.dropTombstones {
		// Without dropping tombstones the inputs hold live entries, so an empty
		// result would delete live data.
		mlog.Error(t.ctx, "pkindex compaction returned no output for inputs that hold live entries", t.logFields()...)
		t.fail("empty output")
		return
	}
	inputs := make([]int64, 0, len(t.inputs))
	for _, s := range t.inputs {
		inputs = append(inputs, s.GetId())
	}
	ctx, cancel := context.WithTimeout(t.ctx, commitTimeout)
	defer cancel()
	_, err := t.committer.CommitCompaction(ctx, t.target.CollectionID, t.target.VChannel, manifest.Compaction{Inputs: inputs, Outputs: outputs})
	switch {
	case err == nil:
		t.mu.Lock()
		t.state, t.outputs = taskcommon.Finished, outputs
		t.mu.Unlock()
		mlog.Info(t.ctx, "pkindex compaction committed", t.logFields(mlog.Int("outputs", len(outputs)))...)
		t.end()
	case errors.Is(err, pkindex.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		t.mu.Lock()
		if t.commitBackoff == 0 {
			t.commitBackoff = commitBackoffInitial
		} else {
			t.commitBackoff = min(t.commitBackoff*2, commitBackoffMax)
		}
		t.nextCommitAt = t.now().Add(t.commitBackoff)
		wait := t.commitBackoff
		t.mu.Unlock()
		mlog.Warn(t.ctx, "pkindex compaction commit is unavailable, will commit again", t.logFields(mlog.Duration("backoff", wait), mlog.Err(err))...)
	case errors.Is(err, manifest.ErrStaleEdit) || errors.Is(err, manifest.ErrNoManifest):
		// Expected: an active/standby race, or a dropped collection.
		mlog.Warn(t.ctx, "pkindex compaction commit was rejected", t.logFields(mlog.Err(err))...)
		t.fail(err.Error())
	default:
		mlog.Error(t.ctx, "pkindex compaction commit was rejected", t.logFields(mlog.Err(err))...)
		t.fail(err.Error())
	}
}

func (t *Task) fail(reason string) {
	t.mu.Lock()
	t.state, t.reason = taskcommon.Failed, reason
	t.mu.Unlock()
	mlog.Warn(t.ctx, "pkindex compaction failed", t.logFields(mlog.String("reason", reason))...)
	t.end()
}

func (t *Task) end() {
	if t.onTerminal != nil {
		t.onTerminal(t)
	}
}
