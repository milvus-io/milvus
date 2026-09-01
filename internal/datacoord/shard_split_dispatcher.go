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
	"time"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// compactionDispatcher is the subset of the compaction inspector the rewrite
// needs: enqueue a plan, and look its tasks up to learn whether it committed.
// Kept narrow so the split task does not depend on the whole inspector.
type compactionDispatcher interface {
	enqueueCompaction(task *datapb.CompactionTask) error
}

// compactionPlanReader looks up dispatched plans to judge their state.
type compactionPlanReader interface {
	GetCompactionTasksByTriggerID(ctx context.Context, triggerID int64) []*datapb.CompactionTask
}

// inspectorRewriteDispatcher is the production rewritePlanDispatcher: it turns
// one source segment into a HashSplitCompaction plan and hands it to the
// compaction inspector, then reads the plan's state back from compaction meta.
//
// The split task id doubles as the plans' TriggerID, so every rewrite plan of a
// task can be found with one lookup, and a datacoord restart re-discovers the
// dispatched plans from meta instead of from memory.
type inspectorRewriteDispatcher struct {
	ctx        context.Context
	meta       *meta
	inspector  compactionDispatcher
	planReader compactionPlanReader
	alloc      allocator.Allocator
	// currentTaskID scopes plan lookups to one split task; set via forTask for
	// the duration of a rewrite round.
	currentTaskID int64
}

func newInspectorRewriteDispatcher(
	ctx context.Context,
	m *meta,
	inspector compactionDispatcher,
	planReader compactionPlanReader,
	alloc allocator.Allocator,
) *inspectorRewriteDispatcher {
	return &inspectorRewriteDispatcher{
		ctx:        ctx,
		meta:       m,
		inspector:  inspector,
		planReader: planReader,
		alloc:      alloc,
	}
}

// DispatchHashSplit enqueues the rewrite of one source segment.
//
// Idempotency: a plan already enqueued for this (task, segment) is returned
// as-is instead of enqueuing a second one, so a repeated round — or a restart
// that replays the pending list — does not fan out duplicate rewrites of the
// same segment.
func (d *inspectorRewriteDispatcher) DispatchHashSplit(
	task *datapb.SplitShardTask,
	segmentID int64,
) (int64, error) {
	d.currentTaskID = task.GetTaskId()
	if existing := d.livePlanFor(task, segmentID); existing != 0 {
		return existing, nil
	}

	segment := d.meta.GetSegment(d.ctx, segmentID)
	if segment == nil {
		return 0, merr.WrapErrSegmentNotFound(segmentID, "cannot rewrite a segment absent from meta")
	}
	collection := d.meta.GetCollection(task.GetCollectionId())
	if collection == nil {
		return 0, merr.WrapErrCollectionNotFound(task.GetCollectionId())
	}

	planID, err := d.alloc.AllocID(d.ctx)
	if err != nil {
		return 0, err
	}
	// One output per target for each source segment. Pre-allocating the ids
	// keeps the datanode from calling back for them mid-rewrite.
	segIDBegin, segIDEnd, err := d.alloc.AllocN(int64(len(task.GetTargets())))
	if err != nil {
		return 0, err
	}

	plan := &datapb.CompactionTask{
		PlanID:       planID,
		TriggerID:    task.GetTaskId(),
		State:        datapb.CompactionTaskState_pipelining,
		StartTime:    time.Now().Unix(),
		Type:         datapb.CompactionType_HashSplitCompaction,
		CollectionID: task.GetCollectionId(),
		PartitionID:  segment.GetPartitionID(),
		// The plan runs on the SOURCE channel: that is where its input segment
		// lives and which datanode owns it. Taken from the segment rather than
		// the task, since a task may rewrite several sources at once. Its outputs
		// are attributed to the target vchannels through HashSplitTargets, not
		// through this field.
		Channel:                segment.GetInsertChannel(),
		InputSegments:          []int64{segmentID},
		ResultSegments:         []int64{},
		TotalRows:              segment.GetNumOfRows(),
		Schema:                 collection.Schema,
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: segIDBegin, End: segIDEnd},
		HashSplitTargets:       task.GetTargets(),
		// Without the modulus the datanode cannot read the residues, and would
		// partition the rewrite by a different rule than the one the routing
		// commit published.
		HashSplitModulus: task.GetRoutingModulus(),
	}
	if err := d.inspector.enqueueCompaction(plan); err != nil {
		return 0, err
	}
	return planID, nil
}

// HashSplitPlanState reports whether a dispatched plan committed (done), is
// still in flight (running), or is gone and must be re-dispatched (neither).
//
// State is read from compaction meta rather than kept in memory, so the answer
// survives a datacoord restart: a plan that completed while the coordinator was
// down is still reported done. A plan absent from meta entirely reads as
// neither, which is exactly the "lost, re-dispatch it" case.
func (d *inspectorRewriteDispatcher) HashSplitPlanState(planID int64) (done bool, running bool) {
	plan := d.findPlan(planID)
	if plan == nil {
		return false, false
	}
	return hashSplitPlanTerminalState(plan.GetState())
}

// findPlan locates a dispatched rewrite plan by id. Plans are indexed by
// TriggerID (the split task id), so the lookup scans the task's own plans; the
// task ids in flight are few and each task's plan set is bounded by the rewrite
// batch size.
func (d *inspectorRewriteDispatcher) findPlan(planID int64) *datapb.CompactionTask {
	for _, taskID := range d.trackedTaskIDs() {
		for _, plan := range d.planReader.GetCompactionTasksByTriggerID(d.ctx, taskID) {
			if plan.GetPlanID() == planID {
				return plan
			}
		}
	}
	return nil
}

// trackedTaskIDs lists the split task ids whose plans this dispatcher may be
// asked about. Set by the manager when it drives a task's rewrite round, so the
// lookup above stays scoped to the task at hand.
func (d *inspectorRewriteDispatcher) trackedTaskIDs() []int64 {
	if d.currentTaskID == 0 {
		return nil
	}
	return []int64{d.currentTaskID}
}

// forTask scopes the dispatcher to one split task for the duration of a rewrite
// round, so plan lookups know which TriggerID to scan.
func (d *inspectorRewriteDispatcher) forTask(taskID int64) *inspectorRewriteDispatcher {
	scoped := *d
	scoped.currentTaskID = taskID
	return &scoped
}

// hashSplitPlanTerminalState maps a compaction task state onto the
// (done, running) pair the rewrite rounds consume.
func hashSplitPlanTerminalState(state datapb.CompactionTaskState) (done bool, running bool) {
	switch state {
	case datapb.CompactionTaskState_completed:
		return true, false
	case datapb.CompactionTaskState_pipelining,
		datapb.CompactionTaskState_executing,
		datapb.CompactionTaskState_meta_saved,
		datapb.CompactionTaskState_statistic,
		datapb.CompactionTaskState_indexing:
		return false, true
	default:
		// failed, timeout, cleaned, unknown: the plan will not commit, so the
		// round re-dispatches its segment. A rewrite is deterministic, so the
		// retry reproduces the same outputs and the dead plan's partial ones
		// are collected as ordinary failed-compaction garbage.
		return false, false
	}
}

// livePlanFor returns the id of a plan already dispatched for this segment that
// has not failed, or 0 when there is none.
func (d *inspectorRewriteDispatcher) livePlanFor(task *datapb.SplitShardTask, segmentID int64) int64 {
	for _, plan := range d.planReader.GetCompactionTasksByTriggerID(d.ctx, task.GetTaskId()) {
		if plan.GetType() != datapb.CompactionType_HashSplitCompaction {
			continue
		}
		if len(plan.GetInputSegments()) != 1 || plan.GetInputSegments()[0] != segmentID {
			continue
		}
		if done, running := hashSplitPlanTerminalState(plan.GetState()); done || running {
			return plan.GetPlanID()
		}
	}
	return 0
}
