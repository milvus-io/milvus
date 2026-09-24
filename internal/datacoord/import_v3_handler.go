// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package datacoord

import (
	"fmt"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// importV3StateHandler is one state of the Import V3 job lifecycle. Handle runs
// once per checker tick while the job is in this state. A returned error is
// classified by the checker's dispatch: terminal fails the job, anything else
// retries on the next tick.
type importV3StateHandler interface {
	Handle(jc *importV3JobContext) error
}

// handlerFor maps a job state to its handler. Handlers are stateless: they
// receive everything they need through the jc passed to Handle, so one shared
// instance per state is enough and the wiring stays explicit.
func handlerFor(state internalpb.ImportJobState) importV3StateHandler {
	switch state {
	case internalpb.ImportJobState_Pending:
		return pendingHandler{}
	case internalpb.ImportJobState_PreImporting:
		return preImportingHandler{}
	case internalpb.ImportJobState_AssigningIDRange:
		return assigningIDRangeHandler{}
	case internalpb.ImportJobState_Resharding:
		return reshardingHandler{}
	case internalpb.ImportJobState_Planning:
		return planningHandler{}
	case internalpb.ImportJobState_Importing:
		return importingHandler{}
	case internalpb.ImportJobState_IndexBuilding:
		return indexBuildingHandler{}
	case internalpb.ImportJobState_Uncommitted:
		return uncommittedHandler{}
	case internalpb.ImportJobState_Committing:
		return committingHandler{}
	case internalpb.ImportJobState_Failed:
		return failedHandler{}
	default:
		return nil
	}
}

// assigningIDRangeHandler is the wait state for the per-file ID ranges. The
// primary allocates the exact ranges and broadcasts the UpdateImport message;
// a secondary waits for the replicated copy, and the ack callback applies it
// to the job meta. Once present the job runs the divergence gate and the
// shared tail.
type assigningIDRangeHandler struct{}

func (h assigningIDRangeHandler) Handle(jc *importV3JobContext) error {
	if !jobIDRangesSet(jc.job) {
		ensurePreImportV3IDRanges(jc)
		return nil
	}
	return finishPreImportV3(jc)
}

// committingHandler handles jobs in the Committing state. Once all vchannels
// have acknowledged the commit fence, the job transitions to Completed.
type committingHandler struct{}

func (h committingHandler) Handle(jc *importV3JobContext) error {
	// When Vchannels is empty, len == len is trivially true. This handles the degenerate
	// case of a zero-channel import (e.g., empty collection); proceed to Completed immediately.
	if len(jc.job.GetCommittedVchannels()) < len(jc.job.GetVchannels()) {
		return nil // still waiting for remaining vchannels
	}
	completeTime := time.Now().Format(time.RFC3339)
	if err := jc.transitionTo(internalpb.ImportJobState_Completed, UpdateJobCompleteTime(completeTime)); err != nil {
		return nil
	}
	totalDuration := jc.job.GetTR().ElapseSpan()
	jc.metrics.observeTotal(totalDuration)
	jc.log.Info(jc.ctx, "import job Committing done, all vchannels committed",
		mlog.Duration("jobTimeCost/total", totalDuration))
	return nil
}

// failedHandler marks the job's still-live tasks as failed so the dispatch
// loop stops rescheduling them.
type failedHandler struct{}

func (h failedHandler) Handle(jc *importV3JobContext) error {
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithStates(datapb.ImportTaskStateV2_Pending,
		datapb.ImportTaskStateV2_InProgress, datapb.ImportTaskStateV2_Completed, datapb.ImportTaskStateV2_Retry))
	if len(tasks) == 0 {
		return nil
	}
	jc.log.Warn(jc.ctx, "Import job has failed, all tasks with the same jobID will be marked as failed",
		mlog.String("reason", jc.job.GetReason()))
	for _, task := range tasks {
		err := jc.importMeta.UpdateTask(jc.ctx, task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed),
			UpdateReason(jc.job.GetReason()))
		if err != nil {
			jc.log.Warn(jc.ctx, "failed to update import task state to failed", WrapTaskLog(task, mlog.Err(err))...)
			continue
		}
	}
	return nil
}

// importingHandler waits for every ImportTaskV3 to complete, bumps the
// completed segments' schema version and advances the job to IndexBuilding.
type importingHandler struct{}

func (h importingHandler) Handle(jc *importV3JobContext) error {
	currentSchemaVersion, err := validateImportV3Schema(jc.meta, jc.job.GetCollectionID(), jc.job.GetSchema())
	if err != nil {
		return err
	}
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(ImportTaskV3Type))
	if len(tasks) == 0 {
		return nil
	}
	if failed, done := allTasksDone(tasks); failed != nil {
		// Recover the crash window between the task marker and the job marker:
		// a Failed task can never become Completed, so without this branch the
		// job would wait forever (or until an external timeout).
		jc.failJob(fmt.Sprintf("import v3 task %d failed: %s", failed.GetTaskID(), failed.GetReason()))
		return nil
	} else if !done {
		return nil
	}
	// Persist all completed-segment schema-version bumps before advancing the
	// job. IndexBuilding is the marker-last write: a crash or a failed segment
	// update leaves the job in Importing and this loop retries on the next tick.
	segmentOps := make([]UpdateOperator, 0, len(tasks))
	for _, task := range tasks {
		segmentID := task.(*importTaskV3).task.Load().GetSegmentId()
		if segmentID == 0 {
			continue
		}
		segment := jc.meta.GetSegment(jc.ctx, segmentID)
		if segment != nil && segment.GetNumOfRows() > 0 && segment.GetSchemaVersion() != currentSchemaVersion {
			segmentOps = append(segmentOps, updateImportV3SchemaVersion(segmentID, currentSchemaVersion))
		}
	}
	if len(segmentOps) > 0 {
		if err := jc.meta.UpdateSegmentsInfo(jc.ctx, segmentOps...); err != nil {
			jc.log.Warn(jc.ctx, "update import v3 segment schema version failed", mlog.Err(err))
			return nil
		}
	}
	if err := jc.transitionTo(internalpb.ImportJobState_IndexBuilding); err != nil {
		return nil
	}
	jc.log.Info(jc.ctx, "import v3 import done")
	return nil
}

// indexBuildingHandler waits for the imported segments' indexes to be built,
// then advances the job to Uncommitted.
type indexBuildingHandler struct{}

func (h indexBuildingHandler) Handle(jc *importV3JobContext) error {
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(ImportTaskV3Type))
	segmentIDs := make([]int64, 0)
	for _, task := range tasks {
		if segmentID := task.(*importTaskV3).task.Load().GetSegmentId(); segmentID != 0 {
			segment := jc.meta.GetHealthySegment(jc.ctx, segmentID)
			if segment == nil || segment.GetNumOfRows() == 0 {
				continue
			}
			segmentIDs = append(segmentIDs, segmentID)
		}
	}
	healthySegments := jc.meta.GetSegments(segmentIDs, isSegmentHealthy)
	unindexed := jc.meta.indexMeta.GetUnindexedSegments(jc.job.GetCollectionID(), healthySegments)
	if Params.DataCoordCfg.WaitForIndex.GetAsBool() && len(unindexed) > 0 {
		for _, segmentID := range unindexed {
			select {
			case getBuildIndexChSingleton() <- segmentID:
			default:
			}
		}
		return nil
	}
	if err := jc.transitionTo(internalpb.ImportJobState_Uncommitted); err != nil {
		return nil
	}
	jc.log.Info(jc.ctx, "import v3 build index done")
	return nil
}

// pendingHandler is the first V3 state. Every import runs the count-only
// preimport phase first: an ordinary import needs the exact per-file row count
// before Reshard can generate deterministic PK/RowID (the exact ID ranges are
// then allocated and broadcast in AssigningIDRange), while a backup import,
// which carries its own PK/RowID and needs no ranges, runs the same stage in a
// size-only mode so DataCoord has the per-file packing size for BFD.
type pendingHandler struct{}

func (h pendingHandler) Handle(jc *importV3JobContext) error {
	return createPreImportV3Tasks(jc)
}

// planningHandler reads the completed ReshardTasks' manifests, packs fragments
// into per-segment ImportTaskV3s and advances the job to Importing. The logic
// lives in import_v3_planning.go.
type planningHandler struct{}

func (h planningHandler) Handle(jc *importV3JobContext) error {
	return planImportV3(jc)
}

// preImportingHandler waits for every count-only PreImportV3 task to complete.
// Once the exact per-file counts are known the job either parks in
// AssigningIDRange for the primary to allocate and broadcast the exact ranges,
// or (when ranges are already present from a previous tick) runs the shared
// tail.
type preImportingHandler struct{}

func (h preImportingHandler) Handle(jc *importV3JobContext) error {
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(PreImportTaskV3Type))
	if len(tasks) == 0 {
		return nil
	}
	failed, done := allTasksDone(tasks)
	if failed != nil {
		// A Failed PreImportV3 task can never reach Completed, so without this
		// branch the job would wait forever (or until an external timeout).
		// Propagate the task's reason so it stays visible.
		jc.failJob(fmt.Sprintf("import v3 preimport task %d failed: %s", failed.GetTaskID(), failed.GetReason()))
		return nil
	}
	if !done {
		return nil
	}
	if importV3NeedsIDRanges(jc.job) && !jobIDRangesSet(jc.job) {
		_ = jc.transitionTo(internalpb.ImportJobState_AssigningIDRange)
		return nil
	}
	return finishPreImportV3(jc)
}

// reshardingHandler waits for every ReshardTask of the job to reach the
// catalog-verified Completed marker, then advances to Planning. It inspects
// only task states — no result manifests are read while waiting for slow
// tasks. Empty input jobs (zero total rows) are detected by Planning after
// reading the manifests, shortcutting to Uncommitted/Completed exactly like
// the legacy import path does for empty preimports.
type reshardingHandler struct{}

func (h reshardingHandler) Handle(jc *importV3JobContext) error {
	completed, err := reshardTasksAllCompleted(jc)
	if err != nil {
		return err
	}
	if !completed {
		return nil
	}
	_ = jc.transitionTo(internalpb.ImportJobState_Planning)
	return nil
}

// reshardTasksAllCompleted reports whether every ReshardTask has reached the
// catalog-verified Completed marker. It inspects only task states and never
// reads result manifests — the Completed marker is gated on acceptResult
// validating the manifest, so this check needs no object-store access and the
// Resharding tail does no redundant reads while waiting for slow tasks.
func reshardTasksAllCompleted(jc *importV3JobContext) (bool, error) {
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(ReshardTaskType))
	if len(tasks) == 0 {
		return false, nil
	}
	for _, generic := range tasks {
		if _, ok := generic.(*reshardTask); !ok {
			return false, merr.WrapErrDataIntegrityMsg("import v3 reshard result set contains an unexpected task type")
		}
	}
	// A failed task must terminate the job even when DataCoord crashed between
	// the task marker and the job marker; without this the job would wait for a
	// Completed marker that can never arrive.
	if failed, done := allTasksDone(tasks); failed != nil {
		return false, merr.WrapErrImportSysFailedMsg("import v3 reshard task %d failed: %s", failed.GetTaskID(), failed.GetReason())
	} else if !done {
		return false, nil
	}
	return true, nil
}

// uncommittedHandler handles jobs in the Uncommitted state. If
// auto_commit=true, it triggers a commit via broadcastCommitImportMessage. If
// auto_commit=false, it waits for an explicit CommitImport RPC from the
// platform.
type uncommittedHandler struct{}

func (h uncommittedHandler) Handle(jc *importV3JobContext) error {
	if !jc.job.GetAutoCommit() {
		// Wait for explicit CommitImport from the replication platform.
		return nil
	}
	// auto_commit=true: trigger commit by broadcasting the WAL message.
	// Repeated invocations across ticks are safe: the broadcaster's exclusive
	// collection-level resource-key lock serializes overlapping broadcasts, the
	// ack callback only transitions when the job is still Uncommitted, and
	// HandleCommitVchannel is idempotent on committed_vchannels.
	if jc.hooks.commitImport == nil {
		jc.log.Error(jc.ctx, "commit hook is nil but auto_commit=true; this is a programming error")
		return nil
	}
	if err := jc.hooks.commitImport(jc.ctx, jc.job); err != nil {
		jc.log.Warn(jc.ctx, "auto-commit broadcast failed, will retry on next tick", mlog.Err(err))
	}
	return nil
}
