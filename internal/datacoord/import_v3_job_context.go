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

// This file owns importV3JobContext — the one working context every state
// handler, planning stage and the GC pipeline receives — together with its
// generic helpers. It embeds the checker, so the shared dependencies (ctx,
// meta, broker, alloc, importMeta, cluster, hooks) are reachable directly, and
// adds the single job this tick works on plus a job-scoped logger.
//
// Only state-machine-agnostic helpers live here. Anything that knows about a
// particular state belongs next to that state's handler.

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// importV3JobContext is the one working context every handler, planning stage
// and the GC pipeline receives. It embeds the checker, so the shared
// dependencies are reachable directly, and adds the single job this tick works
// on plus a job-scoped logger. The checker remains the single owner of the
// dependencies; jc is the per-job view of it.
type importV3JobContext struct {
	*importCheckerV3
	job     ImportJob
	log     *mlog.Logger
	metrics *importV3JobMetrics
}

func newImportV3JobContext(checker *importCheckerV3, job ImportJob) *importV3JobContext {
	return &importV3JobContext{
		importCheckerV3: checker,
		job:             job,
		log:             mlog.With(mlog.FieldJobID(job.GetJobID())),
		metrics:         newImportV3JobMetrics(job),
	}
}

// allTasksDone reports whether every task reached Completed. A Failed task is
// returned so the caller can fail the job with its reason; a task that is not
// terminal yet returns nil with done=false (keep waiting).
func allTasksDone(tasks []ImportTask) (failed ImportTask, done bool) {
	for _, t := range tasks {
		switch t.GetState() {
		case datapb.ImportTaskStateV2_Failed:
			return t, false
		case datapb.ImportTaskStateV2_Completed:
			// nothing to do, keep checking
		default:
			return nil, false
		}
	}
	return nil, true
}

// importV3StageOfState maps a job state to the stage whose latency is attributed
// to it when the job leaves it. Uncommitted, Committing and Failed have no
// stage.
var importV3StageOfState = map[internalpb.ImportJobState]string{
	internalpb.ImportJobState_Pending:          metrics.ImportStagePending,
	internalpb.ImportJobState_PreImporting:     metrics.ImportStagePreImport,
	internalpb.ImportJobState_AssigningIDRange: metrics.ImportStagePreImport,
	internalpb.ImportJobState_Resharding:       metrics.ImportStageReshard,
	internalpb.ImportJobState_Planning:         metrics.ImportStagePlanning,
	internalpb.ImportJobState_Importing:        metrics.ImportStageImport,
	internalpb.ImportJobState_IndexBuilding:    metrics.ImportStageBuildIndex,
}

// ImportV3JobTransitTo is the single writer of the V3 job state. It persists the
// transition, then records the latency of the state being left. Every call site
// is responsible for taking a legal edge. The DDL callbacks are shared with
// ImportV2 (CommitImport / UpdateImport acks), so they keep their own writes.
func ImportV3JobTransitTo(
	ctx context.Context,
	importMeta ImportMeta,
	job ImportJob,
	to internalpb.ImportJobState,
	actions ...UpdateJobAction,
) error {
	from := job.GetState()
	if err := importMeta.UpdateJob(ctx, job.GetJobID(), append(actions, UpdateJobState(to))...); err != nil {
		mlog.Warn(ctx, "failed to update import v3 job state",
			mlog.FieldJobID(job.GetJobID()), mlog.String("state", to.String()), mlog.Err(err))
		return err
	}
	if stage, ok := importV3StageOfState[from]; ok {
		if tr := job.GetTR(); tr != nil {
			newImportV3JobMetrics(job).observeStage(stage, tr.RecordSpan())
		}
	}
	return nil
}

// transitionTo is the thin per-job view of ImportV3JobTransitTo, the single
// writer of the V3 job state. The caller decides whether the error is worth
// propagating; a failed catalog write simply replays on the next tick.
//
// A failed write leaves the job in its current state, so the checker re-enters
// this handler on the next tick and retries — callers may therefore
// deliberately ignore the returned error. Use failJob (or return the error)
// instead when this tick must terminate the job.
func (jc *importV3JobContext) transitionTo(state internalpb.ImportJobState, actions ...UpdateJobAction) error {
	return ImportV3JobTransitTo(jc.ctx, jc.importMeta, jc.job, state, actions...)
}

// failJob marks the job Failed with the given reason.
func (jc *importV3JobContext) failJob(reason string) {
	if err := jc.transitionTo(internalpb.ImportJobState_Failed, UpdateJobReason(reason)); err != nil {
		jc.log.Warn(jc.ctx, "failed to update job state to Failed", mlog.Err(err))
	}
}

// finishEmptyJob is the shared zero-rows exit: a job whose input holds no rows
// finishes immediately, without any Reshard task or cursor. auto_commit jobs
// go straight to Completed (stamping the completion time); the rest park in
// Uncommitted for the platform's explicit commit. A failed catalog write is
// returned so the caller can decide whether to retry or ignore it.
func (jc *importV3JobContext) finishEmptyJob() error {
	if !jc.job.GetAutoCommit() {
		return jc.transitionTo(internalpb.ImportJobState_Uncommitted)
	}
	completeTime := time.Now().Format(time.RFC3339)
	return jc.transitionTo(internalpb.ImportJobState_Completed, UpdateJobCompleteTime(completeTime))
}
