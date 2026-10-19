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

// This file is the terminal-job GC pipeline the checker's low-frequency loop
// runs once per terminal job per tick. Its phases are derived from durable
// facts on every tick; no separate GC record exists. It is not a state handler.

import (
	"context"
	"path"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// collectImportV3JobGC runs the GC phases in order: quiesce makes progress as
// soon as the job is terminal and keeps retrying version-aware Drops; task
// removal inside it waits for cleanupTs. Delete only starts once cleanupTs has
// passed and every task is unbound/removed.
func collectImportV3JobGC(jc *importV3JobContext) {
	if jc.job.GetState() != internalpb.ImportJobState_Completed &&
		jc.job.GetState() != internalpb.ImportJobState_Failed {
		return
	}
	if !quiesceImportV3JobGC(jc) {
		return
	}
	if !time.Now().After(tsoutil.PhysicalTime(jc.job.GetCleanupTs())) {
		return
	}
	if !rollbackFailedReplicateImportV3JobGC(jc) {
		return
	}
	deleteImportV3JobGC(jc)
}

// quiesceImportV3JobGC drops still-bound V3 tasks and removes tasks that are no
// longer pinned to a node. Removal only starts once cleanupTs has passed so the
// task records keep backing the job's row accounting during the retention
// window. It reports true only when every task of the job is already unbound
// and removed from catalog.
func quiesceImportV3JobGC(jc *importV3JobContext) bool {
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID())
	ready := true
	for _, task := range tasks {
		if task.GetNodeID() != NullNodeID {
			// The scheduler drops a terminal task once when it observes the state.
			// GC retries that best-effort version-aware Drop every tick, so a
			// transient RPC failure or a lost node cannot pin the job forever.
			if jc.cluster != nil {
				task.DropTaskOnWorker(jc.cluster)
			}
			ready = false
			continue
		}
		if jc.job.GetState() == internalpb.ImportJobState_Failed && task.GetType() == ImportTaskV3Type {
			v3 := task.(*importTaskV3).task.Load()
			segmentIDs := []int64(nil)
			if segmentID := v3.GetSegmentId(); segmentID != 0 {
				segmentIDs = append(segmentIDs, segmentID)
			}
			// The superseded segments of earlier runs are dropped here too, so a
			// failed job never leaves a retry's old segment behind. The drop is
			// idempotent (missing/dropped ids are skipped).
			segmentIDs = append(segmentIDs, v3.GetOldSegmentIds()...)
			if len(segmentIDs) > 0 {
				if err := jc.meta.UpdateSegmentsInfo(jc.ctx, dropImportV3Segments(segmentIDs)); err != nil {
					jc.log.Warn(jc.ctx, "drop import v3 segments during failed job GC", WrapTaskLog(task, mlog.Err(err))...)
					ready = false
					continue
				}
			}
		}
		// Task records back the job's row accounting (getImportRowsInfo sums
		// them), so they must survive the retention window like V2 GC does;
		// dropping their worker binding above is still safe to do early.
		if !time.Now().After(tsoutil.PhysicalTime(jc.job.GetCleanupTs())) {
			ready = false
			continue
		}
		if err := jc.importMeta.RemoveTask(jc.ctx, task.GetTaskID()); err != nil {
			jc.log.Warn(jc.ctx, "remove task failed during GC", WrapTaskLog(task, mlog.Err(err))...)
			ready = false
			continue
		}
		jc.log.Info(jc.ctx, "task removed during GC quiesce", WrapTaskLog(task)...)
	}
	return ready
}

// rollbackFailedReplicateImportV3JobGC releases the peer cluster before source
// GC drops a failed 2PC import. It is called only on the deletion tick, so a
// job sitting inside the retention window does not re-broadcast RollbackImport
// every tick. A crash between a successful broadcast and RemoveJob repeats the
// broadcast at most once, which is the same idempotency boundary V2 GC already
// has.
func rollbackFailedReplicateImportV3JobGC(jc *importV3JobContext) bool {
	if jc.hooks.rollbackImport == nil || jc.hooks.getReplicationRole == nil ||
		jc.job.GetState() != internalpb.ImportJobState_Failed || jc.job.GetAutoCommit() {
		return true
	}
	replicateCheckCtx, cancel := context.WithTimeout(jc.ctx, 10*time.Second)
	_, replicating, err := jc.hooks.getReplicationRole(replicateCheckCtx)
	cancel()
	switch {
	case err != nil:
		jc.log.Warn(jc.ctx, "cannot determine replication status before GC of failed import job, will retry", mlog.Err(err))
		return false
	case replicating:
		rollbackCtx, rollbackCancel := context.WithTimeout(jc.ctx, 10*time.Second)
		err := jc.hooks.rollbackImport(rollbackCtx, jc.job)
		rollbackCancel()
		if err != nil && !isPermanentRollbackErr(err) {
			jc.log.Warn(jc.ctx, "failed to broadcast rollback before GC of failed replicate import job, will retry", mlog.Err(err))
			return false
		}
		jc.log.Info(jc.ctx, "proceeding with GC of failed replicate import job after rollback attempt")
	}
	return true
}

// deleteImportV3JobGC removes the job's temporary OSS prefix, any remaining
// task catalog entries, and the job itself. Every step is idempotent, so a
// crash in between simply resumes on the next GC tick.
func deleteImportV3JobGC(jc *importV3JobContext) {
	prefix := path.Join(jc.meta.chunkManager.RootPath(), metautil.BuildImportV3JobPath(jc.job.GetJobID())) + "/"
	if err := jc.meta.chunkManager.RemoveWithPrefix(jc.ctx, prefix); err != nil {
		jc.log.Warn(jc.ctx, "remove import job temporary objects failed", mlog.Err(err))
		return
	}
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID())
	for _, task := range tasks {
		if err := jc.importMeta.RemoveTask(jc.ctx, task.GetTaskID()); err != nil {
			jc.log.Warn(jc.ctx, "remove task failed during GC delete", WrapTaskLog(task, mlog.Err(err))...)
			return
		}
	}
	if err := jc.importMeta.RemoveJob(jc.ctx, jc.job.GetJobID()); err != nil {
		jc.log.Warn(jc.ctx, "remove import job failed", mlog.Err(err))
		return
	}
	jc.log.Info(jc.ctx, "import job removed")
}
