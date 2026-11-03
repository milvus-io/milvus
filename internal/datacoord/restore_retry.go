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
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func isForwardRetryRestore(job CopySegmentJob) bool {
	return job != nil && job.(*copySegmentJob).GetCreatesCollection()
}

// The worker publishes Failed only after all copy goroutines have exited.
// Remove that attempt before rescheduling, so its late cleanup cannot delete
// files written by the next attempt. Never redispatch an admitted result: its
// files may already be referenced by partially installed segment metadata.
func (t *copySegmentTask) retryFailedCopy(cluster session.Cluster, reason string) {
	if t.GetCleanupRequired() {
		t.markTaskAndJobFailed("copy worker failed after result installation was admitted; manual intervention required: " + reason)
		return
	}
	if err := cluster.DropCopySegment(t.GetNodeId(), t.GetTaskId()); err != nil {
		mlog.Warn(t.ctx, "retrying failed copy worker removal", mlog.FieldTaskID(t.GetTaskId()), mlog.Err(err))
		return
	}
	if err := t.copyMeta.UpdateTask(t.ctx, t.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID), UpdateCopyTaskReason(reason)); err != nil {
		mlog.Warn(t.ctx, "retrying copy task rescheduling", mlog.FieldTaskID(t.GetTaskId()), mlog.Err(err))
	}
}
