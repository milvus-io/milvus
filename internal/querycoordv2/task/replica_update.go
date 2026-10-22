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

package task

import (
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// applyReplicaUpdate publishes a complete replica update and retires invalid
// plans before another Add or executor admission can observe the new assignment.
// ReplicaManager already holds its own collection write lock; scheduler paths
// only read replica snapshots and must never acquire that write lock in reverse.
func (scheduler *taskScheduler) applyReplicaUpdate(collectionID int64, publish func()) {
	scheduler.lifecycleMu.RLock()
	defer scheduler.lifecycleMu.RUnlock()
	scheduler.collKeyLock.Lock(collectionID)
	defer scheduler.collKeyLock.Unlock(collectionID)

	publish()
	if scheduler.closed {
		return
	}
	scheduler.tasks.Range(func(_ int64, entry scheduledTask) bool {
		task := entry.task
		if task.CollectionID() != collectionID {
			return true
		}
		if err := scheduler.checkReplica(task); err != nil {
			mlog.Info(task.Context(), "cancel task after replica assignment changed",
				mlog.Int64("taskID", task.ID()), mlog.Int64("collectionID", collectionID),
				mlog.Int64("replicaID", task.ReplicaID()), mlog.Err(err))
			task.Cancel(err)
			// Do not perform target-refresh RPCs while ReplicaManager is publishing.
			// Executor ownership is independent and stays until its callback returns.
			scheduler.retire(task, false)
		}
		return true
	})
}

// checkReplica validates the plan against current ownership, not the replica
// snapshot captured when the checker produced it. Call under the collection
// gate on admission and before advancing a task, including waiting-dist tasks.
func (scheduler *taskScheduler) checkReplica(task Task) error {
	ctx := task.Context()
	if task.ReplicaID() == meta.NilReplica.GetID() {
		// ReleaseCollection removes load metadata first. Its cleanup must remain
		// valid even while old replica metadata is still present.
		if !scheduler.meta.Exist(ctx, task.CollectionID()) {
			return nil
		}
		for _, action := range task.Actions() {
			if action.Type() != ActionTypeReduce {
				continue
			}
			if segment, ok := action.(*SegmentAction); ok && segment.GetScope() == querypb.DataScope_Streaming {
				continue
			}
			if scheduler.meta.GetByCollectionAndNode(ctx, task.CollectionID(), action.Node()) != nil {
				return merr.WrapErrNodeStateUnexpected(action.Node(), "cleanup task no longer matches replica assignment")
			}
		}
		return nil
	}

	replica := scheduler.meta.Get(ctx, task.ReplicaID())
	if replica == nil || replica.GetCollectionID() != task.CollectionID() {
		return merr.WrapErrReplicaNotFound(task.ReplicaID())
	}
	for _, action := range task.Actions() {
		// LeaderAction updates the delegator; its worker may legitimately be RO.
		node := actionExecutorNode(action)
		if !replica.Contains(node) {
			return merr.WrapErrNodeStateUnexpected(node, "task node no longer belongs to replica")
		}
		if _, leader := action.(*LeaderAction); leader && action.Type() == ActionTypeGrow && !replica.Contains(action.Node()) {
			return merr.WrapErrNodeStateUnexpected(action.Node(), "leader route worker no longer belongs to replica")
		}
		if action.Type() == ActionTypeGrow && (replica.ContainRONode(node) || replica.ContainROSQNode(node)) {
			return merr.WrapErrNodeStateUnexpected(node, "node becomes ro node")
		}
	}
	return nil
}
