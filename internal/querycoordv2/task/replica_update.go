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
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// checkReplica validates the plan against current ownership, not the replica
// snapshot captured when the checker produced it, and returns that current
// replica (nil for a NilReplica cleanup task).
//
// ReplicaManager does not notify the scheduler of an update. Every gate a task
// passes calls this instead: admission, promotion, and each Dispatch before the
// task advances, including waiting-dist tasks. A task made stale by an update
// is therefore retired by the next Dispatch of one of its nodes, and until then
// cannot commit another action.
func (scheduler *taskScheduler) checkReplica(task Task) (*meta.Replica, error) {
	ctx := task.Context()
	if task.ReplicaID() == meta.NilReplica.GetID() {
		// ReleaseCollection removes load metadata first. Its cleanup must remain
		// valid even while old replica metadata is still present.
		if !scheduler.meta.Exist(ctx, task.CollectionID()) {
			return nil, nil
		}
		for _, action := range task.Actions() {
			if action.Type() != ActionTypeReduce {
				continue
			}
			if segment, ok := action.(*SegmentAction); ok && segment.GetScope() == querypb.DataScope_Streaming {
				continue
			}
			if scheduler.meta.GetByCollectionAndNode(ctx, task.CollectionID(), action.Node()) != nil {
				return nil, merr.WrapErrNodeStateUnexpected(action.Node(), "cleanup task no longer matches replica assignment")
			}
		}
		return nil, nil
	}

	// ReleaseCollection removes load metadata before replicas; a regular task
	// must not keep its reservations meanwhile.
	if !scheduler.meta.Exist(ctx, task.CollectionID()) {
		return nil, merr.WrapErrCollectionNotLoaded(task.CollectionID())
	}
	replica := scheduler.meta.Get(ctx, task.ReplicaID())
	if replica == nil || replica.GetCollectionID() != task.CollectionID() {
		return nil, merr.WrapErrReplicaNotFound(task.ReplicaID())
	}
	for _, action := range task.Actions() {
		// LeaderAction updates the delegator; its worker may legitimately be RO.
		node := actionExecutorNode(action)
		if !replica.Contains(node) {
			// ReplicaObserver drops an RO node as soon as its distribution is
			// empty, which can precede the Dispatch that reports the release
			// finished. Nothing is left on the node for this action to touch.
			if scheduler.reduceApplied(action) {
				continue
			}
			return nil, merr.WrapErrNodeStateUnexpected(node, "task node no longer belongs to replica")
		}
		if _, leader := action.(*LeaderAction); leader && action.Type() == ActionTypeGrow && !replica.Contains(action.Node()) {
			return nil, merr.WrapErrNodeStateUnexpected(action.Node(), "leader route worker no longer belongs to replica")
		}
		if action.Type() == ActionTypeGrow && (replica.ContainRONode(node) || replica.ContainROSQNode(node)) {
			return nil, merr.WrapErrNodeStateUnexpected(node, "node becomes ro node")
		}
	}
	return replica, nil
}

// reduceApplied reports whether the copy a reduce action removes is already
// absent from the distribution, whether or not its RPC has returned.
func (scheduler *taskScheduler) reduceApplied(action Action) bool {
	if action.Type() != ActionTypeReduce {
		return false
	}
	switch action := action.(type) {
	case *SegmentAction:
		return action.isDistMatched(scheduler.distMgr)
	case *ChannelAction:
		return action.IsFinished(scheduler.distMgr)
	}
	return false
}
