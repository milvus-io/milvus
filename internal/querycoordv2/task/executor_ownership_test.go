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
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestExecutorRetainsPhysicalOwnershipUntilCallbackReturns(t *testing.T) {
	for _, kind := range []string{"segment", "channel"} {
		t.Run(kind, func(t *testing.T) {
			replica := ownershipReplica(1)
			scheduler := newChannelMembershipScheduler(t, replica)
			require.NoError(t, scheduler.meta.Put(scheduler.ctx, ownershipReplica(2)))
			ex := NewExecutor(1, scheduler.meta, scheduler.distMgr, nil, nil, nil, scheduler.nodeMgr)
			scheduler.executors.Insert(1, ex)
			var old, newTask Task
			if kind == "segment" {
				old = ownershipSegmentTask(t, meta.NilReplica, NewSegmentActionWithScope(1, ActionTypeReduce, membershipChannel, 10, querypb.DataScope_Historical, 100))
				newTask = ownershipSegmentTask(t, ownershipReplica(2), NewSegmentAction(1, ActionTypeGrow, membershipChannel, 10))
			} else {
				old = ownershipChannelTask(t, meta.NilReplica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
				newTask = ownershipChannelTask(t, ownershipReplica(2), NewChannelAction(1, ActionTypeGrow, membershipChannel))
				scheduler.distMgr.ChannelDistManager.Update(1, membershipChannelOn(1, 1))
			}
			require.NoError(t, scheduler.Add(old))
			started, resume, oldDone, newDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(resume) }) }
			run := func(ex *Executor, task Task, step int) {
				if task == old {
					defer close(oldDone)
					defer ex.removeTask(task, step)
					close(started)
					<-resume
				} else {
					defer close(newDone)
					defer ex.removeTask(task, step)
				}
			}
			var unpatch func()
			if kind == "segment" {
				patch := mockey.Mock((*Executor).executeSegmentAction).To(func(ex *Executor, task *SegmentTask, step int) { run(ex, task, step) }).Build()
				unpatch = func() { patch.UnPatch() }
			} else {
				patch := mockey.Mock((*Executor).executeDmChannelAction).To(func(ex *Executor, task *ChannelTask, step int) { run(ex, task, step) }).Build()
				unpatch = func() { patch.UnPatch() }
			}
			defer unpatch()
			require.True(t, ex.Execute(old, 0))
			defer func() { release(); waitRegistrationBarrier(t, oldDone) }()
			waitRegistrationBarrier(t, started)
			if kind == "channel" {
				// ChannelAction can finish from distribution before its callback returns.
				scheduler.distMgr.ChannelDistManager.Update(1)
				require.True(t, old.Actions()[0].IsFinished(scheduler.distMgr))
				require.False(t, scheduler.preProcess(old))
				require.Equal(t, TaskStatusSucceeded, old.Status())
			} else {
				old.Cancel(merr.WrapErrServiceUnavailable("retire old cleanup"))
			}
			retireOwnershipTask(scheduler, old)
			require.Zero(t, scheduler.tasks.Len())
			require.NoError(t, scheduler.Add(newTask), "scheduler retirement releases queued task ownership")
			require.False(t, ex.Execute(newTask, 0), "cancellation or an early distribution report must not free an in-flight callback")
			require.Equal(t, 1, ex.executingActions.Len(), "failed All admission must roll back its acquired Streaming slot while retaining Historical")
			release()
			waitRegistrationBarrier(t, oldDone)
			require.True(t, ex.Execute(newTask, 0), "new generation can execute after the original callback returns")
			waitRegistrationBarrier(t, newDone)
			require.Zero(t, ex.executingActions.Len())
			require.Empty(t, ex.executingTasks.Collect())
			require.Zero(t, ex.channelTaskNum.Load())
			require.Zero(t, ex.nonChannelTaskNum.Load())
		})
	}
}

func TestExecutorAdmissionRollbackPreservesEarlierCallback(t *testing.T) {
	paramtable.Init()
	ex := newTestExecutor(1)
	task := ownershipSegmentTask(t, ownershipReplica(1), NewSegmentAction(1, ActionTypeGrow, membershipChannel, 10))
	keys := actionResourceKeys(task.Actions()[0])
	require.Len(t, keys, 2)
	// Model an earlier callback retaining Historical while another admission
	// temporarily acquires Streaming. The matching Task pointer is not ownership
	// proof for the current attempt.
	ex.executingActions.Insert(keys[1], task)
	require.False(t, ex.reserveActionResources(task, 0))
	owner, ok := ex.executingActions.Get(keys[1])
	require.True(t, ok)
	require.Same(t, task, owner)
	require.False(t, ex.executingActions.Contain(keys[0]))
	ex.releaseActionResources(task, 0)
}

func TestExecutorCapacityRejectionReleasesPhysicalSlots(t *testing.T) {
	paramtable.Init()
	ex := newTestExecutor(1)
	ex.channelTaskNum.Store(ex.GetChannelTaskCap())
	task := ownershipChannelTask(t, ownershipReplica(1), NewChannelAction(1, ActionTypeGrow, membershipChannel))
	require.False(t, ex.Execute(task, 0))
	require.Zero(t, ex.executingActions.Len())
	require.Empty(t, ex.executingTasks.Collect())
}
