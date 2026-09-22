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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestDropIndexTaskAdmissionAndCleanup(t *testing.T) {
	paramtable.Init()
	for _, finish := range []string{"complete", "fail", "cancel", "node removed", "stop"} {
		t.Run(finish, func(t *testing.T) {
			ctx := context.Background()
			nodeMgr := session.NewNodeManager()
			metadata := meta.NewMeta(nil, nil, nodeMgr)
			scheduler := NewScheduler(ctx, metadata, meta.NewDistributionManager(nodeMgr), nil, nil, nil, nodeMgr)
			t.Cleanup(scheduler.Stop)
			replica := utils.CreateTestReplica(200, 1, []int64{1})
			newDrop := func() *DropIndexTask {
				drop := NewDropIndexTask(ctx, utils.IndexChecker, 1, replica, 2,
					NewDropIndexAction(1, ActionTypeDropIndex, "channel", []int64{1000}))
				drop.SetPriority(TaskPriorityLow)
				return drop
			}
			newLoad := func(segmentID int64) *SegmentTask {
				load, err := NewSegmentTask(ctx, time.Minute, utils.IndexChecker, 1, replica, commonpb.LoadPriority_LOW,
					NewSegmentActionWithScope(1, ActionTypeUpdate, "channel", segmentID, querypb.DataScope_Historical, 0))
				require.NoError(t, err)
				load.SetPriority(TaskPriorityLow)
				return load
			}
			drop := newDrop()
			require.NoError(t, scheduler.Add(drop))
			require.Error(t, scheduler.Add(newDrop()), "duplicate drop must be rejected")
			require.Error(t, scheduler.Add(newLoad(2)), "load must not pass an in-flight drop")
			other := newLoad(3)
			require.NoError(t, scheduler.Add(other), "unrelated segments remain schedulable")
			scheduler.remove(other)
			switch finish {
			case "complete":
				drop.SetStatus(TaskStatusSucceeded)
				scheduler.remove(drop)
			case "fail":
				drop.Fail(merr.WrapErrServiceUnavailable("drop failed"))
				scheduler.remove(drop)
			case "cancel":
				drop.Cancel(context.Canceled)
				scheduler.remove(drop)
			case "node removed":
				scheduler.RemoveByNode(1)
			case "stop":
				scheduler.Stop()
			}
			require.Zero(t, scheduler.segmentTasks.Len())
			require.Zero(t, scheduler.tasks.Len())
			retry := newDrop()
			require.NoError(t, scheduler.Add(retry), "cleanup must allow a retry")
			scheduler.remove(retry)
			require.NoError(t, scheduler.Add(newLoad(2)), "cleanup must allow subsequent load")
		})
	}
}

func TestDropIndexTargetsSegmentNode(t *testing.T) {
	paramtable.Init()
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			ctx := context.Background()
			nodeMgr := session.NewNodeManager()
			replica := utils.CreateTestReplica(200, 1, []int64{1, 2})
			cluster := session.NewMockCluster(t)
			// There is deliberately no shard leader. Unloading a local index must
			// not depend on a delegator or any of its cached worker connections.
			metadata := meta.NewMeta(nil, nil, nodeMgr)
			dist := meta.NewDistributionManager(nodeMgr)
			executor := NewExecutor(2, metadata, dist, nil, nil, cluster, nodeMgr)
			status := merr.Success()
			if fail {
				status = merr.Status(merr.WrapErrServiceUnavailable("drop failed"))
			}
			cluster.EXPECT().DropIndex(mock.Anything, int64(2), mock.MatchedBy(func(req *querypb.DropIndexRequest) bool {
				return req.GetSegmentID() == 3 && !req.GetNeedTransfer() &&
					len(req.GetIndexIDs()) == 1 && req.GetIndexIDs()[0] == 1000
			})).Return(status, nil).Once()
			action := NewDropIndexAction(2, ActionTypeDropIndex, "channel", []int64{1000})
			drop := NewDropIndexTask(ctx, utils.IndexChecker, 1, replica, 3, action)
			require.True(t, executor.Execute(drop, 0))
			require.Eventually(t, action.rpcReturned.Load, 10*time.Second, time.Millisecond)
			if fail {
				require.ErrorIs(t, drop.Err(), merr.ErrServiceUnavailable)
			} else {
				require.NoError(t, drop.Err())
			}
		})
	}
}

func TestDropIndexRemovalPreservesReplacement(t *testing.T) {
	paramtable.Init()
	for _, oldIsDrop := range []bool{true, false} {
		t.Run(map[bool]string{true: "drop then load", false: "load then drop"}[oldIsDrop], func(t *testing.T) {
			ctx := context.Background()
			nodeMgr := session.NewNodeManager()
			scheduler := NewScheduler(ctx, meta.NewMeta(nil, nil, nodeMgr), meta.NewDistributionManager(nodeMgr), nil, nil, nil, nodeMgr)
			t.Cleanup(scheduler.Stop)
			replica := utils.CreateTestReplica(200, 1, []int64{1})
			newDrop := func() *DropIndexTask {
				return NewDropIndexTask(ctx, utils.IndexChecker, 1, replica, 2,
					NewDropIndexAction(1, ActionTypeDropIndex, "channel", []int64{1000}))
			}
			load, err := NewSegmentTask(ctx, time.Minute, utils.IndexChecker, 1, replica, commonpb.LoadPriority_LOW,
				NewSegmentActionWithScope(1, ActionTypeUpdate, "channel", 2, querypb.DataScope_Historical, 0))
			require.NoError(t, err)
			var old, replacement Task = newDrop(), load
			if !oldIsDrop {
				old, replacement = replacement, old
			}
			old.SetPriority(TaskPriorityLow)
			replacement.SetPriority(TaskPriorityNormal)
			require.NoError(t, scheduler.Add(old))
			require.NoError(t, scheduler.Add(replacement))
			// Dispatch can have collected the old task before Add preempts it.
			// Its later cleanup must not remove the new owner's conflict entry.
			scheduler.remove(old)
			require.Equal(t, 1, scheduler.segmentTasks.Len())
			owner, ok := scheduler.segmentTasks.Get(NewReplicaSegmentIndex(load))
			require.True(t, ok)
			require.Same(t, replacement, owner)
			require.Error(t, scheduler.Add(newDrop()), "the replacement still owns this segment")
			scheduler.remove(replacement)
			require.NoError(t, scheduler.Add(newDrop()))
		})
	}
}

func TestDropIndexRPCDeadlineAllowsRetry(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryCoordCfg.SegmentTaskTimeout.Key, "20"))
	t.Cleanup(func() { params.Reset(params.QueryCoordCfg.SegmentTaskTimeout.Key) })
	ctx := context.Background()
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	metadata := meta.NewMeta(nil, catalog, nodeMgr)
	dist := meta.NewDistributionManager(nodeMgr)
	cluster := session.NewMockCluster(t)
	scheduler := NewScheduler(ctx, metadata, dist, nil, nil, cluster, nodeMgr)
	t.Cleanup(scheduler.Stop)
	scheduler.AddExecutor(1)
	replica := utils.CreateTestReplica(200, 1, []int64{1})
	require.NoError(t, metadata.Put(ctx, replica))
	action := NewDropIndexAction(1, ActionTypeDropIndex, "channel", []int64{1000})
	drop := NewDropIndexTask(ctx, utils.IndexChecker, 1, replica, 2, action)
	drop.SetPriority(TaskPriorityLow)
	t.Cleanup(func() { drop.Cancel(context.Canceled) })
	cluster.EXPECT().DropIndex(mock.Anything, int64(1), mock.Anything).
		RunAndReturn(func(rpcCtx context.Context, _ int64, _ *querypb.DropIndexRequest) (*commonpb.Status, error) {
			// Model a live node whose DropIndex response never arrives. gRPC
			// unblocks on the caller's deadline even if native work continues.
			<-rpcCtx.Done()
			return nil, rpcCtx.Err()
		}).Once()
	require.NoError(t, scheduler.Add(drop))
	scheduler.Dispatch(1)
	require.Eventually(t, action.rpcReturned.Load, time.Second, time.Millisecond)
	require.ErrorIs(t, drop.Err(), context.DeadlineExceeded)
	// Dispatch performs the normal terminal-task cleanup, without a manual
	// remove call masking a stuck task.
	scheduler.Dispatch(1)
	retry := NewDropIndexTask(ctx, utils.IndexChecker, 1, replica, 2,
		NewDropIndexAction(1, ActionTypeDropIndex, "channel", []int64{1000}))
	retry.SetPriority(TaskPriorityLow)
	require.NoError(t, scheduler.Add(retry))
}
