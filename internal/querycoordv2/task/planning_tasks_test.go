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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPendingSegmentLoadsTracksQueuedGrowAndNotFinishedMove(t *testing.T) {
	paramtable.Init()
	scheduler := NewScheduler(context.Background(), nil, nil, nil, nil, nil, nil)
	work, err := NewSegmentTask(context.Background(), time.Minute, utils.SegmentChecker, 1, meta.NilReplica, commonpb.LoadPriority_HIGH,
		NewSegmentActionWithScope(2, ActionTypeGrow, "ch", 7, querypb.DataScope_Historical, 99), NewSegmentActionWithScope(1, ActionTypeReduce, "ch", 7, querypb.DataScope_Historical, 99))
	require.NoError(t, err)
	scheduler.segmentTasks.Insert(NewReplicaSegmentIndex(work), work)
	loads := scheduler.PendingSegmentLoads()
	require.Len(t, loads[2], 1)
	require.EqualValues(t, 99, loads[2][0].GetNumRows())
	work.StepUp()
	require.Empty(t, scheduler.PendingSegmentLoads(), "Reduce must not be counted as a fresh outstanding Grow")
}

func TestRecoveryQueueWaitIsBoundedBeforeExecutionStarts(t *testing.T) {
	paramtable.Init()
	scheduler := NewScheduler(context.Background(), nil, nil, nil, nil, nil, nil)
	work, err := NewSegmentTask(context.Background(), time.Minute, utils.SegmentChecker, 1, meta.NilReplica, commonpb.LoadPriority_HIGH, NewSegmentAction(2, ActionTypeGrow, "ch", 7))
	require.NoError(t, err)
	work.SetID(11)
	now := time.Now()
	scheduler.admittedAt.Insert(work.ID(), now)
	require.NoError(t, scheduler.queueWaitError(work, now))
	require.ErrorIs(t, scheduler.queueWaitError(work, now.Add(time.Hour)), context.DeadlineExceeded)
	work.ActivateDeadline(0)
	require.NoError(t, scheduler.queueWaitError(work, now.Add(time.Hour)), "execution has its own deadline")
	work.Cancel(nil)
}

func TestExecutorDefersGrowUntilTargetHasFreshReport(t *testing.T) {
	paramtable.Init()
	manager := session.NewNodeManager()
	node := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2})
	manager.Add(node)
	node.RequireLoadReport()
	executor := NewExecutor(2, nil, nil, nil, nil, nil, manager)
	work, err := NewSegmentTask(context.Background(), time.Minute, utils.SegmentChecker, 1, meta.NilReplica, commonpb.LoadPriority_HIGH, NewSegmentAction(2, ActionTypeGrow, "ch", 7))
	require.NoError(t, err)
	require.False(t, executor.Execute(work, 0))
	_, deadlineArmed := work.Context().Deadline()
	require.False(t, deadlineArmed, "deferring a task must not start its execution deadline")
	node.UpdateLoadReport(nil, true, true, time.Now())
	require.False(t, executor.Execute(work, 0), "failed resource sampling still defers")
	node.UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, time.Now())
	require.True(t, node.LoadAdmissionReady())
	manager.Remove(node.ID())
	require.False(t, executor.Execute(work, 0), "a disappeared target must not receive work through its old executor")
	_, deadlineArmed = work.Context().Deadline()
	require.False(t, deadlineArmed)
	work.Cancel(nil)
}
