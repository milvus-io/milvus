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

package checkers

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func epochTestTask(t *testing.T, segmentID int64) *task.SegmentTask {
	t.Helper()
	work, err := task.NewSegmentTask(context.Background(), time.Minute, utils.BalanceChecker, 1, meta.NilReplica, commonpb.LoadPriority_LOW, task.NewSegmentAction(2, task.ActionTypeGrow, "ch", segmentID))
	require.NoError(t, err)
	return work
}

func TestBalanceEpochRejectedAdmissionReleasesProjection(t *testing.T) {
	paramtable.Init()
	b := createTestBalanceChecker()
	ledger := assign.NewPlanningLedgerState(map[int64]assign.PlanningNode{2: {Ready: true, CapacityBytes: 1000}}, nil, nil, nil, 1)
	ctx := assign.WithPlanningLedger(context.Background(), ledger)
	plan := assign.SegmentAssignPlan{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 7, CollectionID: 1, NumOfRows: 99}}, To: 2, From: 1}
	require.True(t, ledger.ReserveSegment(plan))
	work, err := task.NewSegmentTask(ctx, time.Minute, utils.BalanceChecker, 1, meta.NilReplica, commonpb.LoadPriority_LOW, task.NewSegmentAction(2, task.ActionTypeGrow, "ch", 7))
	require.NoError(t, err)
	patch := mockey.Mock(mockey.GetMethod(b.scheduler, "Add")).Return(context.DeadlineExceeded).Build()
	defer patch.UnPatch()
	segments, channels := b.submitTasks([]task.Task{work}, nil)
	require.Zero(t, segments)
	require.Zero(t, channels)
	require.Empty(t, b.epochs)
	require.True(t, ledger.ReserveSegment(plan), "a rejected plan must not retain its target slot or rows")
}

func TestCollectionPriorityUsesActualHeapOrder(t *testing.T) {
	queue := assign.NewPriorityQueuePtr()
	queue.Push(newCollectionBalanceItem(1, 10, "byrowcount"))
	queue.Push(newCollectionBalanceItem(2, 100, "byrowcount"))
	require.EqualValues(t, 2, queue.Pop().(*collectionBalanceItem).collectionID)
}

func TestBalanceEpochWaitsForBatchWithoutCancelingSiblings(t *testing.T) {
	paramtable.Init()
	b := &BalanceChecker{epochSequence: 1}
	a, c := epochTestTask(t, 1), epochTestTask(t, 2)
	b.trackEpochWork(a)
	b.trackEpochWork(c)
	require.False(t, b.epochBlocks(""), "other collections can join the same planning batch")
	b.epochSequence++
	require.True(t, b.epochBlocks(""))
	a.Fail(context.DeadlineExceeded)
	b.advanceEpochs(time.Now())
	require.True(t, b.epochBlocks(""))
	require.NotEqual(t, task.TaskStatusCanceled, c.Status())
	c.SetStatus(task.TaskStatusSucceeded)
	b.advanceEpochs(time.Now())
	require.False(t, b.epochBlocks(""))
}

func TestBalanceEpochBoundsUndispatchedTaskAndDoesNotBlockOtherRG(t *testing.T) {
	paramtable.Init()
	b := &BalanceChecker{epochSequence: 1}
	a := epochTestTask(t, 1)
	b.trackEpochWork(a)
	b.epochSequence++
	require.False(t, b.epochBlocks("another-rg"))
	b.advanceEpochs(time.Now().Add(time.Hour))
	require.Equal(t, task.TaskStatusCanceled, a.Status())
	require.ErrorIs(t, a.Err(), context.DeadlineExceeded)
	require.False(t, b.epochBlocks(""))
}
