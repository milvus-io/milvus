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

package tasks

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// newGroupMember builds a standalone search task compatible with every other
// task built by this helper, so that Merge succeeds.
func newGroupMember(ctx context.Context, nq int64) *SearchTask {
	return &SearchTask{
		ctx:       ctx,
		nq:        nq,
		topk:      10,
		groupSize: 1,
		req: &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				DbID:               1,
				CollectionID:       1000,
				MvccTimestamp:      100,
				PartitionIDs:       []int64{1},
				SerializedExprPlan: []byte("plan"),
			},
			DmlChannels: []string{"channel1"},
			SegmentIDs:  []int64{1, 2},
		},
		originTopks: []int64{10},
		originNqs:   []int64{nq},
		notifier:    make(chan error, 1),
	}
}

func waitResult(t *testing.T, task *SearchTask) error {
	t.Helper()
	select {
	case err := <-task.notifier:
		return err
	case <-time.After(time.Second):
		t.Fatalf("task was never notified")
		return nil
	}
}

func assertNotNotified(t *testing.T, task *SearchTask) {
	t.Helper()
	select {
	case err := <-task.notifier:
		t.Fatalf("task was notified unexpectedly with %v", err)
	default:
	}
}

func TestMergeRefusesCancelledTasks(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	owner := newGroupMember(context.Background(), 1)
	assert.False(t, owner.Merge(newGroupMember(cancelledCtx, 1)), "a cancelled task must not join a group")
	assert.Empty(t, owner.others)

	cancelledOwner := newGroupMember(cancelledCtx, 1)
	assert.False(t, cancelledOwner.Merge(newGroupMember(context.Background(), 1)), "a cancelled owner must not accept members")

	assert.True(t, owner.Merge(newGroupMember(context.Background(), 2)))
	assert.Equal(t, int64(3), owner.nq)
}

func TestPruneCancelledKeepsGroupIntact(t *testing.T) {
	owner := newGroupMember(context.Background(), 1)
	m1 := newGroupMember(context.Background(), 2)
	require.True(t, owner.Merge(m1))

	pruned := owner.PruneCancelled()
	assert.Same(t, owner, pruned)
	assert.Equal(t, []*SearchTask{m1}, owner.others)
	assert.Equal(t, int64(3), owner.nq)
	assertNotNotified(t, owner)
	assertNotNotified(t, m1)
}

func TestPruneCancelledDropsCancelledMember(t *testing.T) {
	m1Ctx, cancelM1 := context.WithCancel(context.Background())
	owner := newGroupMember(context.Background(), 1)
	m1 := newGroupMember(m1Ctx, 2)
	m2 := newGroupMember(context.Background(), 4)
	require.True(t, owner.Merge(m1))
	require.True(t, owner.Merge(m2))
	require.Equal(t, int64(7), owner.nq)

	cancelM1()
	pruned := owner.PruneCancelled()

	require.NotNil(t, pruned)
	assert.Same(t, owner, pruned, "the owner survives and keeps owning the group")
	assert.Equal(t, []*SearchTask{m2}, owner.others)
	assert.Equal(t, int64(5), owner.nq)
	assert.Equal(t, []int64{1, 4}, owner.originNqs)
	assert.Equal(t, int64(2), owner.groupSize)
	// the cancelled member is told its own error, right away
	assert.ErrorIs(t, waitResult(t, m1), context.Canceled)
	assertNotNotified(t, owner)
	assertNotNotified(t, m2)
}

func TestPruneCancelledRegroupsWhenOwnerIsCancelled(t *testing.T) {
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	owner := newGroupMember(ownerCtx, 1)
	m1 := newGroupMember(context.Background(), 2)
	m2 := newGroupMember(context.Background(), 4)
	require.True(t, owner.Merge(m1))
	require.True(t, owner.Merge(m2))

	cancelOwner()
	pruned := owner.PruneCancelled()

	require.NotNil(t, pruned)
	newOwner, ok := pruned.(*SearchTask)
	require.True(t, ok)
	assert.Same(t, m1, newOwner, "the first surviving member becomes the owner")
	assert.False(t, newOwner.merged)
	assert.Equal(t, []*SearchTask{m2}, newOwner.others)
	assert.True(t, m2.merged)
	assert.Equal(t, int64(6), newOwner.nq)
	assert.Equal(t, []int64{2, 4}, newOwner.originNqs)
	assert.Equal(t, []int64{10, 10}, newOwner.originTopks)
	assert.Equal(t, int64(2), newOwner.groupSize)
	// the cancelled owner is out of the picture: notified once, with its own error
	assert.ErrorIs(t, waitResult(t, owner), context.Canceled)
	assertNotNotified(t, m1)
	assertNotNotified(t, m2)
	// the old owner no longer references the survivors
	assert.Empty(t, owner.others)
}

func TestPruneCancelledReturnsNilWhenEveryoneIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	owner := newGroupMember(ctx, 1)
	m1 := newGroupMember(ctx, 2)
	require.True(t, owner.Merge(m1))

	cancel()
	assert.Nil(t, owner.PruneCancelled())
	assert.ErrorIs(t, waitResult(t, owner), context.Canceled)
	assert.ErrorIs(t, waitResult(t, m1), context.Canceled)
}

func TestPruneCancelledStandaloneTask(t *testing.T) {
	live := newGroupMember(context.Background(), 1)
	assert.Same(t, live, live.PruneCancelled())
	assertNotNotified(t, live)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dead := newGroupMember(ctx, 1)
	assert.Nil(t, dead.PruneCancelled())
	assert.ErrorIs(t, waitResult(t, dead), context.Canceled)
}

func TestDoneGivesCancelledMembersTheirOwnError(t *testing.T) {
	m1Ctx, cancelM1 := context.WithCancel(context.Background())
	owner := newGroupMember(context.Background(), 1)
	m1 := newGroupMember(m1Ctx, 2)
	m2 := newGroupMember(context.Background(), 4)
	require.True(t, owner.Merge(m1))
	require.True(t, owner.Merge(m2))

	// m1 is cancelled while the group is executing; the group still succeeds
	cancelM1()
	owner.Done(nil)
	assert.NoError(t, waitResult(t, owner))
	assert.ErrorIs(t, waitResult(t, m1), context.Canceled)
	assert.NoError(t, waitResult(t, m2))
}

func TestDonePropagatesGroupErrorToLiveMembers(t *testing.T) {
	owner := newGroupMember(context.Background(), 1)
	m1 := newGroupMember(context.Background(), 2)
	require.True(t, owner.Merge(m1))

	groupErr := errors.New("segcore failed")
	owner.Done(groupErr)
	assert.ErrorIs(t, waitResult(t, owner), groupErr)
	assert.ErrorIs(t, waitResult(t, m1), groupErr)
}

func TestDoneStandaloneTaskKeepsGroupOutcome(t *testing.T) {
	// A task that is not part of a group keeps the historical behaviour:
	// whatever Execute returned is delivered, even if the ctx ended meanwhile.
	ctx, cancel := context.WithCancel(context.Background())
	task := newGroupMember(ctx, 1)
	cancel()
	task.Done(nil)
	assert.NoError(t, waitResult(t, task))
}

func TestGroupContextCancelsOnlyWhenEveryMemberIsCancelled(t *testing.T) {
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	m1Ctx, cancelM1 := context.WithCancel(context.Background())
	owner := newGroupMember(ownerCtx, 1)
	m1 := newGroupMember(m1Ctx, 2)
	require.True(t, owner.Merge(m1))

	restore := owner.useGroupContext()
	groupCtx := owner.ctx
	assert.NotSame(t, ownerCtx, groupCtx)
	assert.NoError(t, groupCtx.Err())

	cancelOwner()
	time.Sleep(10 * time.Millisecond)
	assert.NoError(t, groupCtx.Err(), "one cancelled member must not stop the group")

	cancelM1()
	select {
	case <-groupCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("the group context must end once every member is cancelled")
	}

	restore()
	assert.Same(t, ownerCtx, owner.ctx, "the owner's own context is restored for Done")
}

func TestGroupContextRestoreReleasesListeners(t *testing.T) {
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	defer cancelOwner()
	owner := newGroupMember(ownerCtx, 1)
	m1 := newGroupMember(context.Background(), 2)
	require.True(t, owner.Merge(m1))

	restore := owner.useGroupContext()
	groupCtx := owner.ctx
	restore()
	// after restore the group context is released and the owner context is back
	assert.ErrorIs(t, groupCtx.Err(), context.Canceled)
	assert.Same(t, ownerCtx, owner.ctx)
	assert.NoError(t, owner.ctx.Err())
}

// TestSearchTaskIsPrunableThroughTheSchedulerInterface closes the gap between
// "SearchTask prunes correctly" and "the scheduler prunes SearchTasks": the
// scheduler only ever holds a scheduler.Task and reaches PruneCancelled
// through a run-time type assertion, so a group must still be pruned when it
// is held by that interface. Without this a group could silently take the
// single-task path and one member's cancellation would end the others.
func TestSearchTaskIsPrunableThroughTheSchedulerInterface(t *testing.T) {
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	owner := newGroupMember(ownerCtx, 1)
	survivor := newGroupMember(context.Background(), 2)
	require.True(t, owner.Merge(survivor))

	var asTask scheduler.Task = owner
	prunable, ok := asTask.(scheduler.PrunableTask)
	require.True(t, ok, "the scheduler must recognise a search group as prunable")

	cancelOwner()
	pruned := prunable.PruneCancelled()
	require.NotNil(t, pruned)
	assert.Same(t, scheduler.Task(survivor), pruned, "the surviving member is what the scheduler executes")
	assert.ErrorIs(t, waitResult(t, owner), context.Canceled)
	assertNotNotified(t, survivor)
}
