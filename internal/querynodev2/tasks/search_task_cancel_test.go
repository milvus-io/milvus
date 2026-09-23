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

func TestMergeRefusesCanceledTasks(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	owner := newGroupMember(context.Background(), 1)
	assert.False(t, owner.Merge(newGroupMember(canceledCtx, 1)), "a canceled task must not join a group")
	assert.Empty(t, owner.others)

	canceledOwner := newGroupMember(canceledCtx, 1)
	assert.False(t, canceledOwner.Merge(newGroupMember(context.Background(), 1)), "a canceled owner must not accept members")

	assert.True(t, owner.Merge(newGroupMember(context.Background(), 2)))
	assert.Equal(t, int64(3), owner.nq)
}

// survivorOf prunes t and returns only the task left to execute.
func survivorOf(t scheduler.PrunableTask) scheduler.Task {
	survivor, _, _ := t.PruneCanceled()
	return survivor
}

func TestPruneCanceledKeepsGroupIntact(t *testing.T) {
	owner := newGroupMember(context.Background(), 1)
	m1 := newGroupMember(context.Background(), 2)
	require.True(t, owner.Merge(m1))

	pruned, dropped, cause := owner.PruneCanceled()
	assert.Same(t, owner, pruned)
	assert.Zero(t, dropped)
	assert.NoError(t, cause)
	assert.Equal(t, []*SearchTask{m1}, owner.others)
	assert.Equal(t, int64(3), owner.nq)
	assertNotNotified(t, owner)
	assertNotNotified(t, m1)
}

func TestPruneCanceledDropsCanceledMember(t *testing.T) {
	m1Ctx, cancelM1 := context.WithCancel(context.Background())
	owner := newGroupMember(context.Background(), 1)
	m1 := newGroupMember(m1Ctx, 2)
	m2 := newGroupMember(context.Background(), 4)
	require.True(t, owner.Merge(m1))
	require.True(t, owner.Merge(m2))
	require.Equal(t, int64(7), owner.nq)

	cancelM1()
	pruned, dropped, cause := owner.PruneCanceled()

	require.NotNil(t, pruned)
	assert.Equal(t, 1, dropped)
	assert.ErrorIs(t, cause, context.Canceled, "the caller is told why, so it can log it")
	assert.Same(t, owner, pruned, "the owner survives and keeps owning the group")
	assert.Equal(t, []*SearchTask{m2}, owner.others)
	assert.Equal(t, int64(5), owner.nq)
	assert.Equal(t, []int64{1, 4}, owner.originNqs)
	assert.Equal(t, int64(2), owner.groupSize)
	// the canceled member is told its own error, right away
	assert.ErrorIs(t, waitResult(t, m1), context.Canceled)
	assertNotNotified(t, owner)
	assertNotNotified(t, m2)
}

func TestPruneCanceledRegroupsWhenOwnerIsCanceled(t *testing.T) {
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	owner := newGroupMember(ownerCtx, 1)
	m1 := newGroupMember(context.Background(), 2)
	m2 := newGroupMember(context.Background(), 4)
	require.True(t, owner.Merge(m1))
	require.True(t, owner.Merge(m2))

	cancelOwner()
	pruned, dropped, _ := owner.PruneCanceled()

	require.NotNil(t, pruned)
	assert.Equal(t, 1, dropped)
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
	// the canceled owner is out of the picture: notified once, with its own error
	assert.ErrorIs(t, waitResult(t, owner), context.Canceled)
	assertNotNotified(t, m1)
	assertNotNotified(t, m2)
	// the old owner no longer references the survivors
	assert.Empty(t, owner.others)
}

func TestPruneCanceledReturnsNilWhenEveryoneIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	owner := newGroupMember(ctx, 1)
	m1 := newGroupMember(ctx, 2)
	require.True(t, owner.Merge(m1))

	cancel()
	survivor, dropped, cause := owner.PruneCanceled()
	assert.Nil(t, survivor)
	assert.Equal(t, 2, dropped)
	assert.ErrorIs(t, cause, context.Canceled)
	assert.ErrorIs(t, waitResult(t, owner), context.Canceled)
	assert.ErrorIs(t, waitResult(t, m1), context.Canceled)
}

func TestPruneCanceledStandaloneTask(t *testing.T) {
	live := newGroupMember(context.Background(), 1)
	assert.Same(t, live, survivorOf(live))
	assertNotNotified(t, live)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dead := newGroupMember(ctx, 1)
	assert.Nil(t, survivorOf(dead))
	assert.ErrorIs(t, waitResult(t, dead), context.Canceled)
}

func TestDoneGivesCanceledMembersTheirOwnError(t *testing.T) {
	m1Ctx, cancelM1 := context.WithCancel(context.Background())
	owner := newGroupMember(context.Background(), 1)
	m1 := newGroupMember(m1Ctx, 2)
	m2 := newGroupMember(context.Background(), 4)
	require.True(t, owner.Merge(m1))
	require.True(t, owner.Merge(m2))

	// m1 is canceled while the group is executing; the group still succeeds
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
	// A task that is not part of a group keeps the historical behavior:
	// whatever Execute returned is delivered, even if the ctx ended meanwhile.
	ctx, cancel := context.WithCancel(context.Background())
	task := newGroupMember(ctx, 1)
	cancel()
	task.Done(nil)
	assert.NoError(t, waitResult(t, task))
}

func TestGroupContextCancelsOnlyWhenEveryMemberIsCanceled(t *testing.T) {
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
	assert.NoError(t, groupCtx.Err(), "one canceled member must not stop the group")

	cancelM1()
	select {
	case <-groupCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("the group context must end once every member is canceled")
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
// scheduler only ever holds a scheduler.Task and reaches PruneCanceled
// through a run-time type assertion, so a group must still be pruned when it
// is held by that interface. Without this a group could silently take the
// single-task path and one member's cancellation would end the others.
// A SearchTask built directly rather than through NewSearchTask carries no
// context. The merge rules must still apply to it: the cancellation check
// added in front of them may not turn a task with nothing to cancel into a
// panic, which is how the pre-existing merge tests build their tasks.
func TestMergeWithoutContext(t *testing.T) {
	newTask := func(nq int64) *SearchTask {
		return &SearchTask{
			nq:   nq,
			topk: 100,
			req: &querypb.SearchRequest{
				Req: &internalpb.SearchRequest{
					DbID:               1,
					CollectionID:       1000,
					MvccTimestamp:      100,
					PartitionIDs:       []int64{1},
					SerializedExprPlan: []byte("plan"),
				},
				DmlChannels: []string{"channel1"},
				SegmentIDs:  []int64{1},
			},
			originTopks: []int64{100},
			originNqs:   []int64{nq},
			groupSize:   1,
		}
	}

	owner, other := newTask(10), newTask(5)
	require.True(t, owner.Merge(other), "a task with no context is not a canceled task")
	assert.Equal(t, int64(15), owner.nq)
	assert.Equal(t, int64(2), owner.groupSize)

	// The rest of the group handling has to survive the absent context too.
	assert.Same(t, owner, survivorOf(owner))
	assert.NoError(t, owner.resultErr(nil))
}

func TestSearchTaskIsPrunableThroughTheSchedulerInterface(t *testing.T) {
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	owner := newGroupMember(ownerCtx, 1)
	survivor := newGroupMember(context.Background(), 2)
	require.True(t, owner.Merge(survivor))

	var asTask scheduler.Task = owner
	prunable, ok := asTask.(scheduler.PrunableTask)
	require.True(t, ok, "the scheduler must recognize a search group as prunable")

	cancelOwner()
	pruned, _, _ := prunable.PruneCanceled()
	require.NotNil(t, pruned)
	assert.Same(t, scheduler.Task(survivor), pruned, "the surviving member is what the scheduler executes")
	assert.ErrorIs(t, waitResult(t, owner), context.Canceled)
	assertNotNotified(t, survivor)
}

// The queue's expiry sweep runs when the queue is full and takes out the
// waiting tasks that are done. For a merged group, done has to mean every
// request in it. Read from the owner's context alone, it would end the whole
// group as soon as the owner's client went away, which is the collateral
// failure the rest of this change prevents at the other two exits from the
// queue.
func TestExpiryWaitsForTheWholeGroup(t *testing.T) {
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	memberCtx, cancelMember := context.WithCancel(context.Background())
	owner := newGroupMember(ownerCtx, 1)
	member := newGroupMember(memberCtx, 2)
	require.True(t, owner.Merge(member))

	now := time.Now()
	cancelOwner()
	assert.False(t, owner.ExpiryReady(now), "a group with a live request is not done")

	cancelMember()
	assert.True(t, owner.ExpiryReady(now), "once every request is canceled, it is")
}

// Members can carry deadlines up to the merge gap apart, and the sweep acts a
// little ahead of a deadline. One member coming due says nothing about the
// others.
func TestExpiryByDeadlineIsPerMember(t *testing.T) {
	now := time.Now()
	ownerCtx, cancelOwner := context.WithDeadline(context.Background(), now.Add(time.Hour))
	defer cancelOwner()
	memberCtx, cancelMember := context.WithDeadline(context.Background(), now.Add(2*time.Hour))
	defer cancelMember()
	owner := newGroupMember(ownerCtx, 1)
	member := newGroupMember(memberCtx, 2)
	require.True(t, owner.Merge(member))

	assert.False(t, owner.ExpiryReady(now.Add(90*time.Minute)), "the owner is due, the member is not")
	assert.True(t, owner.ExpiryReady(now.Add(2*time.Hour)), "both are due")
}

// When the sweep does take a group out, each request is told its own reason.
func TestFinishExpiredTellsEachRequestItsOwnReason(t *testing.T) {
	now := time.Now()
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	// Due within the sweep's window but not yet past its deadline, so its own
	// context has no error of its own to report.
	memberCtx, cancelMember := context.WithDeadline(context.Background(), now.Add(time.Hour))
	defer cancelMember()
	owner := newGroupMember(ownerCtx, 1)
	member := newGroupMember(memberCtx, 2)
	require.True(t, owner.Merge(member))

	cancelOwner()
	require.True(t, owner.ExpiryReady(now.Add(time.Hour)))
	owner.FinishExpired()

	assert.ErrorIs(t, waitResult(t, owner), context.Canceled, "the owner's client went away")
	assert.ErrorIs(t, waitResult(t, member), context.DeadlineExceeded, "the member ran out of time")
	assertNotNotified(t, owner)
	assertNotNotified(t, member)
}

// A standalone search is a group of one, and expires exactly as it did.
func TestExpiryOfAStandaloneSearch(t *testing.T) {
	now := time.Now()
	live := newGroupMember(context.Background(), 1)
	assert.False(t, live.ExpiryReady(now), "no deadline and not canceled: never due")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dead := newGroupMember(ctx, 1)
	require.True(t, dead.ExpiryReady(now))
	dead.FinishExpired()
	assert.ErrorIs(t, waitResult(t, dead), context.Canceled)
}

func TestSearchTaskAnswersTheExpirySweepThroughTheSchedulerInterface(t *testing.T) {
	var asTask scheduler.Task = newGroupMember(context.Background(), 1)
	_, ok := asTask.(scheduler.ExpirableGroup)
	assert.True(t, ok, "the sweep must recognize a search group, or it falls back to the owner's context")
}
