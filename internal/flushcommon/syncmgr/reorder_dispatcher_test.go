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

package syncmgr

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
)

type reorderTestTask struct {
	segmentID int64
	bytes     int64
	prepare   func(context.Context) error
	commit    func(context.Context) error
}

func (t *reorderTestTask) SegmentID() int64                     { return t.segmentID }
func (t *reorderTestTask) Checkpoint() *msgpb.MsgPosition       { return &msgpb.MsgPosition{} }
func (t *reorderTestTask) StartPosition() *msgpb.MsgPosition    { return &msgpb.MsgPosition{} }
func (t *reorderTestTask) ChannelName() string                  { return "test" }
func (t *reorderTestTask) HandleError(error)                    {}
func (t *reorderTestTask) IsFlush() bool                        { return false }
func (t *reorderTestTask) IsDrop() bool                         { return false }
func (t *reorderTestTask) SetChunkManager(storage.ChunkManager) {}
func (t *reorderTestTask) SetDrop()                             {}
func (t *reorderTestTask) Prepare(ctx context.Context) error    { return t.prepare(ctx) }
func (t *reorderTestTask) Commit(ctx context.Context) error     { return t.commit(ctx) }

func newTestReorderDispatcher(t *testing.T, parallel int) *reorderDispatcher[int64] {
	d := newReorderDispatcher[int64](parallel)
	t.Cleanup(func() {
		d.beginClose()
		_ = d.releasePools(time.Second)
	})
	return d
}

func TestReorderDispatcherParallelPrepareOrderedCommit(t *testing.T) {
	d := newTestReorderDispatcher(t, 4)

	started := make(chan int, 3)
	release := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	var mu sync.Mutex
	commitOrder := make([]int, 0, 3)
	futures := make([]*conc.Future[struct{}], 0, 3)
	for i := range 3 {
		idx := i
		task := &reorderTestTask{
			segmentID: 1,
			bytes:     10,
			prepare: func(context.Context) error {
				started <- idx
				<-release[idx]
				return nil
			},
			commit: func(context.Context) error {
				mu.Lock()
				commitOrder = append(commitOrder, idx)
				mu.Unlock()
				return nil
			},
		}
		futures = append(futures, d.Submit(context.Background(), 1, task))
	}

	for range 3 {
		<-started
	}
	close(release[2])
	close(release[1])
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	require.Empty(t, commitOrder)
	mu.Unlock()

	close(release[0])
	for _, future := range futures {
		_, err := future.Await()
		require.NoError(t, err)
	}
	require.Equal(t, []int{0, 1, 2}, commitOrder)
	require.Zero(t, d.Pending())
}

func TestReorderDispatcherCommitWaitsForPriorAckCallback(t *testing.T) {
	d := newTestReorderDispatcher(t, 2)

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	secondCommit := make(chan struct{})
	first := d.Submit(context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare:   func(context.Context) error { return nil },
		commit:    func(context.Context) error { return nil },
	}, func(err error) error {
		close(callbackStarted)
		<-releaseCallback
		return err
	})
	second := d.Submit(context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare:   func(context.Context) error { return nil },
		commit: func(context.Context) error {
			close(secondCommit)
			return nil
		},
	})

	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("first ACK callback did not start")
	}
	select {
	case <-secondCommit:
		t.Fatal("second Commit started before the first ACK callback completed")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseCallback)
	_, err := first.Await()
	require.NoError(t, err)
	_, err = second.Await()
	require.NoError(t, err)
}

// The dispatcher runs each phase exactly once. Whole-task retry belongs to the
// write buffer, which owns the queue and re-drives it from the oldest task.
func TestReorderDispatcherRunsEachPhaseOnce(t *testing.T) {
	d := newTestReorderDispatcher(t, 1)
	var prepareAttempts atomic.Int32
	var commitAttempts atomic.Int32
	failure := errors.New("commit failed")
	task := &reorderTestTask{
		segmentID: 1,
		prepare: func(context.Context) error {
			prepareAttempts.Add(1)
			return nil
		},
		commit: func(context.Context) error {
			commitAttempts.Add(1)
			return failure
		},
	}

	_, err := d.Submit(context.Background(), 1, task).Await()
	require.ErrorIs(t, err, failure)
	require.EqualValues(t, 1, prepareAttempts.Load())
	require.EqualValues(t, 1, commitAttempts.Load())
}

// A failed task takes the rest of its segment with it: the tasks behind it were
// built against state its Commit was supposed to publish, so they go back to the
// write buffer together and are re-submitted from the oldest.
func TestReorderDispatcherFailureAbortsSuffix(t *testing.T) {
	d := newTestReorderDispatcher(t, 4)

	failure := errors.New("head commit failed")
	var laterCommits atomic.Int32
	// The head's commit must not fail before the suffix is queued: a head that
	// fails and fully drains first deletes its emptied key, and a suffix
	// submitted after that lands on a FRESH key and legitimately commits —
	// that is a new round, not the aborted one this test is about.
	suffixQueued := make(chan struct{})
	head := d.Submit(context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare:   func(context.Context) error { return nil },
		commit: func(context.Context) error {
			<-suffixQueued
			return failure
		},
	})
	futures := []*conc.Future[struct{}]{head}
	for range 2 {
		futures = append(futures, d.Submit(context.Background(), 1, &reorderTestTask{
			segmentID: 1,
			prepare:   func(context.Context) error { return nil },
			commit: func(context.Context) error {
				laterCommits.Add(1)
				return nil
			},
		}))
	}

	close(suffixQueued)

	_, err := futures[0].Await()
	require.ErrorIs(t, err, failure)
	for _, future := range futures[1:] {
		_, err := future.Await()
		require.Error(t, err)
	}
	require.Zero(t, laterCommits.Load(), "a task behind a failed head must not commit")
	require.Zero(t, d.Pending())
}

// A task submitted while its key is draining a failure must not have its
// callbacks overtake earlier entries that are still blocked on a running phase:
// it joins the FIFO and completes in submission order.
func TestReorderDispatcherSubmitDuringAbortKeepsOrder(t *testing.T) {
	d := newTestReorderDispatcher(t, 4)

	failure := errors.New("sibling prepare failed")
	releaseHead := make(chan struct{})
	var mu sync.Mutex
	var order []int

	record := func(id int) func(error) error {
		return func(err error) error {
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			return err
		}
	}

	// Head blocks in Prepare, so the abort below cannot drain past it.
	f1 := d.Submit(context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare: func(context.Context) error {
			<-releaseHead
			return nil
		},
		commit: func(context.Context) error { return nil },
	}, record(1))
	// Sibling fails its Prepare, aborting the key while the head still runs.
	f2 := d.Submit(context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare:   func(context.Context) error { return failure },
		commit:    func(context.Context) error { return nil },
	}, record(2))

	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		state := d.keys[1]
		return state != nil && state.aborted
	}, 5*time.Second, time.Millisecond)

	// Submitted into the aborted key: its phases never run, and its callbacks
	// must wait for the entries ahead of it.
	f3 := d.Submit(context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare: func(context.Context) error {
			t.Error("a task rejected at Submit must not run Prepare")
			return nil
		},
		commit: func(context.Context) error {
			t.Error("a task rejected at Submit must not run Commit")
			return nil
		},
	}, record(3))

	close(releaseHead)

	for _, future := range []*conc.Future[struct{}]{f1, f2, f3} {
		_, err := future.Await()
		require.ErrorIs(t, err, failure)
	}
	mu.Lock()
	require.Equal(t, []int{1, 2, 3}, order, "completion callbacks must keep submission order across an abort-time Submit")
	mu.Unlock()
	require.Zero(t, d.Pending())
}
