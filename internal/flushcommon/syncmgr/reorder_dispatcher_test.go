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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type reorderTestTask struct {
	segmentID int64
	prepare   func(context.Context) error
	commit    func(context.Context) error
	handleErr func(error)
}

func (t *reorderTestTask) SegmentID() int64                  { return t.segmentID }
func (t *reorderTestTask) Checkpoint() *msgpb.MsgPosition    { return &msgpb.MsgPosition{} }
func (t *reorderTestTask) StartPosition() *msgpb.MsgPosition { return &msgpb.MsgPosition{} }
func (t *reorderTestTask) ChannelName() string               { return "test" }
func (t *reorderTestTask) HandleError(err error) {
	if t.handleErr != nil {
		t.handleErr(err)
	}
}
func (t *reorderTestTask) IsFlush() bool                        { return false }
func (t *reorderTestTask) IsDrop() bool                         { return false }
func (t *reorderTestTask) PayloadBytes() int64                  { return 0 }
func (t *reorderTestTask) InsertPayloadBytes() int64            { return 0 }
func (t *reorderTestTask) DeletePayloadBytes() int64            { return 0 }
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

//nolint:revive // Keep testing.T first, matching the convention for test helpers.
func submitTestTask(
	t *testing.T,
	d *reorderDispatcher[int64],
	ctx context.Context,
	key int64,
	task Task,
	callbacks ...func(error) error,
) *conc.Future[struct{}] {
	future, err := d.Submit(ctx, key, task, callbacks...)
	require.NoError(t, err)
	require.NotNil(t, future)
	return future
}

type dispatcherSubmitResult struct {
	id     int
	future *conc.Future[struct{}]
	err    error
}

type histogramState struct {
	count uint64
	sum   float64
}

func readHistogram(t *testing.T, observer prometheus.Observer) histogramState {
	metric, ok := observer.(prometheus.Metric)
	require.True(t, ok)
	pb := &dto.Metric{}
	require.NoError(t, metric.Write(pb))
	return histogramState{
		count: pb.GetHistogram().GetSampleCount(),
		sum:   pb.GetHistogram().GetSampleSum(),
	}
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
		futures = append(futures, submitTestTask(t, d, context.Background(), 1, task))
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
	first := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare:   func(context.Context) error { return nil },
		commit:    func(context.Context) error { return nil },
	}, func(err error) error {
		close(callbackStarted)
		<-releaseCallback
		return err
	})
	second := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
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

	_, err := submitTestTask(t, d, context.Background(), 1, task).Await()
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
	head := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare:   func(context.Context) error { return nil },
		commit: func(context.Context) error {
			<-suffixQueued
			return failure
		},
	})
	futures := []*conc.Future[struct{}]{head}
	for range 2 {
		futures = append(futures, submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
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
	headStarted := make(chan struct{})
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
	f1 := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare: func(context.Context) error {
			close(headStarted)
			<-releaseHead
			return nil
		},
		commit: func(context.Context) error { return nil },
	}, record(1))
	select {
	case <-headStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("head Prepare did not start")
	}
	// Sibling fails its Prepare, aborting the key while the head still runs.
	f2 := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
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
	f3 := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
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

func TestReorderDispatcherAdmissionCancellationIsNotVisible(t *testing.T) {
	d := newTestReorderDispatcher(t, 1)
	capacity := d.admission.Cap()
	nodeID := paramtable.GetStringNodeID()
	pendingBefore := testutil.ToFloat64(metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(nodeID))
	totalBefore := testutil.ToFloat64(metrics.WALFlusherSyncDispatcherTaskTotal.WithLabelValues(nodeID))

	release := make(chan struct{})
	futures := make([]*conc.Future[struct{}], 0, capacity)
	for i := range capacity {
		futures = append(futures, submitTestTask(t, d, context.Background(), int64(i), &reorderTestTask{
			segmentID: int64(i),
			prepare: func(ctx context.Context) error {
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			commit: func(context.Context) error { return nil },
		}))
	}
	require.Equal(t, capacity, d.admission.Current())
	require.Equal(t, capacity, d.Pending())

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan dispatcherSubmitResult, 1)
	var acceptedHookCalls atomic.Int32
	go func() {
		future, err := d.submit(ctx, 999, &reorderTestTask{
			segmentID: 999,
			prepare:   func(context.Context) error { return nil },
			commit:    func(context.Context) error { return nil },
		}, func() {
			acceptedHookCalls.Add(1)
		})
		resultCh <- dispatcherSubmitResult{future: future, err: err}
	}()

	select {
	case result := <-resultCh:
		t.Fatalf("submission unexpectedly passed full admission: %v", result.err)
	case <-time.After(30 * time.Millisecond):
	}
	d.mu.Lock()
	_, visible := d.keys[999]
	d.mu.Unlock()
	require.False(t, visible)
	require.Zero(t, acceptedHookCalls.Load())
	require.Equal(t, capacity, d.Pending())
	require.Equal(t, pendingBefore+float64(capacity), testutil.ToFloat64(metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(nodeID)))
	require.Equal(t, totalBefore+float64(capacity), testutil.ToFloat64(metrics.WALFlusherSyncDispatcherTaskTotal.WithLabelValues(nodeID)))

	cancel()
	select {
	case result := <-resultCh:
		require.ErrorIs(t, result.err, context.Canceled)
		require.Nil(t, result.future)
	case <-time.After(time.Second):
		t.Fatal("canceled admission did not return")
	}
	require.Zero(t, acceptedHookCalls.Load())
	require.Equal(t, capacity, d.Pending())
	require.Equal(t, totalBefore+float64(capacity), testutil.ToFloat64(metrics.WALFlusherSyncDispatcherTaskTotal.WithLabelValues(nodeID)))

	close(release)
	for _, future := range futures {
		_, err := future.Await()
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return d.admission.Current() == 0 &&
			testutil.ToFloat64(metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(nodeID)) == pendingBefore
	}, time.Second, time.Millisecond)
}

func TestReorderDispatcherReleaseUnblocksOneSubmitter(t *testing.T) {
	d := newTestReorderDispatcher(t, 2)
	capacity := d.admission.Cap()
	releases := make([]chan struct{}, capacity)
	started := make(chan int, capacity)
	futures := make([]*conc.Future[struct{}], 0, capacity)
	for i := range capacity {
		id := i
		releases[i] = make(chan struct{})
		release := releases[i]
		futures = append(futures, submitTestTask(t, d, context.Background(), int64(i), &reorderTestTask{
			segmentID: int64(i),
			prepare: func(ctx context.Context) error {
				started <- id
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			commit: func(context.Context) error { return nil },
		}))
	}
	require.Equal(t, capacity, d.admission.Current())
	runningID := <-started

	extraRelease := make(chan struct{})
	startedSubmit := make(chan struct{}, 2)
	resultCh := make(chan dispatcherSubmitResult, 2)
	cancels := make([]context.CancelFunc, 2)
	for i := range 2 {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancels[i] = cancel
		go func(id int) {
			startedSubmit <- struct{}{}
			future, err := d.Submit(ctx, int64(100+id), &reorderTestTask{
				segmentID: int64(100 + id),
				prepare: func(ctx context.Context) error {
					select {
					case <-extraRelease:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
				commit: func(context.Context) error { return nil },
			})
			resultCh <- dispatcherSubmitResult{id: id, future: future, err: err}
		}(i)
	}
	<-startedSubmit
	<-startedSubmit
	select {
	case result := <-resultCh:
		t.Fatalf("submission unexpectedly passed full admission: id=%d err=%v", result.id, result.err)
	case <-time.After(30 * time.Millisecond):
	}

	close(releases[runningID])
	var admitted dispatcherSubmitResult
	select {
	case admitted = <-resultCh:
		require.NoError(t, admitted.err)
		require.NotNil(t, admitted.future)
	case <-time.After(time.Second):
		t.Fatal("releasing one task did not unblock a submitter")
	}
	select {
	case result := <-resultCh:
		t.Fatalf("one released slot unblocked two submitters: id=%d err=%v", result.id, result.err)
	case <-time.After(30 * time.Millisecond):
	}

	blockedID := 1 - admitted.id
	cancels[blockedID]()
	select {
	case blocked := <-resultCh:
		require.Equal(t, blockedID, blocked.id)
		require.ErrorIs(t, blocked.err, context.Canceled)
		require.Nil(t, blocked.future)
	case <-time.After(time.Second):
		t.Fatal("blocked submitter did not observe cancellation")
	}
	for i := range releases {
		if i != runningID {
			close(releases[i])
		}
	}
	close(extraRelease)
	for _, future := range futures {
		_, err := future.Await()
		require.NoError(t, err)
	}
	_, err := admitted.future.Await()
	require.NoError(t, err)
}

func TestReorderDispatcherCallbackRetainsAdmissionAtCapacity(t *testing.T) {
	d := newTestReorderDispatcher(t, 1)
	capacity := d.admission.Cap()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	releaseOthers := make(chan struct{})

	first := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare: func(ctx context.Context) error {
			close(firstStarted)
			select {
			case <-releaseFirst:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		commit: func(context.Context) error { return nil },
	}, func(err error) error {
		close(callbackStarted)
		<-releaseCallback
		return err
	})
	<-firstStarted

	others := make([]*conc.Future[struct{}], 0, capacity-1)
	for i := 1; i < capacity; i++ {
		others = append(others, submitTestTask(t, d, context.Background(), int64(i+1), &reorderTestTask{
			segmentID: int64(i + 1),
			prepare: func(ctx context.Context) error {
				select {
				case <-releaseOthers:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			commit: func(context.Context) error { return nil },
		}))
	}
	require.Equal(t, capacity, d.admission.Current())
	close(releaseFirst)
	<-callbackStarted
	require.Equal(t, capacity, d.admission.Current(), "callback-held task must remain inside admission")

	extraResult := make(chan dispatcherSubmitResult, 1)
	go func() {
		future, err := d.Submit(context.Background(), 999, &reorderTestTask{
			segmentID: 999,
			prepare: func(ctx context.Context) error {
				select {
				case <-releaseOthers:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			commit: func(context.Context) error { return nil },
		})
		extraResult <- dispatcherSubmitResult{future: future, err: err}
	}()
	select {
	case result := <-extraResult:
		t.Fatalf("callback released admission before lifecycle completion: %v", result.err)
	case <-time.After(30 * time.Millisecond):
	}

	close(releaseCallback)
	var extra dispatcherSubmitResult
	select {
	case extra = <-extraResult:
		require.NoError(t, extra.err)
		require.NotNil(t, extra.future)
	case <-time.After(time.Second):
		t.Fatal("callback completion did not release admission")
	}
	close(releaseOthers)
	_, err := first.Await()
	require.NoError(t, err)
	for _, future := range others {
		_, err := future.Await()
		require.NoError(t, err)
	}
	_, err = extra.future.Await()
	require.NoError(t, err)
}

func TestReorderDispatcherResizeUpdatesAdmissionCapacity(t *testing.T) {
	d := newTestReorderDispatcher(t, 1)
	require.Equal(t, 4, d.admission.Cap())

	require.NoError(t, d.resize(3))
	require.Equal(t, 6, d.admission.Cap())
	require.Equal(t, 3, d.preparePool.Cap())
	require.Equal(t, 3, d.commitPool.Cap())

	require.NoError(t, d.resize(1))
	require.Equal(t, 4, d.admission.Cap())
	require.Equal(t, 1, d.preparePool.Cap())
	require.Equal(t, 1, d.commitPool.Cap())
}

func TestReorderDispatcherCloseWakesAdmissionWaiters(t *testing.T) {
	d := newTestReorderDispatcher(t, 1)
	capacity := d.admission.Cap()
	futures := make([]*conc.Future[struct{}], 0, capacity)
	for i := range capacity {
		futures = append(futures, submitTestTask(t, d, context.Background(), int64(i), &reorderTestTask{
			segmentID: int64(i),
			prepare: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			commit: func(context.Context) error { return nil },
		}))
	}
	require.Equal(t, capacity, d.admission.Current())

	startedSubmit := make(chan struct{})
	resultCh := make(chan dispatcherSubmitResult, 1)
	go func() {
		close(startedSubmit)
		future, err := d.Submit(context.Background(), 999, &reorderTestTask{
			segmentID: 999,
			prepare:   func(context.Context) error { return nil },
			commit:    func(context.Context) error { return nil },
		})
		resultCh <- dispatcherSubmitResult{future: future, err: err}
	}()
	<-startedSubmit
	select {
	case result := <-resultCh:
		t.Fatalf("submission unexpectedly passed full admission: %v", result.err)
	case <-time.After(30 * time.Millisecond):
	}

	d.Close()
	select {
	case result := <-resultCh:
		require.ErrorIs(t, result.err, context.Canceled)
		require.Nil(t, result.future)
	case <-time.After(time.Second):
		t.Fatal("dispatcher close did not wake blocked admission")
	}
	for _, future := range futures {
		_, err := future.Await()
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Eventually(t, func() bool {
		return d.Pending() == 0 && d.admission.Current() == 0
	}, time.Second, time.Millisecond)
}

func TestReorderDispatcherMetricBoundaries(t *testing.T) {
	d := newTestReorderDispatcher(t, 1)
	nodeID := paramtable.GetStringNodeID()
	queueObserver := metrics.WALFlusherSyncDispatcherQueueDuration.WithLabelValues(nodeID)
	prepareQueueObserver := metrics.WALFlusherSyncDispatcherPrepareQueueDuration.WithLabelValues(nodeID)
	prepareObserver := metrics.WALFlusherSyncDispatcherPrepareDuration.WithLabelValues(nodeID)
	commitWaitObserver := metrics.WALFlusherSyncDispatcherCommitWaitDuration.WithLabelValues(nodeID)
	commitObserver := metrics.WALFlusherSyncDispatcherCommitDuration.WithLabelValues(nodeID)
	executeObserver := metrics.WALFlusherSyncDispatcherExecuteDuration.WithLabelValues(nodeID)
	queueBefore := readHistogram(t, queueObserver)
	prepareQueueBefore := readHistogram(t, prepareQueueObserver)
	prepareBefore := readHistogram(t, prepareObserver)
	commitWaitBefore := readHistogram(t, commitWaitObserver)
	commitBefore := readHistogram(t, commitObserver)
	executeBefore := readHistogram(t, executeObserver)

	// Occupy the Commit pool directly so the dispatcher task accumulates Commit
	// queue time without attributing the blocker itself to dispatcher metrics.
	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	blocker := d.commitPool.Submit(func() (struct{}, error) {
		close(blockerStarted)
		<-releaseBlocker
		return struct{}{}, nil
	})
	<-blockerStarted

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	future := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare:   func(context.Context) error { return nil },
		commit:    func(context.Context) error { return nil },
	}, func(err error) error {
		close(callbackStarted)
		<-releaseCallback
		return err
	})
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		state := d.keys[1]
		return state != nil && state.committing
	}, time.Second, time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	close(releaseBlocker)
	_, err := blocker.Await()
	require.NoError(t, err)
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("task callback did not start")
	}

	queueMid := readHistogram(t, queueObserver)
	prepareQueueMid := readHistogram(t, prepareQueueObserver)
	prepareMid := readHistogram(t, prepareObserver)
	commitWaitMid := readHistogram(t, commitWaitObserver)
	commitMid := readHistogram(t, commitObserver)
	executeMid := readHistogram(t, executeObserver)
	require.Equal(t, queueBefore.count+1, queueMid.count)
	require.Equal(t, prepareQueueBefore.count+1, prepareQueueMid.count)
	require.Equal(t, prepareBefore.count+1, prepareMid.count)
	require.Equal(t, commitWaitBefore.count+1, commitWaitMid.count)
	require.Equal(t, commitBefore.count+1, commitMid.count)
	// The task is still in its callback, so the legacy execution aggregate must
	// not be published yet.
	require.Equal(t, executeBefore.count, executeMid.count)
	queueDelta := queueMid.sum - queueBefore.sum
	prepareQueueDelta := prepareQueueMid.sum - prepareQueueBefore.sum
	commitWaitDelta := commitWaitMid.sum - commitWaitBefore.sum
	require.InDelta(t, prepareQueueDelta+commitWaitDelta, queueDelta, 1e-6,
		"legacy queue duration must equal Prepare wait plus Commit wait",
	)
	require.Greater(t, queueDelta, 0.12, "Commit-pool wait must be represented as queueing")

	time.Sleep(30 * time.Millisecond)
	close(releaseCallback)
	_, err = future.Await()
	require.NoError(t, err)
	executeAfter := readHistogram(t, executeObserver)
	require.Equal(t, executeBefore.count+1, executeAfter.count)
	executeDelta := executeAfter.sum - executeBefore.sum
	require.Greater(t, executeDelta, 0.02,
		"execute duration must include callback time")
	require.Less(t, executeDelta, queueDelta,
		"execute duration must exclude Commit queue wait")
}

func TestReorderDispatcherCommitWaitStartsWhenPrepareReturns(t *testing.T) {
	d := newTestReorderDispatcher(t, 1)
	prepareStarted := make(chan struct{})
	allowPrepareReturn := make(chan struct{})
	commitStarted := make(chan struct{})
	releaseCommit := make(chan struct{})
	future := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare: func(context.Context) error {
			close(prepareStarted)
			<-allowPrepareReturn
			return nil
		},
		commit: func(context.Context) error {
			close(commitStarted)
			<-releaseCommit
			return nil
		},
	})
	<-prepareStarted

	// Delay the worker after Prepare returns but before it can publish its result.
	// preparedAt must retain the actual phase boundary, not this later scheduler
	// handoff time.
	d.mu.Lock()
	close(allowPrepareReturn)
	time.Sleep(50 * time.Millisecond)
	unlockAt := time.Now()
	d.mu.Unlock()

	select {
	case <-commitStarted:
	case <-time.After(time.Second):
		t.Fatal("task did not reach Commit")
	}
	d.mu.Lock()
	preparedAt := d.keys[1].entries[0].preparedAt
	d.mu.Unlock()
	require.False(t, preparedAt.IsZero())
	require.Greater(t, unlockAt.Sub(preparedAt), 30*time.Millisecond,
		"Commit wait must include the scheduling delay after Prepare returned")

	close(releaseCommit)
	_, err := future.Await()
	require.NoError(t, err)
}

func TestReorderDispatcherPreparedSuffixAbortObservesCommitWait(t *testing.T) {
	d := newTestReorderDispatcher(t, 2)
	nodeID := paramtable.GetStringNodeID()
	queueObserver := metrics.WALFlusherSyncDispatcherQueueDuration.WithLabelValues(nodeID)
	commitWaitObserver := metrics.WALFlusherSyncDispatcherCommitWaitDuration.WithLabelValues(nodeID)
	queueBefore := readHistogram(t, queueObserver)
	commitWaitBefore := readHistogram(t, commitWaitObserver)

	failure := errors.New("head commit failed")
	headCommitStarted := make(chan struct{})
	releaseHeadCommit := make(chan struct{})
	head := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare:   func(context.Context) error { return nil },
		commit: func(context.Context) error {
			close(headCommitStarted)
			<-releaseHeadCommit
			return failure
		},
	})
	<-headCommitStarted

	suffixPrepared := make(chan struct{})
	suffix := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare: func(context.Context) error {
			close(suffixPrepared)
			return nil
		},
		commit: func(context.Context) error {
			t.Error("aborted suffix must not Commit")
			return nil
		},
	})
	<-suffixPrepared
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		state := d.keys[1]
		return state != nil && len(state.entries) == 2 && state.entries[1].prepared
	}, time.Second, time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	close(releaseHeadCommit)

	_, err := head.Await()
	require.ErrorIs(t, err, failure)
	_, err = suffix.Await()
	require.ErrorIs(t, err, failure)

	queueAfter := readHistogram(t, queueObserver)
	commitWaitAfter := readHistogram(t, commitWaitObserver)
	require.Equal(t, queueBefore.count+2, queueAfter.count)
	require.Equal(t, commitWaitBefore.count+2, commitWaitAfter.count)
	require.Greater(t, commitWaitAfter.sum-commitWaitBefore.sum, 0.02,
		"a prepared suffix aborted before Commit must report its real Commit wait")
}

func TestReorderDispatcherPrepareFailureDoesNotObserveCommitWait(t *testing.T) {
	d := newTestReorderDispatcher(t, 1)
	nodeID := paramtable.GetStringNodeID()
	queueObserver := metrics.WALFlusherSyncDispatcherQueueDuration.WithLabelValues(nodeID)
	commitWaitObserver := metrics.WALFlusherSyncDispatcherCommitWaitDuration.WithLabelValues(nodeID)
	queueBefore := readHistogram(t, queueObserver)
	commitWaitBefore := readHistogram(t, commitWaitObserver)

	failure := errors.New("prepare failed")
	future := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare:   func(context.Context) error { return failure },
		commit: func(context.Context) error {
			t.Error("Prepare failure must not Commit")
			return nil
		},
	})
	_, err := future.Await()
	require.ErrorIs(t, err, failure)

	queueAfter := readHistogram(t, queueObserver)
	commitWaitAfter := readHistogram(t, commitWaitObserver)
	require.Equal(t, queueBefore.count+1, queueAfter.count)
	require.Equal(t, commitWaitBefore.count, commitWaitAfter.count)
}

func TestReorderDispatcherCallbackOnlyAbortObservesExecute(t *testing.T) {
	d := newTestReorderDispatcher(t, 2)
	nodeID := paramtable.GetStringNodeID()
	executeObserver := metrics.WALFlusherSyncDispatcherExecuteDuration.WithLabelValues(nodeID)
	executeBefore := readHistogram(t, executeObserver)

	releaseHead := make(chan struct{})
	headStarted := make(chan struct{})
	head := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare: func(context.Context) error {
			close(headStarted)
			<-releaseHead
			return nil
		},
		commit: func(context.Context) error {
			t.Error("aborted head must not Commit")
			return nil
		},
	})
	<-headStarted

	failure := errors.New("sibling prepare failed")
	sibling := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare:   func(context.Context) error { return failure },
		commit: func(context.Context) error {
			t.Error("failed sibling must not Commit")
			return nil
		},
	})
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.keys[1] != nil && d.keys[1].aborted
	}, time.Second, time.Millisecond)

	var phaseCalls atomic.Int32
	var callbackCalls atomic.Int32
	callbackOnly := submitTestTask(t, d, context.Background(), 1, &reorderTestTask{
		segmentID: 1,
		prepare: func(context.Context) error {
			phaseCalls.Add(1)
			return nil
		},
		commit: func(context.Context) error {
			phaseCalls.Add(1)
			return nil
		},
	}, func(err error) error {
		callbackCalls.Add(1)
		return err
	})
	close(releaseHead)

	for _, future := range []*conc.Future[struct{}]{head, sibling, callbackOnly} {
		_, err := future.Await()
		require.ErrorIs(t, err, failure)
	}
	require.Zero(t, phaseCalls.Load())
	require.EqualValues(t, 1, callbackCalls.Load())
	executeAfter := readHistogram(t, executeObserver)
	require.Equal(t, executeBefore.count+3, executeAfter.count,
		"every admitted task, including callback-only aborts, needs one execution observation")
}

func TestReorderDispatcherReleasePoolsClosesCommitAfterPrepareTimeout(t *testing.T) {
	d := newReorderDispatcher[int64](1)
	started := make(chan struct{})
	release := make(chan struct{})
	blocker := d.preparePool.Submit(func() (struct{}, error) {
		close(started)
		<-release
		return struct{}{}, nil
	})
	<-started

	err := d.releasePools(10 * time.Millisecond)
	require.Error(t, err)
	require.True(t, d.preparePool.IsClosed())
	require.True(t, d.commitPool.IsClosed(), "Commit pool must close even when Prepare consumes the shutdown budget")
	close(release)
	_, blockerErr := blocker.Await()
	require.NoError(t, blockerErr)
}

// TestReorderDispatcherCrossKeyIndependence is the regression coverage for what
// key_lock_dispatcher_test.go used to prove (TestCrossKeyConcurrent /
// TestMixedKeysConcurrency): a key stuck inside a phase must not stall another
// key's full Prepare -> Commit -> callback progress. Positive direction is
// gated on channels only — no sleeps.
func TestReorderDispatcherCrossKeyIndependence(t *testing.T) {
	awaitDone := func(t *testing.T, future *conc.Future[struct{}], what string) {
		t.Helper()
		done := make(chan error, 1)
		go func() {
			_, err := future.Await()
			done <- err
		}()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not complete", what)
		}
	}

	run := func(t *testing.T, blockPrepare bool) {
		d := newTestReorderDispatcher(t, 4)

		blockedEntered := make(chan struct{})
		releaseBlocked := make(chan struct{})
		gate := func(context.Context) error {
			close(blockedEntered)
			<-releaseBlocked
			return nil
		}
		pass := func(context.Context) error { return nil }
		blockedTask := &reorderTestTask{segmentID: 1, prepare: pass, commit: pass}
		if blockPrepare {
			blockedTask.prepare = gate
		} else {
			blockedTask.commit = gate
		}
		blocked := submitTestTask(t, d, context.Background(), 1, blockedTask)
		<-blockedEntered

		// While key 1 sits inside its phase, key 2 must run to full completion,
		// including its completion callback.
		callbackRan := make(chan struct{})
		other := submitTestTask(t, d, context.Background(), 2, &reorderTestTask{
			segmentID: 2,
			prepare:   pass,
			commit:    pass,
		}, func(err error) error {
			close(callbackRan)
			return err
		})
		awaitDone(t, other, "cross-key task blocked behind an unrelated stuck key")
		<-callbackRan

		close(releaseBlocked)
		awaitDone(t, blocked, "blocked key after release")
		require.Zero(t, d.Pending())
	}

	t.Run("key_blocked_in_prepare", func(t *testing.T) { run(t, true) })
	t.Run("key_blocked_in_commit", func(t *testing.T) { run(t, false) })
}

// runCallbacks is the dispatcher's panic containment: HandleError may be fatal,
// but lifecycle callbacks after it still have to release segment state before
// the panic is propagated.
func TestRunCallbacksFirstPanicPreservesErrorForLaterCallbacks(t *testing.T) {
	initial := errors.New("commit failed")
	var seen []error
	panicValue, finalErr := runCallbacks(initial, []func(error) error{
		func(error) error { panic("callback exploded") },
		func(err error) error {
			seen = append(seen, err)
			// A cleanup callback swallowing the error must not let the NEXT
			// callback (or the Future) observe success after a panic.
			return nil
		},
		func(err error) error {
			seen = append(seen, err)
			return err
		},
	})
	require.Equal(t, "callback exploded", panicValue,
		"the first panic value must be returned for the caller to re-raise")
	require.Equal(t, []error{initial, initial}, seen,
		"every callback after a panic must see the preserved failure")
	require.ErrorIs(t, finalErr, initial)
}

func TestRunCallbacksOnlyFirstPanicWins(t *testing.T) {
	panicValue, finalErr := runCallbacks(errors.New("initial"), []func(error) error{
		func(error) error { panic("first") },
		func(error) error { panic("second") },
	})
	require.Equal(t, "first", panicValue)
	require.Error(t, finalErr)
}

// A panic on an otherwise successful attempt must not be reported as success:
// it becomes a System error carrying the panic message.
func TestRunCallbacksPanicOnSuccessBecomesSystemError(t *testing.T) {
	panicValue, finalErr := runCallbacks(nil, []func(error) error{
		func(err error) error {
			require.NoError(t, err)
			panic("boom")
		},
		func(err error) error {
			require.ErrorIs(t, err, merr.ErrServiceInternal,
				"later callbacks must already see the substituted System error")
			return err
		},
	})
	require.Equal(t, "boom", panicValue)
	require.ErrorIs(t, finalErr, merr.ErrServiceInternal)
	require.ErrorContains(t, finalErr, "callback panicked")
}

// panic(nil) (a nil-ish panic value) still surfaces as a recovered panic in Go
// 1.21+ (*runtime.PanicNilError); the machinery must convert it to a System
// error rather than report success.
func TestRunCallbacksNilPanicBecomesSystemError(t *testing.T) {
	var nilValue any
	panicValue, finalErr := runCallbacks(nil, []func(error) error{
		func(error) error { panic(nilValue) },
	})
	require.NotNil(t, panicValue)
	require.ErrorIs(t, finalErr, merr.ErrServiceInternal)
}

// The re-raise itself happens on a detached goroutine (repanicAsync), which is
// deliberately unrecoverable — it must crash the process. The only part
// observable without killing the test process is that a nil panic value is a
// no-op; the non-nil path's contract is covered above by asserting runCallbacks
// hands the exact panic value back to finish for re-raising.
func TestRepanicAsyncNilIsNoop(t *testing.T) {
	require.NotPanics(t, func() { repanicAsync(nil) })
	// Give a wrongly-spawned goroutine the chance to crash the run.
	time.Sleep(10 * time.Millisecond)
}

func TestReorderDispatcherConcurrentResizeKeepsCapacitiesConsistent(t *testing.T) {
	d := newTestReorderDispatcher(t, 1)
	start := make(chan struct{})
	errCh := make(chan error, 32)
	var wg sync.WaitGroup
	for i := range 32 {
		size := i%7 + 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errCh <- d.resize(size)
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	require.Equal(t, d.preparePool.Cap(), d.commitPool.Cap())
	require.Equal(t, dispatcherAdmissionCapacity(d.preparePool.Cap()), d.admission.Cap())
}
