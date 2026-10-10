// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// fakeTask is a test Task: identity plus a scripted Execute.
type fakeTask struct {
	taskID  int64
	runID   int64
	slot    int64
	execute func(ctx context.Context) (any, error)
}

func (t *fakeTask) TaskID() int64 { return t.taskID }
func (t *fakeTask) RunID() int64  { return t.runID }
func (t *fakeTask) Kind() string  { return "import" }
func (t *fakeTask) Slot() int64   { return t.slot }
func (t *fakeTask) Execute(ctx context.Context) (any, error) {
	return t.execute(ctx)
}

func newFakeTask(taskID, runID, slot int64, execute func(ctx context.Context) (any, error)) *fakeTask {
	return &fakeTask{taskID: taskID, runID: runID, slot: slot, execute: execute}
}

func newTestScheduler(capacity int64) *Scheduler {
	return NewScheduler(context.Background(), func() int64 { return capacity }, NewMetrics(1))
}

// submitAdmit queues a task and runs one admission pass, mirroring what the
// loop does when it is woken by Submit.
func submitAdmit(t *testing.T, s *Scheduler, task Task) {
	t.Helper()
	require.NoError(t, s.Submit(task))
	s.admit()
}

func waitSnapshot(t *testing.T, s *Scheduler, taskID, runID int64, state datapb.ImportTaskStateV2) Snapshot {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if snapshot, ok := s.Query(taskID, runID); ok && snapshot.State == state {
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
	snapshot, _ := s.Query(taskID, runID)
	t.Fatalf("task did not reach state %s: %+v", state, snapshot)
	return Snapshot{}
}

// --- admission ---

func TestSchedulerStartsWhenFree(t *testing.T) {
	s := newTestScheduler(4)
	started := make(chan struct{})
	release := make(chan struct{})
	submitAdmit(t, s, newFakeTask(10, 1, 2, func(context.Context) (any, error) {
		close(started)
		<-release
		return nil, nil
	}))
	<-started
	require.Equal(t, int64(2), s.Slots())
	require.Zero(t, s.QueuedSlots())
	close(release)
	require.Eventually(t, func() bool { return s.Slots() == 0 }, time.Second, time.Millisecond)
}

func TestSchedulerQueuesThenStartsWhenFreed(t *testing.T) {
	s := newTestScheduler(4)
	startedA := make(chan struct{})
	releaseA := make(chan struct{})
	submitAdmit(t, s, newFakeTask(11, 1, 3, func(context.Context) (any, error) {
		close(startedA)
		<-releaseA
		return nil, nil
	}))
	<-startedA
	require.Equal(t, int64(3), s.Slots())

	startedB := make(chan struct{})
	releaseB := make(chan struct{})
	submitAdmit(t, s, newFakeTask(12, 1, 2, func(context.Context) (any, error) {
		close(startedB)
		<-releaseB
		return nil, nil
	}))
	require.Equal(t, int64(3), s.Slots(), "an over-slot task must stay queued")
	require.Equal(t, int64(2), s.QueuedSlots())

	close(releaseA)
	require.Eventually(t, func() bool { return s.Slots() == 0 }, time.Second, time.Millisecond)
	s.admit()
	<-startedB
	require.Equal(t, int64(2), s.Slots())
	require.Zero(t, s.QueuedSlots())
	close(releaseB)
}

// A task that does not fit keeps its place; a smaller task behind it may still
// start (no head-of-line blocking), and the skipped task runs once slots free.
func TestSchedulerSkipsTaskThatDoesNotFit(t *testing.T) {
	s := newTestScheduler(4)
	startedA := make(chan struct{})
	releaseA := make(chan struct{})
	submitAdmit(t, s, newFakeTask(13, 1, 3, func(context.Context) (any, error) {
		close(startedA)
		<-releaseA
		return nil, nil
	}))
	<-startedA

	startedB := make(chan struct{})
	releaseB := make(chan struct{})
	require.NoError(t, s.Submit(newFakeTask(14, 1, 2, func(context.Context) (any, error) {
		close(startedB)
		<-releaseB
		return nil, nil
	})))
	startedC := make(chan struct{})
	releaseC := make(chan struct{})
	require.NoError(t, s.Submit(newFakeTask(15, 1, 1, func(context.Context) (any, error) {
		close(startedC)
		<-releaseC
		return nil, nil
	})))
	s.admit()
	<-startedC
	require.Equal(t, int64(2), s.QueuedSlots(), "the skipped task keeps its place")

	close(releaseA)
	require.Eventually(t, func() bool { return s.Slots() == 1 }, time.Second, time.Millisecond,
		"only the smaller task that started keeps its slot once the large one exits")
	s.admit()
	<-startedB
	require.Equal(t, int64(3), s.Slots())
	close(releaseB)
	close(releaseC)
}

// A task whose slot exceeds the node's total capacity can never fit alongside
// anything, so it must still run alone once the node is idle instead of being
// queued forever.
func TestSchedulerRunsOverCapacityTaskAlone(t *testing.T) {
	s := newTestScheduler(4)
	startedX := make(chan struct{})
	releaseX := make(chan struct{})
	submitAdmit(t, s, newFakeTask(20, 1, 8, func(context.Context) (any, error) {
		close(startedX)
		<-releaseX
		return nil, nil
	}))
	<-startedX
	require.Equal(t, int64(8), s.Slots(), "an over-capacity task must run, not queue forever")

	startedY := make(chan struct{})
	releaseY := make(chan struct{})
	submitAdmit(t, s, newFakeTask(21, 1, 1, func(context.Context) (any, error) {
		close(startedY)
		<-releaseY
		return nil, nil
	}))
	require.Equal(t, int64(1), s.QueuedSlots(), "a task must not overlap the over-capacity run")

	close(releaseX)
	require.Eventually(t, func() bool { return s.Slots() == 0 }, time.Second, time.Millisecond)
	s.admit()
	<-startedY
	require.Equal(t, int64(1), s.Slots())
	close(releaseY)
}

// --- execution, fencing, query/drop ---

func TestSchedulerRunFenceAndCompletion(t *testing.T) {
	s := newTestScheduler(100)
	started := make(chan struct{})
	release := make(chan struct{})
	submitAdmit(t, s, newFakeTask(10, 20, 2, func(ctx context.Context) (any, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []*datapb.ImportTaskV3Result{{Rows: 10}}, nil
	}))
	<-started
	running, ok := s.Query(10, 20)
	require.True(t, ok)
	require.Equal(t, datapb.ImportTaskStateV2_InProgress, running.State)
	require.Nil(t, running.Result)
	_, ok = s.Query(10, 21)
	require.False(t, ok, "a stale run query must be a no-op")
	close(release)
	snapshot := waitSnapshot(t, s, 10, 20, datapb.ImportTaskStateV2_Completed)
	segments, ok := snapshot.Result.([]*datapb.ImportTaskV3Result)
	require.True(t, ok)
	require.Equal(t, int64(10), segments[0].GetRows())
	response := &datapb.QueryImportTaskV3Response{State: datapb.ImportTaskStateV2_Completed, Segments: segments}
	payload, err := proto.Marshal(response)
	require.NoError(t, err)
	roundTrip := &datapb.QueryImportTaskV3Response{}
	require.NoError(t, proto.Unmarshal(payload, roundTrip))
	require.Equal(t, int64(10), roundTrip.GetSegments()[0].GetRows())
}

func TestSchedulerDropCancelsRun(t *testing.T) {
	s := newTestScheduler(100)
	started := make(chan struct{})
	canceled := make(chan struct{})
	submitAdmit(t, s, newFakeTask(11, 22, 1, func(ctx context.Context) (any, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}))
	<-started
	require.True(t, s.Drop(11, 22))
	<-canceled
	_, ok := s.Query(11, 22)
	require.False(t, ok)
}

func TestSchedulerCreateRunFencing(t *testing.T) {
	s := newTestScheduler(100)
	oldCanceled := make(chan struct{})
	oldStarted := make(chan struct{})
	newStarted := make(chan struct{})
	newRelease := make(chan struct{})
	submitAdmit(t, s, newFakeTask(12, 30, 2, func(ctx context.Context) (any, error) {
		close(oldStarted)
		<-ctx.Done()
		close(oldCanceled)
		return nil, ctx.Err()
	}))
	<-oldStarted
	// Same and smaller runs are idempotent/stale no-ops. Their callbacks must
	// never run.
	require.NoError(t, s.Submit(newFakeTask(12, 30, 9, func(context.Context) (any, error) {
		t.Fatal("same run callback must not run twice")
		return nil, nil
	})))
	require.NoError(t, s.Submit(newFakeTask(12, 29, 9, func(context.Context) (any, error) {
		t.Fatal("stale run callback must not run")
		return nil, nil
	})))
	submitAdmit(t, s, newFakeTask(12, 31, 4, func(ctx context.Context) (any, error) {
		close(newStarted)
		select {
		case <-newRelease:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	<-oldCanceled
	<-newStarted
	snapshot, ok := s.Query(12, 31)
	require.True(t, ok)
	require.Equal(t, int64(31), snapshot.RunID)
	close(newRelease)
	waitSnapshot(t, s, 12, 31, datapb.ImportTaskStateV2_Completed)
}

func TestSchedulerRetryAndFailedState(t *testing.T) {
	s := newTestScheduler(100)
	submitAdmit(t, s, newFakeTask(13, 40, 1, func(context.Context) (any, error) {
		return nil, merr.ErrServiceUnavailable
	}))
	waitSnapshot(t, s, 13, 40, datapb.ImportTaskStateV2_Retry)

	submitAdmit(t, s, newFakeTask(14, 41, 1, func(context.Context) (any, error) {
		return nil, merr.ErrImportSysFailed
	}))
	waitSnapshot(t, s, 14, 41, datapb.ImportTaskStateV2_Failed)
}

// A task kind without its own run fencing (the count-only preimport) is
// re-created with the same run id after a Retry: the scheduler must replace the
// finished attempt and execute again. reshard/import retries arrive as fresh
// run ids, so this never re-executes fenced work.
func TestSchedulerRetryRecreateSameRunReexecutes(t *testing.T) {
	s := newTestScheduler(100)
	attempts := make(chan int64, 2)
	submitAdmit(t, s, newFakeTask(18, 70, 1, func(context.Context) (any, error) {
		attempts <- 1
		return nil, merr.ErrServiceUnavailable
	}))
	waitSnapshot(t, s, 18, 70, datapb.ImportTaskStateV2_Retry)
	require.Equal(t, int64(1), <-attempts)

	submitAdmit(t, s, newFakeTask(18, 70, 1, func(context.Context) (any, error) {
		attempts <- 2
		return nil, nil
	}))
	snapshot := waitSnapshot(t, s, 18, 70, datapb.ImportTaskStateV2_Completed)
	require.Equal(t, int64(2), <-attempts)
	require.Equal(t, datapb.ImportTaskStateV2_Completed, snapshot.State)
}

// The worker classifier shares the checker's terminal denylist
// (common.IsTerminalImportV3Err): a transient object-store 5xx surfaces as
// typed ErrIoFailed and must retry; raw errors must retry too
// (RemoteChunkManager.Write does not map manifest-write failures); only
// provably permanent errors fail the task.
func TestSchedulerClassifiesWithCheckerDenylist(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want datapb.ImportTaskStateV2
	}{
		{"typed ErrIoFailed retries", merr.WrapErrIoFailed("manifest", errors.New("connection reset")), datapb.ImportTaskStateV2_Retry},
		{"raw non-milvus error retries", errors.New("write failed: 503 service unavailable"), datapb.ImportTaskStateV2_Retry},
		// ID exhaustion used to be an explicit allowlist special case; it stays
		// retryable through the denylist because ErrServiceInternal is not a
		// terminal code. DataCoord's prepareRetry re-derives the ID width.
		{"ID exhaustion retries", allocator.NewIDExhaustedError(0, 10, 5), datapb.ImportTaskStateV2_Retry},
		{"ErrImportFailed fails", merr.WrapErrImportFailedMsg("parquet: invalid magic"), datapb.ImportTaskStateV2_Failed},
		{"ErrIoKeyNotFound fails", merr.WrapErrIoKeyNotFound("manifest", "not found"), datapb.ImportTaskStateV2_Failed},
		// Caller-input defects (oversized file, dimension mismatch) are
		// deterministic; without this case every retry burns a new segment
		// and log range with no cap.
		{"ErrParameterInvalid fails", merr.WrapErrParameterInvalidMsg("file exceeds maxImportFileSizeInGB"), datapb.ImportTaskStateV2_Failed},
		// An RLS write-predicate denial is deterministic: the rows violate the
		// principal's policy, so Import V3 must fail the task like Import V2
		// rather than retry the same rows until the job times out.
		{"RLS write-predicate denial fails", merr.WrapErrPrivilegeNotPermitted("import operation denied by RLS check expression at row 3"), datapb.ImportTaskStateV2_Failed},
	}
	s := newTestScheduler(100)
	for i, c := range cases {
		taskID, runID := int64(30+i), int64(70+i)
		submitAdmit(t, s, newFakeTask(taskID, runID, 1, func(context.Context) (any, error) {
			return nil, c.err
		}))
		snapshot := waitSnapshot(t, s, taskID, runID, c.want)
		require.Equal(t, c.want, snapshot.State)
		require.Equal(t, c.err.Error(), snapshot.Reason)
	}
}

// A canceled run must never record Failed even when its error is terminal:
// DataCoord's stale query would fail the whole job for a run it already
// superseded or dropped.
func TestSchedulerCanceledRunNeverFails(t *testing.T) {
	mctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewScheduler(mctx, func() int64 { return 100 }, NewMetrics(1))
	started := make(chan struct{})
	submitAdmit(t, s, newFakeTask(17, 60, 1, func(ctx context.Context) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, merr.ErrImportFailed
	}))
	<-started
	cancel()
	snapshot := waitSnapshot(t, s, 17, 60, datapb.ImportTaskStateV2_Retry)
	require.Contains(t, snapshot.Reason, "context canceled")
}

func TestSchedulerLoonTransientIsRetry(t *testing.T) {
	s := newTestScheduler(100)

	submitAdmit(t, s, newFakeTask(15, 50, 1, func(context.Context) (any, error) {
		return nil, merr.Wrapf(packed.ErrLoonTransient, "FFI operation failed: simulated transient")
	}))
	waitSnapshot(t, s, 15, 50, datapb.ImportTaskStateV2_Retry)

	submitAdmit(t, s, newFakeTask(16, 51, 1, func(context.Context) (any, error) {
		return nil, merr.WrapErrStorage(packed.ErrLoonTransient, "commit manifest begin")
	}))
	waitSnapshot(t, s, 16, 51, datapb.ImportTaskStateV2_Retry)
}

func TestSchedulerSlotsFollowCurrentRun(t *testing.T) {
	s := newTestScheduler(100)
	require.Error(t, s.Submit(newFakeTask(20, 1, 0, func(context.Context) (any, error) { return nil, nil })))

	started := make(chan struct{})
	release := make(chan struct{})
	submitAdmit(t, s, newFakeTask(20, 1, 2, func(ctx context.Context) (any, error) {
		close(started)
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	<-started
	require.Equal(t, int64(2), s.Slots())

	// Same-run Create is idempotent even if the duplicate request carries a
	// different literal slot value.
	require.NoError(t, s.Submit(newFakeTask(20, 1, 9, func(context.Context) (any, error) {
		t.Fatal("same run must not start twice")
		return nil, nil
	})))
	require.Equal(t, int64(2), s.Slots())

	close(release)
	waitSnapshot(t, s, 20, 1, datapb.ImportTaskStateV2_Completed)
	require.Zero(t, s.Slots())
	require.True(t, s.Drop(20, 1))
	require.Zero(t, s.Slots())
}

func TestSchedulerStatsByKindAndState(t *testing.T) {
	s := newTestScheduler(100)
	release := make(chan struct{})
	defer close(release)
	submitAdmit(t, s, newFakeTask(40, 1, 1, func(ctx context.Context) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	submitAdmit(t, s, newFakeTask(41, 2, 1, func(context.Context) (any, error) {
		return nil, nil
	}))
	waitSnapshot(t, s, 41, 2, datapb.ImportTaskStateV2_Completed)
	s.mu.Lock()
	_, _, _, stats := s.selectUnsafe(s.capacity())
	s.mu.Unlock()
	require.Equal(t, 1, stats["import"][datapb.ImportTaskStateV2_InProgress])
	require.Equal(t, 1, stats["import"][datapb.ImportTaskStateV2_Completed])
}

// A pending (submitted, never started) task is visible to Query as Pending and
// reserves slots for backpressure, but does not consume running slots and is
// absent from the started-task state gauge.
func TestSchedulerQueuedTaskIsPendingAndReservesSlots(t *testing.T) {
	s := newTestScheduler(100)
	started := make(chan struct{})
	require.NoError(t, s.Submit(newFakeTask(50, 1, 3, func(context.Context) (any, error) {
		close(started)
		return nil, nil
	})))
	snapshot, ok := s.Query(50, 1)
	require.True(t, ok)
	require.Equal(t, datapb.ImportTaskStateV2_Pending, snapshot.State)
	require.Zero(t, s.Slots(), "a queued task holds no running slot")
	require.Equal(t, int64(3), s.QueuedSlots())

	s.admit()
	<-started
	waitSnapshot(t, s, 50, 1, datapb.ImportTaskStateV2_Completed)
	require.Zero(t, s.QueuedSlots())
}

func TestSchedulerDropQueuedTaskNeverRuns(t *testing.T) {
	s := newTestScheduler(100)
	require.NoError(t, s.Submit(newFakeTask(51, 1, 1, func(context.Context) (any, error) {
		t.Fatal("a dropped queued task must never run")
		return nil, nil
	})))
	require.True(t, s.Drop(51, 1))
	require.Zero(t, s.QueuedSlots())
	_, ok := s.Query(51, 1)
	require.False(t, ok)
	// The dropped task must stay gone: a later admission pass starts nothing.
	s.admit()
}

func TestSchedulerSubmitValidationAndClosed(t *testing.T) {
	s := newTestScheduler(100)
	require.Error(t, s.Submit(newFakeTask(0, 1, 1, nil)))
	require.Error(t, s.Submit(newFakeTask(1, 0, 1, nil)))
	require.Error(t, s.Submit(newFakeTask(1, 1, 0, nil)))

	s.Close()
	require.Error(t, s.Submit(newFakeTask(2, 1, 1, nil)))
}

// A dropped run must keep counting toward Slots until its Execute actually
// returns: otherwise admit() could start a pending task on slots the canceled
// run still uses, overcommitting the node's memory budget.
func TestSchedulerDropRunningKeepsSlotUntilExit(t *testing.T) {
	s := newTestScheduler(100)
	started := make(chan struct{})
	release := make(chan struct{})
	submitAdmit(t, s, newFakeTask(60, 1, 5, func(ctx context.Context) (any, error) {
		close(started)
		<-release
		return nil, ctx.Err()
	}))
	<-started
	require.Equal(t, int64(5), s.Slots())

	require.True(t, s.Drop(60, 1))
	require.Equal(t, int64(5), s.Slots(), "a dropped but still-running run must keep its slot")
	// The retired run is invisible to Query even while it still holds its slot.
	_, ok := s.Query(60, 1)
	require.False(t, ok, "a dropped run answers not-found")

	close(release)
	require.Eventually(t, func() bool { return s.Slots() == 0 }, time.Second, time.Millisecond)
}

// A run superseded by a newer run id keeps its slot until it exits, and the
// replacement queues without consuming a second slot.
func TestSchedulerSupersededRunKeepsSlotUntilExit(t *testing.T) {
	s := newTestScheduler(100)
	oldStarted := make(chan struct{})
	oldRelease := make(chan struct{})
	submitAdmit(t, s, newFakeTask(61, 1, 7, func(ctx context.Context) (any, error) {
		close(oldStarted)
		<-oldRelease
		return nil, ctx.Err()
	}))
	<-oldStarted
	require.Equal(t, int64(7), s.Slots())

	require.NoError(t, s.Submit(newFakeTask(61, 2, 3, func(context.Context) (any, error) {
		return nil, nil
	})))
	require.Equal(t, int64(7), s.Slots(), "the superseded run must keep its slot until it exits")
	// The retired superseded run is invisible; the replacement is queryable.
	_, ok := s.Query(61, 1)
	require.False(t, ok, "the superseded run answers not-found")
	require.Equal(t, int64(3), s.QueuedSlots(), "the replacement queues without consuming a second slot")

	close(oldRelease)
	require.Eventually(t, func() bool { return s.Slots() == 0 }, time.Second, time.Millisecond,
		"the queued replacement holds no running slot")
}

// --- loop and shutdown ---

// The admission loop is woken by Submit, so a submitted task is admitted
// without any explicit admit() call.
func TestSchedulerLoopAdmitsOnWake(t *testing.T) {
	s := newTestScheduler(100)
	done := make(chan struct{})
	go func() {
		s.Start()
		close(done)
	}()
	defer func() {
		s.Close()
		<-done
	}()

	completed := make(chan struct{})
	require.NoError(t, s.Submit(newFakeTask(70, 1, 1, func(context.Context) (any, error) {
		close(completed)
		return nil, nil
	})))
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("the loop did not admit the submitted task")
	}
}

// wake coalesces and never blocks, and Close stops the loop.
func TestSchedulerWakeCoalescesAndCloseStopsLoop(t *testing.T) {
	s := newTestScheduler(4)
	// Repeated wakeups must not block even with no admission loop running.
	require.NotPanics(t, func() { s.wake(); s.wake(); s.wake() })

	done := make(chan struct{})
	go func() {
		s.Start()
		close(done)
	}()
	s.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop after Close")
	}
}

// Close cancels and drains an in-flight run before returning.
func TestSchedulerCloseDrainsInFlightRun(t *testing.T) {
	s := newTestScheduler(100)
	started := make(chan struct{})
	canceled := make(chan struct{})
	submitAdmit(t, s, newFakeTask(80, 1, 2, func(ctx context.Context) (any, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}))
	<-started

	closed := make(chan struct{})
	go func() {
		s.Close()
		close(closed)
	}()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the in-flight run")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not drain the in-flight run")
	}
}
