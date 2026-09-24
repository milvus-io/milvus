// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"
	"sync/atomic"
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

// submitStart mirrors what the scheduler does: queue the task, then admit it.
func submitStart(t *testing.T, manager *TaskManager, task Task) {
	t.Helper()
	require.NoError(t, manager.Submit(task))
	require.NoError(t, manager.Start(task.TaskID(), task.RunID()))
}

func waitSnapshot(t *testing.T, manager *TaskManager, taskID, runID int64, state datapb.ImportTaskStateV2) Snapshot {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if snapshot, ok := manager.Query(taskID, runID); ok && snapshot.State == state {
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
	snapshot, _ := manager.Query(taskID, runID)
	t.Fatalf("task did not reach state %s: %+v", state, snapshot)
	return Snapshot{}
}

func TestTaskManagerRunFenceAndCompletion(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	started := make(chan struct{})
	release := make(chan struct{})
	submitStart(t, manager, newFakeTask(10, 20, 2, func(ctx context.Context) (any, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []*datapb.SegmentResult{{Rows: 10}}, nil
	}))
	<-started
	running, ok := manager.Query(10, 20)
	require.True(t, ok)
	require.Equal(t, datapb.ImportTaskStateV2_InProgress, running.State)
	require.Nil(t, running.Result)
	_, ok = manager.Query(10, 21)
	require.False(t, ok, "a stale run query must be a no-op")
	close(release)
	snapshot := waitSnapshot(t, manager, 10, 20, datapb.ImportTaskStateV2_Completed)
	segments, ok := snapshot.Result.([]*datapb.SegmentResult)
	require.True(t, ok)
	require.Equal(t, int64(10), segments[0].GetRows())
	response := &datapb.QueryImportTaskV3Response{State: datapb.ImportTaskStateV2_Completed, Segments: segments}
	payload, err := proto.Marshal(response)
	require.NoError(t, err)
	roundTrip := &datapb.QueryImportTaskV3Response{}
	require.NoError(t, proto.Unmarshal(payload, roundTrip))
	require.Equal(t, int64(10), roundTrip.GetSegments()[0].GetRows())
}

func TestTaskManagerDropCancelsRun(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	started := make(chan struct{})
	canceled := make(chan struct{})
	submitStart(t, manager, newFakeTask(11, 22, 1, func(ctx context.Context) (any, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}))
	<-started
	require.True(t, manager.Drop(11, 22))
	<-canceled
	_, ok := manager.Query(11, 22)
	require.False(t, ok)
}

func TestTaskManagerCreateRunFencing(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	oldCanceled := make(chan struct{})
	oldStarted := make(chan struct{})
	newStarted := make(chan struct{})
	newRelease := make(chan struct{})
	submitStart(t, manager, newFakeTask(12, 30, 2, func(ctx context.Context) (any, error) {
		close(oldStarted)
		<-ctx.Done()
		close(oldCanceled)
		return nil, ctx.Err()
	}))
	<-oldStarted
	// Same and smaller runs are idempotent/stale no-ops. Their callbacks must
	// never run.
	require.NoError(t, manager.Submit(newFakeTask(12, 30, 9, func(context.Context) (any, error) {
		t.Fatal("same run callback must not run twice")
		return nil, nil
	})))
	require.NoError(t, manager.Submit(newFakeTask(12, 29, 9, func(context.Context) (any, error) {
		t.Fatal("stale run callback must not run")
		return nil, nil
	})))
	submitStart(t, manager, newFakeTask(12, 31, 4, func(ctx context.Context) (any, error) {
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
	snapshot, ok := manager.Query(12, 31)
	require.True(t, ok)
	require.Equal(t, int64(31), snapshot.RunID)
	close(newRelease)
	waitSnapshot(t, manager, 12, 31, datapb.ImportTaskStateV2_Completed)
}

func TestTaskManagerRetryAndFailedState(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	submitStart(t, manager, newFakeTask(13, 40, 1, func(context.Context) (any, error) {
		return nil, merr.ErrServiceUnavailable
	}))
	waitSnapshot(t, manager, 13, 40, datapb.ImportTaskStateV2_Retry)

	submitStart(t, manager, newFakeTask(14, 41, 1, func(context.Context) (any, error) {
		return nil, merr.ErrImportSysFailed
	}))
	waitSnapshot(t, manager, 14, 41, datapb.ImportTaskStateV2_Failed)
}

// A task kind without its own run fencing (the count-only preimport) is
// re-created with the same run id after a Retry: the manager must replace the
// finished attempt and execute again. reshard/import retries arrive as fresh
// run ids, so this never re-executes fenced work.
func TestTaskManagerRetryRecreateSameRunReexecutes(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	attempts := make(chan int64, 2)
	submitStart(t, manager, newFakeTask(18, 70, 1, func(context.Context) (any, error) {
		attempts <- 1
		return nil, merr.ErrServiceUnavailable
	}))
	waitSnapshot(t, manager, 18, 70, datapb.ImportTaskStateV2_Retry)
	require.Equal(t, int64(1), <-attempts)

	submitStart(t, manager, newFakeTask(18, 70, 1, func(context.Context) (any, error) {
		attempts <- 2
		return nil, nil
	}))
	snapshot := waitSnapshot(t, manager, 18, 70, datapb.ImportTaskStateV2_Completed)
	require.Equal(t, int64(2), <-attempts)
	require.Equal(t, datapb.ImportTaskStateV2_Completed, snapshot.State)
}

// The worker classifier shares the checker's terminal denylist
// (common.IsTerminalImportV3Err): a transient object-store 5xx surfaces as
// typed ErrIoFailed and must retry; raw errors must retry too
// (RemoteChunkManager.Write does not map manifest-write failures); only
// provably permanent errors fail the task.
func TestTaskManagerClassifiesWithCheckerDenylist(t *testing.T) {
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
	}
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	for i, c := range cases {
		taskID, runID := int64(30+i), int64(70+i)
		submitStart(t, manager, newFakeTask(taskID, runID, 1, func(context.Context) (any, error) {
			return nil, c.err
		}))
		snapshot := waitSnapshot(t, manager, taskID, runID, c.want)
		require.Equal(t, c.want, snapshot.State)
		require.Equal(t, c.err.Error(), snapshot.Reason)
	}
}

// A canceled run must never record Failed even when its error is terminal:
// DataCoord's stale query would fail the whole job for a run it already
// superseded or dropped.
func TestTaskManagerCanceledRunNeverFails(t *testing.T) {
	mctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := NewTaskManager(mctx, NewMetrics(1))
	started := make(chan struct{})
	submitStart(t, manager, newFakeTask(17, 60, 1, func(ctx context.Context) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, merr.ErrImportFailed
	}))
	<-started
	cancel()
	snapshot := waitSnapshot(t, manager, 17, 60, datapb.ImportTaskStateV2_Retry)
	require.Contains(t, snapshot.Reason, "context canceled")
}

func TestTaskManagerLoonTransientIsRetry(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))

	submitStart(t, manager, newFakeTask(15, 50, 1, func(context.Context) (any, error) {
		return nil, merr.Wrapf(packed.ErrLoonTransient, "FFI operation failed: simulated transient")
	}))
	waitSnapshot(t, manager, 15, 50, datapb.ImportTaskStateV2_Retry)

	submitStart(t, manager, newFakeTask(16, 51, 1, func(context.Context) (any, error) {
		return nil, merr.WrapErrStorage(packed.ErrLoonTransient, "commit manifest begin")
	}))
	waitSnapshot(t, manager, 16, 51, datapb.ImportTaskStateV2_Retry)
}

func TestTaskManagerSlotsFollowCurrentRun(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	require.Error(t, manager.Submit(newFakeTask(20, 1, 0, func(context.Context) (any, error) { return nil, nil })))

	started := make(chan struct{})
	release := make(chan struct{})
	submitStart(t, manager, newFakeTask(20, 1, 2, func(ctx context.Context) (any, error) {
		close(started)
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	<-started
	require.Equal(t, int64(2), manager.Slots())

	// Same-run Create is idempotent even if the duplicate request carries a
	// different literal slot value.
	require.NoError(t, manager.Submit(newFakeTask(20, 1, 9, func(context.Context) (any, error) {
		t.Fatal("same run must not start twice")
		return nil, nil
	})))
	require.Equal(t, int64(2), manager.Slots())

	close(release)
	waitSnapshot(t, manager, 20, 1, datapb.ImportTaskStateV2_Completed)
	require.Zero(t, manager.Slots())
	require.True(t, manager.Drop(20, 1))
	require.Zero(t, manager.Slots())
}

func TestTaskManagerStatsByKindAndState(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	release := make(chan struct{})
	defer close(release)
	submitStart(t, manager, newFakeTask(40, 1, 1, func(ctx context.Context) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	submitStart(t, manager, newFakeTask(41, 2, 1, func(context.Context) (any, error) {
		return nil, nil
	}))
	waitSnapshot(t, manager, 41, 2, datapb.ImportTaskStateV2_Completed)
	stats := manager.Stats()
	require.Equal(t, 1, stats["import"][datapb.ImportTaskStateV2_InProgress])
	require.Equal(t, 1, stats["import"][datapb.ImportTaskStateV2_Completed])
}

// A queued (submitted, never started) task is visible to Query as Pending and
// reserves slots for backpressure, but does not consume running slots and is
// absent from the started-task state gauge.
func TestTaskManagerQueuedTaskIsPendingAndReservesSlots(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	started := make(chan struct{})
	require.NoError(t, manager.Submit(newFakeTask(50, 1, 3, func(context.Context) (any, error) {
		close(started)
		return nil, nil
	})))
	snapshot, ok := manager.Query(50, 1)
	require.True(t, ok)
	require.Equal(t, datapb.ImportTaskStateV2_Pending, snapshot.State)
	require.Zero(t, manager.Slots(), "a queued task holds no running slot")
	require.Equal(t, int64(3), manager.QueuedSlots())
	require.Empty(t, manager.Stats(), "queued tasks are not part of the started-task gauge")

	require.NoError(t, manager.Start(50, 1))
	<-started
	waitSnapshot(t, manager, 50, 1, datapb.ImportTaskStateV2_Completed)
	require.Zero(t, manager.QueuedSlots())
}

func TestTaskManagerDropQueuedTaskNeverRuns(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	require.NoError(t, manager.Submit(newFakeTask(51, 1, 1, func(context.Context) (any, error) {
		t.Fatal("a dropped queued task must never run")
		return nil, nil
	})))
	require.True(t, manager.Drop(51, 1))
	require.Zero(t, manager.QueuedSlots())
	_, ok := manager.Query(51, 1)
	require.False(t, ok)
}

func TestTaskManagerSubmitValidationAndClosed(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	require.Error(t, manager.Submit(newFakeTask(0, 1, 1, nil)))
	require.Error(t, manager.Submit(newFakeTask(1, 0, 1, nil)))
	require.Error(t, manager.Submit(newFakeTask(1, 1, 0, nil)))

	manager.Close()
	require.Error(t, manager.Submit(newFakeTask(2, 1, 1, nil)))
	require.Error(t, manager.Start(2, 1))
}

// The manager wakes the scheduler on every change to the runnable set: a
// Submit and a completion both notify.
func TestTaskManagerNotifiesOnChange(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	var notifications int32
	manager.SetNotify(func() { atomic.AddInt32(&notifications, 1) })

	started := make(chan struct{})
	submitStart(t, manager, newFakeTask(52, 1, 1, func(context.Context) (any, error) {
		close(started)
		return nil, nil
	}))
	<-started
	waitSnapshot(t, manager, 52, 1, datapb.ImportTaskStateV2_Completed)
	require.GreaterOrEqual(t, atomic.LoadInt32(&notifications), int32(2))
}

// A dropped run must keep counting toward Slots until its Execute actually
// returns: otherwise admit() could start a queued task on slots the canceled
// run still uses, overcommitting the node's memory budget.
func TestTaskManagerDropRunningKeepsSlotUntilExit(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	started := make(chan struct{})
	release := make(chan struct{})
	submitStart(t, manager, newFakeTask(60, 1, 5, func(ctx context.Context) (any, error) {
		close(started)
		<-release
		return nil, ctx.Err()
	}))
	<-started
	require.Equal(t, int64(5), manager.Slots())

	require.True(t, manager.Drop(60, 1))
	require.Equal(t, int64(5), manager.Slots(), "a dropped but still-running run must keep its slot")

	close(release)
	require.Eventually(t, func() bool { return manager.Slots() == 0 }, time.Second, time.Millisecond)
}

// A run superseded by a newer run id keeps its slot until it exits, and the
// replacement queues without consuming a second slot.
func TestTaskManagerSupersededRunKeepsSlotUntilExit(t *testing.T) {
	manager := NewTaskManager(context.Background(), NewMetrics(1))
	oldStarted := make(chan struct{})
	oldRelease := make(chan struct{})
	submitStart(t, manager, newFakeTask(61, 1, 7, func(ctx context.Context) (any, error) {
		close(oldStarted)
		<-oldRelease
		return nil, ctx.Err()
	}))
	<-oldStarted
	require.Equal(t, int64(7), manager.Slots())

	require.NoError(t, manager.Submit(newFakeTask(61, 2, 3, func(context.Context) (any, error) {
		return nil, nil
	})))
	require.Equal(t, int64(7), manager.Slots(), "the superseded run must keep its slot until it exits")

	close(oldRelease)
	require.Eventually(t, func() bool { return manager.Slots() == 0 }, time.Second, time.Millisecond,
		"the queued replacement holds no running slot")
}
