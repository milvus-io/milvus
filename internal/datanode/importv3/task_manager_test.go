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

func waitSnapshot(t *testing.T, manager *TaskManager, taskID, runID int64, state State) Snapshot {
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
	manager := NewTaskManagerWithContext(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	require.NoError(t, manager.Add(10, 20, 2, func(ctx context.Context, runID int64) ([]*datapb.SegmentResult, error) {
		require.Equal(t, int64(20), runID)
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
	require.Equal(t, StateRunning, running.State)
	require.Empty(t, running.Segments)
	_, ok = manager.Query(10, 21)
	require.False(t, ok, "a stale run query must be a no-op")
	close(release)
	snapshot := waitSnapshot(t, manager, 10, 20, StateCompleted)
	require.Equal(t, int64(10), snapshot.Segments[0].GetRows())
	response := &datapb.QueryImportTaskV3Response{State: datapb.ImportTaskStateV2_Completed, Segments: snapshot.Segments}
	payload, err := proto.Marshal(response)
	require.NoError(t, err)
	roundTrip := &datapb.QueryImportTaskV3Response{}
	require.NoError(t, proto.Unmarshal(payload, roundTrip))
	require.Equal(t, int64(10), roundTrip.GetSegments()[0].GetRows())
}

func TestTaskManagerDropCancelsRun(t *testing.T) {
	manager := NewTaskManagerWithContext(context.Background())
	started := make(chan struct{})
	canceled := make(chan struct{})
	require.NoError(t, manager.Add(11, 22, 1, func(ctx context.Context, _ int64) ([]*datapb.SegmentResult, error) {
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
	manager := NewTaskManagerWithContext(context.Background())
	oldCanceled := make(chan struct{})
	oldStarted := make(chan struct{})
	newStarted := make(chan struct{})
	newRelease := make(chan struct{})
	require.NoError(t, manager.Add(12, 30, 2, func(ctx context.Context, _ int64) ([]*datapb.SegmentResult, error) {
		close(oldStarted)
		<-ctx.Done()
		close(oldCanceled)
		return nil, ctx.Err()
	}))
	<-oldStarted
	// Same and smaller runs are idempotent/stale no-ops. Their callbacks must
	// never run.
	require.NoError(t, manager.Add(12, 30, 9, func(context.Context, int64) ([]*datapb.SegmentResult, error) {
		t.Fatal("same run callback must not run twice")
		return nil, nil
	}))
	require.NoError(t, manager.Add(12, 29, 9, func(context.Context, int64) ([]*datapb.SegmentResult, error) {
		t.Fatal("stale run callback must not run")
		return nil, nil
	}))
	require.NoError(t, manager.Add(12, 31, 4, func(ctx context.Context, _ int64) ([]*datapb.SegmentResult, error) {
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
	waitSnapshot(t, manager, 12, 31, StateCompleted)
}

func TestTaskManagerRetryAndFailedState(t *testing.T) {
	manager := NewTaskManagerWithContext(context.Background())
	require.NoError(t, manager.Add(13, 40, 1, func(context.Context, int64) ([]*datapb.SegmentResult, error) {
		return nil, merr.ErrServiceUnavailable
	}))
	waitSnapshot(t, manager, 13, 40, StateRetry)

	require.NoError(t, manager.Add(14, 41, 1, func(context.Context, int64) ([]*datapb.SegmentResult, error) {
		return nil, merr.ErrImportSysFailed
	}))
	waitSnapshot(t, manager, 14, 41, StateFailed)
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
		want State
	}{
		{"typed ErrIoFailed retries", merr.WrapErrIoFailed("manifest", errors.New("connection reset")), StateRetry},
		{"raw non-milvus error retries", errors.New("write failed: 503 service unavailable"), StateRetry},
		// ID exhaustion used to be an explicit allowlist special case; it stays
		// retryable through the denylist because ErrServiceInternal is not a
		// terminal code. DataCoord's prepareRetry re-derives the ID width.
		{"ID exhaustion retries", allocator.NewIDExhaustedError(0, 10, 5), StateRetry},
		{"ErrImportFailed fails", merr.WrapErrImportFailedMsg("parquet: invalid magic"), StateFailed},
		{"ErrIoKeyNotFound fails", merr.WrapErrIoKeyNotFound("manifest", "not found"), StateFailed},
		// Caller-input defects (oversized file, dimension mismatch) are
		// deterministic; without this case every retry burns a new segment
		// and log range with no cap.
		{"ErrParameterInvalid fails", merr.WrapErrParameterInvalidMsg("file exceeds maxImportFileSizeInGB"), StateFailed},
	}
	manager := NewTaskManagerWithContext(context.Background())
	for i, c := range cases {
		taskID, runID := int64(30+i), int64(70+i)
		require.NoError(t, manager.Add(taskID, runID, 1, func(context.Context, int64) ([]*datapb.SegmentResult, error) {
			return nil, c.err
		}))
		snapshot := waitSnapshot(t, manager, taskID, runID, c.want)
		require.Equal(t, c.want, snapshot.State)
		require.Equal(t, c.err.Error(), snapshot.Reason)
	}
}

// A canceled run must never record StateFailed even when its error is
// terminal: DataCoord's stale query would fail the whole job for a run it
// already superseded or dropped.
func TestTaskManagerCanceledRunNeverFails(t *testing.T) {
	mctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := NewTaskManagerWithContext(mctx)
	started := make(chan struct{})
	require.NoError(t, manager.Add(17, 60, 1, func(ctx context.Context, _ int64) ([]*datapb.SegmentResult, error) {
		close(started)
		<-ctx.Done()
		return nil, merr.ErrImportFailed
	}))
	<-started
	cancel()
	snapshot := waitSnapshot(t, manager, 17, 60, StateRetry)
	require.Contains(t, snapshot.Reason, "context canceled")
}

func TestTaskManagerLoonTransientIsRetry(t *testing.T) {
	manager := NewTaskManagerWithContext(context.Background())

	require.NoError(t, manager.Add(15, 50, 1, func(context.Context, int64) ([]*datapb.SegmentResult, error) {
		return nil, merr.Wrapf(packed.ErrLoonTransient, "FFI operation failed: simulated transient")
	}))
	waitSnapshot(t, manager, 15, 50, StateRetry)

	require.NoError(t, manager.Add(16, 51, 1, func(context.Context, int64) ([]*datapb.SegmentResult, error) {
		return nil, merr.WrapErrStorage(packed.ErrLoonTransient, "commit manifest begin")
	}))
	waitSnapshot(t, manager, 16, 51, StateRetry)
}

func TestTaskManagerSlotsFollowCurrentRun(t *testing.T) {
	manager := NewTaskManagerWithContext(context.Background())
	require.Error(t, manager.Add(20, 1, 0, func(context.Context, int64) ([]*datapb.SegmentResult, error) { return nil, nil }))

	started := make(chan struct{})
	release := make(chan struct{})
	require.NoError(t, manager.Add(20, 1, 2, func(ctx context.Context, _ int64) ([]*datapb.SegmentResult, error) {
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
	require.NoError(t, manager.Add(20, 1, 9, func(context.Context, int64) ([]*datapb.SegmentResult, error) {
		t.Fatal("same run must not start twice")
		return nil, nil
	}))
	require.Equal(t, int64(2), manager.Slots())

	close(release)
	waitSnapshot(t, manager, 20, 1, StateCompleted)
	require.Zero(t, manager.Slots())
	require.True(t, manager.Drop(20, 1))
	require.Zero(t, manager.Slots())
}
