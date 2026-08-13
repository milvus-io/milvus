// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

func TestTaskManagerRunFenceAndCompletedResult(t *testing.T) {
	manager := NewTaskManager()
	started := make(chan struct{})
	release := make(chan struct{})
	require.NoError(t, manager.Add(10, 20, func(ctx context.Context, runID int64) (*Result, error) {
		require.Equal(t, int64(20), runID)
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &Result{Ref: "result/10", Digest: []byte{1, 2}, Rows: 3, Bytes: 4}, nil
	}))
	<-started
	_, ok := manager.Query(10, 21)
	require.False(t, ok, "a stale run query must be a no-op")
	require.True(t, manager.UpdateProgress(10, 20, .5))
	close(release)
	snapshot := waitSnapshot(t, manager, 10, 20, StateCompleted)
	require.Equal(t, float32(1), snapshot.Progress)
	require.Equal(t, int64(3), snapshot.Result.Rows)
	snapshot.Result.Digest[0] = 99
	snapshotAgain, _ := manager.Query(10, 20)
	require.Equal(t, byte(1), snapshotAgain.Result.Digest[0], "Query must return a cloned digest")
}

func TestTaskManagerDropCancelsRun(t *testing.T) {
	manager := NewTaskManager()
	started := make(chan struct{})
	canceled := make(chan struct{})
	require.NoError(t, manager.Add(11, 22, func(ctx context.Context, _ int64) (*Result, error) {
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
	manager := NewTaskManager()
	oldCanceled := make(chan struct{})
	oldStarted := make(chan struct{})
	newStarted := make(chan struct{})
	newRelease := make(chan struct{})
	require.NoError(t, manager.Add(12, 30, func(ctx context.Context, _ int64) (*Result, error) {
		close(oldStarted)
		<-ctx.Done()
		close(oldCanceled)
		return nil, ctx.Err()
	}))
	<-oldStarted
	// Same and smaller runs are idempotent/stale no-ops. Their callbacks must
	// never run.
	require.NoError(t, manager.Add(12, 30, func(context.Context, int64) (*Result, error) {
		t.Fatal("same run callback must not run twice")
		return nil, nil
	}))
	require.NoError(t, manager.Add(12, 29, func(context.Context, int64) (*Result, error) {
		t.Fatal("stale run callback must not run")
		return nil, nil
	}))
	require.NoError(t, manager.Add(12, 31, func(ctx context.Context, _ int64) (*Result, error) {
		close(newStarted)
		select {
		case <-newRelease:
			return &Result{Ref: "new", Digest: []byte{1}}, nil
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

func TestTaskManagerRetryAndFailureCode(t *testing.T) {
	manager := NewTaskManager()
	require.NoError(t, manager.Add(13, 40, func(context.Context, int64) (*Result, error) {
		return nil, merr.ErrServiceUnavailable
	}))
	snapshot := waitSnapshot(t, manager, 13, 40, StateRetry)
	require.Equal(t, merr.Code(merr.ErrServiceUnavailable), snapshot.FailureCode)

	require.NoError(t, manager.Add(14, 41, func(context.Context, int64) (*Result, error) {
		return nil, merr.ErrImportSysFailed
	}))
	snapshot = waitSnapshot(t, manager, 14, 41, StateFailed)
	require.Equal(t, merr.Code(merr.ErrImportSysFailed), snapshot.FailureCode)
}
