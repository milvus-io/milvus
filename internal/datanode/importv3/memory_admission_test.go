// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datanode/taskcost"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

const memoryGiB = int64(1) << 30

func memoryScheduler(t *testing.T, slots int64, budget *taskcost.MemoryAdmission) *Scheduler {
	t.Helper()
	s := newTestScheduler(slots)
	s.SetMemoryAdmission(budget)
	t.Cleanup(s.Close)
	return s
}

func memoryGate(t *testing.T) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return ch, release
}

func memoryTask(id, runID, memory int64, gate <-chan struct{}, started chan<- int64) *fakeTask {
	task := newFakeTask(id, runID, 128, func(context.Context) (any, error) {
		started <- id
		if gate != nil {
			<-gate
		}
		return nil, nil
	})
	task.resource = taskcommon.Resource{CPU: 8, Memory: memory}
	return task
}

func waitMemoryTask(t *testing.T, started <-chan int64) int64 {
	t.Helper()
	select {
	case id := <-started:
		return id
	case <-time.After(5 * time.Second):
		t.Fatal("memory admission did not make progress")
		return 0
	}
}

func assertMemoryWaiting(t *testing.T, started <-chan int64) {
	t.Helper()
	select {
	case <-started:
		t.Fatal("task ran without memory")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestMemoryAdmissionContinuesBeyondScalarSlots(t *testing.T) {
	budget := taskcost.NewMemoryAdmission(func() int64 { return 8 * memoryGiB }, nil)
	s := memoryScheduler(t, 64, budget)
	gate, release := memoryGate(t)
	started := make(chan int64, 8)
	for id := int64(1); id <= 5; id++ {
		require.NoError(t, s.Submit(memoryTask(id, 1, 2*memoryGiB, gate, started)))
	}
	s.admit()
	for i := 0; i < 4; i++ {
		waitMemoryTask(t, started)
	}
	assertMemoryWaiting(t, started)
	require.EqualValues(t, 512, s.Slots(), "old capacity is only 64 slots")
	require.EqualValues(t, 128, s.QueuedSlots())
	require.EqualValues(t, 10*memoryGiB, s.Resource().Memory)
	release()
	require.Eventually(t, func() bool { return s.Slots() == 0 }, time.Second, time.Millisecond)
	s.admit()
	require.EqualValues(t, 5, waitMemoryTask(t, started))
}

func TestMemoryAdmissionQueuedWorkDoesNotBlockItself(t *testing.T) {
	budget := taskcost.NewMemoryAdmission(func() int64 { return 8 * memoryGiB }, nil)
	s := memoryScheduler(t, 64, budget)
	gate, release := memoryGate(t)
	started := make(chan int64, 2)
	require.NoError(t, s.Submit(memoryTask(1, 1, 6*memoryGiB, gate, started)))
	require.NoError(t, s.Submit(memoryTask(2, 1, 4*memoryGiB, nil, started)))
	require.EqualValues(t, 10*memoryGiB, s.Resource().Memory)
	s.admit()
	require.EqualValues(t, 1, waitMemoryTask(t, started))
	assertMemoryWaiting(t, started)
	release()
	require.Eventually(t, func() bool { return s.Slots() == 0 }, time.Second, time.Millisecond)
	s.admit()
	require.EqualValues(t, 2, waitMemoryTask(t, started))
}

func TestMemoryAdmissionDroppedRunKeepsReservationUntilReturn(t *testing.T) {
	budget := taskcost.NewMemoryAdmission(func() int64 { return 8 * memoryGiB }, nil)
	s := memoryScheduler(t, 64, budget)
	gate, release := memoryGate(t)
	started := make(chan int64, 2)
	first := memoryTask(1, 1, 6*memoryGiB, gate, started) // ignores ctx cancellation
	submitAdmit(t, s, first)
	waitMemoryTask(t, started)
	require.True(t, s.Drop(1, 1))
	replacement := memoryTask(1, 2, 4*memoryGiB, nil, started)
	submitAdmit(t, s, replacement)
	assertMemoryWaiting(t, started)
	require.EqualValues(t, 10*memoryGiB, s.Resource().Memory)
	release()
	require.Eventually(t, func() bool { return s.Slots() == 0 }, time.Second, time.Millisecond)
	s.admit()
	waitMemoryTask(t, started)
	require.Eventually(t, func() bool { return s.Resource().Memory == 0 }, time.Second, time.Millisecond)
}

func TestMemoryAdmissionLegacyShareAndImmutablePrice(t *testing.T) {
	budget := taskcost.NewMemoryAdmission(func() int64 { return 8 * memoryGiB }, nil)
	s := memoryScheduler(t, 64, budget)
	started := make(chan int64, 2)
	legacy := memoryTask(1, 1, 0, nil, started)
	legacy.resource = taskcommon.Resource{}
	legacy.slot = 32
	require.NoError(t, s.Submit(legacy))
	require.EqualValues(t, 4*memoryGiB, s.Resource().Memory)
	// The worker object changing later cannot rewrite the accepted booking.
	legacy.resource = taskcommon.Resource{Memory: memoryGiB}
	require.EqualValues(t, 4*memoryGiB, s.Resource().Memory)
	s.admit()
	waitMemoryTask(t, started)
	require.Eventually(t, func() bool { return s.Resource().Memory == 0 }, time.Second, time.Millisecond)
}

func TestMemoryAdmissionCapacityRecoveryAndPoolRejection(t *testing.T) {
	var capacity atomic.Int64
	budget := taskcost.NewMemoryAdmission(capacity.Load, nil)
	s := memoryScheduler(t, 64, budget)
	gate, release := memoryGate(t)
	started := make(chan int64, 3)
	first := memoryTask(1, 1, 2*memoryGiB, gate, started)
	submitAdmit(t, s, first)
	waitMemoryTask(t, started)
	next := memoryTask(2, 1, 2*memoryGiB, gate, started)
	submitAdmit(t, s, next)
	assertMemoryWaiting(t, started) // unknown memory: retain legacy slots
	capacity.Store(8 * memoryGiB)
	s.admit()
	require.EqualValues(t, 2, waitMemoryTask(t, started))
	release()
	require.Eventually(t, func() bool { return s.Slots() == 0 }, time.Second, time.Millisecond)
	s.exec.Release()
	require.NoError(t, s.Submit(memoryTask(3, 1, 2*memoryGiB, nil, started)))
	s.admit()
	result, ok := s.Query(3, 1)
	require.True(t, ok)
	require.Equal(t, datapb.ImportTaskStateV2_Retry, result.State)
	require.Zero(t, s.Resource().Memory)
	releaseBudget, ok := budget.TryAcquire(8 * memoryGiB)
	require.True(t, ok)
	releaseBudget()
}
