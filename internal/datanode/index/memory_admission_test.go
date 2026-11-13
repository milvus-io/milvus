// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package index

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const admissionGiB = int64(1) << 30

type memoryAdmissionTask struct {
	ctx      context.Context
	name     string
	slot     int64
	resource taskcommon.Resource
	vector   bool
	state    atomic.Int32
	reset    atomic.Bool
	started  chan struct{}
	execute  func(context.Context) error
	cleanup  func()
}

func (t *memoryAdmissionTask) Ctx() context.Context                      { return t.ctx }
func (t *memoryAdmissionTask) Name() string                              { return t.name }
func (t *memoryAdmissionTask) GetSlot() int64                            { return t.slot }
func (t *memoryAdmissionTask) GetResource() taskcommon.Resource          { return t.resource }
func (t *memoryAdmissionTask) IsVectorIndex() bool                       { return t.vector }
func (t *memoryAdmissionTask) OnEnqueue(context.Context) error           { return nil }
func (t *memoryAdmissionTask) SetState(state indexpb.JobState, _ string) { t.state.Store(int32(state)) }
func (t *memoryAdmissionTask) GetState() indexpb.JobState {
	if t.reset.Load() {
		panic("registry read state after cleanup")
	}
	return indexpb.JobState(t.state.Load())
}
func (t *memoryAdmissionTask) PreExecute(context.Context) error { close(t.started); return nil }
func (t *memoryAdmissionTask) Execute(ctx context.Context) error {
	if t.execute != nil {
		return t.execute(ctx)
	}
	return nil
}
func (t *memoryAdmissionTask) PostExecute(context.Context) error { return nil }
func (t *memoryAdmissionTask) Reset() {
	if t.cleanup != nil {
		t.cleanup()
	}
	// Real tasks invalidate fields during cleanup; bookkeeping must use the
	// accepted immutable booking, not a value read from the reset task.
	t.resource = taskcommon.Resource{}
	t.slot = 0
	t.reset.Store(true)
}

func memoryTask(id string, memory int64) *memoryAdmissionTask {
	return &memoryAdmissionTask{
		ctx: context.Background(), name: id, slot: 128,
		resource: taskcommon.Resource{CPU: 8, Memory: memory}, started: make(chan struct{}),
	}
}

func memoryScheduler(t *testing.T, capacity int64, external func() int64) *TaskScheduler {
	t.Helper()
	paramtable.Init()
	s := NewTaskScheduler(context.Background())
	s.totalSlots = func() int64 { return 64 }
	s.SetMemoryAdmission(func() int64 { return capacity }, external)
	require.NoError(t, s.Start())
	t.Cleanup(s.Close)
	return s
}

func memoryGate(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	ch := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return ch, release
}

func awaitMemoryTask(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("memory admission did not make progress")
	}
}

func assertMemoryTaskWaiting(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("task ran without execution memory or a vector permit")
	case <-time.After(150 * time.Millisecond):
	}
}

func TestMemoryAdmissionScalarRunsBesideSlotOversizedVector(t *testing.T) {
	s := memoryScheduler(t, 8*admissionGiB, nil)
	gate, release := memoryGate(t)
	vector := memoryTask("vector", 6*admissionGiB)
	vector.vector = true
	vector.execute = func(context.Context) error { <-gate; return nil }
	require.NoError(t, s.TaskQueue.Enqueue(vector))
	awaitMemoryTask(t, vector.started)
	scalar := memoryTask("scalar", admissionGiB)
	require.NoError(t, s.TaskQueue.Enqueue(scalar))
	awaitMemoryTask(t, scalar.started)
	require.Eventually(t, func() bool { return s.usingMemory.Load() == 6*admissionGiB }, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 128, s.TaskQueue.GetUsingSlot(), "old slot capacity is only 64")
	release()
}

func TestMemoryAdmissionWaitsThroughCancellationAndNativeCleanup(t *testing.T) {
	s := memoryScheduler(t, 8*admissionGiB, nil)
	native, releaseNative := memoryGate(t)
	cleanup, releaseCleanup := memoryGate(t)
	first := memoryTask("first", 6*admissionGiB)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first.ctx = ctx
	first.execute = func(context.Context) error { <-native; return nil } // ignores cancellation
	cleaning := make(chan struct{})
	first.cleanup = func() { close(cleaning); <-cleanup }
	require.NoError(t, s.TaskQueue.Enqueue(first))
	awaitMemoryTask(t, first.started)
	second := memoryTask("second", 4*admissionGiB)
	require.NoError(t, s.TaskQueue.Enqueue(second))
	require.EqualValues(t, 10*admissionGiB, s.TaskQueue.GetUsingResource().Memory)
	cancel()
	assertMemoryTaskWaiting(t, second.started)
	releaseNative()
	awaitMemoryTask(t, cleaning)
	assertMemoryTaskWaiting(t, second.started)
	require.EqualValues(t, 6*admissionGiB, s.usingMemory.Load())
	releaseCleanup()
	awaitMemoryTask(t, second.started)
	require.Eventually(t, func() bool {
		return s.usingMemory.Load() == 0 && s.TaskQueue.GetUsingResource().Memory == 0
	}, 5*time.Second, time.Millisecond)
}

func TestMemoryAdmissionExternalUsageAndQueuedCancellation(t *testing.T) {
	var external atomic.Int64
	external.Store(8 * admissionGiB)
	s := memoryScheduler(t, 8*admissionGiB, external.Load)
	task := memoryTask("waiting", 2*admissionGiB)
	require.NoError(t, s.TaskQueue.Enqueue(task))
	assertMemoryTaskWaiting(t, task.started)
	external.Store(0) // periodic resync observes other executors freeing memory
	awaitMemoryTask(t, task.started)
	require.Eventually(t, func() bool { return task.reset.Load() }, 5*time.Second, time.Millisecond)

	external.Store(8 * admissionGiB)
	pending := memoryTask("canceled", 2*admissionGiB)
	require.NoError(t, s.TaskQueue.Enqueue(pending))
	assertMemoryTaskWaiting(t, pending.started)
	s.Close()
	require.True(t, pending.reset.Load())
	require.Equal(t, indexpb.JobState_JobStateRetry, indexpb.JobState(pending.state.Load()))
	require.Zero(t, s.TaskQueue.GetUsingResource().Memory)
	select {
	case <-pending.started:
		t.Fatal("queued task prepared after scheduler cancellation")
	default:
	}
}

func TestMemoryAdmissionVectorPermitAllowsScalarBypass(t *testing.T) {
	paramtable.Init()
	pool := conc.NewPool[any](1)
	defer pool.Release()
	patch := mockey.Mock(GetVecIndexBuildPool).Return(pool).Build()
	defer patch.UnPatch()
	s := memoryScheduler(t, 16*admissionGiB, nil)
	// Cleanup hooks execute in reverse: release the blocking task before Close.
	gate, release := memoryGate(t)
	first := memoryTask("first-vector", 2*admissionGiB)
	first.vector = true
	first.execute = func(context.Context) error { <-gate; return nil }
	require.NoError(t, s.TaskQueue.Enqueue(first))
	awaitMemoryTask(t, first.started)
	next := memoryTask("next-vector", 2*admissionGiB)
	next.vector = true
	require.NoError(t, s.TaskQueue.Enqueue(next))
	scalar := memoryTask("scalar", admissionGiB)
	require.NoError(t, s.TaskQueue.Enqueue(scalar))
	awaitMemoryTask(t, scalar.started)
	assertMemoryTaskWaiting(t, next.started)
	release()
	awaitMemoryTask(t, next.started)
	s.Close()
}

func TestMemoryAdmissionRejectedVectorPoolReturnsBooking(t *testing.T) {
	paramtable.Init()
	pool := conc.NewPool[any](1)
	pool.Release()
	patch := mockey.Mock(GetVecIndexBuildPool).Return(pool).Build()
	defer patch.UnPatch()
	s := memoryScheduler(t, 8*admissionGiB, nil)
	task := memoryTask("rejected", 2*admissionGiB)
	task.vector = true
	require.NoError(t, s.TaskQueue.Enqueue(task))
	require.Eventually(t, func() bool {
		return task.reset.Load() && s.usingMemory.Load() == 0 && s.TaskQueue.GetUsingResource().Memory == 0
	}, 5*time.Second, time.Millisecond)
	s.Close()
	require.Equal(t, indexpb.JobState_JobStateRetry, indexpb.JobState(task.state.Load()))
}

func TestMemoryAdmissionSameNameExecutionsKeepIndependentBookings(t *testing.T) {
	s := memoryScheduler(t, 16*admissionGiB, nil)
	gate1, release1 := memoryGate(t)
	gate2, release2 := memoryGate(t)
	first := memoryTask("same", 6*admissionGiB)
	second := memoryTask("same", 6*admissionGiB)
	first.execute = func(context.Context) error { <-gate1; return nil }
	second.execute = func(context.Context) error { <-gate2; return nil }
	require.NoError(t, s.TaskQueue.Enqueue(first))
	awaitMemoryTask(t, first.started)
	require.NoError(t, s.TaskQueue.Enqueue(second))
	awaitMemoryTask(t, second.started)
	require.EqualValues(t, 12*admissionGiB, s.usingMemory.Load())
	release1()
	require.Eventually(t, func() bool { return s.TaskQueue.GetUsingResource().Memory == 6*admissionGiB }, 5*time.Second, time.Millisecond)
	_, active := s.TaskQueue.GetTaskNum()
	require.Equal(t, 1, active)
	release2()
	require.Eventually(t, func() bool { return s.usingMemory.Load() == 0 && s.TaskQueue.GetUsingResource().Memory == 0 }, 5*time.Second, time.Millisecond)
	s.TaskQueue.ReleaseResource(first) // duplicate release is harmless
	require.Zero(t, s.TaskQueue.GetUsingResource().Memory)
}

func TestMemoryAdmissionWaitingQueueAndLegacyBookings(t *testing.T) {
	paramtable.Init()
	s := NewTaskScheduler(context.Background())
	defer s.Close()
	s.totalSlots = func() int64 { return 64 }
	s.SetMemoryAdmission(func() int64 { return 8 * admissionGiB }, nil)
	accepted := 0
	for i := 0; i < 16; i++ {
		if err := s.TaskQueue.Enqueue(memoryTask(fmt.Sprint(i), 2*admissionGiB)); err == nil {
			accepted++
		}
	}
	require.Equal(t, 4, accepted)
	require.EqualValues(t, 8*admissionGiB, s.TaskQueue.GetUsingResource().Memory)
	for task := s.TaskQueue.PopUnissuedTask(true); task != nil; task = s.TaskQueue.PopUnissuedTask(true) {
		s.processTask(task)
	}
	legacy := memoryTask("legacy", 0)
	legacy.resource = taskcommon.Resource{}
	legacy.slot = 32
	require.NoError(t, s.TaskQueue.Enqueue(legacy))
	require.EqualValues(t, 4*admissionGiB, s.TaskQueue.GetUsingResource().Memory)
	task := s.TaskQueue.PopUnissuedTask(true)
	require.Same(t, legacy, task)
	s.processTask(task)
	require.Zero(t, s.TaskQueue.GetUsingResource().Memory)
}

func TestMemoryAdmissionCanceledQueuedVectorDoesNotWaitForPool(t *testing.T) {
	paramtable.Init()
	pool := conc.NewPool[any](1)
	defer pool.Release()
	patch := mockey.Mock(GetVecIndexBuildPool).Return(pool).Build()
	defer patch.UnPatch()
	s := memoryScheduler(t, 8*admissionGiB, nil)
	gate, release := memoryGate(t)
	first := memoryTask("running", 2*admissionGiB)
	first.vector = true
	first.execute = func(context.Context) error { <-gate; return nil }
	require.NoError(t, s.TaskQueue.Enqueue(first))
	awaitMemoryTask(t, first.started)
	canceled := memoryTask("canceled-before-start", 2*admissionGiB)
	canceled.vector = true
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled.ctx = ctx
	require.NoError(t, s.TaskQueue.Enqueue(canceled))
	require.Eventually(t, func() bool {
		return canceled.reset.Load() && s.TaskQueue.GetUsingResource().Memory == 2*admissionGiB
	}, 5*time.Second, time.Millisecond)
	select {
	case <-canceled.started:
		t.Fatal("canceled task prepared native work")
	default:
	}
	require.Equal(t, indexpb.JobState_JobStateRetry, indexpb.JobState(canceled.state.Load()))
	release()
	s.Close()
}
