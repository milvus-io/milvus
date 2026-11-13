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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const admissionGiB = int64(1) << 30

// Expose only Task, as stats/analyze do, rather than PreparedTask's pipeline.
type nonPreparedMemoryTask struct {
	Task
	reset atomic.Bool
}

func (t *nonPreparedMemoryTask) Reset() {
	t.reset.Store(true)
	t.Task.Reset()
}

func (t *nonPreparedMemoryTask) GetState() indexpb.JobState {
	if t.reset.Load() {
		panic("registry read task state after Reset")
	}
	return t.Task.GetState()
}

func memoryScheduler(t *testing.T, capacity int64) *TaskScheduler {
	t.Helper()
	paramtable.Init()
	s := newTestTaskScheduler(context.Background())
	s.capacity = func() taskcommon.Resource { return taskcommon.Resource{Memory: capacity} }
	require.NoError(t, s.Start())
	t.Cleanup(func() { s.Close(); s.Wait() })
	return s
}

func memoryTask(id string, memory int64) *pipelineTestTask {
	return &pipelineTestTask{id: id, slots: 128, ctx: context.Background(), resource: taskcommon.Resource{CPU: 8, Memory: memory}}
}

// A vector waiting for its backend has a large scalar-slot demand. A scalar
// build can still use the remaining memory without a vector build reservation.
func TestMemoryAdmissionScalarRunsBesideHighSlotVector(t *testing.T) {
	s := memoryScheduler(t, 8*admissionGiB)
	var attempts atomic.Int32
	vector := memoryTask("vector", 6*admissionGiB)
	vector.reserve = func() (bool, error) { attempts.Add(1); return false, nil }
	require.NoError(t, s.TaskQueue.Enqueue(vector))
	require.Eventually(t, func() bool { return attempts.Load() > 0 }, 5*time.Second, time.Millisecond)

	built := make(chan struct{})
	scalar := &scalarPipelineTestTask{memoryTask("scalar", admissionGiB)}
	scalar.build = func() { close(built) }
	require.NoError(t, s.TaskQueue.Enqueue(scalar))
	awaitPipeline(t, built)
	require.Eventually(t, func() bool { return s.executionMemory() == 6*admissionGiB }, 5*time.Second, time.Millisecond)
}

func TestMemoryAdmissionQueuedBookingsDoNotBlockTheirOwnExecution(t *testing.T) {
	s := memoryScheduler(t, 8*admissionGiB)
	gate, release := pipelineGate(t)
	started := make(chan struct{})
	first := memoryTask("first", 6*admissionGiB)
	first.build = func() { close(started); <-gate }
	require.NoError(t, s.TaskQueue.Enqueue(first))
	awaitPipeline(t, started)
	secondStarted := make(chan struct{})
	second := memoryTask("second", 6*admissionGiB)
	second.build = func() { close(secondStarted) }
	require.NoError(t, s.TaskQueue.Enqueue(second))
	require.EqualValues(t, 12*admissionGiB, s.TaskQueue.GetUsingResource().Memory)
	require.EqualValues(t, 6*admissionGiB, s.executionMemory())
	release()
	awaitPipeline(t, secondStarted)
	require.Eventually(t, func() bool {
		return s.executionMemory() == 0 && s.TaskQueue.GetUsingResource().Memory == 0
	}, 5*time.Second, time.Millisecond)
}

func TestMemoryAdmissionOversizedRunsAlone(t *testing.T) {
	s := memoryScheduler(t, 8*admissionGiB)
	gate, release := pipelineGate(t)
	started := make(chan struct{})
	large := memoryTask("oversized", 16*admissionGiB)
	large.build = func() { close(started); <-gate }
	require.NoError(t, s.TaskQueue.Enqueue(large))
	awaitPipeline(t, started)
	scalarStarted := make(chan struct{})
	scalar := &scalarPipelineTestTask{memoryTask("small", admissionGiB)}
	scalar.build = func() { close(scalarStarted) }
	require.NoError(t, s.TaskQueue.Enqueue(scalar))
	// Inspect an admission attempt directly; no sleep is needed to prove it
	// cannot start while the oversized execution owns the node.
	require.Nil(t, s.TaskQueue.PopUnissuedTask(true))
	require.EqualValues(t, 16*admissionGiB, s.executionMemory())
	release()
	awaitPipeline(t, scalarStarted)
}

func TestMemoryAdmissionCanceledNativeCallKeepsItsReservation(t *testing.T) {
	s := memoryScheduler(t, 8*admissionGiB)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	gate, release := pipelineGate(t)
	started := make(chan struct{})
	task := memoryTask("ignores-cancellation", 6*admissionGiB)
	task.ctx = ctx
	task.build = func() { close(started); <-gate }
	require.NoError(t, s.TaskQueue.Enqueue(task))
	awaitPipeline(t, started)
	cancel()
	nextStarted := make(chan struct{})
	next := &scalarPipelineTestTask{memoryTask("next", 4*admissionGiB)}
	next.build = func() { close(nextStarted) }
	require.NoError(t, s.TaskQueue.Enqueue(next))
	require.Nil(t, s.TaskQueue.PopUnissuedTask(true))
	require.EqualValues(t, 6*admissionGiB, s.executionMemory())
	require.EqualValues(t, 10*admissionGiB, s.TaskQueue.GetUsingResource().Memory)
	release()
	awaitPipeline(t, nextStarted)
}

func TestMemoryAdmissionPreservesVectorConcurrencyLimit(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().DataNodeCfg.MaxVecIndexBuildConcurrency.Key
	require.NoError(t, paramtable.Get().Save(key, "2"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	s := memoryScheduler(t, 16*admissionGiB)
	gate, release := pipelineGate(t)
	started := make(chan string, 3)
	for i := range 3 {
		task := memoryTask(fmt.Sprint(i), admissionGiB)
		task.build = func() { started <- task.id; <-gate }
		require.NoError(t, s.TaskQueue.Enqueue(task))
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("vector task did not start")
		}
	}
	scalarDone := make(chan struct{})
	scalar := &scalarPipelineTestTask{memoryTask("scalar", admissionGiB)}
	scalar.build = func() { close(scalarDone) }
	require.NoError(t, s.TaskQueue.Enqueue(scalar))
	awaitPipeline(t, scalarDone)
	select {
	case <-started:
		t.Fatal("third vector exceeded the retained concurrency limit")
	default:
	}
	release()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("vector permit was not returned")
	}
}

func TestMemoryAdmissionRetainsBudgetThroughCleanup(t *testing.T) {
	for _, outcome := range []string{"success", "stats-analyze", "load-error", "build-error", "cancel", "close"} {
		t.Run(outcome, func(t *testing.T) {
			s := memoryScheduler(t, 8*admissionGiB)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			gate, release := pipelineGate(t)
			cleaning := make(chan struct{})
			task := memoryTask(outcome, 6*admissionGiB)
			task.ctx = ctx
			task.cleanup = func() {
				// Releasing after Reset must use the accepted snapshot.
				task.resource = taskcommon.Resource{}
				close(cleaning)
				<-gate
			}
			if outcome == "load-error" {
				task.prepareErr = merr.WrapErrServiceUnavailableMsg("injected download failure")
			}
			if outcome == "build-error" {
				task.buildErr = merr.WrapErrServiceUnavailableMsg("injected build failure")
			}
			if outcome == "cancel" || outcome == "close" {
				attempted := make(chan struct{})
				var once sync.Once
				task.reserve = func() (bool, error) { once.Do(func() { close(attempted) }); return false, nil }
				require.NoError(t, s.TaskQueue.Enqueue(task))
				awaitPipeline(t, attempted)
				if outcome == "cancel" {
					cancel()
				} else {
					s.Close()
				}
			} else {
				var submitted Task = task
				if outcome == "stats-analyze" {
					submitted = &nonPreparedMemoryTask{Task: &scalarPipelineTestTask{task}}
				}
				require.NoError(t, s.TaskQueue.Enqueue(submitted))
			}
			awaitPipeline(t, cleaning)
			if outcome == "stats-analyze" {
				_, active := s.TaskQueue.GetTaskNum()
				require.Zero(t, active)
			}
			require.EqualValues(t, 6*admissionGiB, s.executionMemory())
			require.EqualValues(t, 6*admissionGiB, s.TaskQueue.GetUsingResource().Memory)
			blocked := memoryTask("cannot-start", 4*admissionGiB)
			require.False(t, s.acquireTestResources(blocked))
			s.releaseResources(blocked)
			release()
			require.Eventually(t, func() bool {
				return s.executionMemory() == 0 && s.TaskQueue.GetUsingResource().Memory == 0
			}, 5*time.Second, time.Millisecond)
		})
	}
}

func TestMemoryQueueConcurrentAdmission(t *testing.T) {
	paramtable.Init()
	s := newTestTaskScheduler(context.Background())
	s.capacity = func() taskcommon.Resource { return taskcommon.Resource{Memory: 8 * admissionGiB} }
	t.Cleanup(s.Close)
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.TaskQueue.Enqueue(memoryTask(fmt.Sprint(i), 2*admissionGiB)); err == nil {
				accepted.Add(1)
			} else {
				require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 4, accepted.Load())
	require.EqualValues(t, 8*admissionGiB, s.TaskQueue.GetUsingResource().Memory)
	for task := s.TaskQueue.PopUnissuedTask(true); task != nil; task = s.TaskQueue.PopUnissuedTask(true) {
		s.finishTask(s.beginTask(task), nil)
	}
	require.Zero(t, s.executionMemory())
}

func TestMemoryAdmissionSameNameExecutionsOwnSeparateReservations(t *testing.T) {
	for _, newerFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(newerFirst), func(t *testing.T) {
			paramtable.Init()
			s := newTestTaskScheduler(context.Background())
			s.capacity = func() taskcommon.Resource { return taskcommon.Resource{Memory: 8 * admissionGiB} }
			t.Cleanup(s.Close)
			older := memoryTask("same-name", 3*admissionGiB)
			newer := memoryTask("same-name", 3*admissionGiB)
			start := func(task Task) *taskExecution {
				require.NoError(t, s.TaskQueue.Enqueue(task))
				require.Same(t, task, s.TaskQueue.PopUnissuedTask(true))
				return s.beginTask(task)
			}
			first, second := start(older), start(newer)
			if newerFirst {
				first, second = second, first
			}
			require.EqualValues(t, 6*admissionGiB, s.executionMemory())
			s.finishTask(first, nil)
			require.EqualValues(t, 3*admissionGiB, s.executionMemory())
			require.EqualValues(t, 3*admissionGiB, s.TaskQueue.GetUsingResource().Memory)
			s.finishTask(second, nil)
			require.Zero(t, s.executionMemory())
			require.Zero(t, s.TaskQueue.GetUsingResource().Memory)
			require.Empty(t, s.pendingTasks)
		})
	}
}

func TestMemoryAdmissionReadsCapacityOncePerScan(t *testing.T) {
	paramtable.Init()
	s := newTestTaskScheduler(context.Background())
	t.Cleanup(s.Close)
	reads := 0
	s.capacity = func() taskcommon.Resource {
		reads++
		return taskcommon.Resource{Memory: 8 * admissionGiB}
	}
	newScalar := func(id string, size int64) Task { return &scalarPipelineTestTask{memoryTask(id, size)} }
	running := newScalar("running", 7*admissionGiB)
	require.NoError(t, s.TaskQueue.Enqueue(running))
	require.Same(t, running, s.TaskQueue.PopUnissuedTask(true))
	for _, id := range []string{"blocked-a", "blocked-b"} {
		require.NoError(t, s.TaskQueue.Enqueue(newScalar(id, 3*admissionGiB)))
	}
	small := newScalar("small", admissionGiB)
	require.NoError(t, s.TaskQueue.Enqueue(small))
	reads = 0
	require.Same(t, small, s.TaskQueue.PopUnissuedTask(true))
	require.Equal(t, 1, reads)
	require.Nil(t, s.TaskQueue.PopUnissuedTask(true))
	require.Equal(t, 2, reads)
}

// Build a legal accepted execution for unit tests that drive admission directly.
func (s *TaskScheduler) acquireTestResources(task Task) bool {
	s.resourceMu.Lock()
	if _, ok := s.executions[task]; !ok {
		resource := task.GetResource()
		s.executions[task] = &taskExecution{task: task, resource: resource, slots: max(task.GetSlot(), 0)}
	}
	defer s.resourceMu.Unlock()
	return s.acquireResourcesLocked(task, s.availableExecutionMemoryLocked())
}

func (s *TaskScheduler) executionMemory() int64 {
	s.resourceMu.Lock()
	defer s.resourceMu.Unlock()
	var used int64
	for _, exec := range s.executions {
		if exec.admitted {
			used += exec.resource.Memory
		}
	}
	return used
}
