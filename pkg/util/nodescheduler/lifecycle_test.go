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

package nodescheduler

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type memoryTask struct {
	payload []byte
	execute func(context.Context) error
}

func (t *memoryTask) Execute(ctx context.Context) error { return t.execute(ctx) }

// Return only weak references to the execution data. The completed handle must
// remain usable without keeping either the task or its scheduler alive.
func completedHandleForMemoryTest(t *testing.T, mode string) (*taskHandle, weak.Pointer[memoryTask], weak.Pointer[nodeScheduler]) {
	t.Helper()
	s := New(1)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task := &memoryTask{payload: make([]byte, 1<<20), execute: func(context.Context) error { return nil }}
	started := make(chan struct{})
	switch mode {
	case "error":
		task.execute = func(context.Context) error { return merr.ErrServiceNotReady }
	case "cancel-delayed", "close-delayed":
		task.execute = func(context.Context) error { return DelayUntil(time.Now().Add(time.Hour)) }
	case "cancel-queued", "close-queued":
		s.Submit(TaskFunc(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return nil
		}))
		<-started
	case "cancel-running":
		task.execute = func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return nil
		}
	case "submit-after-close":
		s.Close()
	}
	h := s.Submit(task).(*taskHandle)
	switch mode {
	case "cancel-delayed", "close-delayed":
		// With one worker, this barrier executes only after the task enters the heap.
		barrier := s.Submit(TaskFunc(func(context.Context) error { return nil }))
		require.NoError(t, barrier.Wait(ctx))
	case "cancel-running":
		<-started
	}
	switch mode {
	case "cancel-queued", "cancel-delayed", "cancel-running":
		h.Cancel()
	case "close-queued", "close-delayed":
		s.Close()
	}
	require.NoError(t, h.Wait(ctx))
	h.mu.Lock()
	detached := h.cancelTask == nil
	h.mu.Unlock()
	require.True(t, detached)
	return h, weak.Make(task), weak.Make(s)
}

func TestCompletedHandlesReleaseExecutionReferences(t *testing.T) {
	for _, mode := range []string{
		"success", "error", "cancel-queued", "cancel-delayed", "cancel-running",
		"close-queued", "close-delayed", "submit-after-close",
	} {
		t.Run(mode, func(t *testing.T) {
			h, task, scheduler := completedHandleForMemoryTest(t, mode)
			require.Eventually(t, func() bool {
				runtime.GC()
				return task.Value() == nil && scheduler.Value() == nil
			}, 5*time.Second, time.Millisecond)
			h.Cancel()
			require.NoError(t, h.Wait(context.Background()))
			runtime.KeepAlive(h)
		})
	}
}

func submitMemoryTask(t *testing.T, s Scheduler) (TaskHandle, weak.Pointer[memoryTask]) {
	t.Helper()
	task := &memoryTask{payload: make([]byte, 1<<20), execute: func(context.Context) error { return nil }}
	h := s.Submit(task)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, h.Wait(ctx))
	return h, weak.Make(task)
}

func TestCompletedHandleReleasesTaskWhileSchedulerIsAlive(t *testing.T) {
	s := New(1)
	defer s.Close()
	h, task := submitMemoryTask(t, s)
	require.Eventually(t, func() bool {
		runtime.GC()
		return task.Value() == nil
	}, 5*time.Second, time.Millisecond)
	h.Cancel()
	require.NoError(t, h.Wait(context.Background()))
	runtime.KeepAlive(h)
	runtime.KeepAlive(s)
}

func TestFinishedEntryIgnoresStaleCancelCallback(t *testing.T) {
	s := New(1)
	defer s.Close()
	started, release := make(chan struct{}), make(chan struct{})
	h := s.Submit(TaskFunc(func(context.Context) error {
		close(started)
		<-release
		return nil
	})).(*taskHandle)
	<-started
	h.mu.Lock()
	staleCancel := h.cancelTask
	h.mu.Unlock()
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, h.Wait(ctx))
	// Models Cancel copying its callback, then losing the scheduler lock to finish.
	staleCancel()
	h.Cancel()
	require.NoError(t, h.Wait(ctx))
}

func TestCancelCloseAndCompletionConcurrent(t *testing.T) {
	s := New(2)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var waiters sync.WaitGroup
	for range 64 {
		h := s.Submit(TaskFunc(func(ctx context.Context) error {
			runtime.Gosched()
			return ctx.Err()
		}))
		for range 2 {
			waiters.Go(func() {
				for range 4 {
					h.Cancel()
				}
				require.NoError(t, h.Wait(ctx))
			})
		}
	}
	s.Close()
	waiters.Wait()
	require.Zero(t, s.queue.Len())
	require.Nil(t, s.delayed)
	require.Nil(t, s.timer)
}

func TestEmptyDeadlineHeapReleasesBackingArray(t *testing.T) {
	for _, closeScheduler := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "close"}[closeScheduler], func(t *testing.T) {
			s := New(1)
			defer s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			handles := make([]TaskHandle, 0, 128)
			for range 128 {
				handles = append(handles, s.Submit(TaskFunc(func(context.Context) error {
					return DelayUntil(time.Now().Add(time.Hour))
				})))
			}
			barrier := s.Submit(TaskFunc(func(context.Context) error { return nil }))
			require.NoError(t, barrier.Wait(ctx))
			s.mu.Lock()
			count := len(s.delayed)
			s.mu.Unlock()
			require.Equal(t, len(handles), count)
			if closeScheduler {
				s.Close()
			} else {
				for _, h := range handles {
					h.Cancel()
				}
			}
			for _, h := range handles {
				require.NoError(t, h.Wait(ctx))
			}
			s.mu.Lock()
			released := s.delayed == nil
			s.mu.Unlock()
			require.True(t, released)
		})
	}
}
