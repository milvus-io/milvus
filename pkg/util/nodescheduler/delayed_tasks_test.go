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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDeadlineRetriesOrderAndDoNotBlockReadyTasks(t *testing.T) {
	s := New(1)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type execution struct {
		id int
		at time.Time
	}
	ran := make(chan execution, 4)
	later := time.Now().Add(300 * time.Millisecond)
	earlier := later.Add(-150 * time.Millisecond)
	for _, item := range []struct {
		id int
		at time.Time
	}{{1, later}, {2, earlier}, {3, earlier}} {
		item := item
		attempt := 0
		s.Submit(TaskFunc(func(context.Context) error {
			attempt++
			if attempt == 1 {
				return errors.Wrap(DelayUntil(item.at), "waiting for publication")
			}
			ran <- execution{item.id, time.Now()}
			return nil
		}))
	}
	s.Submit(TaskFunc(func(context.Context) error { ran <- execution{4, time.Now()}; return nil }))
	for _, expected := range []int{4, 2, 3, 1} {
		select {
		case result := <-ran:
			require.Equal(t, expected, result.id)
			if expected != 4 {
				deadline := earlier
				if expected == 1 {
					deadline = later
				}
				require.False(t, result.at.Before(deadline))
			}
		case <-ctx.Done():
			t.Fatal("deadline task was not scheduled")
		}
	}
	require.ErrorIs(t, DelayUntil(later), ErrDelay)
}

func TestCancelAndCloseFinishDelayedTasksImmediately(t *testing.T) {
	s := New(1)
	defer s.Close()
	var executions atomic.Int32
	handles := make([]TaskHandle, 0, 3)
	for range 3 {
		handles = append(handles, s.Submit(TaskFunc(func(context.Context) error {
			executions.Add(1)
			return DelayUntil(time.Now().Add(time.Hour))
		})))
	}
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.delayed) == 3
	}, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	handles[1].Cancel() // Remove a non-root heap entry.
	require.NoError(t, handles[1].Wait(ctx))
	s.mu.Lock()
	require.Len(t, s.delayed, 2)
	s.mu.Unlock()
	s.Close()
	for _, handle := range handles {
		require.NoError(t, handle.Wait(ctx))
		handle.Cancel()
	}
	require.EqualValues(t, 3, executions.Load())
}

func TestExpiredDeadlineRequeuesAtReadyQueueTail(t *testing.T) {
	s := New(1)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var attempts atomic.Int32
	h := s.Submit(TaskFunc(func(context.Context) error {
		if attempts.Add(1) == 1 {
			return DelayUntil(time.Now().Add(-time.Second))
		}
		return nil
	}))
	require.NoError(t, h.Wait(ctx))
	require.EqualValues(t, 2, attempts.Load())
}
