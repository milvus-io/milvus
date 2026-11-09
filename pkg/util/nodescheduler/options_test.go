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
	"math"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDefaultBackoffReachesCapWithoutStopping(t *testing.T) {
	b := newTaskBackoff()
	for _, expected := range []time.Duration{
		10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond,
		80 * time.Millisecond, 160 * time.Millisecond, 320 * time.Millisecond,
		640 * time.Millisecond, 1280 * time.Millisecond, 2560 * time.Millisecond,
		5 * time.Second, 5 * time.Second,
	} {
		require.Equal(t, expected, b.NextBackOff())
	}
	require.Zero(t, b.MaxElapsedTime)
}

func TestCustomBackoff(t *testing.T) {
	for _, test := range []struct {
		name     string
		config   typeutil.BackoffConfig
		expected []time.Duration
	}{
		{"capped", typeutil.BackoffConfig{InitialInterval: time.Millisecond, Multiplier: 2, MaxInterval: 3 * time.Millisecond}, []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 3 * time.Millisecond}},
		{"fractional", typeutil.BackoffConfig{InitialInterval: 2 * time.Millisecond, Multiplier: 1.5, MaxInterval: 5 * time.Millisecond}, []time.Duration{2 * time.Millisecond, 3 * time.Millisecond, 4500 * time.Microsecond, 5 * time.Millisecond}},
		{"constant", typeutil.BackoffConfig{InitialInterval: time.Millisecond, Multiplier: 1, MaxInterval: time.Millisecond}, []time.Duration{time.Millisecond, time.Millisecond}},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := newTaskBackoff(WithBackoff(test.config))
			for _, expected := range test.expected {
				require.Equal(t, expected, b.NextBackOff())
			}
		})
	}
}

func TestBackoffRejectsInvalidParameters(t *testing.T) {
	for _, config := range []typeutil.BackoffConfig{
		{InitialInterval: 0, Multiplier: 2, MaxInterval: time.Second},
		{InitialInterval: -time.Second, Multiplier: 2, MaxInterval: time.Second},
		{InitialInterval: time.Second, Multiplier: 2, MaxInterval: time.Millisecond},
		{InitialInterval: time.Second, Multiplier: 0, MaxInterval: time.Second},
		{InitialInterval: time.Second, Multiplier: .5, MaxInterval: time.Second},
		{InitialInterval: time.Second, Multiplier: math.NaN(), MaxInterval: time.Second},
		{InitialInterval: time.Second, Multiplier: math.Inf(1), MaxInterval: time.Second},
		{InitialInterval: time.Second, Multiplier: math.Inf(-1), MaxInterval: time.Second},
	} {
		require.Panics(t, func() { WithBackoff(config) })
	}
}

func TestSubmitKeepsBackoffIndependentWhenTaskAndOptionAreReused(t *testing.T) {
	s := New(1)
	defer s.Close()
	var scheduler Scheduler = s
	option := WithBackoff(typeutil.BackoffConfig{
		InitialInterval: 5 * time.Millisecond, Multiplier: 2, MaxInterval: 40 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	attempts := 0
	task := TaskFunc(func(context.Context) error {
		attempts++
		if attempts%3 != 0 {
			return ErrDelay
		}
		return nil
	})
	var previous *backoff.ExponentialBackOff
	for range 2 {
		started := make(chan struct{})
		release := make(chan struct{})
		scheduler.Submit(TaskFunc(func(context.Context) error {
			close(started)
			<-release
			return nil
		}))
		<-started
		handle := scheduler.Submit(task, option)
		s.mu.Lock()
		b := s.queue.Back().Value.(*taskEntry).backoff
		s.mu.Unlock()
		close(release)
		require.NoError(t, handle.Wait(ctx))
		if previous != nil {
			require.NotSame(t, previous, b)
		}
		require.Equal(t, 20*time.Millisecond, b.NextBackOff())
		previous = b
	}
	require.Equal(t, 6, attempts)
}

func TestDeadlineRetriesDoNotAdvanceOrResetBackoff(t *testing.T) {
	s := New(1)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var runs []time.Time
	var deadline time.Time
	handle := s.Submit(TaskFunc(func(context.Context) error {
		runs = append(runs, time.Now())
		switch len(runs) {
		case 1:
			return ErrDelay
		case 2:
			deadline = time.Now().Add(5 * time.Millisecond)
			return merr.Wrap(DelayUntil(deadline), "wait for publication")
		case 3:
			return errors.Mark(merr.ErrServiceNotReady, ErrDelay)
		case 4:
			return DelayUntil(time.Now().Add(-time.Second))
		case 5:
			return merr.Wrap(ErrDelay, "wait for resource")
		default:
			return nil
		}
	}), WithBackoff(typeutil.BackoffConfig{
		InitialInterval: 10 * time.Millisecond, Multiplier: 2, MaxInterval: time.Second,
	}))
	require.NoError(t, handle.Wait(ctx))
	require.Len(t, runs, 6)
	require.GreaterOrEqual(t, runs[1].Sub(runs[0]), 10*time.Millisecond)
	require.False(t, runs[2].Before(deadline))
	require.GreaterOrEqual(t, runs[3].Sub(runs[2]), 20*time.Millisecond)
	require.GreaterOrEqual(t, runs[5].Sub(runs[4]), 40*time.Millisecond)
}

func TestLaneSubmissionPreservesBackoff(t *testing.T) {
	for _, lane := range []Lane{LaneDefault, LaneControl} {
		t.Run(lane.String(), func(t *testing.T) {
			scheduler := newLaneSchedulers(1)
			defer scheduler.Close()
			handle := scheduler.Submit(laneRetryTask{lane}, WithBackoff(typeutil.BackoffConfig{
				InitialInterval: time.Hour, MaxInterval: time.Hour, Multiplier: 1,
			}))
			selected := scheduler[lane]
			require.Eventually(t, func() bool {
				selected.mu.Lock()
				defer selected.mu.Unlock()
				return len(selected.delayed) == 1 && time.Until(selected.delayed[0].nextRun) > 59*time.Minute
			}, time.Second, time.Millisecond)
			handle.Cancel()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, handle.Wait(ctx))
		})
	}
}

type laneRetryTask struct{ lane Lane }

func (t laneRetryTask) SchedulerLane() Lane { return t.lane }

func (laneRetryTask) Execute(context.Context) error { return ErrDelay }
