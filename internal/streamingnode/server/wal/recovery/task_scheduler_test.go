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

package recovery

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestScopedTaskSchedulerWaitIdle(t *testing.T) {
	inner := nodescheduler.New(2)
	defer inner.Close()

	scheduler := newScopedTaskScheduler(inner)
	releaseTracked := make(chan struct{})
	scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		<-releaseTracked
		return nil
	}))

	releaseUnrelated := make(chan struct{})
	unrelated := inner.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		<-releaseUnrelated
		return nil
	}))

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, scheduler.WaitIdle(waitCtx), context.DeadlineExceeded)

	close(releaseTracked)
	require.NoError(t, scheduler.WaitIdle(context.Background()))
	close(releaseUnrelated)
	require.NoError(t, unrelated.Wait(context.Background()))
}

func TestScopedTaskSchedulerTracksDelayedTaskUntilSuccess(t *testing.T) {
	inner := nodescheduler.New(1)
	defer inner.Close()
	scheduler := newScopedTaskScheduler(inner)

	ready := atomic.Bool{}
	scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		if !ready.Load() {
			return nodescheduler.ErrDelay
		}
		return nil
	}))

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, scheduler.WaitIdle(waitCtx), context.DeadlineExceeded)
	ready.Store(true)
	require.NoError(t, scheduler.WaitIdle(context.Background()))
}

func TestScopedTaskSchedulerCloseCancelsDelayedTask(t *testing.T) {
	inner := nodescheduler.New(1)
	defer inner.Close()
	scheduler := newScopedTaskScheduler(inner)

	started := make(chan struct{})
	scheduler.Submit(nodeschedulerTaskFunc(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return errors.Mark(ctx.Err(), nodescheduler.ErrDelay)
	}))
	<-started

	scheduler.Close()
	require.NoError(t, scheduler.WaitIdle(context.Background()))
}

type nodeschedulerTaskFunc func(context.Context) error

func (f nodeschedulerTaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}
