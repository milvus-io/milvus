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

package syncutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func waitForFlightWaiters[K comparable, V any](t *testing.T, group *FlightGroup[K, V], key K, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		call := group.flights[key]
		return call != nil && call.waiters == count
	}, time.Second, time.Millisecond)
}

func receiveFlightResult[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shared work")
		var zero T
		return zero
	}
}

func TestFlightCancellationBelongsToAllWaiters(t *testing.T) {
	var group FlightGroup[string, int]
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	firstCtx, firstCancel := context.WithTimeout(t.Context(), time.Hour)
	defer firstCancel()
	secondCtx, secondCancel := context.WithCancel(t.Context())
	defer secondCancel()
	work := func(ctx context.Context) (int, error) {
		entered <- ctx
		select {
		case <-release:
			return 42, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := group.Do(firstCtx, "dictionary", work); firstDone <- err }()
	workerCtx := receiveFlightResult(t, entered)
	go func() {
		value, err := group.Do(secondCtx, "dictionary", work)
		if err == nil && value != 42 {
			err = merr.ErrServiceInternal
		}
		secondDone <- err
	}()
	waitForFlightWaiters(t, &group, "dictionary", 2)
	firstCancel()
	require.ErrorIs(t, receiveFlightResult(t, firstDone), context.Canceled)
	require.NoError(t, workerCtx.Err())
	_, bounded := workerCtx.Deadline()
	require.False(t, bounded)
	unblock()
	require.NoError(t, receiveFlightResult(t, secondDone))
	require.Empty(t, entered, "only one worker should have run")
}

func TestFlightLastCancellationDetachesAbandonedWorker(t *testing.T) {
	var group FlightGroup[string, int]
	entered := make(chan context.Context, 1)
	releaseOld, releaseNew := make(chan struct{}), make(chan struct{})
	var oldOnce, newOnce sync.Once
	unblockOld := func() { oldOnce.Do(func() { close(releaseOld) }) }
	unblockNew := func() { newOnce.Do(func() { close(releaseNew) }) }
	defer unblockOld()
	defer unblockNew()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	oldDone := make(chan error, 1)
	go func() {
		_, err := group.Do(ctx, "dictionary", func(ctx context.Context) (int, error) {
			entered <- ctx
			<-releaseOld // Simulate I/O taking time to unwind after cancellation.
			return 0, ctx.Err()
		})
		oldDone <- err
	}()
	oldCtx := receiveFlightResult(t, entered)
	group.mu.Lock()
	oldCall := group.flights["dictionary"]
	group.mu.Unlock()
	cancel()
	require.ErrorIs(t, receiveFlightResult(t, oldDone), context.Canceled)
	require.ErrorIs(t, oldCtx.Err(), context.Canceled)

	newDone := make(chan error, 1)
	go func() {
		_, err := group.Do(t.Context(), "dictionary", func(ctx context.Context) (int, error) {
			entered <- ctx
			<-releaseNew
			return 42, ctx.Err()
		})
		newDone <- err
	}()
	newCtx := receiveFlightResult(t, entered)
	unblockOld()
	receiveFlightResult(t, oldCall.done)
	waitForFlightWaiters(t, &group, "dictionary", 1)
	require.NoError(t, newCtx.Err(), "completion of the abandoned flight cannot cancel its replacement")
	unblockNew()
	require.NoError(t, receiveFlightResult(t, newDone))
}

func TestFlightFailureIsSharedButNotCached(t *testing.T) {
	var group FlightGroup[string, int]
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	work := func(context.Context) (int, error) {
		<-release
		return 0, merr.ErrServiceUnavailable
	}
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, err := group.Do(t.Context(), "dictionary", work); results <- err }()
	}
	waitForFlightWaiters(t, &group, "dictionary", 2)
	unblock()
	for i := 0; i < 2; i++ {
		require.ErrorIs(t, receiveFlightResult(t, results), merr.ErrServiceUnavailable)
	}
	value, err := group.Do(t.Context(), "dictionary", func(context.Context) (int, error) { return 42, nil })
	require.NoError(t, err)
	require.Equal(t, 42, value)
}

func TestFlightCancelsOnlyAfterLastWaiterLeaves(t *testing.T) {
	var group FlightGroup[string, int]
	firstCtx, firstCancel := context.WithCancel(t.Context())
	defer firstCancel()
	secondCtx, secondCancel := context.WithCancel(t.Context())
	defer secondCancel()
	entered := make(chan context.Context, 1)
	work := func(ctx context.Context) (int, error) {
		entered <- ctx
		<-ctx.Done()
		return 0, ctx.Err()
	}
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := group.Do(firstCtx, "dictionary", work); firstDone <- err }()
	workerCtx := receiveFlightResult(t, entered)
	go func() { _, err := group.Do(secondCtx, "dictionary", work); secondDone <- err }()
	waitForFlightWaiters(t, &group, "dictionary", 2)
	group.mu.Lock()
	call := group.flights["dictionary"]
	group.mu.Unlock()
	firstCancel()
	require.ErrorIs(t, receiveFlightResult(t, firstDone), context.Canceled)
	require.NoError(t, workerCtx.Err())
	secondCancel()
	require.ErrorIs(t, receiveFlightResult(t, secondDone), context.Canceled)
	receiveFlightResult(t, call.done)
	require.ErrorIs(t, workerCtx.Err(), context.Canceled)
}
