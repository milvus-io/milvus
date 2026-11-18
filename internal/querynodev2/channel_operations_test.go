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

package querynodev2

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelOperationsCancellationAndIsolation(t *testing.T) {
	operations := newChannelOperations()
	release, err := operations.acquire(context.Background(), "channel")
	require.NoError(t, err)
	// An unrelated channel can proceed while this channel is held.
	other, err := operations.acquire(context.Background(), "other")
	require.NoError(t, err)
	other()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		release, err := operations.acquire(ctx, "channel")
		if err == nil {
			release()
		}
		done <- err
	}()
	require.Eventually(t, func() bool {
		operations.mu.Lock()
		defer operations.mu.Unlock()
		return operations.channels["channel"].refs == 2
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled caller did not stop waiting")
	}
	operations.mu.Lock()
	require.Equal(t, 1, operations.channels["channel"].refs)
	operations.mu.Unlock()
	release()
	require.Empty(t, operations.channels)
}

func TestChannelOperationsSerializeAndRetire(t *testing.T) {
	operations := newChannelOperations()
	var wg sync.WaitGroup
	var value int // All access must be serialized by the same channel lock.
	const callers = 64
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := operations.acquire(context.Background(), "channel")
			if err != nil {
				errs <- err
				return
			}
			defer release()
			value++
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, callers, value)
	require.Empty(t, operations.channels)
}

func TestChannelOperationsCancelDuringRelease(t *testing.T) {
	operations := newChannelOperations()
	release, err := operations.acquire(context.Background(), "channel")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		release, err := operations.acquire(ctx, "channel")
		if err == nil {
			release()
		}
		done <- err
	}()
	require.Eventually(t, func() bool {
		operations.mu.Lock()
		defer operations.mu.Unlock()
		return operations.channels["channel"].refs == 2
	}, time.Second, time.Millisecond)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		cancel()
	}()
	go func() {
		defer wg.Done()
		<-start
		release()
	}()
	close(start)
	wg.Wait()
	select {
	case err := <-done:
		// Either acquisition or cancellation can win the race.
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not finish after release and cancellation")
	}
	require.Empty(t, operations.channels)
	nextCtx, nextCancel := context.WithTimeout(context.Background(), time.Second)
	defer nextCancel()
	nextRelease, err := operations.acquire(nextCtx, "channel")
	require.NoError(t, err)
	nextRelease()
	require.Empty(t, operations.channels)
}
