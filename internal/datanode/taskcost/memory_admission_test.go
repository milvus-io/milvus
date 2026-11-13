// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package taskcost

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryAdmissionConcurrentReservationsAndIdempotentRelease(t *testing.T) {
	budget := NewMemoryAdmission(func() int64 { return 8 }, nil)
	var successes atomic.Int32
	var entered, finished sync.WaitGroup
	releaseAll := make(chan struct{})
	entered.Add(64)
	finished.Add(64)
	for i := 0; i < 64; i++ {
		go func() {
			defer finished.Done()
			release, ok := budget.TryAcquire(2)
			if ok {
				successes.Add(1)
			}
			entered.Done()
			<-releaseAll
			if ok {
				release()
				release()
			}
		}()
	}
	entered.Wait()
	require.EqualValues(t, 4, successes.Load())
	close(releaseAll)
	finished.Wait()
	release, ok := budget.TryAcquire(8)
	require.True(t, ok)
	release()
	require.Zero(t, budget.used)
}

func TestMemoryAdmissionExternalUsageAndCapacityRecovery(t *testing.T) {
	var capacity, external atomic.Int64
	external.Store(4)
	budget := NewMemoryAdmission(capacity.Load, external.Load)
	first, ok := budget.TryAcquire(6)
	require.True(t, ok)
	capacity.Store(8)
	_, ok = budget.TryAcquire(1)
	require.False(t, ok, "existing and external reservations exceed capacity")
	first()
	external.Store(0)
	release, ok := budget.TryAcquire(8)
	require.True(t, ok)
	release()
}

func TestMemoryAdmissionOversizedReservationCannotWrapUsage(t *testing.T) {
	var external atomic.Int64
	budget := NewMemoryAdmission(func() int64 { return 8 }, external.Load)
	release, ok := budget.TryAcquire(math.MaxInt64)
	require.True(t, ok, "retain the existing idle-worker progress rule")
	external.Store(math.MaxInt64)
	_, ok = budget.TryAcquire(1)
	require.False(t, ok)
	release()
	external.Store(0)
	release, ok = budget.TryAcquire(8)
	require.True(t, ok)
	release()

	unknown := NewMemoryAdmission(nil, nil)
	release, ok = unknown.TryAcquire(math.MaxInt64)
	require.True(t, ok)
	_, ok = unknown.TryAcquire(1)
	require.False(t, ok)
	release()
}
