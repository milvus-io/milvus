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

package segcore

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJemallocThreadStats_TracksAllocationAndFree(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if _, _, ok := GetJemallocThreadStatsForTest(); !ok {
		t.Skip("jemalloc thread stats not available on this platform")
	}

	const size = 256 // Small enough to exercise jemalloc's default tcache.
	allocatedBefore, deallocatedBefore, ok := GetJemallocThreadStatsForTest()
	require.True(t, ok)
	ptr := jemallocTestAlloc(size)
	require.NotNil(t, ptr)
	defer func() {
		if ptr != nil {
			jemallocTestFree(ptr)
		}
	}()
	allocatedAfter, deallocatedAfter, ok := GetJemallocThreadStatsForTest()
	require.True(t, ok)
	// Positive control: an outstanding C allocation must remain visible, even
	// when jemalloc serves it from a thread cache.
	require.GreaterOrEqual(t, int64(allocatedAfter-allocatedBefore)-int64(deallocatedAfter-deallocatedBefore), int64(size))
	jemallocTestFree(ptr)
	ptr = nil
	_, deallocatedFreed, ok := GetJemallocThreadStatsForTest()
	require.True(t, ok)
	require.GreaterOrEqual(t, deallocatedFreed-deallocatedAfter, uint64(size))
}

func TestJemallocThreadStats_IgnoresOtherThreads(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if _, _, ok := GetJemallocThreadStatsForTest(); !ok {
		t.Skip("jemalloc thread stats not available on this platform")
	}

	ready, allocate, allocated, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer close(done)
		close(ready)
		<-allocate
		ptr := jemallocTestAlloc(1 << 20)
		defer jemallocTestFree(ptr)
		close(allocated)
		<-release
	}()
	defer func() {
		close(release)
		<-done
	}()
	<-ready
	allocatedBefore, deallocatedBefore, ok := GetJemallocThreadStatsForTest()
	close(allocate)
	require.True(t, ok)
	<-allocated
	allocatedAfter, deallocatedAfter, ok := GetJemallocThreadStatsForTest()
	require.True(t, ok)
	require.Equal(t, allocatedBefore, allocatedAfter)
	require.Equal(t, deallocatedBefore, deallocatedAfter)
}
