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

package resource

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPinnedRefRelease(t *testing.T) {
	var count atomic.Int32
	ref := NewPinnedRef(42, func(v int) { count.Add(1) }, "test")

	ref.Release()
	require.EqualValues(t, 1, count.Load())
	require.True(t, ref.IsReleased())

	// idempotent
	ref.Release()
	require.EqualValues(t, 1, count.Load())
}

func TestSharedPinnedRefsReleasedOnLastHolder(t *testing.T) {
	var count atomic.Int32
	refs, err := NewSharedPinnedRefs("shared", 3, func(string) { count.Add(1) }, "shared-test")
	require.NoError(t, err)

	refs[0].Release()
	require.EqualValues(t, 0, count.Load())
	refs[1].Release()
	require.EqualValues(t, 0, count.Load())
	refs[2].Release()
	require.EqualValues(t, 1, count.Load())
}

func TestPinnedResourceManagerConcurrentPinRelease(t *testing.T) {
	r := NewPinnedResourceManager("concurrent-test")

	const goroutines = 50
	type obj struct{ id int }
	objs := make([]*obj, goroutines)
	for i := range objs {
		objs[i] = &obj{i}
	}

	var released atomic.Int32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		o := objs[i]
		r.Pin(o, func() { released.Add(1) })
		go func() {
			defer wg.Done()
			r.Release(o)
		}()
	}
	wg.Wait()
	require.EqualValues(t, goroutines, released.Load())
}

func TestPinnedRefValue(t *testing.T) {
	ref := NewPinnedRef("hello", func(string) {}, "val-test")
	require.Equal(t, "hello", ref.Value())
	require.False(t, ref.IsReleased())
	ref.Release()
	require.True(t, ref.IsReleased())
}

func TestSharedPinnedRefsInvalidN(t *testing.T) {
	_, err := NewSharedPinnedRefs("val", 0, func(string) {}, "bad")
	require.Error(t, err)

	_, err = NewSharedPinnedRefs("val", -1, func(string) {}, "bad")
	require.Error(t, err)
}

func TestSharedPinnedRefsConcurrentRelease(t *testing.T) {
	const n = 20
	var released atomic.Int32
	refs, err := NewSharedPinnedRefs("shared", n, func(string) { released.Add(1) }, "concurrent-shared")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			refs[i].Release()
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, released.Load())
}

func TestPinnedResourceManagerNilIsNoOp(t *testing.T) {
	r := NewPinnedResourceManager("nil-test")
	type obj struct{}
	var nilObj *obj
	// nil Pin is a no-op — must not panic and must not register anything
	r.Pin(nilObj, func() {})
	require.False(t, r.HasPinned(nilObj))
}

func TestPinnedResourceManagerReleaseUnpinned(t *testing.T) {
	r := NewPinnedResourceManager("release-unpinned")
	type obj struct{}
	o := &obj{}
	// Release on never-pinned object is a no-op — must not panic
	r.Release(o)
	require.False(t, r.HasPinned(o))
}

func TestPinnedResourceManagerReleaseNil(t *testing.T) {
	r := NewPinnedResourceManager("release-nil")
	type obj struct{}
	var nilObj *obj
	// Release nil is a no-op — must not panic
	r.Release(nilObj)
}

func TestPinnedResourceManagerPinnedCount(t *testing.T) {
	r := NewPinnedResourceManager("count-test")
	type obj struct{ id int }

	o1 := &obj{1}
	o2 := &obj{2}
	require.EqualValues(t, 0, r.PinnedCount())

	r.Pin(o1, func() {})
	require.EqualValues(t, 1, r.PinnedCount())

	r.Pin(o2, func() {})
	require.EqualValues(t, 2, r.PinnedCount())

	r.Release(o1)
	require.EqualValues(t, 1, r.PinnedCount())

	r.Release(o2)
	require.EqualValues(t, 0, r.PinnedCount())

	// Double release doesn't go negative
	r.Release(o2)
	require.EqualValues(t, 0, r.PinnedCount())
}

func TestPinnedResourceManagerFinalizerCleansUp(t *testing.T) {
	r := NewPinnedResourceManager("finalizer-test")
	done := make(chan struct{})

	// Pin an object, then drop all references so GC can collect it.
	// The finalizer on the object should trigger Release → cleanup.
	func() {
		o := &struct{ x int }{42}
		r.Pin(o, func() { close(done) })
		runtime.KeepAlive(o)
	}()

	for i := 0; i < 20; i++ {
		runtime.GC()
		select {
		case <-done:
			return // success
		default:
			runtime.Gosched()
			time.Sleep(time.Millisecond)
		}
	}
	t.Fatal("finalizer should have triggered cleanup")
}

func TestPinnedResourceManagerReleaseDisarmsFinalizer(t *testing.T) {
	r := NewPinnedResourceManager("disarm-test")
	var cleaned atomic.Int32

	o := &struct{ x int }{7}
	r.Pin(o, func() { cleaned.Add(1) })

	// Explicit Release should trigger cleanup and disarm the finalizer.
	r.Release(o)
	require.EqualValues(t, 1, cleaned.Load())

	// Drop reference and force GC — finalizer must NOT fire again.
	runtime.KeepAlive(o)
	for i := 0; i < 5; i++ {
		runtime.GC()
		runtime.Gosched()
	}
	require.EqualValues(t, 1, cleaned.Load(), "cleanup must not be called twice")
}
