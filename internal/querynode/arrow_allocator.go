// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynode

// #cgo pkg-config: milvus_common
// #include "common/allocator_c.h"
import "C"
import (
	"reflect"
	"runtime"
	"unsafe"
)

// CgoArrowAllocator is an allocator which exposes the C++ memory pool class
// from the Arrow C++ Library as an allocator for memory buffers to use in Go.
// The build tag 'ccalloc' must be used in order to include it as it requires
// linking against the arrow library.
//
// The primary reason to use this would be as an allocator when dealing with
// exporting data across the cdata interface in order to ensure that the memory
// is allocated safely on the C side so it can be held on the CGO side beyond
// the context of a single function call. If the memory in use isn't allocated
// on the C side, then it is not safe for any pointers to data to be held outside
// of Go beyond the context of a single Cgo function call as it will be invisible
// to the Go garbage collector and could potentially get moved without being updated.
//
// As an alternative, if the arrow C++ libraries aren't available, remember that
// Allocator is an interface, so anything which can allocate data using C/C++ can
// be exposed and then used to meet the Allocator interface if wanting to export data
// across the Cgo interfaces.
type CgoArrowAllocator struct {
	pool CGOMemPool
}

// Allocate does what it says on the tin, allocates a chunk of memory using the underlying
// memory pool, however CGO calls are 'relatively' expensive, which means doing tons of
// small allocations can end up being expensive and potentially slower than just using
// go memory. This means that preallocating via reserve becomes much more important when
// using this allocator.
//
// Future development TODO: look into converting this more into a slab style allocator
// which amortizes the cost of smaller allocations by allocating bigger chunks of memory
// and passes them out.
func (alloc *CgoArrowAllocator) Allocate(size int) []byte {
	b := CgoPoolAlloc(alloc.pool, size)
	return b
}

func (alloc *CgoArrowAllocator) Free(b []byte) {
	CgoPoolFree(alloc.pool, b)
}

func (alloc *CgoArrowAllocator) Reallocate(size int, b []byte) []byte {
	oldSize := len(b)
	out := CgoPoolRealloc(alloc.pool, size, b)

	if size > oldSize {
		// zero initialize the slice like go would do normally
		// C won't zero initialize the memory.
		Set(out[oldSize:], 0)
	}
	return out
}

// AllocatedBytes returns the current total of bytes that have been allocated by
// the memory pool on the C++ side.
func (alloc *CgoArrowAllocator) AllocatedBytes() int64 {
	return CgoPoolCurBytes(alloc.pool)
}

// AssertSize can be used for testing to ensure and check that there are no memory
// leaks using the allocator.
// func (alloc *CgoArrowAllocator) AssertSize(t TestingT, sz int) {
// 	cur := alloc.AllocatedBytes()
// 	if int64(sz) != cur {
// 		t.Helper()
// 		t.Errorf("invalid memory size exp=%d, got=%d", sz, cur)
// 	}
// }

// NewCgoArrowAllocator creates a new allocator which is backed by the C++ Arrow
// memory pool object which could potentially be using jemalloc or mimalloc or
// otherwise as its backend. Memory allocated by this is invisible to the Go
// garbage collector, and as such care should be taken to avoid any memory leaks.
//
// A finalizer is set on the allocator so when the allocator object itself is eventually
// cleaned up by the garbage collector, it will delete the associated C++ memory pool
// object. If the build tag 'cclog' is added, then the memory pool will output a log line
// for every time memory is allocated, freed or reallocated.
func NewCgoArrowAllocator() *CgoArrowAllocator {
	pool := C.arrow_create_memory_pool(C.bool(false))
	alloc := &CgoArrowAllocator{pool: pool}
	runtime.SetFinalizer(alloc, func(a *CgoArrowAllocator) { ReleaseCGOMemPool(a.pool) })
	return alloc
}

// CGOMemPool is an alias to the typedef'd uintptr from the allocator.h file
type CGOMemPool = C.ArrowMemoryPool

// CgoPoolAlloc allocates a block of memory of length 'size' using the memory
// pool that is passed in.
func CgoPoolAlloc(pool CGOMemPool, size int) []byte {
	var ret []byte
	if size == 0 {
		return ret
	}

	var out *C.uint8_t
	C.arrow_pool_allocate(pool, C.int64_t(size), (**C.uint8_t)(unsafe.Pointer(&out)))

	s := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	s.Data = uintptr(unsafe.Pointer(out))
	s.Len = size
	s.Cap = size

	return ret
}

// CgoPoolRealloc calls 'reallocate' on the block of memory passed in which must
// be a slice that was returned by CgoPoolAlloc or CgoPoolRealloc.
func CgoPoolRealloc(pool CGOMemPool, size int, b []byte) []byte {
	if len(b) == 0 {
		return CgoPoolAlloc(pool, size)
	}

	oldSize := C.int64_t(len(b))
	data := (*C.uint8_t)(unsafe.Pointer(&b[0]))
	C.arrow_pool_reallocate(pool, oldSize, C.int64_t(size), &data)

	var ret []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	s.Data = uintptr(unsafe.Pointer(data))
	s.Len = size
	s.Cap = size

	return ret
}

// CgoPoolFree uses the indicated memory pool to free a block of memory. The
// slice passed in *must* be a slice which was returned by CgoPoolAlloc or
// CgoPoolRealloc.
func CgoPoolFree(pool CGOMemPool, b []byte) {
	if len(b) == 0 {
		return
	}

	oldSize := C.int64_t(len(b))
	data := (*C.uint8_t)(unsafe.Pointer(&b[0]))
	C.arrow_pool_free(pool, data, oldSize)
}

// CgoPoolCurBytes returns the current number of bytes allocated by the
// passed in memory pool.
func CgoPoolCurBytes(pool CGOMemPool) int64 {
	return int64(C.arrow_pool_bytes_allocated(pool))
}

// ReleaseCGOMemPool deletes and frees the memory associated with the
// passed in memory pool on the C++ side.
func ReleaseCGOMemPool(pool CGOMemPool) {
	C.arrow_release_pool(pool)
}

// NewCgoArrowAllocator constructs a new memory pool in C++ and returns
// a reference to it which can then be used with the other functions
// here in order to use it.
//
// Optionally if logging is true, a logging proxy will be wrapped around
// the memory pool so that it will output a line every time memory is
// allocated, reallocated or freed along with the size of the allocation.
// func NewCgoArrowAllocator(logging bool) CGOMemPool {
// return C.arrow_create_memory_pool(C.bool(logging))
// }

var (
	memset func(b []byte, c byte) = memory_memset_go
)

// Set assigns the value c to every element of the slice buf.
func Set(buf []byte, c byte) {
	memset(buf, c)
}

// memory_memset_go reference implementation
func memory_memset_go(buf []byte, c byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] = c
	}
}
