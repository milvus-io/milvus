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

//go:build test && (linux || darwin)

package segcore

/*
#cgo linux LDFLAGS: -ldl

#ifdef __linux__
#define _GNU_SOURCE
#endif
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#ifdef __linux__
#include <dlfcn.h>
#endif

static int jemalloc_thread_stats_for_test(uint64_t* allocated, uint64_t* deallocated) {
#ifdef __linux__
    typedef int (*mallctl_fn)(const char*, void*, size_t*, void*, size_t);
    mallctl_fn mallctl = (mallctl_fn)dlsym(RTLD_DEFAULT, "mallctl");
    if (mallctl == NULL) {
        return 0;
    }
    size_t size = sizeof(uint64_t);
    if (mallctl("thread.allocated", allocated, &size, NULL, 0) != 0) {
        return 0;
    }
    size = sizeof(uint64_t);
    return mallctl("thread.deallocated", deallocated, &size, NULL, 0) == 0;
#else
    return 0;
#endif
}
*/
import "C"

import "unsafe"

// GetJemallocThreadStatsForTest reads the calling OS thread's cumulative malloc
// and free counters, including tcache operations. Callers must LockOSThread for
// the entire measurement and keep allocation and release on that thread. These
// counters do not measure allocations made by C++ worker threads.
func GetJemallocThreadStatsForTest() (allocated, deallocated uint64, ok bool) {
	var alloc, dealloc C.uint64_t
	success := C.jemalloc_thread_stats_for_test(&alloc, &dealloc)
	return uint64(alloc), uint64(dealloc), success != 0
}

func jemallocTestAlloc(size int) unsafe.Pointer {
	return C.malloc(C.size_t(size))
}

func jemallocTestFree(ptr unsafe.Pointer) {
	C.free(ptr)
}
