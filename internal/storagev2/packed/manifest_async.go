// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
extern int32_t milvusManifestSubmit(void*, LoonAsyncTask, void*);
static inline void milvusRunManifestTask(LoonAsyncTask task, void* data) { task(data); }
*/
import "C"

import (
	"context"
	"runtime/cgo"
	"sync"
	"time"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type manifestTask struct {
	run  C.LoonAsyncTask
	data unsafe.Pointer
}

// ManifestIOContext belongs to a coordinator, not the process. Admission waits
// in Go; native tasks and callbacks use a bounded, caller-owned Go executor.
// Close drains callers, then native callbacks, then workers, in that order.
// Constructing a context is cheap: native resources are acquired on first use.
type ManifestIOContext struct {
	mu        sync.Mutex
	closeOnce sync.Once
	callers   sync.WaitGroup
	stopping  chan struct{}
	closed    bool
	once      sync.Once
	initErr   error
	native    C.LoonAsyncContextHandle
	token     cgo.Handle
	executor  unsafe.Pointer
	tasks     chan manifestTask
	workers   sync.WaitGroup
	slots     chan struct{}
	// taskMu protects demand accounting, including tasks executing native code.
	// Workers grow to meet demand and remain available until Close joins them.
	taskMu       sync.Mutex
	workerCount  int
	pendingTasks int
}

func NewManifestIOContext(concurrency int) *ManifestIOContext {
	return &ManifestIOContext{slots: make(chan struct{}, max(1, concurrency)), stopping: make(chan struct{})}
}

func (io *ManifestIOContext) init() {
	io.executor = C.malloc(C.size_t(unsafe.Sizeof(C.uintptr_t(0))))
	if io.executor == nil {
		io.initErr = merr.Wrap(merr.ErrServiceResourceInsufficient, "allocate manifest executor")
		return
	}
	io.token = cgo.NewHandle(io)
	*(*C.uintptr_t)(io.executor) = C.uintptr_t(io.token)
	io.tasks = make(chan manifestTask, 2*cap(io.slots))
	descriptor := C.LoonAsyncExecutor{context: io.executor, submit: (C.LoonAsyncSubmit)(C.milvusManifestSubmit)}
	io.initErr = HandleLoonFFIResult(C.loon_async_context_create(&descriptor, &io.native))
	if io.initErr != nil {
		io.initErr = merr.Wrap(io.initErr, "create manifest IO context")
		io.token.Delete()
		C.free(io.executor)
		io.executor = nil
		return
	}
}

func (io *ManifestIOContext) runWorker() {
	defer io.workers.Done()
	for task := range io.tasks {
		// The native task performs blocking manifest I/O on this goroutine's
		// OS thread. Bound worker execution by the process-wide manifest FFI
		// budget, not just this context's capacity: many short-lived contexts
		// (GC, copy-segment, backfill, index reads) share one process.
		acquireManifestFFI(context.Background())
		C.milvusRunManifestTask(task.run, task.data)
		releaseManifestFFI()
		io.taskMu.Lock()
		io.pendingTasks--
		io.taskMu.Unlock()
	}
}

// Admission registers callers before waiting. Closing wakes queued callers and
// waits for active transactions without holding a lock across native callbacks.
func (io *ManifestIOContext) acquire(ctx context.Context) error {
	io.mu.Lock()
	if io.closed {
		io.mu.Unlock()
		return merr.WrapErrServiceUnavailable("manifest IO context is closed")
	}
	io.callers.Add(1)
	io.mu.Unlock()
	select {
	case io.slots <- struct{}{}:
	case <-ctx.Done():
		io.callers.Done()
		return ctx.Err()
	case <-io.stopping:
		io.callers.Done()
		return merr.WrapErrServiceUnavailable("manifest IO context is closed")
	}
	if err := ctx.Err(); err != nil {
		io.release()
		return err
	}
	select {
	case <-io.stopping:
		io.release()
		return merr.WrapErrServiceUnavailable("manifest IO context is closed")
	default:
	}
	io.once.Do(io.init)
	if io.initErr != nil {
		io.release()
		return io.initErr
	}
	return nil
}

func (io *ManifestIOContext) release() {
	<-io.slots
	io.callers.Done()
}

func (io *ManifestIOContext) Close() {
	io.closeOnce.Do(func() {
		io.mu.Lock()
		io.closed = true
		close(io.stopping)
		io.mu.Unlock()
		io.callers.Wait()
		if io.native == nil {
			return
		}
		C.loon_async_context_destroy(io.native)
		close(io.tasks)
		io.workers.Wait()
		io.token.Delete()
		C.free(io.executor)
		io.native = nil
	})
}

//export milvusManifestSubmit
func milvusManifestSubmit(executor unsafe.Pointer, task C.LoonAsyncTask, data unsafe.Pointer) C.int32_t {
	io := cgo.Handle(*(*C.uintptr_t)(executor)).Value().(*ManifestIOContext)
	io.taskMu.Lock()
	defer io.taskMu.Unlock()
	select {
	case io.tasks <- manifestTask{task, data}:
		io.pendingTasks++
		if io.pendingTasks > io.workerCount && io.workerCount < cap(io.slots) {
			io.workerCount++
			io.workers.Add(1)
			go io.runWorker()
		}
		return 0
	default:
		return 1 // Native admission rolls back; never retain or execute this task.
	}
}

// Async queue exhaustion and deadline codes are outside the native ExtendStatus
// table used by HandleLoonFFIResult. Both are transient, and their producers
// guarantee that the operation has not started execution.
func handleManifestAsyncResult(result C.LoonFFIResult) error {
	if result.err_code == C.loon_errcode_async_overloaded || result.err_code == C.loon_errcode_async_deadline {
		defer C.loon_ffi_free_result(&result)
		return merr.Wrapf(ErrLoonTransient, "manifest async operation did not start (code=%d): %s", int(result.err_code), C.GoString(result.message))
	}
	return HandleLoonFFIResult(result)
}

func manifestAsyncHandle(ctx context.Context) C.LoonAsyncHandle {
	timeout := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(time.Until(deadline), 24*time.Hour)
	}
	return C.LoonAsyncHandle{timeout_ms: C.uint64_t(max(1, timeout.Milliseconds()))}
}
