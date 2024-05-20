package cgo

/*
#cgo pkg-config: milvus_futures

#include "futures/future_c.h"
#include <stdlib.h>

extern void unlockMutex(void*);

static inline void unlockMutexOnC(CLockedGoMutex* m) {
    unlockMutex((void*)(m));
}

static inline void future_go_register_ready_callback(CFuture* f, CLockedGoMutex* m) {
	future_register_ready_callback(f, unlockMutexOnC, m);
}

static inline void future_go_register_releasable_callback(CFuture* f, CLockedGoMutex* m) {
	future_register_releasable_callback(f, unlockMutexOnC, m);
}
*/
import "C"

import (
	"context"
	"runtime"
	"sync"
	"unsafe"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/util/merr"
)

// Would put this in futures.go but for the documented issue with
// exports and functions in preamble
// (https://code.google.com/p/go-wiki/wiki/cgo#Global_functions)
//
//export unlockMutex
func unlockMutex(p unsafe.Pointer) {
	m := (*sync.Mutex)(p)
	m.Unlock()
}

type basicFuture interface {
	// Context return the context of the future.
	Context() context.Context

	// BlockUntilReady block until the future is ready or canceled.
	// caller can call this method multiple times in different concurrent unit.
	BlockUntilReady()

	// cancel the future with error.
	cancel(error)

	// releaseWhenUnderlyingDone release the resources of the future when underlying cgo function is done.
	releaseWhenUnderlyingDone()
}

type Future interface {
	basicFuture

	// BlockAndLeakyGet block until the future is ready or canceled, and return the leaky result.
	//   Caller should only call once for BlockAndLeakyGet, otherwise the behavior is crash.
	//   Caller will get the merr.ErrSegcoreCancel or merr.ErrSegcoreTimeout respectively if the future is canceled or timeout.
	//   Caller will get other error if the underlying cgo function throws, otherwise caller will get result.
	//   Caller should free the result after used (defined by caller), otherwise the memory of result is leaked.
	BlockAndLeakyGet() (unsafe.Pointer, error)
}

type (
	CFuturePtr       unsafe.Pointer
	CGOAsyncFunction = func() CFuturePtr
)

// Async is a helper function to call a C async function that returns a future.
func Async(ctx context.Context, f CGOAsyncFunction, opts ...Opt) Future {
	// create a future for caller to use.
	ctx, cancel := context.WithCancel(ctx)
	future := &futureImpl{
		closure:   f,
		ctx:       ctx,
		ctxCancel: cancel,
		once:      sync.Once{},
		future:    (*C.CFuture)(f()),
		opts:      &options{},
	}
	// apply options.
	for _, opt := range opts {
		opt(future.opts)
	}

	runtime.SetFinalizer(future, func(future *futureImpl) {
		C.future_destroy(future.future)
	})

	// register the future to do timeout notification.
	futureManager.Register(future)
	return future
}

type futureImpl struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	once      sync.Once
	future    *C.CFuture
	closure   CGOAsyncFunction
	opts      *options
}

func (f *futureImpl) Context() context.Context {
	return f.ctx
}

func (f *futureImpl) BlockUntilReady() {
	f.blockUntilReady()
}

func (f *futureImpl) BlockAndLeakyGet() (unsafe.Pointer, error) {
	f.blockUntilReady()

	var ptr unsafe.Pointer
	status := C.future_leak_and_get(f.future, &ptr)
	err := ConsumeCStatusIntoError(&status)

	if errors.Is(err, merr.ErrSegcoreFollyCancel) {
		// mark the error with context error.
		return nil, errors.Mark(err, f.ctx.Err())
	}
	return ptr, err
}

func (f *futureImpl) cancel(err error) {
	if errors.IsAny(err, context.DeadlineExceeded, context.Canceled) {
		C.future_cancel(f.future)
		return
	}
	panic("unreachable: invalid cancel error type")
}

func (f *futureImpl) blockUntilReady() {
	mu := &sync.Mutex{}
	mu.Lock()
	C.future_go_register_ready_callback(f.future, (*C.CLockedGoMutex)(unsafe.Pointer(mu)))
	mu.Lock()

	f.ctxCancel()
}

func (f *futureImpl) releaseWhenUnderlyingDone() {
	mu := &sync.Mutex{}
	mu.Lock()
	C.future_go_register_releasable_callback(f.future, (*C.CLockedGoMutex)(unsafe.Pointer(mu)))
	mu.Lock()

	if f.opts.releaser != nil {
		f.opts.releaser()
	}
	f.ctxCancel()
}
