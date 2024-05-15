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

	// blockUntilReleasable block until the future is releasable.
	blockUntilReleasable()
}

type Future[T any] interface {
	basicFuture

	// BlockAndLeakyGet block until the future is ready or canceled, and return the leaky result.
	//   Caller should only call once for BlockAndLeakyGet, otherwise the behavior is crash.
	//   Caller will get the merr.ErrSegcoreCancel or merr.ErrSegcoreTimeout respectively if the future is canceled or timeout.
	//   Caller will get other error if the underlying cgo function throws, otherwise caller will get result.
	//   Caller should free the result after used (defined by caller), otherwise the memory of result is leaked.
	BlockAndLeakyGet() (*T, error)
}

type CGOAsyncFunction = func() *C.CFuture

// Async is a helper function to call a C async function that returns a future.
func Async[T any](ctx context.Context, f CGOAsyncFunction) Future[T] {
	// create a future for caller to use.
	ctx, cancel := context.WithCancel(ctx)
	future := &futureImpl[T]{
		closure:   f,
		ctx:       ctx,
		ctxCancel: cancel,
		once:      sync.Once{},
		future:    f(),
	}

	runtime.SetFinalizer(future, func(future *futureImpl[T]) {
		C.future_destroy(future.future)
	})

	// register the future to do timeout notification.
	futureManager.Register(future)
	return future
}

type futureImpl[T any] struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	once      sync.Once
	future    *C.CFuture
	closure   CGOAsyncFunction
}

func (f *futureImpl[T]) Context() context.Context {
	return f.ctx
}

func (f *futureImpl[T]) BlockUntilReady() {
	f.blockUntilReady()
}

func (f *futureImpl[T]) BlockAndLeakyGet() (*T, error) {
	f.blockUntilReady()

	var ptr unsafe.Pointer
	status := C.future_leak_and_get(f.future, &ptr)
	err := ConsumeCStatusIntoError(&status)

	if errors.Is(err, merr.ErrSegcoreFollyCancel) {
		// mark the error with context error.
		return nil, errors.Mark(err, f.ctx.Err())
	}
	return (*T)(ptr), err
}

func (f *futureImpl[T]) cancel(err error) {
	if errors.IsAny(err, context.DeadlineExceeded, context.Canceled) {
		C.future_cancel(f.future)
		return
	}
	panic("unreachable: invalid cancel error type")
}

func (f *futureImpl[T]) blockUntilReady() {
	mu := &sync.Mutex{}
	mu.Lock()
	C.future_go_register_ready_callback(f.future, (*C.CLockedGoMutex)(unsafe.Pointer(mu)))
	mu.Lock()

	f.ctxCancel()
}

func (f *futureImpl[T]) blockUntilReleasable() {
	mu := &sync.Mutex{}
	mu.Lock()
	C.future_go_register_releasable_callback(f.future, (*C.CLockedGoMutex)(unsafe.Pointer(mu)))
	mu.Lock()

	f.ctxCancel()
}
