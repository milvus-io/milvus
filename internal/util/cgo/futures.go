package cgo

/*
#cgo pkg-config: milvus_core

#include "futures/future_c.h"
#include <stdlib.h>

extern void unlockMutex(void*);

static inline void unlockMutexOnC(CLockedGoMutex* m) {
    unlockMutex((void*)(m));
}

static inline void future_go_register_ready_callback(CFuture* f, CLockedGoMutex* m) {
	future_register_ready_callback(f, unlockMutexOnC, m);
}
*/
import "C"

import (
	"context"
	"sync"
	"unsafe"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

var ErrConsumed = errors.New("future is already consumed")

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
}

type Future interface {
	basicFuture

	// BlockAndLeakyGet block until the future is ready or canceled, and return the leaky result.
	//   Caller should only call once for BlockAndLeakyGet, otherwise the ErrConsumed will returned.
	//   Caller will get the merr.ErrSegcoreCancel or merr.ErrSegcoreTimeout respectively if the future is canceled or timeout.
	//   Caller will get other error if the underlying cgo function throws, otherwise caller will get result.
	//   Caller should free the result after used (defined by caller), otherwise the memory of result is leaked.
	BlockAndLeakyGet() (unsafe.Pointer, error)

	// Release the resource of the future.
	// !!! Release is not concurrent safe with other methods.
	// It should be called only once after all method of future is returned.
	Release()
}

type (
	CFuturePtr       unsafe.Pointer
	CGOAsyncFunction = func() CFuturePtr
)

// Async is a helper function to call a C async function that returns a future.
func Async(ctx context.Context, f CGOAsyncFunction, opts ...Opt) Future {
	initCGO()

	options := getDefaultOpt()
	// apply options.
	for _, opt := range opts {
		opt(options)
	}

	// create a future for caller to use.
	var cFuturePtr *C.CFuture
	getCGOCaller().call(options.name, func() {
		cFuturePtr = (*C.CFuture)(f())
	})

	ctx, cancel := context.WithCancel(ctx)
	future := &futureImpl{
		closure:   f,
		ctx:       ctx,
		ctxCancel: cancel,
		future:    cFuturePtr,
		opts:      options,
		state:     newFutureState(),
	}

	// register the future to do timeout notification.
	futureManager.Register(future)
	return future
}

type futureImpl struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	future    *C.CFuture
	closure   CGOAsyncFunction
	opts      *options
	state     futureState
}

// Context return the context of the future.
func (f *futureImpl) Context() context.Context {
	return f.ctx
}

// BlockUntilReady block until the future is ready or canceled.
func (f *futureImpl) BlockUntilReady() {
	f.blockUntilReady()
}

// BlockAndLeakyGet block until the future is ready or canceled, and return the leaky result.
func (f *futureImpl) BlockAndLeakyGet() (unsafe.Pointer, error) {
	f.blockUntilReady()

	guard := f.state.LockForConsume()
	if guard == nil {
		return nil, ErrConsumed
	}
	defer guard.Unlock()

	var ptr unsafe.Pointer
	var status C.CStatus
	getCGOCaller().call("future_leak_and_get", func() {
		status = C.future_leak_and_get(f.future, &ptr)
	})
	err := ConsumeCStatusIntoError(&status)

	if errors.Is(err, merr.ErrSegcoreFollyCancel) {
		// mark the error with context error.
		return nil, errors.Mark(err, f.ctx.Err())
	}
	return ptr, err
}

// Release the resource of the future.
func (f *futureImpl) Release() {
	// block until ready to release the future.
	f.blockUntilReady()

	guard := f.state.LockForRelease()
	if guard == nil {
		return
	}
	defer guard.Unlock()

	// release the future.
	getCGOCaller().call("future_destroy", func() {
		C.future_destroy(f.future)
	})
}

// cancel the future with error.
func (f *futureImpl) cancel(err error) {
	// only unready future can be canceled.
	guard := f.state.LockForCancel()
	if guard == nil {
		return
	}
	defer guard.Unlock()

	if errors.IsAny(err, context.DeadlineExceeded, context.Canceled) {
		getCGOCaller().call("future_cancel", func() {
			C.future_cancel(f.future)
		})
		return
	}
	panic("unreachable: invalid cancel error type")
}

// blockUntilReady block until the future is ready or canceled.
func (f *futureImpl) blockUntilReady() {
	if !f.state.CheckUnready() {
		// only unready future should be block until ready.
		return
	}

	mu := &sync.Mutex{}
	mu.Lock()
	getCGOCaller().call("future_go_register_ready_callback", func() {
		C.future_go_register_ready_callback(f.future, (*C.CLockedGoMutex)(unsafe.Pointer(mu)))
	})
	mu.Lock()

	// mark the future as ready at go side to avoid more cgo calls.
	f.state.IntoReady()
	// notify the future manager that the future is ready.
	f.ctxCancel()
}
