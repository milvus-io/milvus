//go:build test
// +build test

package cgo

/*
#cgo pkg-config: milvus_futures

#include "futures/future_c.h"
#include <stdlib.h>

*/
import "C"

import (
	"context"
	"time"
	"unsafe"
)

const (
	caseNoNoInterrupt           int = 0
	caseNoThrowStdException     int = 1
	caseNoThrowFollyException   int = 2
	caseNoThrowSegcoreException int = 3
)

type testCase struct {
	interval time.Duration
	loopCnt  int
	caseNo   int
}

func createFutureWithTestCase(ctx context.Context, testCase testCase) Future[C.int] {
	f := func() *C.CFuture {
		return C.future_create_test_case(C.int(testCase.interval.Milliseconds()), C.int(testCase.loopCnt), C.int(testCase.caseNo))
	}
	future := Async[C.int](ctx, f)
	return future
}

func freeCInt(p *C.int) {
	C.free(unsafe.Pointer(p))
}
