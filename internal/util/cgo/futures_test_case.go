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

	"go.uber.org/atomic"
)

const (
	caseNoNoInterrupt           int = 0
	caseNoThrowStdException     int = 1
	caseNoThrowFollyException   int = 2
	caseNoThrowSegcoreException int = 3
)

var unreleasedCnt = atomic.NewInt32(0)

type testCase struct {
	interval time.Duration
	loopCnt  int
	caseNo   int
}

func createFutureWithTestCase(ctx context.Context, testCase testCase) Future {
	f := func() CFuturePtr {
		return (CFuturePtr)(C.future_create_test_case(C.int(testCase.interval.Milliseconds()), C.int(testCase.loopCnt), C.int(testCase.caseNo)))
	}
	future := Async(ctx, f,
		WithName("createFutureWithTestCase"),
		WithReleaser(func() {
			unreleasedCnt.Dec()
		}))
	unreleasedCnt.Inc()
	return future
}

func getCInt(p unsafe.Pointer) int {
	return int(*(*C.int)(p))
}

func freeCInt(p unsafe.Pointer) {
	C.free(p)
}
