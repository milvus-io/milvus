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

//go:build test

package packed

/*
#include <stdlib.h>
#include <string.h>
#include "milvus-storage/ffi_talon_c.h"
static inline LoonTalonResult* milvusTestTalonPartial(void) {
 LoonTalonResult* r = calloc(1,sizeof(*r));
 r->operation = LOON_TALON_BATCH_LOAD;
 r->data.load.failed = true;
 r->data.load.count = 2;
 r->data.load.entries.failures = calloc(2,sizeof(LoonTalonLoadFailure));
 r->data.load.entries.failures[0] = (LoonTalonLoadFailure){2,false,strdup("object not found")};
 r->data.load.entries.failures[1] = (LoonTalonLoadFailure){7,true,strdup("worker timeout: completion unknown")};
 return r;
}
static inline LoonTalonResult* milvusTestTalonStat(void) {
 LoonTalonResult* r = calloc(1,sizeof(*r));
 r->operation = LOON_TALON_STAT;
 r->data.stat.size = 12345;
 r->data.stat.version = strdup("etag-123");
 return r;
}
static inline LoonTalonResult* milvusTestTalonRead(void) {
 LoonTalonResult* r = calloc(1,sizeof(*r));
 r->operation = LOON_TALON_READ;
 r->data.bytes_read = 7;
 return r;
}
static inline LoonTalonResult* milvusTestTalonLoad(bool batch) {
 LoonTalonResult* r = calloc(1,sizeof(*r));
 r->operation = batch ? LOON_TALON_BATCH_LOAD : LOON_TALON_LOAD;
 r->data.load.count = batch ? 2 : 1;
 r->data.load.entries.results = calloc(r->data.load.count,sizeof(LoonTalonLoadResult));
 r->data.load.entries.results[0] = (LoonTalonLoadResult){17,1};
 if (batch) r->data.load.entries.results[1] = (LoonTalonLoadResult){0,0};
 return r;
}
extern void milvusTestManifestBlock(uintptr_t);
static inline void milvusTestManifestTask(void* data) {
 uintptr_t token = *(uintptr_t*)data;
 free(data);
 milvusTestManifestBlock(token);
}
static inline LoonAsyncTask milvusTestManifestTaskPointer(void) { return milvusTestManifestTask; }
*/
import "C"

import (
	"context"
	"runtime/cgo"
	"unsafe"
)

func testTalonPartialLoadResult() talonResult {
	return takeTalonResult(C.LoonFFIResult{err_code: C.loon_errcode_transient_timeout, message: C.CString("batch failed")}, C.milvusTestTalonPartial())
}

func testTalonStatResult() talonResult {
	return takeTalonResult(C.LoonFFIResult{}, C.milvusTestTalonStat())
}

func testTalonReadResult() talonResult {
	return takeTalonResult(C.LoonFFIResult{}, C.milvusTestTalonRead())
}

func testTalonLoadResult(batch bool) talonResult {
	return takeTalonResult(C.LoonFFIResult{}, C.milvusTestTalonLoad(C.bool(batch)))
}

type testManifestBlock struct {
	entered chan struct{}
	release <-chan struct{}
}

//export milvusTestManifestBlock
func milvusTestManifestBlock(token C.uintptr_t) {
	h := cgo.Handle(token)
	block := h.Value().(testManifestBlock)
	h.Delete()
	close(block.entered)
	<-block.release
}

func testQueueManifestBlock(io *ManifestIOContext, release <-chan struct{}) (<-chan struct{}, error) {
	if err := io.acquire(context.Background()); err != nil {
		return nil, err
	}
	defer io.release()
	entered := make(chan struct{})
	token := cgo.NewHandle(testManifestBlock{entered, release})
	data := C.malloc(C.size_t(unsafe.Sizeof(C.uintptr_t(0))))
	*(*C.uintptr_t)(data) = C.uintptr_t(token)
	if milvusManifestSubmit(io.executor, C.milvusTestManifestTaskPointer(), data) != 0 {
		token.Delete()
		C.free(data)
		panic("test executor queue full")
	}
	return entered, nil
}
