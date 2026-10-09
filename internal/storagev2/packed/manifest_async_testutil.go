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

//go:build test
// +build test

package packed

/*
#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
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

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type testManifestBlock struct {
	entered chan struct{}
	release <-chan struct{}
}

func testOpenManifestCommit(io *ManifestIOContext, base string, config *indexpb.StorageConfig) (func(context.Context) (int64, error), func(), error) {
	if err := io.acquire(context.Background()); err != nil {
		return nil, nil, err
	}
	txn, err := io.open(context.Background(), base, 0, config, C.LOON_TRANSACTION_RESOLVE_OVERWRITE)
	if err != nil {
		io.release()
		return nil, nil, err
	}
	return func(ctx context.Context) (int64, error) {
			done := make(chan manifestCommitResult, 1)
			if err := io.submitCommit(ctx, txn, func(result manifestCommitResult) { done <- result }); err != nil {
				return -1, err
			}
			result := <-done
			return result.finish(ctx)
		}, func() {
			C.loon_transaction_destroy(txn)
			io.release()
		}, nil
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
