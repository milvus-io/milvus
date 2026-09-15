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

package canalyzer

/*
#cgo pkg-config: milvus_core
#include "segcore/tokenizer_c.h"
*/
import "C"

import (
	"runtime"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// BatchTokenizeBM25 returns sorted sparse rows with the same hashes and term
// frequencies as iterating TokenStream in Go. The analyzer must be exclusively
// owned for this call, as with NewTokenStream. Returned rows are Go-owned.
func (impl *CAnalyzer) BatchTokenizeBM25(texts []string) ([][]byte, error) {
	rows := make([][]byte, len(texts))
	if len(texts) == 0 {
		return rows, nil
	}
	offsets := make([]uint64, len(texts)+1)
	total := 0
	for i, text := range texts {
		if !typeutil.IsUTF8(text) {
			return nil, merr.WrapErrParameterInvalidMsg("string data must be utf8 format: %v", text)
		}
		if len(text) > int(^uint(0)>>1)-total {
			return nil, merr.WrapErrFunctionFailedMsg("BM25 input batch exceeds addressable memory")
		}
		total += len(text)
		offsets[i+1] = uint64(total)
	}
	data := make([]byte, total)
	for i, text := range texts {
		copy(data[offsets[i]:offsets[i+1]], text)
	}
	var result C.CBM25Batch
	status := C.batch_tokenize_bm25(impl.ptr,
		(*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(data))), C.uint64_t(len(data)),
		(*C.uint64_t)(unsafe.Pointer(unsafe.SliceData(offsets))), C.uint64_t(len(texts)), &result)
	runtime.KeepAlive(data)
	runtime.KeepAlive(offsets)
	if err := HandleCStatus(&status, "failed to tokenize BM25 batch"); err != nil {
		return nil, err
	}
	defer C.free_bm25_batch(result.handle)
	if result.handle == nil || result.offsets == nil || uint64(result.data_size) > uint64(^uint(0)>>1) ||
		(result.data_size != 0 && result.data == nil) {
		return nil, merr.WrapErrFunctionFailedMsg("invalid native BM25 batch buffers")
	}
	rowOffsets := unsafe.Slice((*uint64)(unsafe.Pointer(result.offsets)), len(texts)+1)
	if rowOffsets[0] != 0 || rowOffsets[len(texts)] != uint64(result.data_size) {
		return nil, merr.WrapErrFunctionFailedMsg("invalid native BM25 batch offsets")
	}
	buffer := make([]byte, int(result.data_size))
	copy(buffer, unsafe.Slice((*byte)(unsafe.Pointer(result.data)), len(buffer)))
	for i := range texts {
		start, end := rowOffsets[i], rowOffsets[i+1]
		if start > end || end > uint64(len(buffer)) || (end-start)%8 != 0 {
			return nil, merr.WrapErrFunctionFailedMsg("invalid native BM25 row offsets")
		}
		rows[i] = buffer[start:end:end]
	}
	return rows, nil
}
