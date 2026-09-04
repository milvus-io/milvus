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

package textindex

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "textindex/fst/text_fst_c.h"
*/
import "C"

import (
	"runtime"
	"sync"
	"unsafe"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// FstReader owns one immutable Milvus text FST reader. The underlying bytes are
// either copied into native heap memory or backed by a read-only file mapping.
type FstReader struct {
	handle       C.CTextFstHandle
	dataSize     int64
	termCount    int64
	memoryMapped bool
	closeOnce    sync.Once
}

func LoadTextFstBytes(data []byte) (*FstReader, error) {
	var ptr *C.uint8_t
	if len(data) > 0 {
		ptr = (*C.uint8_t)(unsafe.Pointer(&data[0]))
	}
	result := C.LoadTextFstBytes(ptr, C.int64_t(len(data)))
	return newFstReader(result)
}

func LoadTextFstFile(filePath string, memoryMapped bool) (*FstReader, error) {
	path := C.CString(filePath)
	defer C.free(unsafe.Pointer(path))
	result := C.LoadTextFstFile(path, C.bool(memoryMapped))
	return newFstReader(result)
}

func newFstReader(result C.CTextFstLoadResult) (*FstReader, error) {
	if result.status.error_code != 0 {
		errorCode := int32(result.status.error_code)
		errorMessage := C.GoString(result.status.error_msg)
		C.free(unsafe.Pointer(result.status.error_msg))
		if bool(result.is_data_integrity_error) {
			return nil, merr.WrapErrDataIntegrityMsg("%s", errorMessage)
		}
		return nil, merr.SegcoreError(errorCode, errorMessage)
	}
	if result.handle == nil || result.data_size < 0 || result.term_count < 0 {
		if result.handle != nil {
			C.DeleteTextFst(result.handle)
		}
		return nil, merr.WrapErrServiceInternalMsg("loaded text FST returned invalid metadata")
	}
	reader := &FstReader{
		handle:       result.handle,
		dataSize:     int64(result.data_size),
		termCount:    int64(result.term_count),
		memoryMapped: bool(result.is_memory_mapped),
	}
	runtime.SetFinalizer(reader, (*FstReader).Close)
	return reader, nil
}

func (r *FstReader) DataSize() int64 {
	if r == nil {
		return 0
	}
	return r.dataSize
}

func (r *FstReader) TermCount() int64 {
	if r == nil {
		return 0
	}
	return r.termCount
}

func (r *FstReader) IsMemoryMapped() bool {
	return r != nil && r.memoryMapped
}

func (r *FstReader) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		runtime.SetFinalizer(r, nil)
		if r.handle != nil {
			C.DeleteTextFst(r.handle)
			r.handle = nil
		}
	})
}
