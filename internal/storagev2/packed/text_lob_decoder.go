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

package packed

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "arrow/c/abi.h"
#include "storage/loon_ffi/text_lob_decoder_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TextLOBDecoder is a read-only logical view of one physical TEXT LOB column.
// The supplied Binary arrays retain the original references for the writer.
type TextLOBDecoder struct {
	handle C.CTextLOBDecoder
}

func NewTextLOBDecoder(fieldID int64, lobBase string, storageConfig *indexpb.StorageConfig) (*TextLOBDecoder, error) {
	if storageConfig == nil || lobBase == "" {
		return nil, merr.WrapErrServiceInternalMsg("TEXT LOB decoder requires storage configuration and base path")
	}
	cBase := C.CString(lobBase)
	defer C.free(unsafe.Pointer(cBase))
	cConfig := GetCStorageConfig(storageConfig)
	defer DeleteCStorageConfig(cConfig)
	var handle C.CTextLOBDecoder
	status := C.NewTextLOBDecoder(C.int64_t(fieldID), cBase, cConfig, &handle)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	return &TextLOBDecoder{handle: handle}, nil
}

func (d *TextLOBDecoder) Decode(ctx context.Context, refs *array.Binary) (*array.String, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d == nil || d.handle == nil || refs == nil {
		return nil, merr.WrapErrServiceInternalMsg("TEXT LOB decoder is closed or references are nil")
	}
	var cRefs cdata.CArrowArray
	var cSchema cdata.CArrowSchema
	cdata.ExportArrowArray(refs, &cRefs, &cSchema)
	defer cdata.ReleaseCArrowArray(&cRefs)
	defer cdata.ReleaseCArrowSchema(&cSchema)

	var cStrings cdata.CArrowArray
	status := C.DecodeTextLOB(d.handle,
		(*C.struct_ArrowArray)(unsafe.Pointer(&cRefs)),
		(*C.struct_ArrowArray)(unsafe.Pointer(&cStrings)))
	runtime.KeepAlive(refs)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		cdata.ReleaseCArrowArray(&cStrings)
		return nil, err
	}
	logical, err := cdata.ImportCArrayWithType(&cStrings, arrow.BinaryTypes.String)
	if err != nil {
		cdata.ReleaseCArrowArray(&cStrings)
		return nil, merr.WrapErrDataIntegrity(err, "import decoded TEXT LOB array")
	}
	strings, ok := logical.(*array.String)
	if !ok {
		logical.Release()
		return nil, merr.WrapErrDataIntegrityMsg("decoded TEXT LOB array has unexpected Arrow type %T", logical)
	}
	return strings, nil
}

func (d *TextLOBDecoder) Close() error {
	if d == nil || d.handle == nil {
		return nil
	}
	handle := d.handle
	d.handle = nil
	status := C.CloseTextLOBDecoder(handle)
	return ConsumeCStatusIntoError(&status)
}
