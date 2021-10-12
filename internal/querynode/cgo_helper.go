// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

/*
#cgo CFLAGS: -I${SRCDIR}/../core/output/include
#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "common/type_c.h"
#include "segcore/segment_c.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"unsafe"
)

// ProtoCGo is protobuf created by go side,
// passed to c side
// memory is managed by go GC
type ProtoCGo struct {
	CProto C.CProto
	blob   []byte
}

// MarshalForCGo convert golang proto to ProtoCGo
func MarshalForCGo(msg proto.Message) (*ProtoCGo, error) {
	blob, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	protoCGo := &ProtoCGo{
		blob: blob,
		CProto: C.CProto{
			proto_size: (C.int64_t)(len(blob)),
			proto_blob: unsafe.Pointer(&blob[0]),
		},
	}
	return protoCGo, nil
}

// destruct free ProtoCGo go memory
func (protoCGo *ProtoCGo) destruct() {
	// NOTE: at ProtoCGo, blob is go heap memory, no need to destruct
	protoCGo.blob = nil
}

// HandleCStatus deal with the error returned from CGO
func HandleCStatus(status *C.CStatus, extraInfo string) error {
	if status.error_code == 0 {
		return nil
	}
	errorCode := status.error_code
	errorName, ok := commonpb.ErrorCode_name[int32(errorCode)]
	if !ok {
		errorName = "UnknownError"
	}
	errorMsg := C.GoString(status.error_msg)
	defer C.free(unsafe.Pointer(status.error_msg))

	finalMsg := fmt.Sprintf("[%s] %s", errorName, errorMsg)
	logMsg := fmt.Sprintf("%s, C Runtime Exception: %s\n", extraInfo, finalMsg)
	log.Warn(logMsg)
	return errors.New(finalMsg)
}

// HandleCProtoResult deal with the result proto returned from CGO
func HandleCProtoResult(cRes *C.CProtoResult, msg proto.Message) error {
	// Standalone CProto is protobuf created by C side,
	// Passed from c side
	// memory is managed manually
	err := HandleCStatus(&cRes.status, "")
	if err != nil {
		return err
	}
	cpro := cRes.proto
	blob := C.GoBytes(unsafe.Pointer(cpro.proto_blob), C.int32_t(cpro.proto_size))
	defer C.free(cpro.proto_blob)
	return proto.Unmarshal(blob, msg)
}

// TestBoolArray this function will accept a BoolArray input,
// and return a BoolArray output
// which negates all elements of the input
func TestBoolArray(cpb *ProtoCGo) (*schemapb.BoolArray, error) {
	res := C.CTestBoolArrayPb(cpb.CProto)
	ba := new(schemapb.BoolArray)
	err := HandleCProtoResult(&res, ba)

	return ba, err
}
