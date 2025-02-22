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

package segcore

/*
#cgo pkg-config: milvus_core

#include "segcore/collection_c.h"
#include "common/type_c.h"
#include "common/protobuf_utils_c.h"
#include "segcore/segment_c.h"
#include "storage/storage_c.h"
*/
import "C"

import (
	"math"
	"reflect"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/cgoconverter"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type CStatus = C.CStatus

// ConsumeCStatusIntoError consumes the CStatus and returns the error
func ConsumeCStatusIntoError(status *C.CStatus) error {
	if status == nil || status.error_code == 0 {
		return nil
	}
	errorCode := status.error_code
	errorMsg := C.GoString(status.error_msg)
	C.free(unsafe.Pointer(status.error_msg))
	return merr.SegcoreError(int32(errorCode), errorMsg)
}

func UnmarshalProtoLayout(protoLayout any, msg proto.Message) error {
	layout := unsafe.Pointer(reflect.ValueOf(protoLayout).Pointer())
	cProtoLayout := (*C.ProtoLayout)(layout)
	blob := (*(*[math.MaxInt32]byte)(cProtoLayout.blob))[:int(cProtoLayout.size):int(cProtoLayout.size)]
	return proto.Unmarshal(blob, msg)
}

// unmarshalCProto unmarshal the proto from C memory
func unmarshalCProto(cRes *C.CProto, msg proto.Message) error {
	blob := (*(*[math.MaxInt32]byte)(cRes.proto_blob))[:int(cRes.proto_size):int(cRes.proto_size)]
	return proto.Unmarshal(blob, msg)
}

// getCProtoBlob returns the raw C memory, invoker should release it itself
func getCProtoBlob(cProto *C.CProto) []byte {
	lease, blob := cgoconverter.UnsafeGoBytes(&cProto.proto_blob, int(cProto.proto_size))
	cgoconverter.Extract(lease)
	return blob
}

// GetLocalUsedSize returns the used size of the local path
func GetLocalUsedSize(path string) (int64, error) {
	var availableSize int64
	cSize := (*C.int64_t)(&availableSize)
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	status := C.GetLocalUsedSize(cPath, cSize)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return 0, err
	}
	return availableSize, nil
}
