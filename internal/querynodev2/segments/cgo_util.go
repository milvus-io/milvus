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

package segments

/*
#cgo pkg-config: milvus_segcore milvus_storage

#include "segcore/collection_c.h"
#include "common/type_c.h"
#include "segcore/segment_c.h"
#include "storage/storage_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/cgoconverter"
)

// HandleCStatus deals with the error returned from CGO
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

// HandleCProto deal with the result proto returned from CGO
func HandleCProto(cRes *C.CProto, msg proto.Message) error {
	// Standalone CProto is protobuf created by C side,
	// Passed from c side
	// memory is managed manually
	lease, blob := cgoconverter.UnsafeGoBytes(&cRes.proto_blob, int(cRes.proto_size))
	defer cgoconverter.Release(lease)

	return proto.Unmarshal(blob, msg)
}

// CopyCProtoBlob returns the copy of C memory
func CopyCProtoBlob(cProto *C.CProto) []byte {
	blob := C.GoBytes(cProto.proto_blob, C.int32_t(cProto.proto_size))
	C.free(cProto.proto_blob)
	return blob
}

// GetCProtoBlob returns the raw C memory, invoker should release it itself
func GetCProtoBlob(cProto *C.CProto) []byte {
	lease, blob := cgoconverter.UnsafeGoBytes(&cProto.proto_blob, int(cProto.proto_size))
	cgoconverter.Extract(lease)
	return blob
}

func GetLocalUsedSize(path string) (int64, error) {
	var availableSize int64
	cSize := C.int64_t(availableSize)
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	status := C.GetLocalUsedSize(cPath, &cSize)
	err := HandleCStatus(&status, "get local used size failed")
	if err != nil {
		return 0, err
	}

	return availableSize, nil
}
