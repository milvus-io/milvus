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

package ncscgowrapper

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include <stdint.h>
typedef uint64_t uint64_t_cgo;
#include "common/type_c.h"
#include "ncs/ncs_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"go.uber.org/zap"
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
	return fmt.Errorf("%s", finalMsg)
}

// CreateBucket creates a bucket with the given bucket ID in NCS.
func CreateBucket(bucketId uint64) error {
	status := C.createBucket(C.uint64_t(bucketId))
	err := HandleCStatus(&status, "failed to create bucket")
	if err != nil {
		log.Warn("CreateBucket failed", zap.Uint64("bucketId", bucketId), zap.Error(err))
		return err
	}
	log.Info("CreateBucket succeeded", zap.Uint64("bucketId", bucketId))
	return nil
}

// DeleteBucket deletes a bucket with the given bucket ID from NCS.
func DeleteBucket(bucketId uint64) error {
	status := C.deleteBucket(C.uint64_t(bucketId))
	err := HandleCStatus(&status, "failed to delete bucket")
	if err != nil {
		log.Warn("DeleteBucket failed", zap.Uint64("bucketId", bucketId), zap.Error(err))
		return err
	}
	log.Info("DeleteBucket succeeded", zap.Uint64("bucketId", bucketId))
	return nil
}

// IsBucketExist checks if a bucket with the given bucket ID exists in NCS.
func IsBucketExist(bucketId uint64) (bool, error) {
	var exists C.bool
	status := C.isBucketExist(C.uint64_t(bucketId), &exists)
	err := HandleCStatus(&status, "failed to check if bucket exists")
	if err != nil {
		log.Warn("IsBucketExist failed", zap.Uint64("bucketId", bucketId), zap.Error(err))
		return false, err
	}
	return bool(exists), nil
}
