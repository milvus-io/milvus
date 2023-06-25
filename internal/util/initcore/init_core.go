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

package initcore

/*
#cgo pkg-config: milvus_common milvus_storage

#include <stdlib.h>
#include <stdint.h>
#include "common/init_c.h"
#include "storage/storage_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func InitLocalChunkManager(path string) {
	CLocalRootPath := C.CString(path)
	defer C.free(unsafe.Pointer(CLocalRootPath))
	C.InitLocalChunkManagerSingleton(CLocalRootPath)
}

func InitTraceConfig(params *paramtable.ComponentParam) {
	config := C.CTraceConfig{
		exporter:       C.CString(params.TraceCfg.Exporter.GetValue()),
		sampleFraction: C.int(params.TraceCfg.SampleFraction.GetAsInt()),
		jaegerURL:      C.CString(params.TraceCfg.JaegerURL.GetValue()),
		otlpEndpoint:   C.CString(params.TraceCfg.OtlpEndpoint.GetValue()),
		nodeID:         C.int(paramtable.GetNodeID()),
	}
	C.InitTrace(&config)
}

func InitRemoteChunkManager(params *paramtable.ComponentParam) error {
	cAddress := C.CString(params.MinioCfg.Address.GetValue())
	cBucketName := C.CString(params.MinioCfg.BucketName.GetValue())
	cAccessKey := C.CString(params.MinioCfg.AccessKeyID.GetValue())
	cAccessValue := C.CString(params.MinioCfg.SecretAccessKey.GetValue())
	cRootPath := C.CString(params.MinioCfg.RootPath.GetValue())
	cStorageType := C.CString(params.CommonCfg.StorageType.GetValue())
	cIamEndPoint := C.CString(params.MinioCfg.IAMEndpoint.GetValue())
	defer C.free(unsafe.Pointer(cAddress))
	defer C.free(unsafe.Pointer(cBucketName))
	defer C.free(unsafe.Pointer(cAccessKey))
	defer C.free(unsafe.Pointer(cAccessValue))
	defer C.free(unsafe.Pointer(cRootPath))
	defer C.free(unsafe.Pointer(cStorageType))
	defer C.free(unsafe.Pointer(cIamEndPoint))
	storageConfig := C.CStorageConfig{
		address:          cAddress,
		bucket_name:      cBucketName,
		access_key_id:    cAccessKey,
		access_key_value: cAccessValue,
		root_path:        cRootPath,
		storage_type:     cStorageType,
		iam_endpoint:     cIamEndPoint,
		useSSL:           C.bool(params.MinioCfg.UseSSL.GetAsBool()),
		useIAM:           C.bool(params.MinioCfg.UseIAM.GetAsBool()),
	}

	status := C.InitRemoteChunkManagerSingleton(storageConfig)
	return HandleCStatus(&status, "InitRemoteChunkManagerSingleton failed")
}

func CleanRemoteChunkManager() {
	C.CleanRemoteChunkManagerSingleton()
}

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
