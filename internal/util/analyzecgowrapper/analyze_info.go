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

package analyzecgowrapper

/*
#cgo pkg-config: milvus_indexbuilder
#include <stdlib.h>	// free
#include "indexbuilder/analyze_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type AnalyzeInfo struct {
	cAnalyzeInfo C.CAnalyzeInfo
}

func NewAnalyzeInfo(config *indexpb.StorageConfig) (*AnalyzeInfo, error) {
	var cAnalyzeInfo C.CAnalyzeInfo

	cAddress := C.CString(config.Address)
	cBucketName := C.CString(config.BucketName)
	cAccessKey := C.CString(config.AccessKeyID)
	cAccessValue := C.CString(config.SecretAccessKey)
	cRootPath := C.CString(config.RootPath)
	cStorageType := C.CString(config.StorageType)
	cIamEndPoint := C.CString(config.IAMEndpoint)
	cRegion := C.CString(config.Region)
	cCloudProvider := C.CString(config.CloudProvider)
	defer C.free(unsafe.Pointer(cAddress))
	defer C.free(unsafe.Pointer(cBucketName))
	defer C.free(unsafe.Pointer(cAccessKey))
	defer C.free(unsafe.Pointer(cAccessValue))
	defer C.free(unsafe.Pointer(cRootPath))
	defer C.free(unsafe.Pointer(cStorageType))
	defer C.free(unsafe.Pointer(cIamEndPoint))
	defer C.free(unsafe.Pointer(cRegion))
	defer C.free(unsafe.Pointer(cCloudProvider))
	storageConfig := C.CStorageConfig{
		address:          cAddress,
		bucket_name:      cBucketName,
		access_key_id:    cAccessKey,
		access_key_value: cAccessValue,
		root_path:        cRootPath,
		storage_type:     cStorageType,
		iam_endpoint:     cIamEndPoint,
		cloud_provider:   cCloudProvider,
		useSSL:           C.bool(config.UseSSL),
		useIAM:           C.bool(config.UseIAM),
		region:           cRegion,
		useVirtualHost:   C.bool(config.UseVirtualHost),
		requestTimeoutMs: C.int64_t(config.RequestTimeoutMs),
	}

	status := C.NewAnalyzeInfo(&cAnalyzeInfo, storageConfig)
	if err := HandleCStatus(&status, "NewAnalyzeInfo failed"); err != nil {
		return nil, err
	}
	return &AnalyzeInfo{cAnalyzeInfo: cAnalyzeInfo}, nil
}

func DeleteAnalyzeInfo(info *AnalyzeInfo) {
	C.DeleteAnalyzeInfo(info.cAnalyzeInfo)
}

func (ai *AnalyzeInfo) AppendAnalyzeFieldMetaInfo(collectionID int64, partitionID int64, fieldID int64, fieldType schemapb.DataType, fieldName string, dim int64) error {
	cColID := C.int64_t(collectionID)
	cParID := C.int64_t(partitionID)
	cFieldID := C.int64_t(fieldID)
	cintDType := uint32(fieldType)
	cFieldName := C.CString(fieldName)
	cDim := C.int64_t(dim)
	defer C.free(unsafe.Pointer(cFieldName))
	status := C.AppendAnalyzeFieldMetaInfo(ai.cAnalyzeInfo, cColID, cParID, cFieldID, cFieldName, cintDType, cDim)
	return HandleCStatus(&status, "appendFieldMetaInfo failed")
}

func (ai *AnalyzeInfo) AppendAnalyzeInfo(taskID int64, version int64) error {
	cTaskID := C.int64_t(taskID)
	cVersion := C.int64_t(version)

	status := C.AppendAnalyzeInfo(ai.cAnalyzeInfo, cTaskID, cVersion)
	return HandleCStatus(&status, "appendAnalyzeMetaInfo failed")
}

func (ai *AnalyzeInfo) AppendSegmentID(segID int64) error {
	cSegID := C.int64_t(segID)

	status := C.AppendSegmentID(ai.cAnalyzeInfo, cSegID)
	return HandleCStatus(&status, "appendAnalyzeSegmentID failed")
}

func (ai *AnalyzeInfo) AppendSegmentInsertFile(segID int64, filePath string) error {
	cSegID := C.int64_t(segID)
	cInsertFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cInsertFilePath))

	status := C.AppendSegmentInsertFile(ai.cAnalyzeInfo, cSegID, cInsertFilePath)
	return HandleCStatus(&status, "appendInsertFile failed")
}

func (ai *AnalyzeInfo) AppendSegmentSize(size int64) error {
	cSize := C.int64_t(size)

	status := C.AppendSegmentSize(ai.cAnalyzeInfo, cSize)
	return HandleCStatus(&status, "appendSegmentSize failed")
}

func (ai *AnalyzeInfo) AppendTrainSize(size int64) error {
	cSize := C.int64_t(size)

	status := C.AppendTrainSize(ai.cAnalyzeInfo, cSize)
	return HandleCStatus(&status, "appendTrainSize failed")
}
