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

package indexcgowrapper

/*
#cgo pkg-config: milvus_indexbuilder
#include <stdlib.h>	// free
#include "indexbuilder/index_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/indexcgopb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type BuildIndexInfo struct {
	cBuildIndexInfo C.CBuildIndexInfo
}

func NewBuildIndexInfo(config *indexpb.StorageConfig) (*BuildIndexInfo, error) {
	var cBuildIndexInfo C.CBuildIndexInfo

	cAddress := C.CString(config.Address)
	cBucketName := C.CString(config.BucketName)
	cAccessKey := C.CString(config.AccessKeyID)
	cAccessValue := C.CString(config.SecretAccessKey)
	cRootPath := C.CString(config.RootPath)
	cStorageType := C.CString(config.StorageType)
	cIamEndPoint := C.CString(config.IAMEndpoint)
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
		useSSL:           C.bool(config.UseSSL),
		useIAM:           C.bool(config.UseIAM),
	}

	status := C.NewBuildIndexInfo(&cBuildIndexInfo, storageConfig)
	if err := HandleCStatus(&status, "NewBuildIndexInfo failed"); err != nil {
		return nil, err
	}
	return &BuildIndexInfo{cBuildIndexInfo: cBuildIndexInfo}, nil
}

func DeleteBuildIndexInfo(info *BuildIndexInfo) {
	C.DeleteBuildIndexInfo(info.cBuildIndexInfo)
}

func (bi *BuildIndexInfo) AppendFieldMetaInfo(collectionID int64, partitionID int64, segmentID int64, fieldID int64, fieldType schemapb.DataType) error {
	cColID := C.int64_t(collectionID)
	cParID := C.int64_t(partitionID)
	cSegID := C.int64_t(segmentID)
	cFieldID := C.int64_t(fieldID)
	cintDType := uint32(fieldType)
	status := C.AppendFieldMetaInfo(bi.cBuildIndexInfo, cColID, cParID, cSegID, cFieldID, cintDType)
	return HandleCStatus(&status, "appendFieldMetaInfo failed")
}

func (bi *BuildIndexInfo) AppendIndexMetaInfo(indexID int64, buildID int64, indexVersion int64) error {
	cIndexID := C.int64_t(indexID)
	cBuildID := C.int64_t(buildID)
	cIndexVersion := C.int64_t(indexVersion)

	status := C.AppendIndexMetaInfo(bi.cBuildIndexInfo, cIndexID, cBuildID, cIndexVersion)
	return HandleCStatus(&status, "appendIndexMetaInfo failed")
}

func (bi *BuildIndexInfo) AppendBuildIndexParam(indexParams map[string]string) error {
	if len(indexParams) == 0 {
		return nil
	}
	protoIndexParams := &indexcgopb.IndexParams{
		Params: make([]*commonpb.KeyValuePair, 0),
	}
	for key, value := range indexParams {
		protoIndexParams.Params = append(protoIndexParams.Params, &commonpb.KeyValuePair{Key: key, Value: value})
	}
	indexParamsBlob, err := proto.Marshal(protoIndexParams)
	if err != nil {
		return fmt.Errorf("failed to marshal index params: %s", err)
	}

	status := C.AppendBuildIndexParam(bi.cBuildIndexInfo, (*C.uint8_t)(unsafe.Pointer(&indexParamsBlob[0])), (C.uint64_t)(len(indexParamsBlob)))
	return HandleCStatus(&status, "appendBuildIndexParam failed")
}

func (bi *BuildIndexInfo) AppendBuildTypeParam(typeParams map[string]string) error {
	if len(typeParams) == 0 {
		return nil
	}
	protoTypeParams := &indexcgopb.TypeParams{
		Params: make([]*commonpb.KeyValuePair, 0),
	}
	for key, value := range typeParams {
		protoTypeParams.Params = append(protoTypeParams.Params, &commonpb.KeyValuePair{Key: key, Value: value})
	}
	typeParamsBlob, err := proto.Marshal(protoTypeParams)
	if err != nil {
		return fmt.Errorf("failed to marshal type params: %s", err)
	}

	status := C.AppendBuildTypeParam(bi.cBuildIndexInfo, (*C.uint8_t)(unsafe.Pointer(&typeParamsBlob[0])), (C.uint64_t)(len(typeParamsBlob)))
	return HandleCStatus(&status, "appendBuildTypeParam failed")
}

func (bi *BuildIndexInfo) AppendInsertFile(filePath string) error {
	cInsertFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cInsertFilePath))

	status := C.AppendInsertFilePath(bi.cBuildIndexInfo, cInsertFilePath)
	return HandleCStatus(&status, "appendInsertFile failed")
}
