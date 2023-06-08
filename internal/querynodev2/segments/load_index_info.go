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
#cgo pkg-config: milvus_common milvus_segcore

#include "segcore/load_index_c.h"
#include "common/binary_set_c.h"
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/indexparams"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// LoadIndexInfo is a wrapper of the underlying C-structure C.CLoadIndexInfo
type LoadIndexInfo struct {
	cLoadIndexInfo C.CLoadIndexInfo
}

// newLoadIndexInfo returns a new LoadIndexInfo and error
func newLoadIndexInfo() (*LoadIndexInfo, error) {
	var cLoadIndexInfo C.CLoadIndexInfo

	// TODO::xige-16 support embedded milvus
	storageType := "minio"
	cAddress := C.CString(paramtable.Get().MinioCfg.Address.GetValue())
	cBucketName := C.CString(paramtable.Get().MinioCfg.BucketName.GetValue())
	cAccessKey := C.CString(paramtable.Get().MinioCfg.AccessKeyID.GetValue())
	cAccessValue := C.CString(paramtable.Get().MinioCfg.SecretAccessKey.GetValue())
	cRootPath := C.CString(paramtable.Get().MinioCfg.RootPath.GetValue())
	cStorageType := C.CString(storageType)
	cIamEndPoint := C.CString(paramtable.Get().MinioCfg.IAMEndpoint.GetValue())
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
		remote_root_path: cRootPath,
		storage_type:     cStorageType,
		iam_endpoint:     cIamEndPoint,
		useSSL:           C.bool(paramtable.Get().MinioCfg.UseSSL.GetAsBool()),
		useIAM:           C.bool(paramtable.Get().MinioCfg.UseIAM.GetAsBool()),
	}

	status := C.NewLoadIndexInfo(&cLoadIndexInfo, storageConfig)
	if err := HandleCStatus(&status, "NewLoadIndexInfo failed"); err != nil {
		return nil, err
	}
	return &LoadIndexInfo{cLoadIndexInfo: cLoadIndexInfo}, nil
}

// deleteLoadIndexInfo would delete C.CLoadIndexInfo
func deleteLoadIndexInfo(info *LoadIndexInfo) {
	C.DeleteLoadIndexInfo(info.cLoadIndexInfo)
}

func (li *LoadIndexInfo) appendLoadIndexInfo(bytesIndex [][]byte, indexInfo *querypb.FieldIndexInfo, collectionID int64, partitionID int64, segmentID int64, fieldType schemapb.DataType) error {
	fieldID := indexInfo.FieldID
	indexPaths := indexInfo.IndexFilePaths

	err := li.appendFieldInfo(collectionID, partitionID, segmentID, fieldID, fieldType)
	if err != nil {
		return err
	}

	err = li.appendIndexInfo(indexInfo.IndexID, indexInfo.BuildID, indexInfo.IndexVersion)
	if err != nil {
		return err
	}

	// some build params also exist in indexParams, which are useless during loading process
	indexParams := funcutil.KeyValuePair2Map(indexInfo.IndexParams)
	indexparams.SetDiskIndexLoadParams(paramtable.Get(), indexParams, indexInfo.GetNumRows())

	jsonIndexParams, err := json.Marshal(indexParams)
	if err != nil {
		err = fmt.Errorf("failed to json marshal index params %w", err)
		return err
	}
	log.Info("start append index params", zap.String("index params", string(jsonIndexParams)))

	for key, value := range indexParams {
		err = li.appendIndexParam(key, value)
		if err != nil {
			return err
		}
	}

	err = li.appendIndexData(bytesIndex, indexPaths)
	return err
}

// appendIndexParam append indexParam to index
func (li *LoadIndexInfo) appendIndexParam(indexKey string, indexValue string) error {
	cIndexKey := C.CString(indexKey)
	defer C.free(unsafe.Pointer(cIndexKey))
	cIndexValue := C.CString(indexValue)
	defer C.free(unsafe.Pointer(cIndexValue))
	status := C.AppendIndexParam(li.cLoadIndexInfo, cIndexKey, cIndexValue)
	return HandleCStatus(&status, "AppendIndexParam failed")
}

func (li *LoadIndexInfo) appendIndexInfo(indexID int64, buildID int64, indexVersion int64) error {
	cIndexID := C.int64_t(indexID)
	cBuildID := C.int64_t(buildID)
	cIndexVersion := C.int64_t(indexVersion)

	status := C.AppendIndexInfo(li.cLoadIndexInfo, cIndexID, cBuildID, cIndexVersion)
	return HandleCStatus(&status, "AppendIndexInfo failed")
}

func (li *LoadIndexInfo) cleanLocalData() error {
	status := C.CleanLoadedIndex(li.cLoadIndexInfo)
	return HandleCStatus(&status, "failed to clean cached data on disk")
}

func (li *LoadIndexInfo) appendIndexFile(filePath string) error {
	cIndexFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cIndexFilePath))

	status := C.AppendIndexFilePath(li.cLoadIndexInfo, cIndexFilePath)
	return HandleCStatus(&status, "AppendIndexIFile failed")
}

// appendFieldInfo appends fieldID & fieldType to index
func (li *LoadIndexInfo) appendFieldInfo(collectionID int64, partitionID int64, segmentID int64, fieldID int64, fieldType schemapb.DataType) error {
	cColID := C.int64_t(collectionID)
	cParID := C.int64_t(partitionID)
	cSegID := C.int64_t(segmentID)
	cFieldID := C.int64_t(fieldID)
	cintDType := uint32(fieldType)
	status := C.AppendFieldInfo(li.cLoadIndexInfo, cColID, cParID, cSegID, cFieldID, cintDType)
	return HandleCStatus(&status, "AppendFieldInfo failed")
}

// appendIndexData appends binarySet index to cLoadIndexInfo
func (li *LoadIndexInfo) appendIndexData(bytesIndex [][]byte, indexKeys []string) error {
	for _, indexPath := range indexKeys {
		err := li.appendIndexFile(indexPath)
		if err != nil {
			return err
		}
	}

	var cBinarySet C.CBinarySet
	status := C.NewBinarySet(&cBinarySet)
	defer C.DeleteBinarySet(cBinarySet)

	if err := HandleCStatus(&status, "NewBinarySet failed"); err != nil {
		return err
	}

	for i, byteIndex := range bytesIndex {
		indexPtr := unsafe.Pointer(&byteIndex[0])
		indexLen := C.int64_t(len(byteIndex))
		binarySetKey := filepath.Base(indexKeys[i])
		indexKey := C.CString(binarySetKey)
		status = C.AppendIndexBinary(cBinarySet, indexPtr, indexLen, indexKey)
		C.free(unsafe.Pointer(indexKey))
		if err := HandleCStatus(&status, "LoadIndexInfo AppendIndexBinary failed"); err != nil {
			return err
		}
	}

	status = C.AppendIndex(li.cLoadIndexInfo, cBinarySet)
	return HandleCStatus(&status, "AppendIndex failed")
}
