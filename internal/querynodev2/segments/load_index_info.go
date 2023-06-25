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
	"path/filepath"
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
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

	status := C.NewLoadIndexInfo(&cLoadIndexInfo)
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
	if indexParams["index_type"] == indexparamcheck.IndexDISKANN {
		err = indexparams.SetDiskIndexLoadParams(paramtable.Get(), indexParams, indexInfo.GetNumRows())
		if err != nil {
			return err
		}
	}

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

// appendIndexData appends index path to cLoadIndexInfo and create index
func (li *LoadIndexInfo) appendIndexData(bytesIndex [][]byte, indexKeys []string) error {
	for _, indexPath := range indexKeys {
		err := li.appendIndexFile(indexPath)
		if err != nil {
			return err
		}
	}

	if bytesIndex != nil {
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

	status := C.AppendIndexV2(li.cLoadIndexInfo)
	return HandleCStatus(&status, "AppendIndex failed")
}
