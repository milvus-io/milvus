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
	"context"
	"unsafe"

	"github.com/pingcap/log"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/common"
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
func newLoadIndexInfo(ctx context.Context) (*LoadIndexInfo, error) {
	var cLoadIndexInfo C.CLoadIndexInfo

	status := C.NewLoadIndexInfo(&cLoadIndexInfo)
	if err := HandleCStatus(ctx, &status, "NewLoadIndexInfo failed"); err != nil {
		return nil, err
	}
	return &LoadIndexInfo{cLoadIndexInfo: cLoadIndexInfo}, nil
}

// deleteLoadIndexInfo would delete C.CLoadIndexInfo
func deleteLoadIndexInfo(info *LoadIndexInfo) {
	C.DeleteLoadIndexInfo(info.cLoadIndexInfo)
}

func (li *LoadIndexInfo) appendLoadIndexInfo(ctx context.Context, indexInfo *querypb.FieldIndexInfo, collectionID int64, partitionID int64, segmentID int64, fieldType schemapb.DataType) error {
	fieldID := indexInfo.FieldID
	indexPaths := indexInfo.IndexFilePaths

	indexParams := funcutil.KeyValuePair2Map(indexInfo.IndexParams)
	enableMmap := indexParams[common.MmapEnabledKey] == "true"
	// as Knowhere reports error if encounter a unknown param, we need to delete it
	delete(indexParams, common.MmapEnabledKey)

	mmapDirPath := paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue()
	err := li.appendFieldInfo(ctx, collectionID, partitionID, segmentID, fieldID, fieldType, enableMmap, mmapDirPath)
	if err != nil {
		return err
	}

	err = li.appendIndexInfo(ctx, indexInfo.IndexID, indexInfo.BuildID, indexInfo.IndexVersion)
	if err != nil {
		return err
	}

	// some build params also exist in indexParams, which are useless during loading process
	if indexParams["index_type"] == indexparamcheck.IndexDISKANN {
		err = indexparams.SetDiskIndexLoadParams(paramtable.Get(), indexParams, indexInfo.GetNumRows())
		if err != nil {
			return err
		}
	}

	err = indexparams.AppendPrepareLoadParams(paramtable.Get(), indexParams)
	if err != nil {
		return err
	}

	log.Info("load with index params", zap.Any("indexParams", indexParams))
	for key, value := range indexParams {
		err = li.appendIndexParam(ctx, key, value)
		if err != nil {
			return err
		}
	}

	if err := li.appendIndexEngineVersion(ctx, indexInfo.GetCurrentIndexVersion()); err != nil {
		return err
	}

	err = li.appendIndexData(ctx, indexPaths)
	return err
}

// appendIndexParam append indexParam to index
func (li *LoadIndexInfo) appendIndexParam(ctx context.Context, indexKey string, indexValue string) error {
	cIndexKey := C.CString(indexKey)
	defer C.free(unsafe.Pointer(cIndexKey))
	cIndexValue := C.CString(indexValue)
	defer C.free(unsafe.Pointer(cIndexValue))
	status := C.AppendIndexParam(li.cLoadIndexInfo, cIndexKey, cIndexValue)
	return HandleCStatus(ctx, &status, "AppendIndexParam failed")
}

func (li *LoadIndexInfo) appendIndexInfo(ctx context.Context, indexID int64, buildID int64, indexVersion int64) error {
	cIndexID := C.int64_t(indexID)
	cBuildID := C.int64_t(buildID)
	cIndexVersion := C.int64_t(indexVersion)

	status := C.AppendIndexInfo(li.cLoadIndexInfo, cIndexID, cBuildID, cIndexVersion)
	return HandleCStatus(ctx, &status, "AppendIndexInfo failed")
}

func (li *LoadIndexInfo) cleanLocalData(ctx context.Context) error {
	status := C.CleanLoadedIndex(li.cLoadIndexInfo)
	return HandleCStatus(ctx, &status, "failed to clean cached data on disk")
}

func (li *LoadIndexInfo) appendIndexFile(ctx context.Context, filePath string) error {
	cIndexFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cIndexFilePath))

	status := C.AppendIndexFilePath(li.cLoadIndexInfo, cIndexFilePath)
	return HandleCStatus(ctx, &status, "AppendIndexIFile failed")
}

// appendFieldInfo appends fieldID & fieldType to index
func (li *LoadIndexInfo) appendFieldInfo(ctx context.Context, collectionID int64, partitionID int64, segmentID int64, fieldID int64, fieldType schemapb.DataType, enableMmap bool, mmapDirPath string) error {
	cColID := C.int64_t(collectionID)
	cParID := C.int64_t(partitionID)
	cSegID := C.int64_t(segmentID)
	cFieldID := C.int64_t(fieldID)
	cintDType := uint32(fieldType)
	cEnableMmap := C.bool(enableMmap)
	cMmapDirPath := C.CString(mmapDirPath)
	defer C.free(unsafe.Pointer(cMmapDirPath))
	status := C.AppendFieldInfo(li.cLoadIndexInfo, cColID, cParID, cSegID, cFieldID, cintDType, cEnableMmap, cMmapDirPath)
	return HandleCStatus(ctx, &status, "AppendFieldInfo failed")
}

// appendIndexData appends index path to cLoadIndexInfo and create index
func (li *LoadIndexInfo) appendIndexData(ctx context.Context, indexKeys []string) error {
	for _, indexPath := range indexKeys {
		err := li.appendIndexFile(ctx, indexPath)
		if err != nil {
			return err
		}
	}

	span := trace.SpanFromContext(ctx)

	traceID := span.SpanContext().TraceID()
	spanID := span.SpanContext().SpanID()
	traceCtx := C.CTraceContext{
		traceID: (*C.uint8_t)(unsafe.Pointer(&traceID[0])),
		spanID:  (*C.uint8_t)(unsafe.Pointer(&spanID[0])),
		flag:    C.uchar(span.SpanContext().TraceFlags()),
	}

	status := C.AppendIndexV2(traceCtx, li.cLoadIndexInfo)
	return HandleCStatus(ctx, &status, "AppendIndex failed")
}

func (li *LoadIndexInfo) appendIndexEngineVersion(ctx context.Context, indexEngineVersion int32) error {
	cIndexEngineVersion := C.int32_t(indexEngineVersion)

	status := C.AppendIndexEngineVersionToLoadInfo(li.cLoadIndexInfo, cIndexEngineVersion)
	return HandleCStatus(ctx, &status, "AppendIndexEngineVersion failed")
}
