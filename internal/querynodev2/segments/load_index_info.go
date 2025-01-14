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
#cgo pkg-config: milvus_core

#include "segcore/load_index_c.h"
#include "common/binary_set_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"time"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// LoadIndexInfo is a wrapper of the underlying C-structure C.CLoadIndexInfo
type LoadIndexInfo struct {
	cLoadIndexInfo C.CLoadIndexInfo
}

// newLoadIndexInfo returns a new LoadIndexInfo and error
func newLoadIndexInfo(ctx context.Context) (*LoadIndexInfo, error) {
	var cLoadIndexInfo C.CLoadIndexInfo

	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		status = C.NewLoadIndexInfo(&cLoadIndexInfo)
		return nil, nil
	}).Await()
	if err := HandleCStatus(ctx, &status, "NewLoadIndexInfo failed"); err != nil {
		return nil, err
	}
	return &LoadIndexInfo{cLoadIndexInfo: cLoadIndexInfo}, nil
}

// deleteLoadIndexInfo would delete C.CLoadIndexInfo
func deleteLoadIndexInfo(info *LoadIndexInfo) {
	GetDynamicPool().Submit(func() (any, error) {
		C.DeleteLoadIndexInfo(info.cLoadIndexInfo)
		return nil, nil
	}).Await()
}

// appendIndexParam append indexParam to index
func (li *LoadIndexInfo) appendIndexParam(ctx context.Context, indexKey string, indexValue string) error {
	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		cIndexKey := C.CString(indexKey)
		defer C.free(unsafe.Pointer(cIndexKey))
		cIndexValue := C.CString(indexValue)
		defer C.free(unsafe.Pointer(cIndexValue))
		status = C.AppendIndexParam(li.cLoadIndexInfo, cIndexKey, cIndexValue)
		return nil, nil
	}).Await()
	return HandleCStatus(ctx, &status, "AppendIndexParam failed")
}

func (li *LoadIndexInfo) appendIndexInfo(ctx context.Context, indexID int64, buildID int64, indexVersion int64) error {
	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		cIndexID := C.int64_t(indexID)
		cBuildID := C.int64_t(buildID)
		cIndexVersion := C.int64_t(indexVersion)

		status = C.AppendIndexInfo(li.cLoadIndexInfo, cIndexID, cBuildID, cIndexVersion)
		return nil, nil
	}).Await()
	return HandleCStatus(ctx, &status, "AppendIndexInfo failed")
}

func (li *LoadIndexInfo) cleanLocalData(ctx context.Context) error {
	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		status = C.CleanLoadedIndex(li.cLoadIndexInfo)
		return nil, nil
	}).Await()
	return HandleCStatus(ctx, &status, "failed to clean cached data on disk")
}

func (li *LoadIndexInfo) appendIndexFile(ctx context.Context, filePath string) error {
	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		cIndexFilePath := C.CString(filePath)
		defer C.free(unsafe.Pointer(cIndexFilePath))

		status = C.AppendIndexFilePath(li.cLoadIndexInfo, cIndexFilePath)
		return nil, nil
	}).Await()
	return HandleCStatus(ctx, &status, "AppendIndexIFile failed")
}

// appendFieldInfo appends fieldID & fieldType to index
func (li *LoadIndexInfo) appendFieldInfo(ctx context.Context, collectionID int64, partitionID int64, segmentID int64, fieldID int64, fieldType schemapb.DataType, enableMmap bool, mmapDirPath string) error {
	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		cColID := C.int64_t(collectionID)
		cParID := C.int64_t(partitionID)
		cSegID := C.int64_t(segmentID)
		cFieldID := C.int64_t(fieldID)
		cintDType := uint32(fieldType)
		cEnableMmap := C.bool(enableMmap)
		cMmapDirPath := C.CString(mmapDirPath)
		defer C.free(unsafe.Pointer(cMmapDirPath))
		status = C.AppendFieldInfo(li.cLoadIndexInfo, cColID, cParID, cSegID, cFieldID, cintDType, cEnableMmap, cMmapDirPath)
		return nil, nil
	}).Await()

	return HandleCStatus(ctx, &status, "AppendFieldInfo failed")
}

func (li *LoadIndexInfo) appendStorageInfo(uri string, version int64) {
	GetDynamicPool().Submit(func() (any, error) {
		cURI := C.CString(uri)
		defer C.free(unsafe.Pointer(cURI))
		cVersion := C.int64_t(version)
		C.AppendStorageInfo(li.cLoadIndexInfo, cURI, cVersion)
		return nil, nil
	}).Await()
}

// appendIndexData appends index path to cLoadIndexInfo and create index
func (li *LoadIndexInfo) appendIndexData(ctx context.Context, indexKeys []string) error {
	for _, indexPath := range indexKeys {
		err := li.appendIndexFile(ctx, indexPath)
		if err != nil {
			return err
		}
	}

	var status C.CStatus
	GetLoadPool().Submit(func() (any, error) {
		if paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool() {
			status = C.AppendIndexV3(li.cLoadIndexInfo)
		} else {
			traceCtx := ParseCTraceContext(ctx)
			status = C.AppendIndexV2(traceCtx.ctx, li.cLoadIndexInfo)
			runtime.KeepAlive(traceCtx)
		}
		return nil, nil
	}).Await()

	return HandleCStatus(ctx, &status, "AppendIndex failed")
}

func (li *LoadIndexInfo) appendIndexEngineVersion(ctx context.Context, indexEngineVersion int32) error {
	cIndexEngineVersion := C.int32_t(indexEngineVersion)

	var status C.CStatus

	GetDynamicPool().Submit(func() (any, error) {
		status = C.AppendIndexEngineVersionToLoadInfo(li.cLoadIndexInfo, cIndexEngineVersion)
		return nil, nil
	}).Await()

	return HandleCStatus(ctx, &status, "AppendIndexEngineVersion failed")
}

func (li *LoadIndexInfo) appendLoadIndexInfo(ctx context.Context, info *cgopb.LoadIndexInfo) error {
	marshaled, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	var status C.CStatus
	_, _ = GetDynamicPool().Submit(func() (any, error) {
		status = C.FinishLoadIndexInfo(li.cLoadIndexInfo, (*C.uint8_t)(unsafe.Pointer(&marshaled[0])), (C.uint64_t)(len(marshaled)))
		return nil, nil
	}).Await()

	return HandleCStatus(ctx, &status, "FinishLoadIndexInfo failed")
}

func (li *LoadIndexInfo) loadIndex(ctx context.Context) error {
	var status C.CStatus

	_, _ = GetLoadPool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				fmt.Sprint(paramtable.GetNodeID()),
				"AppendIndexV2",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()
		if paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool() {
			status = C.AppendIndexV3(li.cLoadIndexInfo)
		} else {
			traceCtx := ParseCTraceContext(ctx)
			status = C.AppendIndexV2(traceCtx.ctx, li.cLoadIndexInfo)
			runtime.KeepAlive(traceCtx)
		}
		return nil, nil
	}).Await()

	return HandleCStatus(ctx, &status, "AppendIndex failed")
}
