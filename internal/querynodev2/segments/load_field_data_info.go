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
#include "segcore/load_field_data_c.h"
*/
import "C"

import (
	"context"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/proto/datapb"
)

type LoadFieldDataInfo struct {
	cLoadFieldDataInfo C.CLoadFieldDataInfo
}

func newLoadFieldDataInfo(ctx context.Context) (*LoadFieldDataInfo, error) {
	var status C.CStatus
	var cLoadFieldDataInfo C.CLoadFieldDataInfo
	GetDynamicPool().Submit(func() (any, error) {
		status = C.NewLoadFieldDataInfo(&cLoadFieldDataInfo)
		return nil, nil
	}).Await()
	if err := HandleCStatus(ctx, &status, "newLoadFieldDataInfo failed"); err != nil {
		return nil, err
	}
	return &LoadFieldDataInfo{cLoadFieldDataInfo: cLoadFieldDataInfo}, nil
}

func deleteFieldDataInfo(info *LoadFieldDataInfo) {
	GetDynamicPool().Submit(func() (any, error) {
		C.DeleteLoadFieldDataInfo(info.cLoadFieldDataInfo)
		return nil, nil
	}).Await()
}

func (ld *LoadFieldDataInfo) appendLoadFieldInfo(ctx context.Context, fieldID int64, rowCount int64) error {
	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		cFieldID := C.int64_t(fieldID)
		cRowCount := C.int64_t(rowCount)

		status = C.AppendLoadFieldInfo(ld.cLoadFieldDataInfo, cFieldID, cRowCount)
		return nil, nil
	}).Await()

	return HandleCStatus(ctx, &status, "appendLoadFieldInfo failed")
}

func (ld *LoadFieldDataInfo) appendLoadFieldDataPath(ctx context.Context, fieldID int64, binlog *datapb.Binlog) error {
	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		cFieldID := C.int64_t(fieldID)
		cEntriesNum := C.int64_t(binlog.GetEntriesNum())
		cFile := C.CString(binlog.GetLogPath())
		defer C.free(unsafe.Pointer(cFile))

		status = C.AppendLoadFieldDataPath(ld.cLoadFieldDataInfo, cFieldID, cEntriesNum, cFile)
		return nil, nil
	}).Await()

	return HandleCStatus(ctx, &status, "appendLoadFieldDataPath failed")
}

func (ld *LoadFieldDataInfo) enableMmap(fieldID int64, enabled bool) {
	GetDynamicPool().Submit(func() (any, error) {
		cFieldID := C.int64_t(fieldID)
		cEnabled := C.bool(enabled)

		C.EnableMmap(ld.cLoadFieldDataInfo, cFieldID, cEnabled)
		return nil, nil
	}).Await()
}

func (ld *LoadFieldDataInfo) appendMMapDirPath(dir string) {
	GetDynamicPool().Submit(func() (any, error) {
		cDir := C.CString(dir)
		defer C.free(unsafe.Pointer(cDir))

		C.AppendMMapDirPath(ld.cLoadFieldDataInfo, cDir)
		return nil, nil
	}).Await()
}

func (ld *LoadFieldDataInfo) appendURI(uri string) {
	GetDynamicPool().Submit(func() (any, error) {
		cURI := C.CString(uri)
		defer C.free(unsafe.Pointer(cURI))
		C.SetUri(ld.cLoadFieldDataInfo, cURI)

		return nil, nil
	}).Await()
}

func (ld *LoadFieldDataInfo) appendStorageVersion(version int64) {
	GetDynamicPool().Submit(func() (any, error) {
		cVersion := C.int64_t(version)
		C.SetStorageVersion(ld.cLoadFieldDataInfo, cVersion)

		return nil, nil
	}).Await()
}
