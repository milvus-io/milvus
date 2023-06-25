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
#cgo pkg-config: milvus_segcore
#include "segcore/load_field_data_c.h"
*/
import "C"
import "unsafe"

type LoadFieldDataInfo struct {
	cLoadFieldDataInfo C.CLoadFieldDataInfo
}

func newLoadFieldDataInfo() (*LoadFieldDataInfo, error) {
	var cLoadFieldDataInfo C.CLoadFieldDataInfo

	status := C.NewLoadFieldDataInfo(&cLoadFieldDataInfo)
	if err := HandleCStatus(&status, "newLoadFieldDataInfo failed"); err != nil {
		return nil, err
	}
	return &LoadFieldDataInfo{cLoadFieldDataInfo: cLoadFieldDataInfo}, nil
}

func deleteFieldDataInfo(info *LoadFieldDataInfo) {
	C.DeleteLoadFieldDataInfo(info.cLoadFieldDataInfo)
}

func (ld *LoadFieldDataInfo) appendLoadFieldInfo(fieldID int64, rowCount int64) error {
	cFieldID := C.int64_t(fieldID)
	cRowCount := C.int64_t(rowCount)

	status := C.AppendLoadFieldInfo(ld.cLoadFieldDataInfo, cFieldID, cRowCount)
	return HandleCStatus(&status, "appendLoadFieldInfo failed")
}

func (ld *LoadFieldDataInfo) appendLoadFieldDataPath(fieldID int64, file string) error {
	cFieldID := C.int64_t(fieldID)
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))

	status := C.AppendLoadFieldDataPath(ld.cLoadFieldDataInfo, cFieldID, cFile)
	return HandleCStatus(&status, "appendLoadFieldDataPath failed")
}

func (ld *LoadFieldDataInfo) appendMMapDirPath(dir string) {
	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))

	C.AppendMMapDirPath(ld.cLoadFieldDataInfo, cDir)
}
