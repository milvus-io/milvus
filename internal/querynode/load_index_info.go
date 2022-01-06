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

package querynode

/*
#cgo CFLAGS: -I${SRCDIR}/../core/output/include
#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/load_index_c.h"

*/
import "C"
import (
	"path/filepath"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
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

// appendIndexParam append indexParam to index
func (li *LoadIndexInfo) appendIndexParam(indexKey string, indexValue string) error {
	cIndexKey := C.CString(indexKey)
	defer C.free(unsafe.Pointer(cIndexKey))
	cIndexValue := C.CString(indexValue)
	defer C.free(unsafe.Pointer(cIndexValue))
	status := C.AppendIndexParam(li.cLoadIndexInfo, cIndexKey, cIndexValue)
	return HandleCStatus(&status, "AppendIndexParam failed")
}

func (li *LoadIndexInfo) appendFieldInfo(fieldID FieldID) error {
	cFieldID := C.long(fieldID)
	status := C.AppendFieldInfo(li.cLoadIndexInfo, cFieldID)
	return HandleCStatus(&status, "AppendFieldInfo failed")
}

func (li *LoadIndexInfo) appendIndex(bytesIndex [][]byte, indexKeys []string) error {
	var cBinarySet C.CBinarySet
	status := C.NewBinarySet(&cBinarySet)
	defer C.DeleteBinarySet(cBinarySet)

	if err := HandleCStatus(&status, "NewBinarySet failed"); err != nil {
		return err
	}

	for i, byteIndex := range bytesIndex {
		indexPtr := unsafe.Pointer(&byteIndex[0])
		indexLen := C.long(len(byteIndex))
		binarySetKey := filepath.Base(indexKeys[i])
		log.Debug("", zap.String("index key", binarySetKey))
		indexKey := C.CString(binarySetKey)
		status = C.AppendBinaryIndex(cBinarySet, indexPtr, indexLen, indexKey)
		C.free(unsafe.Pointer(indexKey))
		if err := HandleCStatus(&status, "AppendBinaryIndex failed"); err != nil {
			return err
		}
	}

	status = C.AppendIndex(li.cLoadIndexInfo, cBinarySet)
	return HandleCStatus(&status, "AppendIndex failed")
}
