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

//go:build test
// +build test

package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include <stdint.h>
#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
*/
import "C"

import (
	"runtime"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func testManifestFieldIDsFromColumns(columnNames []*string, numColumns int) (map[int64]struct{}, error) {
	var group C.LoonColumnGroup
	group.num_of_columns = C.uint32_t(numColumns)

	var cColumns []*C.char
	if columnNames != nil {
		columnSlots := len(columnNames)
		if columnSlots == 0 {
			columnSlots = 1
		}
		cColumns = make([]*C.char, columnSlots)
		for i, columnName := range columnNames {
			if columnName == nil {
				continue
			}
			cColumns[i] = C.CString(*columnName)
			defer C.free(unsafe.Pointer(cColumns[i]))
		}
		group.columns = (**C.char)(unsafe.Pointer(&cColumns[0]))
	}

	var groups C.LoonColumnGroups
	groups.column_group_array = &group
	groups.num_of_column_groups = 1
	fields, err := manifestFieldIDsFromColumnGroups("test-manifest", &groups)
	runtime.KeepAlive(cColumns)
	return fields, err
}

// Construct raw FFI metadata that a serialized manifest cannot represent.
// None of these Go-owned arrays cross into C; only the strings need C.free.
func testManifestDeltaLogs(paths []*string, entries []uint32, count uint32) ([]*datapb.FieldBinlog, error) {
	var manifest C.LoonManifest
	manifest.delta_logs.num_delta_logs = C.uint32_t(count)
	var cPaths []*C.char
	if len(paths) > 0 {
		cPaths = make([]*C.char, len(paths))
		for i, p := range paths {
			if p != nil {
				cPaths[i] = C.CString(*p)
				defer C.free(unsafe.Pointer(cPaths[i]))
			}
		}
		manifest.delta_logs.delta_log_paths = (**C.char)(unsafe.Pointer(&cPaths[0]))
	}
	if len(entries) > 0 {
		manifest.delta_logs.delta_log_num_entries = (*C.uint32_t)(unsafe.Pointer(&entries[0]))
	}
	logs, err := deltaLogsFromManifest("test-manifest", &manifest)
	runtime.KeepAlive(cPaths)
	runtime.KeepAlive(entries)
	return logs, err
}

// Exercise the borrowed-handle bridge, including malformed native metadata.
// The returned Go values must survive freeing all native string storage.
func testManifestIndexProjection(fault string) ([]ManifestIndexInfo, error) {
	value := C.CString("value")
	defer C.free(unsafe.Pointer(value))
	fileKeys, propertyKeys, propertyValues := []*C.char{value}, []*C.char{value}, []*C.char{value}
	index := C.LoonIndexInfo{
		column_name: value, index_name: value, index_type: value, path: value,
		index_file_keys: &fileKeys[0], num_index_file_keys: 1,
		property_keys: &propertyKeys[0], property_values: &propertyValues[0], num_properties: 1,
	}
	manifest := C.LoonManifest{indexes: C.LoonIndexes{indexes: &index, num_indexes: 1}}
	switch fault {
	case "empty":
		manifest.indexes.num_indexes = 0
	case "indexes":
		manifest.indexes.indexes = nil
	case "column":
		index.column_name = nil
	case "type":
		index.index_type = nil
	case "path":
		index.path = nil
	case "file keys":
		index.index_file_keys = nil
	case "property keys":
		index.property_keys = nil
	case "property values":
		index.property_values = nil
	case "property key":
		propertyKeys[0] = nil
	case "property value":
		propertyValues[0] = nil
	case "file key":
		fileKeys[0] = nil
	case "minimal":
		index.index_name, index.num_index_file_keys, index.num_properties = nil, 0, 0
	}
	result, err := manifestIndexInfos(&manifest, "test-manifest")
	runtime.KeepAlive(&manifest)
	return result, err
}
