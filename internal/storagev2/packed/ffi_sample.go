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

package packed

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "segcore/external_utils_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// SampleExternalFieldSizes samples rows from an external segment via Take API
// and returns per-field average memory size (Arrow buffer size, decompressed).
// The returned values are equivalent to Binlog.MemorySize for internal segments.
//
// Properties are constructed from storageConfig + extfs overrides on the Go side,
// matching the pattern used by other FFI calls (explore, manifest, etc.).
// This avoids dependency on the C++ LoonFFIPropertiesSingleton.
func SampleExternalFieldSizes(
	manifestPath string,
	sampleRows int,
	collectionID int64,
	externalSource string,
	externalSpec string,
	storageConfig *indexpb.StorageConfig,
) (map[string]int64, error) {
	if storageConfig == nil {
		return nil, fmt.Errorf("storageConfig is required for SampleExternalFieldSizes")
	}
	if manifestPath == "" {
		return nil, fmt.Errorf("manifest_path is empty for SampleExternalFieldSizes")
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)
	if err := injectExternalSpecProperties(cProperties, collectionID, externalSource, externalSpec); err != nil {
		return nil, fmt.Errorf("inject extfs: %w", err)
	}

	cManifestPath := C.CString(manifestPath)
	defer C.free(unsafe.Pointer(cManifestPath))

	var result C.CFieldMemSizeList
	status := C.SampleExternalSegmentFieldSizes(
		cManifestPath, C.int(sampleRows),
		C.int64_t(collectionID), cProperties,
		&result)
	defer C.FreeCFieldMemSizeList(&result)

	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}

	sizes := make(map[string]int64, int(result.count))
	if result.count > 0 && result.sizes != nil {
		slice := unsafe.Slice(result.sizes, result.count)
		for _, entry := range slice {
			sizes[C.GoString(entry.field_name)] = int64(entry.avg_mem_bytes)
		}
	}
	return sizes, nil
}
