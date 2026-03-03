// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
#cgo pkg-config: milvus_core milvus-storage

#include <stdlib.h>
#include "milvus-storage/ffi_filesystem_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// WriteFile writes raw bytes to a file using milvus-storage filesystem FFI.
// filePath is the full storage path (rootPath/basePath/...).
func WriteFile(
	storageConfig *indexpb.StorageConfig,
	filePath string,
	data []byte,
) error {
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))
	pathLen := C.uint32_t(len(filePath))

	// Get filesystem handle (LRU-cached by C++ side)
	var fsHandle C.FileSystemHandle
	result := C.loon_filesystem_get(cProperties, cPath, pathLen, &fsHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return fmt.Errorf("failed to get filesystem: %w", err)
	}
	defer C.loon_filesystem_destroy(fsHandle)

	// Write file atomically
	var dataPtr *C.uint8_t
	if len(data) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&data[0]))
	}

	result = C.loon_filesystem_write_file(
		fsHandle,
		cPath, pathLen,
		dataPtr, C.uint64_t(len(data)),
		nil, 0, // no file metadata
	)
	return HandleLoonFFIResult(result)
}
