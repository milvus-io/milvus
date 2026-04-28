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
	"strings"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

func writePathForStorage(storageConfig *indexpb.StorageConfig, filePath string) string {
	if storageConfig == nil || storageConfig.GetStorageType() != "local" {
		return filePath
	}

	rootPath := strings.TrimRight(storageConfig.GetRootPath(), "/")
	if rootPath == "" {
		return filePath
	}

	if filePath == rootPath {
		return ""
	}

	if strings.HasPrefix(filePath, rootPath+"/") {
		return strings.TrimPrefix(filePath, rootPath+"/")
	}

	return filePath
}

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

	storagePath := writePathForStorage(storageConfig, filePath)

	cPath := C.CString(storagePath)
	defer C.free(unsafe.Pointer(cPath))
	pathLen := C.uint32_t(len(storagePath))

	// Get filesystem handle (LRU-cached by C++ side)
	var fsHandle C.FileSystemHandle
	result := C.loon_filesystem_get(cProperties, cPath, pathLen, &fsHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return fmt.Errorf("failed to get filesystem: %w", err)
	}
	defer C.loon_filesystem_destroy(fsHandle)

	// Local filesystem requires parent directories to exist before writing.
	// Object stores (S3/MinIO/GCS) don't have real directories.
	if storageConfig.GetStorageType() == "local" {
		if idx := strings.LastIndex(storagePath, "/"); idx > 0 {
			dir := storagePath[:idx]
			cDir := C.CString(dir)
			defer C.free(unsafe.Pointer(cDir))
			result = C.loon_filesystem_create_dir(fsHandle, cDir, C.uint32_t(len(dir)), true)
			if err := HandleLoonFFIResult(result); err != nil {
				return fmt.Errorf("failed to create parent directory %q: %w", dir, err)
			}
		}
	}

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
