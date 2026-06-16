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
	"net/url"
	"strings"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
		return merr.WrapErrStorage(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)

	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))
	pathLen := C.uint32_t(len(filePath))

	// Get filesystem handle (LRU-cached by C++ side)
	var fsHandle C.FileSystemHandle
	result := C.loon_filesystem_get(cProperties, cPath, pathLen, &fsHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return merr.WrapErrStorage(err, "failed to get filesystem")
	}
	defer C.loon_filesystem_destroy(fsHandle)

	// Local filesystem requires parent directories to exist before writing.
	// Object stores (S3/MinIO/GCS) don't have real directories.
	if storageConfig.GetStorageType() == "local" {
		if idx := strings.LastIndex(filePath, "/"); idx > 0 {
			dir := filePath[:idx]
			cDir := C.CString(dir)
			defer C.free(unsafe.Pointer(cDir))
			result = C.loon_filesystem_create_dir(fsHandle, cDir, C.uint32_t(len(dir)), true)
			if err := HandleLoonFFIResult(result); err != nil {
				return merr.WrapErrStorage(err, "failed to create parent directory %q", dir)
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

// ReadFile reads an entire file using milvus-storage filesystem FFI.
func ReadFile(
	storageConfig *indexpb.StorageConfig,
	filePath string,
) ([]byte, error) {
	return ReadFileWithExternalSpec(storageConfig, filePath, ExternalSpecContext{})
}

// ReadFileWithExternalSpec reads an entire file after injecting external spec
// filesystem aliases. This is used when the input path belongs to an external
// object store described by external_spec.extfs.
func ReadFileWithExternalSpec(
	storageConfig *indexpb.StorageConfig,
	filePath string,
	extfs ExternalSpecContext,
) ([]byte, error) {
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)
	if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "inject extfs")
	}

	filesystemPath, normalizedFilePath, err := normalizeExternalPathForFilesystem(filePath, cProperties, extfs)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "normalize external file path")
	}
	cPath := C.CString(normalizedFilePath)
	defer C.free(unsafe.Pointer(cPath))
	pathLen := C.uint32_t(len(normalizedFilePath))

	var fsHandle C.FileSystemHandle
	cFilesystemPath := C.CString(filesystemPath)
	defer C.free(unsafe.Pointer(cFilesystemPath))
	result := C.loon_filesystem_get(cProperties, cFilesystemPath, C.uint32_t(len(filesystemPath)), &fsHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to get filesystem")
	}
	defer C.loon_filesystem_destroy(fsHandle)

	var outData *C.uint8_t
	var outSize C.uint64_t
	result = C.loon_filesystem_read_file_all(fsHandle, cPath, pathLen, &outData, &outSize)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "read file %s via filesystem %s", normalizedFilePath, filesystemPath)
	}
	if outData == nil || outSize == 0 {
		return nil, nil
	}
	defer C.free(unsafe.Pointer(outData))
	return C.GoBytes(unsafe.Pointer(outData), C.int(outSize)), nil
}

func normalizeExternalPathForFilesystem(path string, properties *C.LoonProperties, extfs ExternalSpecContext) (string, string, error) {
	if extfs.Source == "" || path == "" || properties == nil {
		return path, path, nil
	}

	filesystemPath, err := resolveExternalSourceRelativePath(path, properties, extfs)
	if err != nil {
		return "", "", err
	}

	filePath, err := externalFilesystemFilePath(filesystemPath, properties, extfs)
	if err != nil {
		return "", "", err
	}
	return filesystemPath, filePath, nil
}

func externalFilesystemFilePath(path string, properties *C.LoonProperties, extfs ExternalSpecContext) (string, error) {
	u, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return path, nil
	}

	prefix := ExtfsPrefixForCollection(extfs.CollectionID)
	address := loonPropertyString(properties, prefix+"address")
	bucketName := loonPropertyString(properties, prefix+"bucket_name")
	if address == "" || bucketName == "" {
		return path, nil
	}

	addressHost, err := propertyAddressHost(address)
	if err != nil {
		return "", err
	}
	if addressHost != "" && addressHost != u.Host {
		return path, nil
	}

	key := strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return key, nil
	}
	if key == bucketName {
		return "", nil
	}
	if strings.HasPrefix(key, bucketName+"/") {
		return strings.TrimPrefix(key, bucketName+"/"), nil
	}
	if u.Host == bucketName {
		return key, nil
	}
	return path, nil
}
