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
#include "milvus-storage/ffi_c.h"
#include "milvus-storage/ffi_exttable_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// FileInfo represents information about an external file
type FileInfo struct {
	FilePath string
	NumRows  int64
}

// ExploreFiles scans an external directory and returns a list of file paths.
// It internally calls exttable_explore to find files, then reads the manifest
// to extract column group and file information.
func ExploreFiles(
	columns []string,
	format string,
	baseDir string,
	exploreDir string,
	storageConfig *indexpb.StorageConfig,
) ([]string, error) {
	// Create C string arrays for columns
	cColumns := make([]*C.char, len(columns))
	for i, col := range columns {
		cColumns[i] = C.CString(col)
	}
	defer func() {
		for _, c := range cColumns {
			C.free(unsafe.Pointer(c))
		}
	}()

	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	cBaseDir := C.CString(baseDir)
	defer C.free(unsafe.Pointer(cBaseDir))

	cExploreDir := C.CString(exploreDir)
	defer C.free(unsafe.Pointer(cExploreDir))

	// Create properties from storage config
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	var numFiles C.uint64_t
	var outColumnGroupsPath *C.char

	var cColumnsPtr **C.char
	if len(cColumns) > 0 {
		cColumnsPtr = &cColumns[0]
	}

	// Step 1: Call loon_exttable_explore to discover external files
	result := C.loon_exttable_explore(
		cColumnsPtr,
		C.size_t(len(columns)),
		cFormat,
		cBaseDir,
		cExploreDir,
		cProperties,
		&numFiles,
		&outColumnGroupsPath,
	)

	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("loon_exttable_explore failed: %w", err)
	}

	// Check if we got valid column groups path
	if outColumnGroupsPath == nil {
		return nil, fmt.Errorf("loon_exttable_explore succeeded but returned nil column groups path")
	}

	// Ensure we free the C-allocated path when done
	defer C.loon_free_cstr(outColumnGroupsPath)

	// Step 2: Read the manifest file (which contains column groups information)
	var manifest *C.LoonManifest

	result = C.loon_exttable_read_manifest(outColumnGroupsPath, cProperties, &manifest)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("loon_exttable_read_manifest failed: %w", err)
	}

	// Ensure we destroy the manifest when done (this also frees column_groups)
	defer C.loon_manifest_destroy(manifest)

	// Step 3: Extract file paths from manifest's column groups
	var filePaths []string
	cgroups := &manifest.column_groups

	// Validate column groups structure before accessing
	if cgroups.column_group_array == nil && cgroups.num_of_column_groups > 0 {
		return nil, fmt.Errorf("column_group_array is nil but num_of_column_groups is %d", cgroups.num_of_column_groups)
	}

	cgArray := unsafe.Slice(cgroups.column_group_array, int(cgroups.num_of_column_groups))
	for i := range cgArray {
		cg := &cgArray[i]

		// Check if files array is valid
		if cg.files == nil && cg.num_of_files > 0 {
			return nil, fmt.Errorf("column group %d has num_of_files=%d but files array is nil (possible FFI data corruption)",
				i, cg.num_of_files)
		}

		if cg.files == nil {
			continue // Empty column group, skip
		}

		fileArray := unsafe.Slice(cg.files, int(cg.num_of_files))
		for j := range fileArray {
			// Validate file path pointer
			if fileArray[j].path == nil {
				return nil, fmt.Errorf("file path is nil in column group %d, file %d (possible FFI data corruption)", i, j)
			}
			filePaths = append(filePaths, C.GoString(fileArray[j].path))
		}
	}

	return filePaths, nil
}

// GetFileInfo retrieves row count information for a single external file.
// This is used to determine how to split large files into multiple fragments.
func GetFileInfo(
	format string,
	filePath string,
	storageConfig *indexpb.StorageConfig,
) (*FileInfo, error) {
	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	cFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cFilePath))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	var numRows C.uint64_t

	result := C.loon_exttable_get_file_info(cFormat, cFilePath, cProperties, &numRows)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("loon_exttable_get_file_info failed: %w", err)
	}

	return &FileInfo{
		FilePath: filePath,
		NumRows:  int64(numRows),
	}, nil
}
