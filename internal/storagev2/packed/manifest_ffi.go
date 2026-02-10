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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// Fragment represents a data fragment from an external data source.
// A large file (e.g., 10M rows) can be split into multiple fragments.
type Fragment struct {
	FragmentID int64  // Unique fragment identifier
	FilePath   string // File path
	StartRow   int64  // Start row index within the file (inclusive)
	EndRow     int64  // End row index within the file (exclusive)
	RowCount   int64  // Number of rows (EndRow - StartRow)
}

// CreateManifestForSegment creates a manifest file for a segment.
// It creates column groups from fragments and commits them using a transaction.
// Returns the manifest path string that can be stored in SegmentInfo.manifest_path.
func CreateManifestForSegment(
	basePath string,
	columns []string,
	format string,
	fragments []Fragment,
	storageConfig *indexpb.StorageConfig,
) (string, error) {
	if len(fragments) == 0 {
		return "", fmt.Errorf("fragments cannot be empty")
	}

	// Create column groups from fragments
	columnGroups, err := createColumnGroups(columns, format, fragments)
	if err != nil {
		return "", fmt.Errorf("failed to create column groups: %w", err)
	}
	defer C.loon_column_groups_destroy(columnGroups)

	// Create properties from storage config
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	// Convert base path to C string
	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	// Begin transaction (read_version=-1 for latest, retry_limit=1)
	var transactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(-1), C.uint32_t(1), &transactionHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", fmt.Errorf("loon_transaction_begin failed: %w", err)
	}
	defer C.loon_transaction_destroy(transactionHandle)

	// Append files to transaction
	result = C.loon_transaction_append_files(transactionHandle, columnGroups)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", fmt.Errorf("loon_transaction_append_files failed: %w", err)
	}

	// Commit transaction
	var committedVersion C.int64_t
	result = C.loon_transaction_commit(transactionHandle, &committedVersion)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", fmt.Errorf("loon_transaction_commit failed: %w", err)
	}

	// Return manifest path using the helper function
	return MarshalManifestPath(basePath, int64(committedVersion)), nil
}

// createColumnGroups creates a LoonColumnGroups structure from fragments.
// This is an internal function used by CreateManifestForSegment.
func createColumnGroups(
	columns []string,
	format string,
	fragments []Fragment,
) (*C.LoonColumnGroups, error) {
	// Create C string array for columns
	cColumns := make([]*C.char, len(columns))
	for i, col := range columns {
		cColumns[i] = C.CString(col)
	}
	defer func() {
		for _, c := range cColumns {
			C.free(unsafe.Pointer(c))
		}
	}()

	// Create C string for format
	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	// Create C arrays for paths, start indices, and end indices
	cPaths := make([]*C.char, len(fragments))
	cStartIndices := make([]C.int64_t, len(fragments))
	cEndIndices := make([]C.int64_t, len(fragments))

	for i, f := range fragments {
		cPaths[i] = C.CString(f.FilePath)
		cStartIndices[i] = C.int64_t(f.StartRow)
		cEndIndices[i] = C.int64_t(f.EndRow)
	}
	defer func() {
		for _, p := range cPaths {
			C.free(unsafe.Pointer(p))
		}
	}()

	var outColumnGroups *C.LoonColumnGroups
	var cColumnsPtr **C.char
	var cPathsPtr **C.char
	var cStartIndicesPtr *C.int64_t
	var cEndIndicesPtr *C.int64_t

	if len(cColumns) > 0 {
		cColumnsPtr = &cColumns[0]
	}
	if len(cPaths) > 0 {
		cPathsPtr = &cPaths[0]
	}
	if len(fragments) > 0 {
		cStartIndicesPtr = &cStartIndices[0]
		cEndIndicesPtr = &cEndIndices[0]
	}

	result := C.loon_column_groups_create(
		cColumnsPtr,
		C.size_t(len(columns)),
		cFormat,
		cPathsPtr,
		cStartIndicesPtr,
		cEndIndicesPtr,
		C.size_t(len(fragments)),
		&outColumnGroups,
	)

	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("loon_column_groups_create failed: %w", err)
	}

	return outColumnGroups, nil
}

// ReadFragmentsFromManifest reads fragment info from a manifest path.
// This function wraps the C exttable_read_column_groups call.
func ReadFragmentsFromManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
) ([]Fragment, error) {
	// 1. Parse manifest path to get base path
	basePath, _, err := UnmarshalManfestPath(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse manifest path: %w", err)
	}

	// 2. Create properties from storage config
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	// 3. Convert base path to C string
	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	// 4. Call loon_exttable_read_manifest
	var manifest *C.LoonManifest
	result := C.loon_exttable_read_manifest(cBasePath, cProperties, &manifest)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("loon_exttable_read_manifest failed: %w", err)
	}

	// 5. Destroy manifest when done
	defer C.loon_manifest_destroy(manifest)

	// 6. Extract fragments from LoonManifest's column groups
	var fragments []Fragment
	var fragmentID int64 = 0

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
			log.Warn("column group has num_of_files > 0 but files array is nil in ReadFragmentsFromManifest",
				zap.Int("columnGroupIndex", i),
				zap.Uint64("numFiles", uint64(cg.num_of_files)))
			continue
		}

		if cg.files == nil {
			continue // Empty column group, skip
		}

		fileArray := unsafe.Slice(cg.files, int(cg.num_of_files))
		for j := range fileArray {
			file := &fileArray[j]

			// Validate file path pointer
			if file.path == nil {
				log.Warn("file path is nil in ReadFragmentsFromManifest",
					zap.Int("columnGroupIndex", i),
					zap.Int("fileIndex", j))
				continue
			}

			filePath := C.GoString(file.path)
			startRow := int64(file.start_index)
			endRow := int64(file.end_index)

			fragments = append(fragments, Fragment{
				FragmentID: fragmentID,
				FilePath:   filePath,
				StartRow:   startRow,
				EndRow:     endRow,
				RowCount:   endRow - startRow,
			})
			fragmentID++
		}
	}

	return fragments, nil
}
