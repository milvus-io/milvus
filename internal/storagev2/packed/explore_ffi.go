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
#include "milvus-storage/ffi_filesystem_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"fmt"
	"strings"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// formatExtensions maps format names to their expected file extensions.
// lance-table is directory-based and not included (no extension filtering needed).
var formatExtensions = map[string]string{
	"parquet": ".parquet",
	"vortex":  ".vortex",
}

// filterFileInfosByFormat filters out files that don't match the expected format extension.
// Returns the filtered list and the count of skipped files.
func filterFileInfosByFormat(fileInfos []FileInfo, format string) ([]FileInfo, int) {
	ext, ok := formatExtensions[format]
	if !ok {
		return fileInfos, 0
	}
	filtered := make([]FileInfo, 0, len(fileInfos))
	for _, fi := range fileInfos {
		if strings.HasSuffix(strings.ToLower(fi.FilePath), ext) {
			filtered = append(filtered, fi)
		}
	}
	return filtered, len(fileInfos) - len(filtered)
}

// FileInfo represents information about an external file.
//
// WARNING: When produced by ExploreFiles (which calls loon_exttable_explore),
// NumRows is the Loon end_index sentinel (-1 for parquet via PlainFormat::explore)
// rather than a real row count. Do NOT compare NumRows against 0 or treat it as
// a row total at this layer. Real row counts are only available after manifest
// construction where Fragment.RowCount = endRow - startRow.
type FileInfo struct {
	FilePath string
	NumRows  int64
}

// ExploreFiles scans an external directory and returns file information.
// It internally calls exttable_explore to find files, then reads the manifest
// GetFileInfo retrieves row count information for a single external file.
// This is used to determine how to split large files into multiple fragments.
func GetFileInfo(
	format string,
	filePath string,
	storageConfig *indexpb.StorageConfig,
	extfs ExternalSpecContext,
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
	if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
		return nil, fmt.Errorf("inject extfs: %w", err)
	}

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

// ExploreFilesReturnManifestPath is like ExploreFiles but also returns the manifest path
// written by loon_exttable_explore. The caller can pass this path to other nodes so they
// can read the file list via ReadFileInfosFromManifestPath without re-exploring.
// NOTE: The temp dir created here is reclaimed by the datacoord refresh manager via
// ChunkManager once the refresh job reaches a terminal state — see
// externalCollectionRefreshManager.cleanupExploreTempForJob.
func ExploreFilesReturnManifestPath(
	columns []string,
	format string,
	baseDir string,
	exploreDir string,
	storageConfig *indexpb.StorageConfig,
	extfs ExternalSpecContext,
) ([]FileInfo, string, error) {
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

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)
	if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
		return nil, "", fmt.Errorf("inject extfs: %w", err)
	}

	var numFiles C.uint64_t
	var outColumnGroupsPath *C.char

	var cColumnsPtr **C.char
	if len(cColumns) > 0 {
		cColumnsPtr = &cColumns[0]
	}

	result := C.loon_exttable_explore(
		cColumnsPtr, C.size_t(len(columns)),
		cFormat, cBaseDir, cExploreDir, cProperties,
		&numFiles, &outColumnGroupsPath,
	)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, "", fmt.Errorf("loon_exttable_explore failed: %w", err)
	}
	if outColumnGroupsPath == nil {
		return nil, "", fmt.Errorf("loon_exttable_explore returned nil column groups path")
	}
	manifestPath := C.GoString(outColumnGroupsPath)
	C.loon_free_cstr(outColumnGroupsPath)

	// Read manifest to get file infos
	fileInfos, err := ReadFileInfosFromManifestPath(manifestPath, storageConfig)
	if err != nil {
		return nil, "", err
	}

	fileInfos, skipped := filterFileInfosByFormat(fileInfos, format)
	if skipped > 0 {
		log.Info("Skipped files with non-matching format during explore",
			zap.Int("skippedCount", skipped),
			zap.String("format", format))
	}

	return fileInfos, manifestPath, nil
}

// ReadFileInfosFromManifestPath reads the explore manifest and returns file infos.
// This allows DataNode to skip ExploreFiles and directly read the file list.
func ReadFileInfosFromManifestPath(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
) ([]FileInfo, error) {
	cManifestPath := C.CString(manifestPath)
	defer C.free(unsafe.Pointer(cManifestPath))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	var manifest *C.LoonManifest
	result := C.loon_exttable_read_manifest(cManifestPath, cProperties, &manifest)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("loon_exttable_read_manifest failed: %w", err)
	}
	defer C.loon_manifest_destroy(manifest)

	var fileInfos []FileInfo
	cgroups := &manifest.column_groups
	if cgroups.column_group_array == nil && cgroups.num_of_column_groups > 0 {
		return nil, fmt.Errorf("column_group_array is nil but num_of_column_groups is %d", cgroups.num_of_column_groups)
	}

	cgArray := unsafe.Slice(cgroups.column_group_array, int(cgroups.num_of_column_groups))
	for i := range cgArray {
		cg := &cgArray[i]
		if cg.files == nil {
			continue
		}
		fileArray := unsafe.Slice(cg.files, int(cg.num_of_files))
		for j := range fileArray {
			if fileArray[j].path == nil {
				return nil, fmt.Errorf("file path is nil in column group %d, file %d", i, j)
			}
			fileInfos = append(fileInfos, FileInfo{
				FilePath: C.GoString(fileArray[j].path),
				NumRows:  int64(fileArray[j].end_index),
			})
		}
	}

	return fileInfos, nil
}
