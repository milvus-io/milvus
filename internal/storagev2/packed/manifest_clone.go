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
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// CloneManifestWithoutDeltaLogs clones column groups, stats, and LOB metadata
// from sourceManifestPath into targetBasePath, intentionally dropping the
// source delta log list. pathMapper rewrites manifest file paths from source
// layout to target layout and must be non-nil.
func CloneManifestWithoutDeltaLogs(
	sourceManifestPath string,
	targetBasePath string,
	storageConfig *indexpb.StorageConfig,
	pathMapper func(string) (string, error),
) (string, error) {
	stagedColumnGroups := &clonedColumnGroupResources{}
	defer stagedColumnGroups.free()

	sourceManifest, err := GetManifestHandle(sourceManifestPath, storageConfig)
	if err != nil {
		return "", merr.WrapErrStorage(err, "failed to get source manifest")
	}
	defer C.loon_manifest_destroy(sourceManifest)

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", merr.WrapErrStorage(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)

	cTargetBasePath := C.CString(targetBasePath)
	defer C.free(unsafe.Pointer(cTargetBasePath))

	var transactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(
		cTargetBasePath,
		cProperties,
		C.int64_t(0),
		C.LOON_TRANSACTION_RESOLVE_OVERWRITE,
		getRetryLimit(),
		&transactionHandle,
	)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", merr.WrapErrStorage(err, "failed to begin target manifest transaction")
	}
	defer C.loon_transaction_destroy(transactionHandle)

	if err := cloneColumnGroupsWithoutDelta(transactionHandle, sourceManifest, pathMapper, stagedColumnGroups); err != nil {
		return "", err
	}
	if err := cloneStats(transactionHandle, sourceManifest, pathMapper); err != nil {
		return "", err
	}
	if err := cloneLobFiles(transactionHandle, sourceManifest, pathMapper); err != nil {
		return "", err
	}

	var commitVersion C.int64_t
	result = C.loon_transaction_commit(transactionHandle, &commitVersion)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", merr.WrapErrStorage(err, "failed to commit cloned manifest")
	}
	return MarshalManifestPath(targetBasePath, int64(commitVersion)), nil
}

func cloneColumnGroupsWithoutDelta(
	transactionHandle C.LoonTransactionHandle,
	sourceManifest *C.LoonManifest,
	pathMapper func(string) (string, error),
	stagedResources *clonedColumnGroupResources,
) error {
	numGroups := int(sourceManifest.column_groups.num_of_column_groups)
	if numGroups == 0 {
		return merr.WrapErrServiceInternalMsg("source manifest has no column groups")
	}

	groups := unsafe.Slice(sourceManifest.column_groups.column_group_array, numGroups)
	for groupIdx := range groups {
		group := groups[groupIdx]
		numFiles := int(group.num_of_files)
		cStrings := make([]*C.char, 0)
		var cFiles *C.LoonColumnGroupFile
		if numFiles > 0 {
			cFiles = (*C.LoonColumnGroupFile)(C.malloc(C.size_t(numFiles) * C.size_t(unsafe.Sizeof(C.LoonColumnGroupFile{}))))
			if cFiles == nil {
				return merr.WrapErrServiceInternalMsg("failed to allocate cloned column group files")
			}
		}

		if numFiles > 0 {
			sourceFiles := unsafe.Slice(group.files, numFiles)
			clonedFiles := unsafe.Slice(cFiles, numFiles)
			for fileIdx := range sourceFiles {
				sourceFile := sourceFiles[fileIdx]
				mappedPath, err := pathMapper(C.GoString(sourceFile.path))
				if err != nil {
					freeCStrings(cStrings)
					freeColumnGroupFiles(cFiles)
					return merr.Wrap(err, "failed to map column group file path")
				}
				cPath := C.CString(mappedPath)
				cStrings = append(cStrings, cPath)

				clonedFiles[fileIdx] = C.LoonColumnGroupFile{
					path:            cPath,
					start_index:     sourceFile.start_index,
					end_index:       sourceFile.end_index,
					property_keys:   sourceFile.property_keys,
					property_values: sourceFile.property_values,
					num_properties:  sourceFile.num_properties,
				}
			}
		}

		clonedGroup := (*C.LoonColumnGroup)(C.malloc(C.size_t(unsafe.Sizeof(C.LoonColumnGroup{}))))
		if clonedGroup == nil {
			freeCStrings(cStrings)
			freeColumnGroupFiles(cFiles)
			return merr.WrapErrServiceInternalMsg("failed to allocate cloned column group")
		}
		*clonedGroup = C.LoonColumnGroup{
			columns:        group.columns,
			num_of_columns: group.num_of_columns,
			format:         group.format,
			num_of_files:   group.num_of_files,
		}
		if cFiles != nil {
			clonedGroup.files = cFiles
		}

		// The transaction keeps these pointers until commit returns.
		stagedResources.add(clonedGroup, cFiles, cStrings)
		result := C.loon_transaction_add_column_group(transactionHandle, clonedGroup)
		if err := HandleLoonFFIResult(result); err != nil {
			return merr.WrapErrStorage(err, "failed to add cloned column group %d", groupIdx)
		}
	}
	return nil
}

func cloneStats(
	transactionHandle C.LoonTransactionHandle,
	sourceManifest *C.LoonManifest,
	pathMapper func(string) (string, error),
) error {
	numStats := int(sourceManifest.stats.num_stats)
	if numStats == 0 {
		return nil
	}

	statKeys := unsafe.Slice(sourceManifest.stats.stat_keys, numStats)
	statFiles := unsafe.Slice(sourceManifest.stats.stat_files, numStats)
	statFileCounts := unsafe.Slice(sourceManifest.stats.stat_file_counts, numStats)
	statMetaKeys := unsafe.Slice(sourceManifest.stats.stat_metadata_keys, numStats)
	statMetaValues := unsafe.Slice(sourceManifest.stats.stat_metadata_values, numStats)
	statMetaCounts := unsafe.Slice(sourceManifest.stats.stat_metadata_counts, numStats)

	for i := 0; i < numStats; i++ {
		key := C.GoString(statKeys[i])
		fileCount := int(statFileCounts[i])
		files := make([]string, 0, fileCount)
		if fileCount > 0 {
			sourceFiles := unsafe.Slice(statFiles[i], fileCount)
			for j := 0; j < fileCount; j++ {
				mappedPath, err := pathMapper(C.GoString(sourceFiles[j]))
				if err != nil {
					return merr.Wrapf(err, "failed to map stat file path for %s", key)
				}
				files = append(files, mappedPath)
			}
		}

		metaCount := int(statMetaCounts[i])
		metadata := make(map[string]string, metaCount)
		if metaCount > 0 {
			keys := unsafe.Slice(statMetaKeys[i], metaCount)
			values := unsafe.Slice(statMetaValues[i], metaCount)
			for j := 0; j < metaCount; j++ {
				metadata[C.GoString(keys[j])] = C.GoString(values[j])
			}
		}

		if err := UpdateTransactionStat(transactionHandle, key, files, metadata); err != nil {
			return merr.Wrapf(err, "failed to clone stat %s", key)
		}
	}
	return nil
}

func cloneLobFiles(
	transactionHandle C.LoonTransactionHandle,
	sourceManifest *C.LoonManifest,
	pathMapper func(string) (string, error),
) error {
	numLobFiles := int(sourceManifest.lob_files.num_files)
	if numLobFiles == 0 || sourceManifest.lob_files.files == nil {
		return nil
	}

	lobFiles := unsafe.Slice(sourceManifest.lob_files.files, numLobFiles)
	for i := range lobFiles {
		sourceFile := lobFiles[i]
		mappedPath, err := pathMapper(C.GoString(sourceFile.path))
		if err != nil {
			return merr.Wrap(err, "failed to map LOB file path")
		}
		cPath := C.CString(mappedPath)
		cLobFile := C.LoonLobFileInfo{
			path:            cPath,
			field_id:        sourceFile.field_id,
			total_rows:      sourceFile.total_rows,
			valid_rows:      sourceFile.valid_rows,
			file_size_bytes: sourceFile.file_size_bytes,
		}
		result := C.loon_transaction_add_lob_file(transactionHandle, &cLobFile)
		C.free(unsafe.Pointer(cPath))
		if err := HandleLoonFFIResult(result); err != nil {
			return merr.WrapErrStorage(err, "failed to add cloned LOB file %d", i)
		}
	}
	return nil
}

func freeCStrings(values []*C.char) {
	for _, s := range values {
		C.free(unsafe.Pointer(s))
	}
}

func freeColumnGroupFiles(files *C.LoonColumnGroupFile) {
	if files != nil {
		C.free(unsafe.Pointer(files))
	}
}

type clonedColumnGroupResources struct {
	groups  []*C.LoonColumnGroup
	files   []*C.LoonColumnGroupFile
	strings []*C.char
}

func (r *clonedColumnGroupResources) add(group *C.LoonColumnGroup, files *C.LoonColumnGroupFile, strings []*C.char) {
	r.groups = append(r.groups, group)
	if files != nil {
		r.files = append(r.files, files)
	}
	r.strings = append(r.strings, strings...)
}

func (r *clonedColumnGroupResources) free() {
	for _, group := range r.groups {
		C.free(unsafe.Pointer(group))
	}
	for _, files := range r.files {
		freeColumnGroupFiles(files)
	}
	freeCStrings(r.strings)
	r.groups = nil
	r.files = nil
	r.strings = nil
}
