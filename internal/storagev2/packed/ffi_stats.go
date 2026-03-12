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
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// ManifestStat represents a stat entry from the manifest.
type ManifestStat struct {
	Paths    []string
	Metadata map[string]string
}

// UpdateTransactionStat writes a stat entry to a manifest transaction.
// key is "type.fieldID" (e.g. "bloom_filter.100"), files are relative
// paths, metadata is arbitrary key-value pairs (may be nil).
func UpdateTransactionStat(
	handle C.LoonTransactionHandle,
	key string,
	files []string,
	metadata map[string]string,
) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	// Build files array
	cFiles := make([]*C.char, len(files))
	for i, f := range files {
		cFiles[i] = C.CString(f)
	}
	defer func() {
		for _, p := range cFiles {
			C.free(unsafe.Pointer(p))
		}
	}()

	var cFilesPtr **C.char
	if len(cFiles) > 0 {
		cFilesPtr = &cFiles[0]
	}

	// Build metadata parallel arrays
	var cMetaKeys, cMetaValues []*C.char
	for k, v := range metadata {
		cMetaKeys = append(cMetaKeys, C.CString(k))
		cMetaValues = append(cMetaValues, C.CString(v))
	}
	defer func() {
		for _, p := range cMetaKeys {
			C.free(unsafe.Pointer(p))
		}
		for _, p := range cMetaValues {
			C.free(unsafe.Pointer(p))
		}
	}()

	var cMetaKeysPtr, cMetaValuesPtr **C.char
	if len(cMetaKeys) > 0 {
		cMetaKeysPtr = &cMetaKeys[0]
		cMetaValuesPtr = &cMetaValues[0]
	}

	result := C.loon_transaction_update_stat(
		handle,
		cKey,
		(**C.char)(unsafe.Pointer(cFilesPtr)),
		C.size_t(len(files)),
		(**C.char)(unsafe.Pointer(cMetaKeysPtr)),
		(**C.char)(unsafe.Pointer(cMetaValuesPtr)),
		C.size_t(len(metadata)),
	)
	return HandleLoonFFIResult(result)
}

// GetManifestStats reads all stats from a manifest.
// Returns map[statKey]ManifestStat with paths and metadata for each key.
func GetManifestStats(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
) (map[string]ManifestStat, error) {
	cManifest, err := GetManifestHandle(manifestPath, storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}
	defer C.loon_manifest_destroy(cManifest)

	numStats := int(cManifest.stats.num_stats)
	result := make(map[string]ManifestStat, numStats)

	if numStats == 0 {
		return result, nil
	}

	statKeys := unsafe.Slice(cManifest.stats.stat_keys, numStats)
	statFiles := unsafe.Slice(cManifest.stats.stat_files, numStats)
	statFileCounts := unsafe.Slice(cManifest.stats.stat_file_counts, numStats)
	statMetaKeys := unsafe.Slice(cManifest.stats.stat_metadata_keys, numStats)
	statMetaValues := unsafe.Slice(cManifest.stats.stat_metadata_values, numStats)
	statMetaCounts := unsafe.Slice(cManifest.stats.stat_metadata_counts, numStats)

	for i := 0; i < numStats; i++ {
		key := C.GoString(statKeys[i])

		// Read paths
		fileCount := int(statFileCounts[i])
		paths := make([]string, fileCount)
		if fileCount > 0 {
			files := unsafe.Slice(statFiles[i], fileCount)
			for j := 0; j < fileCount; j++ {
				paths[j] = C.GoString(files[j])
			}
		}

		// Read metadata
		metaCount := int(statMetaCounts[i])
		metadata := make(map[string]string, metaCount)
		if metaCount > 0 {
			mKeys := unsafe.Slice(statMetaKeys[i], metaCount)
			mValues := unsafe.Slice(statMetaValues[i], metaCount)
			for j := 0; j < metaCount; j++ {
				metadata[C.GoString(mKeys[j])] = C.GoString(mValues[j])
			}
		}

		result[key] = ManifestStat{
			Paths:    paths,
			Metadata: metadata,
		}
	}

	return result, nil
}
