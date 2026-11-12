// Copyright 2026 Zilliz
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
	"io/fs"
	"sort"
	"strings"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ManifestIndexDir is the manifest-relative directory index artifact paths are
// stored against. milvus-storage strips this prefix when it serializes an index
// entry and restores it on read, so a path built relative to
// <basePath>/<ManifestIndexDir> round-trips back to its absolute form.
const ManifestIndexDir = "_index"

// ManifestIndexRelativePath returns a path strictly inside this segment's
// _index directory. New entries must never reference legacy external layouts.
func ManifestIndexRelativePath(basePath, indexPrefix string) (string, error) {
	prefix := strings.TrimRight(basePath, "/") + "/" + ManifestIndexDir + "/"
	relative, ok := strings.CutPrefix(indexPrefix, prefix)
	if basePath == "" || !ok || relative == "." || !fs.ValidPath(relative) {
		return "", merr.WrapErrServiceInternalMsg("index artifact %s is outside segment index directory %s", indexPrefix, prefix)
	}
	return relative, nil
}

// ManifestIndexInfo identifies a completed index artifact registered in a
// StorageV3 manifest. Path is the artifact root, IndexFileKeys are relative to
// it. Properties preserves optional storage metadata from older manifests;
// Milvus publishers leave it empty rather than copying collection parameters.
//
// The index bytes must already exist before this metadata is published. A
// manifest transaction only makes the metadata visible atomically with a new
// manifest revision; it never writes, moves, or deletes index files.
//
// On write Path is relative to <basePath>/_index, without parent traversal.
// On read storage resolves it to a complete artifact key. Existing legacy
// entries can be read but cannot be added or updated with an external path.
type ManifestIndexInfo struct {
	ColumnName                string
	IndexName                 string
	IndexType                 string
	Path                      string
	FieldID                   int64
	IndexID                   int64
	BuildID                   int64
	IndexVersion              int64
	NumRows                   int64
	SerializedSize            int64
	MemSize                   int64
	CurrentIndexVersion       int32
	CurrentScalarIndexVersion int32
	IndexStorePathVersion     indexpb.IndexStorePathVersion
	IndexFileKeys             []string
	Properties                map[string]string
}

// DropIndexEntry removes one index's metadata from a manifest revision.
//
// milvus-storage drops every entry whose index_id matches, so a drop issued
// from stale metadata could delete a newer artifact published for the same
// user index by a rebuild. ExpectedBuildID closes that window: the drop is
// resolved against the exact revision the transaction is opened at, and is
// rejected when the manifest holds a different build for the index.
type DropIndexEntry struct {
	IndexID         int64
	ExpectedBuildID int64
}

// GetManifestIndexInfos returns all completed index artifacts recorded in a
// StorageV3 manifest.
func GetManifestIndexInfos(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
) ([]ManifestIndexInfo, error) {
	cManifest, err := GetManifestHandle(manifestPath, storageConfig)
	if err != nil {
		return nil, merr.Wrap(err, "failed to get manifest")
	}
	defer C.loon_manifest_destroy(cManifest)
	return manifestIndexInfos(cManifest, manifestPath)
}

// manifestIndexInfos projects the C index section of an already-open manifest
// handle. The caller owns cManifest.
func manifestIndexInfos(cManifest *C.LoonManifest, manifestPath string) ([]ManifestIndexInfo, error) {
	numIndexes := int(cManifest.indexes.num_indexes)
	if numIndexes == 0 {
		return nil, nil
	}
	if cManifest.indexes.indexes == nil {
		return nil, merr.WrapErrServiceInternalMsg("manifest %s has malformed index metadata", manifestPath)
	}

	cIndexes := unsafe.Slice(cManifest.indexes.indexes, numIndexes)
	indexes := make([]ManifestIndexInfo, 0, numIndexes)
	for _, cIndex := range cIndexes {
		if cIndex.column_name == nil || cIndex.index_type == nil || cIndex.path == nil {
			return nil, merr.WrapErrServiceInternalMsg("manifest %s has malformed index metadata", manifestPath)
		}
		numFileKeys := int(cIndex.num_index_file_keys)
		if numFileKeys > 0 && cIndex.index_file_keys == nil {
			return nil, merr.WrapErrServiceInternalMsg("manifest %s has malformed index file keys", manifestPath)
		}
		numProperties := int(cIndex.num_properties)
		if numProperties > 0 && (cIndex.property_keys == nil || cIndex.property_values == nil) {
			return nil, merr.WrapErrServiceInternalMsg("manifest %s has malformed index properties", manifestPath)
		}

		properties := make(map[string]string, numProperties)
		if numProperties > 0 {
			keys := unsafe.Slice(cIndex.property_keys, numProperties)
			values := unsafe.Slice(cIndex.property_values, numProperties)
			for i := range keys {
				if keys[i] == nil || values[i] == nil {
					return nil, merr.WrapErrServiceInternalMsg("manifest %s has malformed index properties", manifestPath)
				}
				properties[C.GoString(keys[i])] = C.GoString(values[i])
			}
		}

		fileKeys := make([]string, 0, numFileKeys)
		if numFileKeys > 0 {
			keys := unsafe.Slice(cIndex.index_file_keys, numFileKeys)
			for _, key := range keys {
				if key == nil {
					return nil, merr.WrapErrServiceInternalMsg("manifest %s has malformed index file keys", manifestPath)
				}
				fileKeys = append(fileKeys, C.GoString(key))
			}
		}

		indexName := ""
		if cIndex.index_name != nil {
			indexName = C.GoString(cIndex.index_name)
		}

		indexes = append(indexes, ManifestIndexInfo{
			ColumnName:                C.GoString(cIndex.column_name),
			IndexName:                 indexName,
			IndexType:                 C.GoString(cIndex.index_type),
			Path:                      C.GoString(cIndex.path),
			FieldID:                   int64(cIndex.field_id),
			IndexID:                   int64(cIndex.index_id),
			BuildID:                   int64(cIndex.build_id),
			IndexVersion:              int64(cIndex.index_version),
			NumRows:                   int64(cIndex.num_rows),
			SerializedSize:            int64(cIndex.serialized_size),
			MemSize:                   int64(cIndex.mem_size),
			CurrentIndexVersion:       int32(cIndex.current_index_version),
			CurrentScalarIndexVersion: int32(cIndex.current_scalar_index_version),
			IndexStorePathVersion:     indexpb.IndexStorePathVersion(cIndex.index_store_path_version),
			IndexFileKeys:             fileKeys,
			Properties:                properties,
		})
	}
	return indexes, nil
}

// stageIndexInfo stages one completed index artifact onto a loon transaction.
// Every C allocation is released when this function returns, which is why the
// per-index staging lives in its own function instead of a loop body.
func stageIndexInfo(handle C.LoonTransactionHandle, index ManifestIndexInfo) error {
	if index.Path == "." || !fs.ValidPath(index.Path) {
		return merr.WrapErrServiceInternalMsg("manifest index %d requires a path inside _index: %s", index.IndexID, index.Path)
	}
	for _, key := range index.IndexFileKeys {
		if key == "." || !fs.ValidPath(key) || strings.Contains(key, "/") {
			return merr.WrapErrServiceInternalMsg("manifest index %d requires plain file names: %s", index.IndexID, key)
		}
	}
	cColumnName := C.CString(index.ColumnName)
	defer C.free(unsafe.Pointer(cColumnName))
	cIndexName := C.CString(index.IndexName)
	defer C.free(unsafe.Pointer(cIndexName))
	cIndexType := C.CString(index.IndexType)
	defer C.free(unsafe.Pointer(cIndexType))
	cPath := C.CString(index.Path)
	defer C.free(unsafe.Pointer(cPath))

	cIndexFileKeys, freeFileKeys := newCStringArray(index.IndexFileKeys)
	defer freeFileKeys()

	propertyNames := make([]string, 0, len(index.Properties))
	for key := range index.Properties {
		propertyNames = append(propertyNames, key)
	}
	// Stable ordering keeps a republished manifest byte-identical for the same
	// logical input, which makes the round-trip assertions in tests meaningful.
	sort.Strings(propertyNames)
	propertyValues := make([]string, 0, len(propertyNames))
	for _, key := range propertyNames {
		propertyValues = append(propertyValues, index.Properties[key])
	}
	cPropertyKeys, freePropertyKeys := newCStringArray(propertyNames)
	defer freePropertyKeys()
	cPropertyValues, freePropertyValues := newCStringArray(propertyValues)
	defer freePropertyValues()

	cIndex := C.LoonIndexInfo{
		column_name:                  cColumnName,
		index_name:                   cIndexName,
		index_type:                   cIndexType,
		path:                         cPath,
		field_id:                     C.int64_t(index.FieldID),
		index_id:                     C.int64_t(index.IndexID),
		build_id:                     C.int64_t(index.BuildID),
		index_version:                C.int64_t(index.IndexVersion),
		num_rows:                     C.int64_t(index.NumRows),
		serialized_size:              C.int64_t(index.SerializedSize),
		mem_size:                     C.int64_t(index.MemSize),
		current_index_version:        C.int32_t(index.CurrentIndexVersion),
		current_scalar_index_version: C.int32_t(index.CurrentScalarIndexVersion),
		index_store_path_version:     C.int32_t(index.IndexStorePathVersion),
		index_file_keys:              cIndexFileKeys,
		num_index_file_keys:          C.uint32_t(len(index.IndexFileKeys)),
		property_keys:                cPropertyKeys,
		property_values:              cPropertyValues,
		num_properties:               C.uint32_t(len(propertyNames)),
	}
	if err := HandleLoonFFIResult(C.loon_transaction_add_index_info(handle, &cIndex)); err != nil {
		return merr.WrapErrStorage(err, "commit manifest add_index_info")
	}
	return nil
}

// stageDropIndex stages metadata removal without touching artifact files.
// The lifecycle caller owns the ordering of file deletion and metadata retraction.
func stageDropIndex(handle C.LoonTransactionHandle, indexID int64) error {
	if err := HandleLoonFFIResult(C.loon_transaction_drop_index(handle, C.int64_t(indexID))); err != nil {
		return merr.WrapErrStorage(err, "commit manifest drop_index")
	}
	return nil
}

// newCStringArray copies values into a C array of C strings. The returned
// release function frees the array and every element; it is safe to call once.
func newCStringArray(values []string) (**C.char, func()) {
	if len(values) == 0 {
		return nil, func() {}
	}
	elements := make([]*C.char, 0, len(values))
	for _, value := range values {
		elements = append(elements, C.CString(value))
	}
	arraySize := C.size_t(len(elements)) * C.size_t(unsafe.Sizeof(uintptr(0)))
	array := (**C.char)(C.malloc(arraySize))
	copy(unsafe.Slice(array, len(elements)), elements)
	return array, func() {
		for _, element := range elements {
			C.free(unsafe.Pointer(element))
		}
		C.free(unsafe.Pointer(array))
	}
}
