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
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

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
