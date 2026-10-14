// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
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
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "segcore/external_utils_c.h"
*/
import "C"

import (
	"encoding/json"
	"runtime"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetExternalFileColumns reads authoritative physical names from file metadata.
// A metadata failure must propagate; it must never imply an absent column.
func GetExternalFileColumns(format, path string, storageConfig *indexpb.StorageConfig, extfs ExternalSpecContext) ([]string, error) {
	properties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, merr.Wrap(err, "create external metadata properties")
	}
	defer C.loon_properties_free(properties)
	if err := injectExternalSpecProperties(properties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
		return nil, merr.Wrap(err, "configure external metadata reader")
	}
	path, err = normalizeExternalResolvedPath(path, properties, extfs)
	if err != nil {
		return nil, merr.Wrap(err, "normalize external metadata path")
	}
	cFormat, cPath := C.CString(format), C.CString(path)
	defer C.free(unsafe.Pointer(cFormat))
	defer C.free(unsafe.Pointer(cPath))
	var output *C.char
	status := C.GetExternalFileColumns(cFormat, cPath, properties, &output)
	defer C.free(unsafe.Pointer(output))
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	var columns []string
	if err := json.Unmarshal([]byte(C.GoString(output)), &columns); err != nil {
		return nil, merr.Wrap(err, "invalid external column metadata")
	}
	return columns, nil
}

// SampleExternalFieldSizes samples rows from an external segment via Take API
// and returns per-field average memory size (Arrow buffer size, decompressed).
// The returned values are equivalent to Binlog.MemorySize for internal segments.
//
// Properties are constructed from storageConfig + extfs overrides on the Go side,
// matching the pattern used by other FFI calls (explore, manifest, etc.).
// This avoids dependency on the C++ LoonFFIPropertiesSingleton.
func SampleExternalFieldSizes(
	manifestPath string,
	sampleRows int,
	collectionID int64,
	externalSource string,
	externalSpec string,
	schema *schemapb.CollectionSchema,
	storageConfig *indexpb.StorageConfig,
) (map[string]int64, error) {
	if storageConfig == nil {
		return nil, merr.WrapErrStorageMsg("storageConfig is required for SampleExternalFieldSizes")
	}
	if manifestPath == "" {
		return nil, merr.WrapErrStorageMsg("manifest_path is empty for SampleExternalFieldSizes")
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)
	if err := injectExternalSpecProperties(cProperties, collectionID, externalSource, externalSpec); err != nil {
		return nil, merr.Wrap(err, "inject extfs")
	}

	cManifestPath := C.CString(manifestPath)
	defer C.free(unsafe.Pointer(cManifestPath))

	var cSchema C.CProto
	var schemaBytes []byte
	if schema != nil {
		schemaForSample := schema
		if schemaForSample.GetExternalSpec() == "" && externalSpec != "" {
			schemaForSample = proto.Clone(schemaForSample).(*schemapb.CollectionSchema)
			schemaForSample.ExternalSpec = externalSpec
		}
		schemaBytes, err = proto.Marshal(schemaForSample)
		if err != nil {
			return nil, merr.WrapErrStorage(err, "failed to marshal collection schema")
		}
		if len(schemaBytes) > 0 {
			cSchema.proto_blob = unsafe.Pointer(&schemaBytes[0])
			cSchema.proto_size = C.int64_t(len(schemaBytes))
		}
	}

	var result C.CFieldMemSizeList
	status := C.SampleExternalSegmentFieldSizes(
		cManifestPath, C.int(sampleRows),
		C.int64_t(collectionID), cProperties,
		cSchema,
		&result)
	runtime.KeepAlive(schemaBytes)
	defer C.FreeCFieldMemSizeList(&result)

	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}

	sizes := make(map[string]int64, int(result.count))
	if result.count > 0 && result.sizes != nil {
		slice := unsafe.Slice(result.sizes, result.count)
		for _, entry := range slice {
			sizes[C.GoString(entry.field_name)] = int64(entry.avg_mem_bytes)
		}
	}
	return sizes, nil
}
