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
#cgo pkg-config: milvus_core milvus-storage

#include <stdlib.h>
#include "milvus-storage/ffi_c.h"

int MilvusTablePathRoutesToClusterStorage(const char* path);

LoonFFIResult loon_milvus_table_resolve_source_column_groups(
    const LoonColumnGroups* source_groups,
    const char* external_source,
    const LoonProperties* properties,
    LoonColumnGroups** out_groups);
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A milvus-table source names its files in snapshot metadata and source
// manifests. milvus-storage reads a path without a URI scheme through this
// cluster's own storage, so with an external source every such path must
// resolve to a URI of the external source storage. The C++ import enforces the
// same rule for the paths it copies into target manifests.

// checkMilvusTableExternalPath rejects resolvedPath when milvus-storage would
// read it through this cluster's own storage. sourcePath is the path as the
// source named it.
func checkMilvusTableExternalPath(resolvedPath, sourcePath string) error {
	cPath := C.CString(resolvedPath)
	defer C.free(unsafe.Pointer(cPath))
	if C.MilvusTablePathRoutesToClusterStorage(cPath) != 0 {
		return merr.WrapErrParameterInvalidMsg(
			"milvus-table source path must be stored in the external source storage: %s", sourcePath)
	}
	return nil
}

// resolveMilvusTableSourcePath resolves a path named by milvus-table snapshot
// metadata against the external source.
func resolveMilvusTableSourcePath(sourcePath string, properties *C.LoonProperties, extfs ExternalSpecContext) (string, error) {
	resolvedPath, err := resolveExternalSourcePath(sourcePath, properties, extfs)
	if err != nil {
		return "", err
	}
	if extfs.Source != "" {
		if err := checkMilvusTableExternalPath(resolvedPath, sourcePath); err != nil {
			return "", err
		}
	}
	return resolvedPath, nil
}

// checkMilvusTableSourceManifestPath rejects a source StorageV3 manifest whose
// base path is not in the external source storage.
func checkMilvusTableSourceManifestPath(manifestPath string, extfs ExternalSpecContext) error {
	if extfs.Source == "" {
		return nil
	}
	basePath, _, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return merr.WrapErrStorage(err, "failed to parse manifest path")
	}
	return checkMilvusTableExternalPath(basePath, basePath)
}

// CheckMilvusTableSourceDeltalogPaths rejects source deltalogs that a reader
// with extfs would read through this cluster's own storage. The paths are
// resolved the same way the deltalog readers resolve them.
func CheckMilvusTableSourceDeltalogPaths(
	deltalogs []*datapb.FieldBinlog,
	storageConfig *indexpb.StorageConfig,
	extfs ExternalSpecContext,
) error {
	if extfs.Source == "" || len(deltalogs) == 0 {
		return nil
	}
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)
	if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
		return merr.Wrap(err, "inject extfs")
	}
	for _, fieldBinlog := range deltalogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if binlog.GetLogPath() == "" {
				continue
			}
			resolvedPath, err := resolveExternalResolvedPath(binlog.GetLogPath(), cProperties, extfs)
			if err != nil {
				return err
			}
			if err := checkMilvusTableExternalPath(resolvedPath, binlog.GetLogPath()); err != nil {
				return err
			}
		}
	}
	return nil
}

// resolveMilvusTableSourceColumnGroups returns a copy of source column groups
// whose files are resolved against the external source, as the C++ import
// records them in target manifests. The caller releases the result with
// loon_column_groups_destroy.
func resolveMilvusTableSourceColumnGroups(
	sourceGroups *C.LoonColumnGroups,
	properties *C.LoonProperties,
	extfs ExternalSpecContext,
) (*C.LoonColumnGroups, error) {
	cSource := C.CString(extfs.Source)
	defer C.free(unsafe.Pointer(cSource))
	var resolved *C.LoonColumnGroups
	if err := HandleLoonFFIResult(C.loon_milvus_table_resolve_source_column_groups(
		sourceGroups, cSource, properties, &resolved)); err != nil {
		return nil, err
	}
	return resolved, nil
}
