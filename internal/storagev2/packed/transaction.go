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
	"path/filepath"
	"strings"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// DeltaLogEntry represents a delta log to be added to the manifest
type DeltaLogEntry struct {
	Path       string // Full path to the deltalog file
	NumEntries int64  // Number of entries in the deltalog
}

// AddDeltaLogsToManifest adds delta logs to an existing manifest and returns the new manifest path.
// This function:
// 1. Parses the existing manifest path to get base path and version
// 2. Starts a loon transaction
// 3. Adds each delta log to the transaction
// 4. Commits the transaction and returns the new manifest path
func AddDeltaLogsToManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	deltaLogs []DeltaLogEntry,
) (string, error) {
	if len(deltaLogs) == 0 {
		return manifestPath, nil
	}

	basePath, version, err := UnmarshalManfestPath(manifestPath)
	if err != nil {
		return "", fmt.Errorf("failed to parse manifest path: %w", err)
	}

	log.Debug("AddDeltaLogsToManifest",
		zap.String("basePath", basePath),
		zap.Int64("version", version),
		zap.Int("numDeltaLogs", len(deltaLogs)))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	// Start transaction
	var transactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), 1, &transactionHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer C.loon_transaction_destroy(transactionHandle)

	// Add each delta log to the transaction
	for _, deltaLog := range deltaLogs {
		// Convert full path to relative path
		relativePath := toRelativePath(deltaLog.Path, basePath, storageConfig.GetRootPath())

		cPath := C.CString(relativePath)
		result = C.loon_transaction_add_delta_log(transactionHandle, cPath, C.int64_t(deltaLog.NumEntries))
		C.free(unsafe.Pointer(cPath))

		if err := HandleLoonFFIResult(result); err != nil {
			return "", fmt.Errorf("failed to add delta log %s: %w", deltaLog.Path, err)
		}

		log.Debug("Added delta log to transaction",
			zap.String("relativePath", relativePath),
			zap.Int64("numEntries", deltaLog.NumEntries))
	}

	// Commit transaction
	var commitVersion C.int64_t
	result = C.loon_transaction_commit(transactionHandle, &commitVersion)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	newManifestPath := MarshalManifestPath(basePath, int64(commitVersion))
	log.Debug("Delta logs committed to manifest", zap.Int64("newVersion", int64(commitVersion)))

	return newManifestPath, nil
}

// toRelativePath converts a full path to a path relative to the base path.
// The deltalog path format is: {rootPath}/delta_log/{collectionID}/{partitionID}/{segmentID}/{logID}
// The basePath format is: {rootPath}/insert_log/{collectionID}/{partitionID}/{segmentID}
// We need to return the relative path from basePath, e.g., "../../../delta_log/..."
func toRelativePath(fullPath, basePath, rootPath string) string {
	// If fullPath starts with rootPath, make it relative to basePath
	if strings.HasPrefix(fullPath, rootPath) {
		// Get the path relative to rootPath for both
		fullRel := strings.TrimPrefix(fullPath, rootPath)
		fullRel = strings.TrimPrefix(fullRel, "/")

		baseRel := strings.TrimPrefix(basePath, rootPath)
		baseRel = strings.TrimPrefix(baseRel, "/")

		// Count the depth of basePath to know how many "../" we need
		baseDepth := len(strings.Split(baseRel, "/"))

		// Build the relative path: go up baseDepth levels, then down to fullRel
		var parts []string
		for i := 0; i < baseDepth; i++ {
			parts = append(parts, "..")
		}
		parts = append(parts, fullRel)

		return filepath.Join(parts...)
	}

	// Fallback: return as-is
	return fullPath
}
