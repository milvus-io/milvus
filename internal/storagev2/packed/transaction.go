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

	basePath, version, err := UnmarshalManifestPath(manifestPath)
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

// GetDeltaLogPathsFromManifest extracts delta log file paths from a Loon manifest.
// It opens a transaction, reads the manifest's delta_logs section, converts relative
// paths to absolute paths, and returns them.
func GetDeltaLogPathsFromManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
) ([]string, error) {
	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse manifest path: %w", err)
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var cTransactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), 1, &cTransactionHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer C.loon_transaction_destroy(cTransactionHandle)

	var cManifest *C.LoonManifest
	result = C.loon_transaction_get_manifest(cTransactionHandle, &cManifest)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}
	defer C.loon_manifest_destroy(cManifest)

	numDeltaLogs := int(cManifest.delta_logs.num_delta_logs)
	if numDeltaLogs == 0 {
		return nil, nil
	}

	// The C loon library resolves relative paths to absolute via ToAbsolute
	// (prepending basePath/_delta/ and normalizing). The returned paths are
	// already absolute and can be used directly.
	cPaths := unsafe.Slice(cManifest.delta_logs.delta_log_paths, numDeltaLogs)
	paths := make([]string, 0, numDeltaLogs)
	for _, cPath := range cPaths {
		paths = append(paths, C.GoString(cPath))
	}

	log.Debug("GetDeltaLogPathsFromManifest",
		zap.String("manifestPath", manifestPath),
		zap.Int("numDeltaLogs", numDeltaLogs),
		zap.Strings("paths", paths))

	return paths, nil
}

// toRelativePath converts a full path to a path relative to basePath/_delta/.
// The C loon library stores delta log paths relative to basePath/_delta/ (the kDeltaPath convention).
// When deserializing, it prepends basePath/_delta/ to the relative path and normalizes.
//
// For V3 delta paths ({basePath}/_delta/{logID}), the file is already under basePath/_delta/,
// so the relative path is simply the filename ({logID}).
//
// For legacy delta paths ({rootPath}/delta_log/{collID}/{partID}/{segID}/{logID}),
// we need baseDepth + 1 levels of "../" to escape basePath/_delta/ back to rootPath.
func toRelativePath(fullPath, basePath, rootPath string) string {
	// V3 path: if fullPath is under basePath/_delta/, just return the filename
	deltaDir := filepath.Join(basePath, "_delta") + "/"
	if strings.HasPrefix(fullPath, deltaDir) {
		return strings.TrimPrefix(fullPath, deltaDir)
	}

	// Legacy path: compute relative path from basePath/_delta/ via "../" traversal
	if strings.HasPrefix(fullPath, rootPath) {
		// Get the path relative to rootPath for both
		fullRel := strings.TrimPrefix(fullPath, rootPath)
		fullRel = strings.TrimPrefix(fullRel, "/")

		baseRel := strings.TrimPrefix(basePath, rootPath)
		baseRel = strings.TrimPrefix(baseRel, "/")

		// Count the depth of basePath to know how many "../" we need.
		// Add 1 for the _delta/ subdirectory that C prepends during deserialization.
		baseDepth := len(strings.Split(baseRel, "/")) + 1

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
