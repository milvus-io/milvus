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
	"math"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// getRetryLimit returns the configured manifest transaction retry limit.
// Multiple stats tasks (text index, JSON key, BM25) can write to the same
// segment's manifest concurrently, causing optimistic transaction conflicts.
// The retry mechanism re-reads the latest manifest version and re-applies
// the changes on each attempt.
func getRetryLimit() C.uint32_t {
	val := paramtable.Get().CommonCfg.ManifestTransactionRetryLimit.GetAsInt64()
	if val <= 0 {
		val = 10
	}
	if val > math.MaxUint32 {
		val = math.MaxUint32
	}
	return C.uint32_t(val)
}

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
	return addDeltaLogsToManifest(manifestPath, storageConfig, deltaLogs, C.int32_t(0))
}

// AddDeltaLogsToManifestOverwrite adds delta logs using overwrite conflict resolution.
func AddDeltaLogsToManifestOverwrite(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	deltaLogs []DeltaLogEntry,
) (string, error) {
	return addDeltaLogsToManifest(manifestPath, storageConfig, deltaLogs, C.LOON_TRANSACTION_RESOLVE_OVERWRITE)
}

func addDeltaLogsToManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	deltaLogs []DeltaLogEntry,
	resolveID C.int32_t,
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
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), resolveID /* resolve_id */, getRetryLimit() /* retry_limit */, &transactionHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer C.loon_transaction_destroy(transactionHandle)

	// Add each delta log to the transaction.
	// The C++ loon library converts absolute paths to relative at commit time
	for _, deltaLog := range deltaLogs {
		cPath := C.CString(deltaLog.Path)
		result = C.loon_transaction_add_delta_log(transactionHandle, cPath, C.int64_t(deltaLog.NumEntries))
		C.free(unsafe.Pointer(cPath))

		if err := HandleLoonFFIResult(result); err != nil {
			return "", fmt.Errorf("failed to add delta log %s: %w", deltaLog.Path, err)
		}

		log.Debug("Added delta log to transaction",
			zap.String("path", deltaLog.Path),
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
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), C.int32_t(0) /* resolve_id */, C.uint32_t(1) /* retry_limit */, &cTransactionHandle)
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

// StatEntry represents a stat entry to be added to the manifest.
type StatEntry struct {
	Key      string            // Manifest stat key, e.g. "bloom_filter.100"
	Files    []string          // Relative file paths under manifest base path
	Metadata map[string]string // Optional key-value metadata
}

// AddStatsToManifest adds stats to an existing manifest and returns the new manifest path.
func AddStatsToManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	stats []StatEntry,
) (string, error) {
	if len(stats) == 0 {
		return manifestPath, nil
	}

	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", fmt.Errorf("failed to parse manifest path: %w", err)
	}

	log.Debug("AddStatsToManifest",
		zap.String("basePath", basePath),
		zap.Int64("version", version),
		zap.Int("numStats", len(stats)))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var transactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), C.LOON_TRANSACTION_RESOLVE_OVERWRITE /* resolve_id */, getRetryLimit() /* retry_limit */, &transactionHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer C.loon_transaction_destroy(transactionHandle)

	// The C++ loon library converts absolute paths to relative at commit time
	for _, stat := range stats {
		if err := UpdateTransactionStat(transactionHandle, stat.Key, stat.Files, stat.Metadata); err != nil {
			return "", fmt.Errorf("failed to update stat %s: %w", stat.Key, err)
		}
		log.Debug("Added stat to transaction",
			zap.String("key", stat.Key),
			zap.Strings("files", stat.Files))
	}

	var commitVersion C.int64_t
	result = C.loon_transaction_commit(transactionHandle, &commitVersion)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	newManifestPath := MarshalManifestPath(basePath, int64(commitVersion))
	log.Debug("Stats committed to manifest", zap.Int64("newVersion", int64(commitVersion)))

	return newManifestPath, nil
}
