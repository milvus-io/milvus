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
	"context"
	"math"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
		return "", merr.WrapErrStorage(err, "failed to parse manifest path")
	}

	mlog.Debug(context.TODO(), "AddDeltaLogsToManifest",
		mlog.String("basePath", basePath),
		mlog.Int64("version", version),
		mlog.Int("numDeltaLogs", len(deltaLogs)))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)

	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	// Start transaction
	var transactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), resolveID /* resolve_id */, getRetryLimit() /* retry_limit */, &transactionHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", merr.WrapErrStorage(err, "failed to begin transaction")
	}
	defer C.loon_transaction_destroy(transactionHandle)

	// Add each delta log to the transaction.
	// The C++ loon library converts absolute paths to relative at commit time
	for _, deltaLog := range deltaLogs {
		cPath := C.CString(deltaLog.Path)
		result = C.loon_transaction_add_delta_log(transactionHandle, cPath, C.int64_t(deltaLog.NumEntries))
		C.free(unsafe.Pointer(cPath))

		if err := HandleLoonFFIResult(result); err != nil {
			return "", merr.WrapErrStorage(err, "failed to add delta log %s", deltaLog.Path)
		}

		mlog.Debug(context.TODO(), "Added delta log to transaction",
			mlog.String("path", deltaLog.Path),
			mlog.Int64("numEntries", deltaLog.NumEntries))
	}

	// Commit transaction
	var commitVersion C.int64_t
	result = C.loon_transaction_commit(transactionHandle, &commitVersion)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", merr.WrapErrStorage(err, "failed to commit transaction")
	}

	newManifestPath := MarshalManifestPath(basePath, int64(commitVersion))
	mlog.Debug(context.TODO(), "Delta logs committed to manifest", mlog.Int64("newVersion", int64(commitVersion)))

	return newManifestPath, nil
}

// GetDeltaLogPathsFromManifest extracts delta log file paths from a Loon manifest.
// It opens a transaction, reads the manifest's delta_logs section, converts relative
// paths to absolute paths, and returns them.
func GetDeltaLogPathsFromManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
) ([]string, error) {
	deltaLogs, err := GetDeltaLogsFromManifestWithExtfs(manifestPath, storageConfig, ExternalSpecContext{})
	if err != nil {
		return nil, err
	}
	if len(deltaLogs) == 0 {
		return nil, nil
	}
	var paths []string
	for _, deltaLog := range deltaLogs {
		for _, binlog := range deltaLog.GetBinlogs() {
			paths = append(paths, binlog.GetLogPath())
		}
	}
	return paths, nil
}

// GetDeltaLogsFromManifestWithExtfs extracts delta log entries from a StorageV3
// manifest. When extfs is present, returned paths are normalized to object keys
// readable by the local chunk manager.
func GetDeltaLogsFromManifestWithExtfs(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	extfs ExternalSpecContext,
) ([]*datapb.FieldBinlog, error) {
	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, merr.WrapErrStorage(err, "failed to parse manifest path")
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)
	if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
		return nil, fmt.Errorf("inject extfs: %w", err)
	}

	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var cTransactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), C.int32_t(0) /* resolve_id */, C.uint32_t(1) /* retry_limit */, &cTransactionHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, merr.WrapErrStorage(err, "failed to begin transaction")
	}
	defer C.loon_transaction_destroy(cTransactionHandle)

	var cManifest *C.LoonManifest
	result = C.loon_transaction_get_manifest(cTransactionHandle, &cManifest)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, merr.WrapErrStorage(err, "failed to get manifest")
	}
	defer C.loon_manifest_destroy(cManifest)

	numDeltaLogs := int(cManifest.delta_logs.num_delta_logs)
	if numDeltaLogs == 0 {
		return nil, nil
	}

	// The C loon library resolves relative paths to absolute via ToAbsolute
	// (prepending basePath/_delta/ and normalizing). The returned paths are
	// already absolute and can be used directly.
	if cManifest.delta_logs.delta_log_paths == nil || cManifest.delta_logs.delta_log_num_entries == nil {
		return nil, fmt.Errorf("manifest %s has malformed delta log metadata", manifestPath)
	}
	cPaths := unsafe.Slice(cManifest.delta_logs.delta_log_paths, numDeltaLogs)
	cNumEntries := unsafe.Slice(cManifest.delta_logs.delta_log_num_entries, numDeltaLogs)
	binlogs := make([]*datapb.Binlog, 0, numDeltaLogs)
	pathsForLog := make([]string, 0, numDeltaLogs)
	for i, cPath := range cPaths {
		if cPath == nil {
			continue
		}
		path := C.GoString(cPath)
		if extfs.Source != "" {
			var err error
			path, err = externalFilesystemFilePath(path, cProperties, extfs)
			if err != nil {
				return nil, err
			}
		}
		pathsForLog = append(pathsForLog, path)
		binlogs = append(binlogs, &datapb.Binlog{
			LogPath:    path,
			EntriesNum: int64(cNumEntries[i]),
		})
	}

	mlog.Debug(context.TODO(), "GetDeltaLogPathsFromManifest",
		mlog.String("manifestPath", manifestPath),
		mlog.Int("numDeltaLogs", numDeltaLogs),
		mlog.Strings("paths", pathsForLog))

	return []*datapb.FieldBinlog{{Binlogs: binlogs}}, nil
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
		return "", merr.WrapErrStorage(err, "failed to parse manifest path")
	}

	mlog.Debug(context.TODO(), "AddStatsToManifest",
		mlog.String("basePath", basePath),
		mlog.Int64("version", version),
		mlog.Int("numStats", len(stats)))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)

	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var transactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), C.LOON_TRANSACTION_RESOLVE_OVERWRITE /* resolve_id */, getRetryLimit() /* retry_limit */, &transactionHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", merr.WrapErrStorage(err, "failed to begin transaction")
	}
	defer C.loon_transaction_destroy(transactionHandle)

	// The C++ loon library converts absolute paths to relative at commit time
	for _, stat := range stats {
		if err := UpdateTransactionStat(transactionHandle, stat.Key, stat.Files, stat.Metadata); err != nil {
			return "", merr.WrapErrStorage(err, "failed to update stat %s", stat.Key)
		}
		mlog.Debug(context.TODO(), "Added stat to transaction",
			mlog.String("key", stat.Key),
			mlog.Strings("files", stat.Files))
	}

	var commitVersion C.int64_t
	result = C.loon_transaction_commit(transactionHandle, &commitVersion)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", merr.WrapErrStorage(err, "failed to commit transaction")
	}

	newManifestPath := MarshalManifestPath(basePath, int64(commitVersion))
	mlog.Debug(context.TODO(), "Stats committed to manifest", mlog.Int64("newVersion", int64(commitVersion)))

	return newManifestPath, nil
}
