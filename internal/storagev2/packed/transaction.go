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
	"sort"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var indexManifestLocks = lock.NewKeyLock[string]()

// ManifestIndexInfo identifies a completed index artifact registered in a
// StorageV3 manifest. Path is the artifact root, IndexFileKeys are relative to
// it, and Properties holds only index-specific parameters.
//
// The index bytes must already exist before this metadata is published. The
// transaction only makes the metadata visible atomically with a new manifest
// revision.
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

// AddIndexInfoToManifest records a completed index artifact in a segment
// manifest. The supplied manifest path identifies the source revision; this
// operation never implicitly rebases onto a newer revision.
func AddIndexInfoToManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	index ManifestIndexInfo,
) (string, error) {
	return AddIndexInfosToManifest(manifestPath, storageConfig, []ManifestIndexInfo{index})
}

// AddIndexInfosToManifest records multiple completed index artifacts in one
// manifest transaction and returns a single new manifest revision.
func AddIndexInfosToManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	indexes []ManifestIndexInfo,
) (string, error) {
	if len(indexes) == 0 {
		return manifestPath, nil
	}
	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", merr.WrapErrStorage(err, "failed to parse manifest path")
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)

	indexManifestLocks.Lock(basePath)
	defer indexManifestLocks.Unlock(basePath)

	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var transactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(
		cBasePath,
		cProperties,
		C.int64_t(version),
		C.int32_t(C.LOON_TRANSACTION_RESOLVE_OVERWRITE),
		getRetryLimit(),
		&transactionHandle,
	)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", merr.WrapErrStorage(err, "failed to begin index manifest transaction")
	}
	defer C.loon_transaction_destroy(transactionHandle)

	for _, index := range indexes {
		cColumnName := C.CString(index.ColumnName)
		defer C.free(unsafe.Pointer(cColumnName))
		cIndexName := C.CString(index.IndexName)
		defer C.free(unsafe.Pointer(cIndexName))
		cIndexType := C.CString(index.IndexType)
		defer C.free(unsafe.Pointer(cIndexType))
		cPath := C.CString(index.Path)
		defer C.free(unsafe.Pointer(cPath))
		cIndexFileKeys := make([]*C.char, 0, len(index.IndexFileKeys))
		for _, key := range index.IndexFileKeys {
			cIndexFileKeys = append(cIndexFileKeys, C.CString(key))
		}
		defer func() {
			for _, key := range cIndexFileKeys {
				C.free(unsafe.Pointer(key))
			}
		}()

		var cIndexFileKeysPtr **C.char
		if len(cIndexFileKeys) > 0 {
			arraySize := C.size_t(len(cIndexFileKeys)) * C.size_t(unsafe.Sizeof(uintptr(0)))
			cIndexFileKeysPtr = (**C.char)(C.malloc(arraySize))
			defer C.free(unsafe.Pointer(cIndexFileKeysPtr))
			copy(unsafe.Slice(cIndexFileKeysPtr, len(cIndexFileKeys)), cIndexFileKeys)
		}

		propertyNames := make([]string, 0, len(index.Properties))
		for key := range index.Properties {
			propertyNames = append(propertyNames, key)
		}
		sort.Strings(propertyNames)

		cPropertyKeys := make([]*C.char, 0, len(propertyNames))
		cPropertyValues := make([]*C.char, 0, len(propertyNames))
		for _, key := range propertyNames {
			cPropertyKeys = append(cPropertyKeys, C.CString(key))
			cPropertyValues = append(cPropertyValues, C.CString(index.Properties[key]))
		}
		defer func() {
			for _, key := range cPropertyKeys {
				C.free(unsafe.Pointer(key))
			}
			for _, value := range cPropertyValues {
				C.free(unsafe.Pointer(value))
			}
		}()

		var cPropertyKeysPtr, cPropertyValuesPtr **C.char
		if len(cPropertyKeys) > 0 {
			arraySize := C.size_t(len(cPropertyKeys)) * C.size_t(unsafe.Sizeof(uintptr(0)))
			cPropertyKeysPtr = (**C.char)(C.malloc(arraySize))
			cPropertyValuesPtr = (**C.char)(C.malloc(arraySize))
			defer C.free(unsafe.Pointer(cPropertyKeysPtr))
			defer C.free(unsafe.Pointer(cPropertyValuesPtr))
			copy(unsafe.Slice(cPropertyKeysPtr, len(cPropertyKeys)), cPropertyKeys)
			copy(unsafe.Slice(cPropertyValuesPtr, len(cPropertyValues)), cPropertyValues)
		}
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
			index_file_keys:              cIndexFileKeysPtr,
			num_index_file_keys:          C.uint32_t(len(cIndexFileKeys)),
			property_keys:                cPropertyKeysPtr,
			property_values:              cPropertyValuesPtr,
			num_properties:               C.uint32_t(len(cPropertyKeys)),
		}
		if err := HandleLoonFFIResult(C.loon_transaction_add_index_info(transactionHandle, &cIndex)); err != nil {
			return "", merr.WrapErrStorage(err, "failed to add index info to manifest")
		}
	}

	var commitVersion C.int64_t
	if err := HandleLoonFFIResult(C.loon_transaction_commit(transactionHandle, &commitVersion)); err != nil {
		return "", merr.WrapErrStorage(err, "failed to commit index manifest transaction")
	}
	return MarshalManifestPath(basePath, int64(commitVersion)), nil
}

// RemoveIndexInfoFromManifest removes one completed index's metadata from a
// StorageV3 segment manifest in one transaction. The index artifacts are not
// touched; callers must delete them only after publishing the returned
// manifest path to metadata.
func RemoveIndexInfoFromManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	index ManifestIndexInfo,
) (string, error) {
	return RemoveIndexInfosFromManifest(manifestPath, storageConfig, []ManifestIndexInfo{index})
}

// RemoveIndexInfosFromManifest removes multiple completed indexes' metadata
// from a StorageV3 segment manifest in one transaction.
func RemoveIndexInfosFromManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	indexes []ManifestIndexInfo,
) (string, error) {
	if len(indexes) == 0 {
		return manifestPath, nil
	}
	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", merr.WrapErrStorage(err, "failed to parse manifest path")
	}
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)

	indexManifestLocks.Lock(basePath)
	defer indexManifestLocks.Unlock(basePath)
	sourceIndexes, err := GetManifestIndexInfos(manifestPath, storageConfig)
	if err != nil {
		return "", merr.Wrap(err, "failed to validate source manifest index metadata")
	}
	matchedIndexes := make([]ManifestIndexInfo, 0, len(indexes))
	for _, requested := range indexes {
		matchingKeys, matchingIdentity := 0, false
		for _, existing := range sourceIndexes {
			if existing.ColumnName != requested.ColumnName || existing.IndexType != requested.IndexType {
				continue
			}
			matchingKeys++
			if existing.IndexID == requested.IndexID && existing.BuildID == requested.BuildID {
				matchingIdentity = true
			}
		}
		if matchingKeys == 0 {
			continue
		}
		if matchingKeys != 1 || !matchingIdentity {
			return "", merr.WrapErrServiceInternalMsg(
				"manifest index changed while removing %s/%s", requested.ColumnName, requested.IndexType)
		}
		matchedIndexes = append(matchedIndexes, requested)
	}
	if len(matchedIndexes) == 0 {
		return manifestPath, nil
	}
	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))
	var transactionHandle C.LoonTransactionHandle
	if err := HandleLoonFFIResult(C.loon_transaction_begin(
		cBasePath, cProperties, C.int64_t(version), C.LOON_TRANSACTION_RESOLVE_OVERWRITE, getRetryLimit(), &transactionHandle,
	)); err != nil {
		return "", merr.WrapErrStorage(err, "failed to begin index manifest transaction")
	}
	defer C.loon_transaction_destroy(transactionHandle)

	for _, index := range matchedIndexes {
		cColumnName := C.CString(index.ColumnName)
		cIndexType := C.CString(index.IndexType)
		if err := HandleLoonFFIResult(C.loon_transaction_drop_index(transactionHandle, cColumnName, cIndexType)); err != nil {
			C.free(unsafe.Pointer(cColumnName))
			C.free(unsafe.Pointer(cIndexType))
			return "", merr.WrapErrStorage(err, "failed to remove index info from manifest")
		}
		C.free(unsafe.Pointer(cColumnName))
		C.free(unsafe.Pointer(cIndexType))
	}
	var commitVersion C.int64_t
	if err := HandleLoonFFIResult(C.loon_transaction_commit(transactionHandle, &commitVersion)); err != nil {
		return "", merr.WrapErrStorage(err, "failed to commit index manifest transaction")
	}
	return MarshalManifestPath(basePath, int64(commitVersion)), nil
}

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

// GetDeltaLogPathsFromManifest extracts readable delta log file paths from a
// Loon manifest. It opens a transaction, reads the manifest's delta_logs
// section, converts relative paths to absolute paths, and skips zero-entry
// manifest-only markers because they do not have a file to open. Callers that
// need marker identity should read the full delta metadata instead.
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
			if binlog.GetEntriesNum() <= 0 {
				continue
			}
			paths = append(paths, binlog.GetLogPath())
		}
	}
	if len(paths) == 0 {
		return nil, nil
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
		return nil, merr.Wrap(err, "inject extfs")
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
		return nil, merr.WrapErrServiceInternalMsg("manifest %s has malformed delta log metadata", manifestPath)
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
