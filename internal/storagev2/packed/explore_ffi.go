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
#include "milvus-storage/ffi_exttable_c.h"
#include "milvus-storage/ffi_filesystem_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"context"
	"net/url"
	"path"
	"sort"
	"strings"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// formatExtensions maps format names to their expected file extensions.
// lance-table is directory-based and not included (no extension filtering needed).
var formatExtensions = map[string]string{
	"parquet": ".parquet",
	"vortex":  ".vortex",
}

// filterFileInfosByFormat filters out files that don't match the expected format extension.
// Returns the filtered list and the count of skipped files.
func filterFileInfosByFormat(fileInfos []FileInfo, format string) ([]FileInfo, int) {
	ext, ok := formatExtensions[format]
	if !ok {
		return fileInfos, 0
	}
	filtered := make([]FileInfo, 0, len(fileInfos))
	for _, fi := range fileInfos {
		if strings.HasSuffix(strings.ToLower(fi.FilePath), ext) {
			filtered = append(filtered, fi)
		}
	}
	return filtered, len(fileInfos) - len(filtered)
}

// NormalizeFileInfos returns the manifest file list as it must be seen by
// every consumer of the explore manifest: sorted lexicographically by
// FilePath, then filtered to the requested format. Sorting is mandatory
// because the underlying arrow filesystem GetFileInfo gives no ordering
// guarantee — without it, DataCoord and DataNode would slice the same
// fileIndex range against different orderings and pick different files
// (leading to silent data loss or "Invalid parquet magic" task failures
// when stray Spark `_SUCCESS`/`.crc`/README files land in the picked
// window). Both DataCoord (when splitting tasks) and DataNode (when
// resolving fileIndexBegin/End) MUST apply this transform on top of
// ReadFileInfosFromManifestPath so they observe the same indexed view.
func NormalizeFileInfos(fileInfos []FileInfo, format string) ([]FileInfo, int) {
	// Sort by path first so lex order is stable across processes.
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].FilePath < fileInfos[j].FilePath
	})
	return filterFileInfosByFormat(fileInfos, format)
}

// FileInfo represents information about an external file.
//
// WARNING: When produced by ExploreFiles (which calls loon_exttable_explore),
// NumRows is the Loon end_index sentinel (-1 for parquet via PlainFormat::explore)
// rather than a real row count. Do NOT compare NumRows against 0 or treat it as
// a row total at this layer. Real row counts are only available after manifest
// construction where Fragment.RowCount = endRow - startRow.
type FileInfo struct {
	FilePath        string
	NumRows         int64
	SourceSegmentID int64
	Deltalogs       []*datapb.FieldBinlog
}

// ExploreFiles scans an external directory and returns file information.
// It internally calls exttable_explore to find files, then reads the manifest
// GetFileInfo retrieves row count information for a single external file.
// This is used to determine how to split large files into multiple fragments.
func GetFileInfo(
	format string,
	filePath string,
	storageConfig *indexpb.StorageConfig,
	extfs ExternalSpecContext,
) (*FileInfo, error) {
	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)
	if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
		return nil, merr.Wrap(err, "inject extfs")
	}

	normalizedFilePath, err := normalizeExternalPathForStorage(filePath, cProperties, extfs)
	if err != nil {
		return nil, merr.WrapErrStorage(err, "normalize external file path")
	}
	cFilePath := C.CString(normalizedFilePath)
	defer C.free(unsafe.Pointer(cFilePath))

	var numRows C.uint64_t

	result := C.loon_exttable_get_file_info(cFormat, cFilePath, cProperties, &numRows)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, merr.WrapErrStorage(err, "loon_exttable_get_file_info failed")
	}

	return &FileInfo{
		FilePath: filePath,
		NumRows:  int64(numRows),
	}, nil
}

func normalizeExternalPathForStorage(path string, properties *C.LoonProperties, extfs ExternalSpecContext) (string, error) {
	if extfs.Source == "" || path == "" || properties == nil {
		return path, nil
	}

	u, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return path, nil
	}

	prefix := ExtfsPrefixForCollection(extfs.CollectionID)
	address := loonPropertyString(properties, prefix+"address")
	bucketName := loonPropertyString(properties, prefix+"bucket_name")
	if address == "" || bucketName == "" || bucketName != u.Host {
		return path, nil
	}

	addressHost, err := propertyAddressHost(address)
	if err != nil {
		return "", err
	}
	if addressHost == "" || addressHost == u.Host {
		return path, nil
	}

	oldPath := strings.TrimPrefix(u.Path, "/")
	if oldPath == "" {
		u.Path = "/" + bucketName
	} else {
		u.Path = "/" + bucketName + "/" + oldPath
	}
	u.RawPath = ""
	u.Host = addressHost
	return u.String(), nil
}

func resolveExternalSourceRelativePath(sourcePath string, properties *C.LoonProperties, extfs ExternalSpecContext) (string, error) {
	if sourcePath == "" || extfs.Source == "" || properties == nil {
		return sourcePath, nil
	}
	if isAbsoluteExternalPath(sourcePath) {
		return normalizeExternalPathForStorage(sourcePath, properties, extfs)
	}

	sourceURI, err := url.Parse(extfs.Source)
	if err != nil {
		return "", err
	}
	if sourceURI.Scheme == "" || sourceURI.Host == "" {
		return sourcePath, nil
	}

	prefix := ExtfsPrefixForCollection(extfs.CollectionID)
	bucketName := loonPropertyString(properties, prefix+"bucket_name")
	if bucketName == "" {
		return "", fmt.Errorf("resolve external source relative path: missing bucket_name for %s", extfs.Source)
	}
	address := loonPropertyString(properties, prefix+"address")
	addressHost, err := propertyAddressHost(address)
	if err != nil {
		return "", err
	}

	resolved := &url.URL{
		Scheme: sourceURI.Scheme,
		Host:   sourceURI.Host,
	}
	relativePath := strings.TrimPrefix(sourcePath, "/")
	if addressHost != "" {
		resolved.Host = addressHost
		resolved.Path = "/" + path.Join(bucketName, relativePath)
	} else if sourceURI.Host == bucketName {
		resolved.Path = "/" + relativePath
	} else if firstPathSegment(sourceURI.Path) == bucketName {
		resolved.Path = "/" + path.Join(bucketName, relativePath)
	} else {
		resolved.Path = "/" + relativePath
	}
	return normalizeExternalPathForStorage(resolved.String(), properties, extfs)
}

func isAbsoluteExternalPath(filePath string) bool {
	u, err := url.Parse(filePath)
	if err != nil {
		return false
	}
	return u.Scheme != "" || path.IsAbs(filePath)
}

func firstPathSegment(filePath string) string {
	trimmed := strings.Trim(filePath, "/")
	if trimmed == "" {
		return ""
	}
	if idx := strings.Index(trimmed, "/"); idx >= 0 {
		return trimmed[:idx]
	}
	return trimmed
}

func loonPropertyString(properties *C.LoonProperties, key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.loon_properties_get(properties, cKey)
	if cValue == nil {
		return ""
	}
	return C.GoString(cValue)
}

func propertyAddressHost(address string) (string, error) {
	if !strings.Contains(address, "://") {
		return address, nil
	}
	u, err := url.Parse(address)
	if err != nil {
		return "", err
	}
	return u.Host, nil
}

// ExploreFilesReturnManifestPath is like ExploreFiles but also returns the manifest path
// written by loon_exttable_explore. The caller can pass this path to other nodes so they
// can read the file list via ReadFileInfosFromManifestPath without re-exploring.
// NOTE: The temp dir created here is reclaimed by the datacoord refresh manager via
// ChunkManager once the refresh job reaches a terminal state — see
// externalCollectionRefreshManager.cleanupExploreTempForJob.
func ExploreFilesReturnManifestPath(
	columns []string,
	format string,
	baseDir string,
	exploreDir string,
	storageConfig *indexpb.StorageConfig,
	extfs ExternalSpecContext,
) ([]FileInfo, string, error) {
	if isMilvusTableFormat(format) {
		metadataPath, err := resolveMilvusTableSnapshotMetadataPath(exploreDir, extfs.Spec)
		if err != nil {
			return nil, "", err
		}
		metadataBytes, err := ReadFileWithExternalSpec(storageConfig, metadataPath, extfs)
		if err != nil {
			return nil, "", fmt.Errorf("read milvus snapshot metadata: %w", err)
		}
		cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
		if err != nil {
			return nil, "", fmt.Errorf("failed to create properties: %w", err)
		}
		defer C.loon_properties_free(cProperties)
		if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
			return nil, "", fmt.Errorf("inject extfs: %w", err)
		}
		resolveSourcePath := func(sourcePath string) (string, error) {
			return resolveExternalSourceRelativePath(sourcePath, cProperties, extfs)
		}
		fileInfos, err := buildMilvusTableFileInfosFromSnapshotMetadata(
			metadataBytes,
			func(manifestPath string, formatVersion int32) (*datapb.SegmentDescription, error) {
				resolvedManifestPath, err := resolveSourcePath(manifestPath)
				if err != nil {
					return nil, err
				}
				segment, err := readMilvusSnapshotSegmentManifest(resolvedManifestPath, formatVersion, func(path string) ([]byte, error) {
					return ReadFileWithExternalSpec(storageConfig, path, extfs)
				})
				if err != nil {
					return nil, err
				}
				if err := resolveMilvusTableSegmentDeltalogPaths(segment, resolveSourcePath); err != nil {
					return nil, err
				}
				return segment, nil
			},
			func(manifestPath string) (string, error) {
				return resolveMilvusTableSourceManifestPath(manifestPath, resolveSourcePath)
			},
		)
		if err != nil {
			return nil, "", err
		}
		manifestPath, err := writeMilvusTableExploreManifest(baseDir, fileInfos, storageConfig)
		if err != nil {
			return nil, "", err
		}
		return fileInfos, manifestPath, nil
	}

	cColumns := make([]*C.char, len(columns))
	for i, col := range columns {
		cColumns[i] = C.CString(col)
	}
	defer func() {
		for _, c := range cColumns {
			C.free(unsafe.Pointer(c))
		}
	}()

	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))
	cBaseDir := C.CString(baseDir)
	defer C.free(unsafe.Pointer(cBaseDir))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, "", merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)
	if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
		return nil, "", merr.Wrap(err, "inject extfs")
	}

	normalizedExploreDir, err := normalizeExternalPathForStorage(exploreDir, cProperties, extfs)
	if err != nil {
		return nil, "", merr.WrapErrStorage(err, "normalize external explore path")
	}
	cExploreDir := C.CString(normalizedExploreDir)
	defer C.free(unsafe.Pointer(cExploreDir))

	var numFiles C.uint64_t
	var outColumnGroupsPath *C.char

	var cColumnsPtr **C.char
	if len(cColumns) > 0 {
		cColumnsPtr = &cColumns[0]
	}

	result := C.loon_exttable_explore(
		cColumnsPtr, C.size_t(len(columns)),
		cFormat, cBaseDir, cExploreDir, cProperties,
		&numFiles, &outColumnGroupsPath,
	)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, "", merr.WrapErrStorage(err, "loon_exttable_explore failed")
	}
	if outColumnGroupsPath == nil {
		return nil, "", merr.WrapErrServiceInternalMsg("loon_exttable_explore returned nil column groups path")
	}
	manifestPath := C.GoString(outColumnGroupsPath)
	C.loon_free_cstr(outColumnGroupsPath)

	// Read manifest to get file infos
	fileInfos, err := ReadFileInfosFromManifestPath(manifestPath, storageConfig)
	if err != nil {
		return nil, "", err
	}

	// Sort + format-filter: produces a deterministic indexed view that
	// DataNode will reproduce against the same manifest. See
	// NormalizeFileInfos doc for the index-drift bug this prevents.
	fileInfos, skipped := NormalizeFileInfos(fileInfos, format)
	if skipped > 0 {
		mlog.Info(context.TODO(), "Skipped files with non-matching format during explore",
			mlog.Int("skippedCount", skipped),
			mlog.String("format", format))
	}

	return fileInfos, manifestPath, nil
}

// ReadFileInfosFromManifestPath reads the explore manifest and returns file infos.
// This allows DataNode to skip ExploreFiles and directly read the file list.
func ReadFileInfosFromManifestPath(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
) ([]FileInfo, error) {
	cManifestPath := C.CString(manifestPath)
	defer C.free(unsafe.Pointer(cManifestPath))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)

	var manifest *C.LoonManifest
	result := C.loon_exttable_read_manifest(cManifestPath, cProperties, &manifest)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, merr.WrapErrStorage(err, "loon_exttable_read_manifest failed")
	}
	defer C.loon_manifest_destroy(manifest)

	var fileInfos []FileInfo
	cgroups := &manifest.column_groups
	if cgroups.column_group_array == nil && cgroups.num_of_column_groups > 0 {
		return nil, merr.WrapErrServiceInternalMsg("column_group_array is nil but num_of_column_groups is %d", cgroups.num_of_column_groups)
	}

	cgArray := unsafe.Slice(cgroups.column_group_array, int(cgroups.num_of_column_groups))
	for i := range cgArray {
		cg := &cgArray[i]
		if cg.files == nil {
			continue
		}
		fileArray := unsafe.Slice(cg.files, int(cg.num_of_files))
		for j := range fileArray {
			if fileArray[j].path == nil {
				return nil, merr.WrapErrServiceInternalMsg("file path is nil in column group %d, file %d", i, j)
			}
			fileInfos = append(fileInfos, FileInfo{
				FilePath: C.GoString(fileArray[j].path),
				NumRows:  int64(fileArray[j].end_index),
			})
		}
	}

	return fileInfos, nil
}
