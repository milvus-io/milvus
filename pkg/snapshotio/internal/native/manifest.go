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

package native

/*
#cgo pkg-config: milvus-storage
#include "milvus-storage/ffi_c.h"
*/
import "C"

import (
	"net/url"
	"path"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	"github.com/milvus-io/milvus/pkg/v3/storagev2"
	storageffi "github.com/milvus-io/milvus/pkg/v3/storagev2/ffi"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ManifestFiles contains resolved source paths, without selecting indexes or
// deciding destination layout. All Go values outlive the native manifest handle.
type ManifestFiles = format.NativeManifestFiles

// ReadManifestFiles opens exactly the recorded revision once and releases its
// native handle before returning. Version zero has no physical manifest object.
func ReadManifestFiles(manifestPath string, config *indexpb.StorageConfig) (*ManifestFiles, error) {
	base, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, err
	}
	if base == "" || version < 0 {
		return nil, merr.WrapErrDataIntegrityMsg("manifest inventory requires a base and pinned revision")
	}
	if version == 0 {
		return &ManifestFiles{}, nil
	}
	manifest, err := GetManifestHandle(manifestPath, config)
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, merr.WrapErrDataIntegrityMsg("manifest inventory returned nil handle")
	}
	defer C.loon_manifest_destroy(manifest)
	files, err := inventory(manifestPath, manifest)
	if err != nil {
		return nil, err
	}
	files.ManifestFile = storagev2.ManifestObjectPath(base, version)
	return files, nil
}

// DecodeManifest uses the backup/restore-only codec. It borrows data for this
// synchronous call; the returned inventory owns its strings after native cleanup.
func DecodeManifest(data []byte, basePath string) (*ManifestFiles, error) {
	if len(data) == 0 || basePath == "" {
		return nil, merr.WrapErrDataIntegrityMsg("manifest bytes and base path are required")
	}
	var manifest *C.LoonManifest
	result := C.loon_manifest_deserialize((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), &manifest)
	if err := handleResult(result); err != nil {
		return nil, merr.Wrap(err, "deserialize native manifest")
	}
	// A successful deserialize owns a non-nil handle; failures return above.
	defer C.loon_manifest_destroy(manifest)
	files, err := inventory(basePath, manifest)
	if err != nil {
		return nil, err
	}
	// Serde returns paths exactly as stored. Match Manifest::ToAbsolutePaths:
	// preserve URIs and absolute local paths, expand relative references using
	// each native section's directory (LOBs live at partition level).
	resolve := func(value, directory string) string {
		if uri, err := url.Parse(value); err == nil && uri.Scheme != "" && uri.Host != "" && len(uri.Path) > 1 {
			return value
		}
		if path.IsAbs(value) {
			return value
		}
		resolved := path.Join(basePath, directory, value)
		// std::filesystem::lexically_normal keeps a trailing directory separator.
		if resolved != "/" && resolved != "." && (value == "" || value[len(value)-1] == '/' || path.Base(value) == "." || path.Base(value) == "..") {
			resolved += "/"
		}
		return resolved
	}
	for _, group := range []struct {
		paths     []string
		directory string
	}{
		{files.ColumnFiles, "_data"}, {files.DeltaFiles, "_delta"}, {files.StatsFiles, "_stats"},
	} {
		for i, value := range group.paths {
			group.paths[i] = resolve(value, group.directory)
		}
	}
	for i := range files.LobFiles {
		files.LobFiles[i].Path = resolve(files.LobFiles[i].Path, "../lobs")
	}
	for i := range files.Indexes {
		files.Indexes[i].Path = resolve(files.Indexes[i].Path, "_index")
	}
	return files, nil
}

func inventory(manifestPath string, manifest *C.LoonManifest) (*ManifestFiles, error) {
	files := &ManifestFiles{}
	var err error
	groups := manifest.column_groups
	if groups.num_of_column_groups > 0 && groups.column_group_array == nil {
		return nil, merr.WrapErrDataIntegrityMsg("manifest has missing column groups")
	}
	for _, group := range unsafe.Slice(groups.column_group_array, int(groups.num_of_column_groups)) {
		if group.num_of_files > 0 && group.files == nil {
			return nil, merr.WrapErrDataIntegrityMsg("manifest has missing column files")
		}
		for _, file := range unsafe.Slice(group.files, int(group.num_of_files)) {
			if file.path == nil || C.GoString(file.path) == "" {
				return nil, merr.WrapErrDataIntegrityMsg("manifest has empty column file path")
			}
			files.ColumnFiles = append(files.ColumnFiles, C.GoString(file.path))
		}
	}
	files.DeltaFiles, err = manifestFilePaths(manifest.delta_logs.delta_log_paths, int(manifest.delta_logs.num_delta_logs))
	if err != nil {
		return nil, err
	}
	stats := manifest.stats
	if stats.num_stats > 0 && (stats.stat_files == nil || stats.stat_file_counts == nil || stats.stat_keys == nil) {
		return nil, merr.WrapErrDataIntegrityMsg("manifest has missing stats files")
	}
	counts := unsafe.Slice(stats.stat_file_counts, int(stats.num_stats))
	for i, paths := range unsafe.Slice(stats.stat_files, int(stats.num_stats)) {
		values, err := manifestFilePaths(paths, int(counts[i]))
		if err != nil {
			return nil, err
		}
		files.StatsFiles = append(files.StatsFiles, values...)
	}
	lobs := manifest.lob_files
	if lobs.num_files > 0 && lobs.files == nil {
		return nil, merr.WrapErrDataIntegrityMsg("manifest has missing LOB files")
	}
	for _, lob := range unsafe.Slice(lobs.files, int(lobs.num_files)) {
		if lob.path == nil || C.GoString(lob.path) == "" {
			return nil, merr.WrapErrDataIntegrityMsg("manifest has empty LOB path")
		}
		files.LobFiles = append(files.LobFiles, LobFileInfo{Path: C.GoString(lob.path),
			FieldID: int64(lob.field_id), TotalRows: int64(lob.total_rows),
			ValidRows: int64(lob.valid_rows), FileSizeBytes: int64(lob.file_size_bytes)})
	}
	files.Indexes, err = storageffi.ManifestIndexInfos(unsafe.Pointer(manifest), manifestPath)
	if err != nil {
		return nil, err
	}
	return files, nil
}

func manifestFilePaths(paths **C.char, count int) ([]string, error) {
	if count > 0 && paths == nil {
		return nil, merr.WrapErrDataIntegrityMsg("manifest has missing file paths")
	}
	result := make([]string, 0, count)
	for _, value := range unsafe.Slice(paths, count) {
		if value == nil || C.GoString(value) == "" {
			return nil, merr.WrapErrDataIntegrityMsg("manifest has empty file path")
		}
		result = append(result, C.GoString(value))
	}
	return result, nil
}

type LobFileInfo = format.LobFileInfo
