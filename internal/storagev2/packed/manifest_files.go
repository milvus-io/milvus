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
#include "milvus-storage/ffi_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ManifestFiles contains resolved source paths, without selecting indexes or
// deciding destination layout. All Go values outlive the native manifest handle.
type ManifestFiles struct {
	ManifestFile string
	ColumnFiles  []string
	DeltaFiles   []string
	StatsFiles   []string
	LobFiles     []LobFileInfo
	Indexes      []ManifestIndexInfo
}

// ReadManifestFiles opens exactly the recorded revision once. Version zero is
// the empty manifest and has no physical manifest object. Latest-version reads
// are unsuitable for immutable snapshots and are rejected.
func ReadManifestFiles(manifestPath string, config *indexpb.StorageConfig) (*ManifestFiles, error) {
	base, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, merr.Wrap(err, "parse manifest inventory descriptor")
	}
	if base == "" || version < 0 {
		return nil, merr.WrapErrDataIntegrityMsg("manifest inventory requires a base and pinned revision")
	}
	files := &ManifestFiles{}
	if version == ManifestEarliest {
		return files, nil
	}
	files.ManifestFile = manifestObjectPath(base, version)
	manifest, err := GetManifestHandle(manifestPath, config)
	if err != nil {
		return nil, merr.Wrap(err, "read manifest inventory")
	}
	if manifest == nil {
		return nil, merr.WrapErrDataIntegrityMsg("manifest inventory returned nil handle")
	}
	defer C.loon_manifest_destroy(manifest)

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
	if stats.num_stats > 0 && (stats.stat_files == nil || stats.stat_file_counts == nil) {
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
		files.LobFiles = append(files.LobFiles, LobFileInfo{
			Path:    C.GoString(lob.path),
			FieldID: int64(lob.field_id), TotalRows: int64(lob.total_rows),
			ValidRows: int64(lob.valid_rows), FileSizeBytes: int64(lob.file_size_bytes),
		})
	}
	files.Indexes, err = manifestIndexInfos(manifest, manifestPath)
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
