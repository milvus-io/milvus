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
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	log "github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Fragment represents a data fragment from an external data source.
// A large file (e.g., 10M rows) can be split into multiple fragments.
type Fragment struct {
	FragmentID int64  // Unique fragment identifier
	FilePath   string // File path
	StartRow   int64  // Start row index within the file (inclusive)
	EndRow     int64  // End row index within the file (exclusive)
	RowCount   int64  // Number of rows (EndRow - StartRow)
}

type manifestColumnGroup struct {
	Columns   []string
	Fragments []Fragment
	Format    string
}

func fragmentIdentity(f Fragment) string {
	return fmt.Sprintf("%s:%d:%d", f.FilePath, f.StartRow, f.EndRow)
}

func sameFragmentSet(a, b []Fragment) bool {
	if len(a) != len(b) {
		return false
	}

	seen := make(map[string]int, len(a))
	for _, fragment := range a {
		seen[fragmentIdentity(fragment)]++
	}
	for _, fragment := range b {
		identity := fragmentIdentity(fragment)
		if seen[identity] == 0 {
			return false
		}
		seen[identity]--
	}
	return true
}

func manifestColumnGroupsToFragments(groups []manifestColumnGroup) []Fragment {
	fragments := make([]Fragment, 0)
	seen := make(map[string]struct{})

	for _, group := range groups {
		for _, fragment := range group.Fragments {
			identity := fragmentIdentity(fragment)
			if _, ok := seen[identity]; ok {
				continue
			}
			fragment.FragmentID = int64(len(fragments))
			fragments = append(fragments, fragment)
			seen[identity] = struct{}{}
		}
	}

	return fragments
}

func columnsToAppend(existing []manifestColumnGroup, requested []string, fragments []Fragment) ([]string, error) {
	existingFragments := make(map[string][][]Fragment)
	for _, group := range existing {
		for _, column := range group.Columns {
			existingFragments[column] = append(existingFragments[column], group.Fragments)
		}
	}

	columns := make([]string, 0, len(requested))
	seenAppend := make(map[string]struct{}, len(requested))
	for _, column := range requested {
		if existingSets, ok := existingFragments[column]; ok {
			for _, existingSet := range existingSets {
				if !sameFragmentSet(existingSet, fragments) {
					return nil, merr.WrapErrServiceInternalMsg("column %s already exists with different fragments", column)
				}
			}
			continue
		}
		if _, ok := seenAppend[column]; ok {
			continue
		}
		columns = append(columns, column)
		seenAppend[column] = struct{}{}
	}

	return columns, nil
}

// CreateManifestForSegment creates a manifest file for a segment.
// It creates column groups from fragments and commits them using a transaction.
// Returns the manifest path string that can be stored in SegmentInfo.manifest_path.
func CreateManifestForSegment(
	basePath string,
	columns []string,
	format string,
	fragments []Fragment,
	storageConfig *indexpb.StorageConfig,
) (string, error) {
	if len(fragments) == 0 {
		return "", merr.WrapErrServiceInternalMsg("fragments cannot be empty")
	}

	// Create column groups from fragments
	columnGroups, err := createColumnGroups(columns, format, fragments)
	if err != nil {
		return "", merr.Wrap(err, "failed to create column groups")
	}
	defer C.loon_column_groups_destroy(columnGroups)

	// Create properties from storage config
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)

	// Convert base path to C string
	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	// Begin transaction (read_version=0 for earliest, retry_limit=10)
	var transactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(0), C.LOON_TRANSACTION_RESOLVE_OVERWRITE /* resolve_id */, getRetryLimit() /* retry_limit */, &transactionHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", merr.WrapErrStorage(err, "loon_transaction_begin failed")
	}
	defer C.loon_transaction_destroy(transactionHandle)

	// Append files to transaction
	result = C.loon_transaction_append_files(transactionHandle, columnGroups)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", merr.WrapErrStorage(err, "loon_transaction_append_files failed")
	}

	// Commit transaction
	var committedVersion C.int64_t
	result = C.loon_transaction_commit(transactionHandle, &committedVersion)
	if err := HandleLoonFFIResult(result); err != nil {
		return "", merr.WrapErrStorage(err, "loon_transaction_commit failed")
	}

	// Return manifest path using the helper function
	return MarshalManifestPath(basePath, int64(committedVersion)), nil
}

// createColumnGroups creates a LoonColumnGroups structure from fragments.
// This is an internal function used by CreateManifestForSegment.
func createColumnGroups(
	columns []string,
	format string,
	fragments []Fragment,
) (*C.LoonColumnGroups, error) {
	// Create C string array for columns
	cColumns := make([]*C.char, len(columns))
	for i, col := range columns {
		cColumns[i] = C.CString(col)
	}
	defer func() {
		for _, c := range cColumns {
			C.free(unsafe.Pointer(c))
		}
	}()

	// Create C string for format
	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	// Create C arrays for paths, start indices, and end indices
	cPaths := make([]*C.char, len(fragments))
	cStartIndices := make([]C.int64_t, len(fragments))
	cEndIndices := make([]C.int64_t, len(fragments))

	for i, f := range fragments {
		cPaths[i] = C.CString(f.FilePath)
		cStartIndices[i] = C.int64_t(f.StartRow)
		cEndIndices[i] = C.int64_t(f.EndRow)
	}
	defer func() {
		for _, p := range cPaths {
			C.free(unsafe.Pointer(p))
		}
	}()

	var outColumnGroups *C.LoonColumnGroups
	var cColumnsPtr **C.char
	var cPathsPtr **C.char
	var cStartIndicesPtr *C.int64_t
	var cEndIndicesPtr *C.int64_t

	if len(cColumns) > 0 {
		cColumnsPtr = &cColumns[0]
	}
	if len(cPaths) > 0 {
		cPathsPtr = &cPaths[0]
	}
	if len(fragments) > 0 {
		cStartIndicesPtr = &cStartIndices[0]
		cEndIndicesPtr = &cEndIndices[0]
	}

	result := C.loon_column_groups_create(
		cColumnsPtr,
		C.size_t(len(columns)),
		cFormat,
		cPathsPtr,
		cStartIndicesPtr,
		cEndIndicesPtr,
		C.size_t(len(fragments)),
		&outColumnGroups,
	)

	if err := HandleLoonFFIResult(result); err != nil {
		return nil, merr.WrapErrStorage(err, "loon_column_groups_create failed")
	}

	return outColumnGroups, nil
}

func GetManifestFieldIDs(manifestPath string, storageConfig *indexpb.StorageConfig) (map[int64]struct{}, error) {
	manifest, err := GetManifestHandle(manifestPath, storageConfig)
	if err != nil {
		return nil, err
	}
	defer C.loon_manifest_destroy(manifest)

	fields := make(map[int64]struct{})
	cgroups := &manifest.column_groups
	if cgroups.column_group_array == nil && cgroups.num_of_column_groups > 0 {
		return nil, merr.WrapErrServiceInternalMsg("column_group_array is nil but num_of_column_groups is %d", cgroups.num_of_column_groups)
	}

	cgArray := unsafe.Slice(cgroups.column_group_array, int(cgroups.num_of_column_groups))
	for i := range cgArray {
		cg := &cgArray[i]
		if cg.columns == nil {
			continue
		}
		columns := unsafe.Slice(cg.columns, int(cg.num_of_columns))
		for _, column := range columns {
			if column == nil {
				continue
			}
			columnName := C.GoString(column)
			fieldID, err := strconv.ParseInt(columnName, 10, 64)
			if err != nil {
				return nil, merr.WrapErrStorage(err, "invalid manifest column name %q", columnName)
			}
			fields[fieldID] = struct{}{}
		}
	}
	return fields, nil
}

// ReadFragmentsFromManifest reads fragment info from a manifest path.
// This function wraps the C exttable_read_column_groups call.
//
// The manifestPath is a JSON string like {"ver":1,"base_path":"external/.../segments/..."}.
// The actual manifest file is at: base_path/_metadata/manifest-{ver}.avro
// When columns is non-empty, only column groups containing at least one of the
// requested columns are considered.
func ReadFragmentsFromManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	columns []string,
) ([]Fragment, error) {
	groups, err := readColumnGroupsFromManifest(manifestPath, storageConfig)
	if err != nil {
		return nil, err
	}
	if len(columns) > 0 {
		columnSet := make(map[string]struct{}, len(columns))
		for _, column := range columns {
			columnSet[column] = struct{}{}
		}

		filtered := make([]manifestColumnGroup, 0, len(groups))
		for _, group := range groups {
			if manifestColumnGroupHasAnyColumn(group, columnSet) {
				filtered = append(filtered, group)
			}
		}
		groups = filtered
	}
	return manifestColumnGroupsToFragments(groups), nil
}

// ManifestHasColumns returns true when the manifest contains every requested
// column in any column group.
func ManifestHasColumns(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	columns []string,
) (bool, error) {
	if len(columns) == 0 {
		return true, nil
	}

	required := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		required[column] = struct{}{}
	}

	groups, err := readColumnGroupsFromManifest(manifestPath, storageConfig)
	if err != nil {
		return false, err
	}
	for _, group := range groups {
		for _, column := range group.Columns {
			delete(required, column)
		}
	}
	return len(required) == 0, nil
}

// ResolveManifestSingleWriterFormat returns the single-policy writer format
// constrained by an existing manifest. When no committed manifest column group
// overlaps columns, fallbackFormat is returned.
func ResolveManifestSingleWriterFormat(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	columns []string,
	fallbackFormat string,
) (string, error) {
	if manifestPath == "" {
		return fallbackFormat, nil
	}
	_, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", err
	}
	if version == ManifestEarliest {
		return fallbackFormat, nil
	}

	columnSet := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		columnSet[column] = struct{}{}
	}

	groups, err := readColumnGroupsFromManifest(manifestPath, storageConfig)
	if err != nil {
		return "", err
	}

	formats := make(map[string]struct{})
	for _, group := range groups {
		if len(group.Fragments) == 0 {
			continue
		}
		if len(columnSet) > 0 && !manifestColumnGroupHasAnyColumn(group, columnSet) {
			continue
		}
		formats[group.Format] = struct{}{}
	}
	if len(formats) == 0 {
		return fallbackFormat, nil
	}
	if len(formats) > 1 {
		return "", merr.WrapErrDataIntegrityMsg("mixed writer formats: single writer columns %v overlap mixed formats in manifest %s: %s",
			columns, manifestPath, formatSetString(formats))
	}
	for format := range formats {
		return format, nil
	}
	return fallbackFormat, nil
}

func manifestColumnGroupHasAnyColumn(group manifestColumnGroup, columns map[string]struct{}) bool {
	for _, column := range group.Columns {
		if _, ok := columns[column]; ok {
			return true
		}
	}
	return false
}

func readColumnGroupsFromManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
) ([]manifestColumnGroup, error) {
	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, merr.Wrap(err, "failed to parse manifest path")
	}

	manifestFilePath := fmt.Sprintf("%s/_metadata/manifest-%d.avro", basePath, version)

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, merr.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)

	cManifestFilePath := C.CString(manifestFilePath)
	defer C.free(unsafe.Pointer(cManifestFilePath))

	var manifest *C.LoonManifest
	result := C.loon_exttable_read_manifest(cManifestFilePath, cProperties, &manifest)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, merr.Wrap(err, "loon_exttable_read_manifest failed")
	}
	if manifest == nil {
		return nil, merr.WrapErrServiceInternalMsg("loon_exttable_read_manifest returned nil manifest")
	}
	defer C.loon_manifest_destroy(manifest)

	cgroups := &manifest.column_groups
	if cgroups.column_group_array == nil && cgroups.num_of_column_groups > 0 {
		return nil, merr.WrapErrServiceInternalMsg("column_group_array is nil but num_of_column_groups is %d", cgroups.num_of_column_groups)
	}
	if cgroups.column_group_array == nil {
		return nil, nil
	}

	groups := make([]manifestColumnGroup, 0, int(cgroups.num_of_column_groups))
	cgArray := unsafe.Slice(cgroups.column_group_array, int(cgroups.num_of_column_groups))
	for i := range cgArray {
		cg := &cgArray[i]
		group := manifestColumnGroup{}

		if cg.columns == nil && cg.num_of_columns > 0 {
			return nil, merr.WrapErrServiceInternalMsg("columns array is nil but num_of_columns is %d in column group %d", cg.num_of_columns, i)
		}
		if cg.columns != nil {
			columnArray := unsafe.Slice(cg.columns, int(cg.num_of_columns))
			group.Columns = make([]string, 0, len(columnArray))
			for j, cColumn := range columnArray {
				if cColumn == nil {
					log.Warn(context.TODO(), "column name is nil in readColumnGroupsFromManifest",
						log.Int("columnGroupIndex", i),
						log.Int("columnIndex", j))
					continue
				}
				group.Columns = append(group.Columns, C.GoString(cColumn))
			}
		}

		if cg.files == nil && cg.num_of_files > 0 {
			return nil, merr.WrapErrServiceInternalMsg("files array is nil but num_of_files is %d in column group %d", cg.num_of_files, i)
		}
		if cg.num_of_files > 0 {
			if cg.format == nil {
				return nil, merr.WrapErrDataIntegrityMsg("manifest column group %d has files but nil format", i)
			}
			group.Format = C.GoString(cg.format)
			if group.Format == "" {
				return nil, merr.WrapErrDataIntegrityMsg("manifest column group %d has files but empty format", i)
			}
		}
		if cg.files != nil {
			fileArray := unsafe.Slice(cg.files, int(cg.num_of_files))
			group.Fragments = make([]Fragment, 0, len(fileArray))
			for j := range fileArray {
				file := &fileArray[j]

				if file.path == nil {
					log.Warn(context.TODO(), "file path is nil in readColumnGroupsFromManifest",
						log.Int("columnGroupIndex", i),
						log.Int("fileIndex", j))
					continue
				}

				filePath := C.GoString(file.path)
				startRow := int64(file.start_index)
				endRow := int64(file.end_index)

				group.Fragments = append(group.Fragments, Fragment{
					FragmentID: int64(len(group.Fragments)),
					FilePath:   filePath,
					StartRow:   startRow,
					EndRow:     endRow,
					RowCount:   endRow - startRow,
				})
			}
		}

		groups = append(groups, group)
	}

	return groups, nil
}

func AppendSegmentManifestColumns(
	ctx context.Context,
	oldManifestPath string,
	format string,
	columns []string,
	fragments []Fragment,
	storageConfig *indexpb.StorageConfig,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return oldManifestPath, nil
	}
	if len(fragments) == 0 {
		return "", merr.WrapErrServiceInternalMsg("fragments cannot be empty")
	}

	existingGroups, err := readColumnGroupsFromManifest(oldManifestPath, storageConfig)
	if err != nil {
		return "", merr.Wrap(err, "failed to read manifest column groups")
	}

	columns, err = columnsToAppend(existingGroups, columns, fragments)
	if err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return oldManifestPath, nil
	}

	basePath, version, err := UnmarshalManifestPath(oldManifestPath)
	if err != nil {
		return "", merr.Wrap(err, "failed to parse manifest path")
	}

	columnGroups, err := createColumnGroups(columns, format, fragments)
	if err != nil {
		return "", merr.Wrap(err, "failed to create column groups")
	}
	if columnGroups == nil {
		return "", merr.WrapErrServiceInternalMsg("loon_column_groups_create returned nil column groups")
	}

	newFiles := &ColumnGroups{
		cColumnGroups:      columnGroups,
		addNewColumnGroups: true,
	}
	defer newFiles.Destroy()

	if columnGroups.column_group_array == nil && columnGroups.num_of_column_groups > 0 {
		return "", merr.WrapErrServiceInternalMsg("column_group_array is nil but num_of_column_groups is %d", columnGroups.num_of_column_groups)
	}

	return CommitManifestUpdates(basePath, version, storageConfig, &ManifestUpdates{
		NewFiles: newFiles,
	})
}

func formatSetString(formats map[string]struct{}) string {
	values := make([]string, 0, len(formats))
	for format := range formats {
		values = append(values, format)
	}
	sort.Strings(values)
	return strings.Join(values, ",")
}
