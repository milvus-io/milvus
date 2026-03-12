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

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

const (
	// DefaultFragmentRowLimit is the default row limit for splitting large files into fragments
	DefaultFragmentRowLimit = 1000000 // 1 million rows
)

// SegmentFragments maps segment ID to its fragments
type SegmentFragments map[int64][]Fragment

// FragmentIDGenerator generates sequential fragment IDs. Not safe for concurrent use.
type FragmentIDGenerator func() int64

// NewFragmentIDGenerator creates a generator that returns sequential IDs starting from start.
func NewFragmentIDGenerator(start int64) FragmentIDGenerator {
	nextFragmentID := start
	return func() int64 {
		id := nextFragmentID
		nextFragmentID++
		return id
	}
}

// SplitFileToFragments splits a large file into multiple fragments based on row count.
// If totalRows <= rowLimit, returns a single fragment covering the entire file.
// Otherwise, splits the file into multiple fragments with rowLimit rows each.
func SplitFileToFragments(
	filePath string,
	totalRows int64,
	rowLimit int64,
	fragmentIDGenerator FragmentIDGenerator,
) []Fragment {
	if totalRows <= rowLimit {
		return []Fragment{{
			FragmentID: fragmentIDGenerator(),
			FilePath:   filePath,
			StartRow:   0,
			EndRow:     totalRows,
			RowCount:   totalRows,
		}}
	}

	var fragments []Fragment
	for start := int64(0); start < totalRows; start += rowLimit {
		end := start + rowLimit
		if end > totalRows {
			end = totalRows
		}
		fragments = append(fragments, Fragment{
			FragmentID: fragmentIDGenerator(),
			FilePath:   filePath,
			StartRow:   start,
			EndRow:     end,
			RowCount:   end - start,
		})
	}
	return fragments
}

// FetchFragmentsFromExternalSource scans the external source and returns fragments.
// It explores files from the external source and splits large files into multiple fragments.
//
// IMPORTANT: The loon_exttable_explore FFI uses a Transaction that writes a manifest file
// to baseDir. If baseDir == exploreDir, the manifest pollutes the data directory and
// accumulates stale column groups across calls (Transaction append semantics).
// To avoid this, we use a unique temporary baseDir for each explore call.
func FetchFragmentsFromExternalSource(
	ctx context.Context,
	format string,
	columns []string,
	externalSource string,
	storageConfig *indexpb.StorageConfig,
) ([]Fragment, error) {
	log := log.Ctx(ctx)

	// Use a unique temp base dir for explore output to prevent:
	// 1. Manifest files polluting the user's data directory
	// 2. Transaction accumulating stale column groups across calls
	exploreBaseDir := fmt.Sprintf("__explore_temp__/%s", uuid.New().String())

	// For lance-table format, BuildLanceBaseUri constructs "scheme://bucket/path" without
	// root_path (unlike Parquet which uses Arrow S3 FileSystemProxy that prepends root_path).
	// Prepend root_path so Lance can locate the dataset in the correct bucket prefix.
	exploreDir := externalSource
	if format == "lance-table" && storageConfig.GetRootPath() != "" {
		exploreDir = strings.TrimRight(storageConfig.GetRootPath(), "/") + "/" + externalSource
	}

	// Call ExploreFiles to get file list with row counts.
	// ExploreFiles writes temp manifest files to exploreBaseDir as a side effect of the FFI.
	// We clean them up immediately after consuming the results.
	fileInfos, err := ExploreFiles(
		columns,
		format,
		exploreBaseDir,
		exploreDir,
		storageConfig,
	)
	// Always clean up temp dir, regardless of whether ExploreFiles succeeded or failed.
	defer CleanupExploreTempDir(exploreBaseDir, storageConfig)

	if err != nil {
		return nil, fmt.Errorf("failed to explore files: %w", err)
	}

	if len(fileInfos) == 0 {
		return nil, fmt.Errorf("no files found in external source %q with format %q", externalSource, format)
	}

	log.Info("Explored external files",
		zap.Int("fileCount", len(fileInfos)),
		zap.String("format", format),
		zap.String("externalSource", externalSource))

	// Get row count for each file and split large files into fragments.
	var fragments []Fragment
	fragmentIDGenerator := NewFragmentIDGenerator(0)

	for _, fi := range fileInfos {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		numRows := fi.NumRows
		if numRows <= 0 {
			// Row count not available from explore (e.g., parquet format).
			// Fetch it separately via GetFileInfo FFI call.
			fetchedInfo, err := GetFileInfo(format, fi.FilePath, storageConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to get file info for %s: %w", fi.FilePath, err)
			}
			numRows = fetchedInfo.NumRows
		}

		fileFragments := SplitFileToFragments(
			fi.FilePath,
			numRows,
			DefaultFragmentRowLimit,
			fragmentIDGenerator,
		)
		fragments = append(fragments, fileFragments...)
	}

	if len(fragments) == 0 {
		return nil, fmt.Errorf("no data files found in external source %q", externalSource)
	}

	log.Info("Created fragments from external files",
		zap.Int("totalFragments", len(fragments)),
		zap.Int("fileCount", len(fileInfos)))

	return fragments, nil
}

// BuildCurrentSegmentFragments builds segment to fragments mapping from current segments.
// It reads fragment info from manifest if available, otherwise creates virtual fragments.
// Returns error if a segment has a manifest path but the manifest cannot be read.
func BuildCurrentSegmentFragments(
	segments []*datapb.SegmentInfo,
	storageConfig *indexpb.StorageConfig,
) (SegmentFragments, error) {
	result := make(SegmentFragments)
	for _, seg := range segments {
		// Try to read from manifest if available
		if seg.GetManifestPath() != "" && storageConfig != nil {
			fragments, err := ReadFragmentsFromManifest(seg.GetManifestPath(), storageConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to read manifest for segment %d at %s: %w",
					seg.GetID(), seg.GetManifestPath(), err)
			}
			if len(fragments) > 0 {
				result[seg.GetID()] = fragments
				continue
			}
			log.Warn("manifest returned 0 fragments, using virtual fragment",
				zap.Int64("segmentID", seg.GetID()),
				zap.String("manifestPath", seg.GetManifestPath()))
		}

		// Virtual fragment for segments without manifest (initial state)
		result[seg.GetID()] = []Fragment{
			{
				FragmentID: seg.GetID(),
				FilePath:   "",
				StartRow:   0,
				EndRow:     seg.GetNumOfRows(),
				RowCount:   seg.GetNumOfRows(),
			},
		}
	}
	return result, nil
}

// CreateSegmentManifest creates a manifest file for a segment with the given fragments.
// This is a convenience wrapper around CreateManifestForSegment.
func CreateSegmentManifest(
	ctx context.Context,
	collectionID int64,
	segmentID int64,
	format string,
	columns []string,
	fragments []Fragment,
	storageConfig *indexpb.StorageConfig,
) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	// Build manifest base path
	basePath := fmt.Sprintf("external/%d/segments/%d", collectionID, segmentID)

	manifestPath, err := CreateManifestForSegment(
		basePath,
		columns,
		format,
		fragments,
		storageConfig,
	)
	if err != nil {
		return "", err
	}

	return manifestPath, nil
}

// CreateSegmentManifestWithBasePath creates a manifest file with a custom base path.
// This allows creating temporary manifests that will be renamed later.
func CreateSegmentManifestWithBasePath(
	ctx context.Context,
	basePath string,
	format string,
	columns []string,
	fragments []Fragment,
	storageConfig *indexpb.StorageConfig,
) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	manifestPath, err := CreateManifestForSegment(
		basePath,
		columns,
		format,
		fragments,
		storageConfig,
	)
	if err != nil {
		return "", err
	}

	return manifestPath, nil
}

// GetColumnNamesFromSchema extracts column names from schema.
// For external collections: only includes fields with ExternalField set
// (system fields like __virtual_pk__ are skipped as they don't exist in parquet data).
// For normal collections: uses field name for all fields.
func GetColumnNamesFromSchema(schema *schemapb.CollectionSchema) []string {
	if schema == nil {
		return nil
	}

	isExternal := schema.GetExternalSource() != ""
	var columns []string
	for _, field := range schema.GetFields() {
		extField := field.GetExternalField()
		if extField != "" {
			columns = append(columns, extField)
		} else if !isExternal {
			// Non-external collections: use field name
			columns = append(columns, field.GetName())
		}
		// External collections: skip fields without ExternalField
		// (e.g., __virtual_pk__, RowID, Timestamp)
	}
	return columns
}
