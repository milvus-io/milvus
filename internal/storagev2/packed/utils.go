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
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

// ExternalFetchOptions groups per-collection external table parameters
// to keep function signatures clean.
type ExternalFetchOptions struct {
	CollectionID int64
	ExternalSpec string // raw external_spec JSON (C++ derives extfs + format props)
	// RowLimit caps rows per fragment when splitting large files. Zero (or
	// negative) falls back to DefaultFragmentRowLimit.
	RowLimit int64
}

// rowLimitOrDefault resolves the effective fragment row limit.
func (o ExternalFetchOptions) rowLimitOrDefault() int64 {
	if o.RowLimit > 0 {
		return o.RowLimit
	}
	return int64(DefaultFragmentRowLimit)
}

// getFileInfoPoolSize is the per-call concurrency for row-count fetches.
// 16 matches the prior hand-rolled worker count; raising it means more
// parallel S3 HEAD / parquet footer reads per DN task.
const getFileInfoPoolSize = 16

// fetchRowCountsConcurrently returns a rowCounts slice aligned with fileInfos.
// Entries with NumRows > 0 are taken as-is; zero/negative entries are filled
// by concurrent GetFileInfo calls via a conc.Pool. Returns the first error
// any worker produced, or ctx.Err() if canceled before launching workers.
func fetchRowCountsConcurrently(
	ctx context.Context,
	format string,
	fileInfos []FileInfo,
	storageConfig *indexpb.StorageConfig,
	extfs ExternalSpecContext,
) ([]int64, error) {
	rowCounts := make([]int64, len(fileInfos))
	needInfo := make([]int, 0, len(fileInfos))
	for i, fi := range fileInfos {
		rowCounts[i] = fi.NumRows
		if fi.NumRows <= 0 {
			needInfo = append(needInfo, i)
		}
	}
	if len(needInfo) == 0 {
		return rowCounts, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	workers := getFileInfoPoolSize
	if workers > len(needInfo) {
		workers = len(needInfo)
	}
	pool := conc.NewPool[struct{}](workers)
	defer pool.Release()

	futures := make([]*conc.Future[struct{}], len(needInfo))
	for k, idx := range needInfo {
		idx := idx
		futures[k] = pool.Submit(func() (struct{}, error) {
			fetchedInfo, err := GetFileInfo(format, fileInfos[idx].FilePath, storageConfig, extfs)
			if err != nil {
				return struct{}{}, merr.Wrapf(err, "failed to get file info for %s", fileInfos[idx].FilePath)
			}
			// Distinct indexes across workers -> no race on rowCounts.
			rowCounts[idx] = fetchedInfo.NumRows
			return struct{}{}, nil
		})
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return nil, err
	}
	// Post-wait ctx check: AwaitAll settles every future, so a ctx canceled
	// mid-run whose workers happened to return nil would slip past the err
	// branch above. Mirrors the pre-conc.Pool behavior.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return rowCounts, nil
}

// FetchFragmentsFromExternalSourceWithRange reads the explore manifest,
// restricts to the [fileIndexBegin, fileIndexEnd) slice, fills missing row
// counts via GetFileInfo (concurrent pool), and splits each file into
// fragments of at most opts.RowLimit rows (DefaultFragmentRowLimit if zero).
// Enables parallel refresh by splitting files across multiple DN tasks.
func FetchFragmentsFromExternalSourceWithRange(
	ctx context.Context,
	format string,
	columns []string,
	externalSource string,
	storageConfig *indexpb.StorageConfig,
	fileIndexBegin, fileIndexEnd int64,
	exploreManifestPath string,
	opts ExternalFetchOptions,
) ([]Fragment, error) {
	if exploreManifestPath == "" {
		return nil, merr.WrapErrServiceInternalMsg("explore manifest path is required")
	}

	extfs := ExternalSpecContext{
		CollectionID: opts.CollectionID,
		Source:       externalSource,
		Spec:         opts.ExternalSpec,
	}

	exploreStart := time.Now()
	fileInfos, err := ReadFileInfosFromManifestPath(exploreManifestPath, storageConfig)
	if err != nil {
		return nil, merr.Wrap(err, "failed to read explore manifest")
	}
	rawCount := len(fileInfos)
	// Apply the same sort+format-filter that DataCoord used to derive
	// fileIndexBegin/End. The manifest persists the raw arrow listing
	// (Spark `_SUCCESS`, `.crc`, README, etc.) so slicing it directly
	// against DataCoord's filtered indices would pick the wrong files —
	// either a non-parquet stray triggering "Invalid parquet magic", or
	// a real parquet getting silently dropped. NormalizeFileInfos must
	// stay byte-for-byte identical to the DataCoord-side call so both
	// indexed views agree.
	fileInfos, skipped := NormalizeFileInfos(fileInfos, format)
	mlog.Info(ctx, "Read file list from explore manifest",
		mlog.String("manifestPath", exploreManifestPath),
		mlog.Int("rawFileCount", rawCount),
		mlog.Int("normalizedFileCount", len(fileInfos)),
		mlog.Int("skippedNonFormat", skipped),
		mlog.Duration("readDuration", time.Since(exploreStart)))

	// Slice to assigned range.
	if fileIndexEnd > int64(len(fileInfos)) {
		fileIndexEnd = int64(len(fileInfos))
	}
	if fileIndexBegin >= int64(len(fileInfos)) {
		return nil, merr.WrapErrServiceInternalMsg("fileIndexBegin %d >= total files %d", fileIndexBegin, len(fileInfos))
	}
	fileInfos = fileInfos[fileIndexBegin:fileIndexEnd]
	if len(fileInfos) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("no files in range [%d, %d)", fileIndexBegin, fileIndexEnd)
	}

	getFileInfoStart := time.Now()
	rowCounts, err := fetchRowCountsConcurrently(ctx, format, fileInfos, storageConfig, extfs)
	if err != nil {
		return nil, err
	}
	mlog.Info(ctx, "GetFileInfo phase completed",
		mlog.Int("totalFiles", len(fileInfos)),
		mlog.Duration("getFileInfoDuration", time.Since(getFileInfoStart)))

	rowLimit := opts.rowLimitOrDefault()
	fragmentIDGenerator := NewFragmentIDGenerator(0)
	var fragments []Fragment
	for i, fi := range fileInfos {
		fragments = append(fragments, SplitFileToFragments(fi.FilePath, rowCounts[i], rowLimit, fragmentIDGenerator)...)
	}
	if len(fragments) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("no data files in range [%d, %d)", fileIndexBegin, fileIndexEnd)
	}

	mlog.Info(ctx, "Created fragments from file range",
		mlog.Int("totalFragments", len(fragments)),
		mlog.Int("fileCount", len(fileInfos)),
		mlog.Int64("fileIndexBegin", fileIndexBegin),
		mlog.Int64("fileIndexEnd", fileIndexEnd))

	return fragments, nil
}

// BuildCurrentSegmentFragments builds segment to fragments mapping from current segments.
// It reads fragment info from manifest if available, otherwise creates virtual fragments.
// When columns is non-empty, only manifest column groups containing at least
// one requested column are considered.
// Returns error if a segment has a manifest path but the manifest cannot be read.
func BuildCurrentSegmentFragments(
	segments []*datapb.SegmentInfo,
	storageConfig *indexpb.StorageConfig,
	columns []string,
) (SegmentFragments, error) {
	result := make(SegmentFragments)
	for _, seg := range segments {
		// Try to read from manifest if available
		if seg.GetManifestPath() != "" && storageConfig != nil {
			fragments, err := ReadFragmentsFromManifest(seg.GetManifestPath(), storageConfig, columns)
			if err != nil {
				return nil, merr.Wrapf(err, "failed to read manifest for segment %d at %s", seg.GetID(), seg.GetManifestPath())
			}
			if len(fragments) > 0 {
				result[seg.GetID()] = fragments
				continue
			}
			mlog.Warn(context.TODO(), "manifest returned 0 fragments, using virtual fragment",
				mlog.FieldSegmentID(seg.GetID()),
				mlog.String("manifestPath", seg.GetManifestPath()))
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

// CreateSegmentManifestWithBasePath creates a manifest file at the given base path.
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
		if field.GetIsFunctionOutput() {
			continue // function output fields don't exist in external data
		}
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
