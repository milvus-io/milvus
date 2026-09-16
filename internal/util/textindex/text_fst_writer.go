// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package textindex

import (
	"context"
	"fmt"
	"math"
	"path"
	"sort"
	"strconv"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// UploadFunc writes a set of fully-qualified object paths atomically from the
// caller's perspective.
type UploadFunc func(context.Context, map[string][]byte) error

// WriteFieldBinlogs writes one complete FST per field for StorageV1/V2
// segments and returns metadata suitable for SegmentInfo Text Log V2.
func WriteFieldBinlogs(
	ctx context.Context,
	upload UploadFunc,
	rootPath string,
	collectionID, partitionID, segmentID int64,
	allocator allocator.Interface,
	fields map[int64][][]byte,
	coverageTimestamp uint64,
) ([]*datapb.FieldBinlog, int64, error) {
	if len(fields) == 0 {
		return nil, 0, nil
	}
	fieldIDs := sortedFieldIDs(fields)
	logs := make([]*datapb.FieldBinlog, 0, len(fieldIDs))
	objects := make(map[string][]byte, len(fieldIDs))
	var totalSize int64
	for _, fieldID := range fieldIDs {
		artifact, err := BuildTextFst(fields[fieldID])
		if err != nil {
			return nil, 0, merr.Wrapf(err, "build text term FST for field %d", fieldID)
		}
		logID, err := allocator.AllocOne()
		if err != nil {
			return nil, 0, err
		}
		logPath := metautil.BuildTextLogV2Path(rootPath, collectionID, partitionID, segmentID, fieldID, logID)
		objects[logPath] = artifact.Data
		size := int64(len(artifact.Data))
		totalSize += size
		logs = append(logs, &datapb.FieldBinlog{
			FieldID: fieldID,
			Format:  MilvusTextFstFormat,
			Binlogs: []*datapb.Binlog{{
				LogID:       logID,
				LogPath:     logPath,
				EntriesNum:  artifact.TermCount,
				LogSize:     size,
				MemorySize:  size,
				TimestampTo: coverageTimestamp,
			}},
		})
	}
	if upload == nil {
		return nil, 0, merr.WrapErrServiceInternalMsg("text term FST uploader is nil")
	}
	if err := upload(ctx, objects); err != nil {
		return nil, 0, merr.Wrap(err, "upload text term FST files")
	}
	return logs, totalSize, nil
}

// BuildManifestEntries writes the FST format of Text Log V2 for StorageV3 and
// returns LOON manifest stat entries. Existing fragments are retained so flush
// generations can advance coverage without losing earlier terms; compaction
// callers normally start from a manifest without Text Log V2 entries and
// therefore get one file per field.
func BuildManifestEntries(
	initialManifestPath string,
	storageConfig *indexpb.StorageConfig,
	allocator allocator.Interface,
	fields map[int64][][]byte,
	coverageTimestamp uint64,
) ([]packed.StatEntry, int64, error) {
	return buildManifestEntries(initialManifestPath, storageConfig, allocator, fields, coverageTimestamp, true)
}

// BuildReplacementManifestEntries writes a complete segment dictionary and
// replaces the prior Text Log V2 manifest entry. Compaction and schema-rewrite
// paths use this form because their output scan already covers every visible
// row.
func BuildReplacementManifestEntries(
	initialManifestPath string,
	storageConfig *indexpb.StorageConfig,
	allocator allocator.Interface,
	fields map[int64][][]byte,
	coverageTimestamp uint64,
) ([]packed.StatEntry, int64, error) {
	return buildManifestEntries(initialManifestPath, storageConfig, allocator, fields, coverageTimestamp, false)
}

func buildManifestEntries(
	initialManifestPath string,
	storageConfig *indexpb.StorageConfig,
	allocator allocator.Interface,
	fields map[int64][][]byte,
	coverageTimestamp uint64,
	retainExisting bool,
) ([]packed.StatEntry, int64, error) {
	if len(fields) == 0 {
		return nil, 0, nil
	}
	basePath, version, err := packed.UnmarshalManifestPath(initialManifestPath)
	if err != nil {
		return nil, 0, err
	}
	type fieldStats struct {
		files      []string
		logSize    int64
		memorySize int64
	}
	existing := make(map[int64]*fieldStats)
	if retainExisting && version != packed.ManifestEarliest {
		existingStats, err := packed.GetManifestStats(initialManifestPath, storageConfig)
		if err != nil {
			return nil, 0, merr.Wrap(err, "failed to read prior text term FST stats")
		}
		for key, stat := range existingStats {
			prefix, fieldID, ok := packed.ParseStatKey(key)
			if !ok || prefix != "text_log_v2" {
				continue
			}
			if len(stat.Paths) == 0 || stat.Metadata["format"] != MilvusTextFstFormat {
				return nil, 0, merr.WrapErrDataIntegrityMsg(
					"invalid retained text-log-v2 metadata for field %d", fieldID)
			}
			logSize, err := strconv.ParseInt(stat.Metadata["log_size"], 10, 64)
			if err != nil || logSize <= 0 {
				return nil, 0, merr.WrapErrDataIntegrityMsg(
					"invalid retained text-log-v2 size for field %d", fieldID)
			}
			memorySize, err := strconv.ParseInt(stat.Metadata["memory_size"], 10, 64)
			if err != nil || memorySize <= 0 {
				return nil, 0, merr.WrapErrDataIntegrityMsg(
					"invalid retained text-log-v2 memory size for field %d", fieldID)
			}
			existing[fieldID] = &fieldStats{
				files:      stat.Paths,
				logSize:    logSize,
				memorySize: memorySize,
			}
		}
	}

	fieldIDs := sortedFieldIDs(fields)
	entries := make([]packed.StatEntry, 0, len(fieldIDs))
	var sizeWritten int64
	for _, fieldID := range fieldIDs {
		artifact, err := BuildTextFst(fields[fieldID])
		if err != nil {
			return nil, 0, merr.Wrapf(err, "build text term FST for field %d", fieldID)
		}
		id, err := allocator.AllocOne()
		if err != nil {
			return nil, 0, err
		}
		fullPath := path.Join(basePath, fmt.Sprintf("_stats/text_log_v2.%d/%d.fst", fieldID, id))
		if err := packed.WriteFile(storageConfig, fullPath, artifact.Data); err != nil {
			return nil, 0, err
		}
		size := int64(len(artifact.Data))
		sizeWritten += size

		fs := existing[fieldID]
		if fs == nil {
			fs = &fieldStats{}
		}
		if fs.logSize > math.MaxInt64-size || fs.memorySize > math.MaxInt64-size {
			return nil, 0, merr.WrapErrDataIntegrityMsg(
				"text-log-v2 aggregate size overflows for field %d", fieldID)
		}
		fs.files = append(fs.files, fullPath)
		fs.logSize += size
		fs.memorySize += size
		entries = append(entries, packed.StatEntry{
			Key:   fmt.Sprintf("text_log_v2.%d", fieldID),
			Files: fs.files,
			Metadata: map[string]string{
				"format":             MilvusTextFstFormat,
				"log_size":           strconv.FormatInt(fs.logSize, 10),
				"memory_size":        strconv.FormatInt(fs.memorySize, 10),
				"coverage_timestamp": strconv.FormatUint(coverageTimestamp, 10),
			},
		})
	}
	return entries, sizeWritten, nil
}

func sortedFieldIDs(fields map[int64][][]byte) []int64 {
	fieldIDs := make([]int64, 0, len(fields))
	for fieldID := range fields {
		fieldIDs = append(fieldIDs, fieldID)
	}
	sort.Slice(fieldIDs, func(i, j int) bool { return fieldIDs[i] < fieldIDs[j] })
	return fieldIDs
}
