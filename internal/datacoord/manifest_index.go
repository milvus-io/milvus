// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"path"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// buildManifestIndexInfo assembles the manifest entry for a completed index
// build entirely from DataCoord metadata: the segment's manifest base path,
// the collection's index definition, and the task record the worker result was
// already projected onto. Nothing here needs the worker to have touched the
// manifest, which is what keeps manifest publication a DataCoord-only step.
func buildManifestIndexInfo(m *meta, segment *SegmentInfo, segIdx *model.SegmentIndex) (packed.ManifestIndexInfo, error) {
	basePath, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return packed.ManifestIndexInfo{}, merr.Wrap(err, "parse segment manifest path for index publication")
	}
	if basePath == "" {
		return packed.ManifestIndexInfo{}, merr.WrapErrServiceInternalMsg(
			"segment %d manifest path has an empty base path", segment.GetID())
	}

	indexPrefix := metautil.NewIndexPathBuilder(
		m.chunkManager.RootPath(),
		segIdx.IndexStorePathVersion,
		segIdx.CollectionID,
		segIdx.PartitionID,
		segIdx.SegmentID,
		segIdx.BuildID,
		segIdx.IndexVersion,
	).BuildPrefix()
	relativePath, err := packed.ManifestIndexRelativePath(basePath, indexPrefix)
	if err != nil {
		return packed.ManifestIndexInfo{}, err
	}

	indexParams := m.indexMeta.GetIndexParams(segIdx.CollectionID, segIdx.IndexID)
	properties := common.KeyValuePairs(m.indexMeta.GetTypeParams(segIdx.CollectionID, segIdx.IndexID)).ToMap()
	for key, value := range common.KeyValuePairs(indexParams).ToMap() {
		properties[key] = value
	}
	indexType := GetIndexType(indexParams)
	// A per-segment override wins: DataCoord may downgrade the index type for
	// one segment (e.g. to a flat index for a tiny segment).
	if segIdx.IndexType != "" {
		indexType = segIdx.IndexType
	}
	properties[common.IndexTypeKey] = indexType

	fieldID := m.indexMeta.GetFieldIDByIndexID(segIdx.CollectionID, segIdx.IndexID)
	return packed.ManifestIndexInfo{
		ColumnName:                collectionFieldName(m, segIdx.CollectionID, fieldID),
		IndexName:                 m.indexMeta.GetIndexNameByID(segIdx.CollectionID, segIdx.IndexID),
		IndexType:                 indexType,
		Path:                      relativePath,
		FieldID:                   fieldID,
		IndexID:                   segIdx.IndexID,
		BuildID:                   segIdx.BuildID,
		IndexVersion:              segIdx.IndexVersion,
		NumRows:                   segIdx.NumRows,
		SerializedSize:            int64(segIdx.IndexSerializedSize),
		MemSize:                   int64(segIdx.IndexMemSize),
		CurrentIndexVersion:       segIdx.CurrentIndexVersion,
		CurrentScalarIndexVersion: segIdx.CurrentScalarIndexVersion,
		IndexStorePathVersion:     segIdx.IndexStorePathVersion,
		IndexFileKeys:             common.CloneStringList(segIdx.IndexFileKeys),
		Properties:                properties,
	}, nil
}

func collectionFieldName(m *meta, collectionID, fieldID int64) string {
	collection := m.GetCollection(collectionID)
	if collection == nil || collection.Schema == nil {
		return ""
	}
	for _, field := range collection.Schema.GetFields() {
		if field.GetFieldID() == fieldID {
			return field.GetName()
		}
	}
	return ""
}

// getManifestIndexesForSegment reads a StorageV3 manifest for a legacy
// metadata fallback. Callers invoke it lazily and reuse the result for every
// missing SegmentIndex entry on the segment.
func (s *Server) getManifestIndexesForSegment(ctx context.Context, segmentID int64) ([]packed.ManifestIndexInfo, string) {
	// A server can briefly be partially initialized while metadata is being
	// wired up. Treat an absent segment store as a segment without a manifest
	// rather than dereferencing a nil SegmentsInfo during fallback.
	if s == nil || s.meta == nil || s.meta.segments == nil {
		return nil, ""
	}
	segment := s.meta.GetSegment(ctx, segmentID)
	if segment == nil || segment.GetStorageVersion() != storage.StorageV3 || segment.GetManifestPath() == "" {
		return nil, ""
	}

	manifestPath := segment.GetManifestPath()
	manifestIndexes, err := packed.GetManifestIndexInfos(manifestPath, createStorageConfig())
	if err != nil {
		mlog.Warn(ctx, "failed to read index metadata from manifest",
			mlog.FieldSegmentID(segmentID),
			mlog.String("manifestPath", manifestPath),
			mlog.Err(err))
		return nil, ""
	}
	return manifestIndexes, manifestPath
}

// manifestIndexFilePathInfo projects one manifest index entry into the load
// metadata QueryNode consumes. It returns false for an entry that cannot
// produce a safe file list, which the caller treats as "no manifest artifact".
func manifestIndexFilePathInfo(segmentID int64, manifestIndex packed.ManifestIndexInfo) (*indexpb.IndexFilePathInfo, bool) {
	if manifestIndex.Path == "" || manifestIndex.IndexName == "" || manifestIndex.IndexType == "" ||
		manifestIndex.IndexStorePathVersion < indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED ||
		manifestIndex.NumRows < 0 || manifestIndex.SerializedSize < 0 || manifestIndex.MemSize < 0 ||
		len(manifestIndex.IndexFileKeys) == 0 {
		return nil, false
	}

	filePaths := make([]string, 0, len(manifestIndex.IndexFileKeys))
	for _, fileKey := range manifestIndex.IndexFileKeys {
		// Index file keys are plain file names under Path. Reject anything that
		// could escape the artifact directory of a manifest we did not write.
		if fileKey == "" || path.IsAbs(fileKey) || path.Base(fileKey) != fileKey || fileKey == "." {
			return nil, false
		}
		filePaths = append(filePaths, path.Join(manifestIndex.Path, fileKey))
	}

	return &indexpb.IndexFilePathInfo{
		SegmentID:                 segmentID,
		FieldID:                   manifestIndex.FieldID,
		IndexID:                   manifestIndex.IndexID,
		BuildID:                   manifestIndex.BuildID,
		IndexName:                 manifestIndex.IndexName,
		IndexParams:               manifestIndexParams(manifestIndex),
		IndexFilePaths:            filePaths,
		SerializedSize:            uint64(manifestIndex.SerializedSize),
		MemSize:                   uint64(manifestIndex.MemSize),
		IndexVersion:              manifestIndex.IndexVersion,
		NumRows:                   manifestIndex.NumRows,
		CurrentIndexVersion:       manifestIndex.CurrentIndexVersion,
		CurrentScalarIndexVersion: manifestIndex.CurrentScalarIndexVersion,
		IndexStorePathVersion:     manifestIndex.IndexStorePathVersion,
	}, true
}

func manifestIndexParams(manifestIndex packed.ManifestIndexInfo) []*commonpb.KeyValuePair {
	properties := make(map[string]string, len(manifestIndex.Properties)+1)
	for key, value := range manifestIndex.Properties {
		properties[key] = value
	}
	properties[common.IndexTypeKey] = manifestIndex.IndexType

	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	params := make([]*commonpb.KeyValuePair, 0, len(keys))
	for _, key := range keys {
		params = append(params, &commonpb.KeyValuePair{Key: key, Value: properties[key]})
	}
	return params
}

// resolveManifestIndexFilePathInfo returns the unique manifest artifact for a
// completed SegmentIndex task. It is used only when the task's etcd metadata
// does not carry index_file_keys.
func resolveManifestIndexFilePathInfo(
	ctx context.Context,
	manifestPath string,
	manifestIndexes []packed.ManifestIndexInfo,
	segIdx *model.SegmentIndex,
	fieldID int64,
) (*indexpb.IndexFilePathInfo, bool) {
	if manifestPath == "" {
		return nil, false
	}

	var matchedIndex *packed.ManifestIndexInfo
	for i := range manifestIndexes {
		manifestIndex := &manifestIndexes[i]
		if !manifestIndexMatches(*manifestIndex, segIdx, fieldID) {
			continue
		}
		if matchedIndex != nil {
			mlog.Warn(ctx, "multiple manifest indexes match one index task",
				mlog.FieldSegmentID(segIdx.SegmentID),
				mlog.FieldBuildID(segIdx.BuildID),
				mlog.FieldIndexID(segIdx.IndexID),
				mlog.String("manifestPath", manifestPath))
			return nil, false
		}
		matchedIndex = manifestIndex
	}
	if matchedIndex == nil {
		return nil, false
	}

	info, ok := manifestIndexFilePathInfo(segIdx.SegmentID, *matchedIndex)
	if !ok {
		mlog.Warn(ctx, "invalid matching manifest index metadata",
			mlog.FieldSegmentID(segIdx.SegmentID),
			mlog.FieldBuildID(segIdx.BuildID),
			mlog.FieldIndexID(segIdx.IndexID),
			mlog.String("manifestPath", manifestPath))
	}
	return info, ok
}

func manifestIndexMatches(manifestIndex packed.ManifestIndexInfo, segIdx *model.SegmentIndex, fieldID int64) bool {
	return manifestIndex.FieldID == fieldID &&
		manifestIndex.IndexID == segIdx.IndexID &&
		manifestIndex.BuildID == segIdx.BuildID &&
		manifestIndex.IndexVersion == segIdx.IndexVersion &&
		(segIdx.IndexType == "" || manifestIndex.IndexType == segIdx.IndexType)
}

// resolveManifestIndexFilePathInfos returns manifest-backed index artifacts
// that still belong to active collection index definitions. SegmentIndex
// metadata may be absent for StorageV3, so the collection-level index
// definitions are the source of truth for filtering dropped indexes.
func resolveManifestIndexFilePathInfos(
	ctx context.Context,
	segmentID int64,
	manifestPath string,
	manifestIndexes []packed.ManifestIndexInfo,
	activeIndexes []*model.Index,
) []*indexpb.IndexFilePathInfo {
	activeByID := make(map[int64]*model.Index, len(activeIndexes))
	for _, index := range activeIndexes {
		if index == nil || index.IsDeleted {
			continue
		}
		activeByID[index.IndexID] = index
	}

	ret := make([]*indexpb.IndexFilePathInfo, 0, len(manifestIndexes))
	seen := make(map[int64]struct{}, len(manifestIndexes))
	for _, manifestIndex := range manifestIndexes {
		index, ok := activeByID[manifestIndex.IndexID]
		if !ok || manifestIndex.FieldID != index.FieldID {
			continue
		}
		if _, ok := seen[manifestIndex.IndexID]; ok {
			mlog.Warn(ctx, "multiple manifest indexes match one active index",
				mlog.FieldSegmentID(segmentID),
				mlog.FieldIndexID(manifestIndex.IndexID),
				mlog.String("manifestPath", manifestPath))
			continue
		}

		info, ok := manifestIndexFilePathInfo(segmentID, manifestIndex)
		if !ok {
			mlog.Warn(ctx, "invalid manifest index metadata for active index",
				mlog.FieldSegmentID(segmentID),
				mlog.FieldIndexID(manifestIndex.IndexID),
				mlog.String("manifestPath", manifestPath))
			continue
		}
		seen[manifestIndex.IndexID] = struct{}{}
		ret = append(ret, info)
	}
	return ret
}
