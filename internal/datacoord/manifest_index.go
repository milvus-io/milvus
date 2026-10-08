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

	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
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
		if fileKey == "" || path.IsAbs(fileKey) || path.Base(fileKey) != fileKey || fileKey == "." || fileKey == ".." {
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

// manifestIndexFilePathInfoForSegment validates resolved reader paths against the
// owning segment and storage root. Writers use manifestIndexFilePathInfo for
// staged relative paths, before milvus-storage resolves them.
func manifestIndexFilePathInfoForSegment(rootPath string, segment *datapb.SegmentInfo,
	entry packed.ManifestIndexInfo,
) (*indexpb.IndexFilePathInfo, bool) {
	expected := metautil.NewIndexPathBuilder(rootPath, entry.IndexStorePathVersion,
		segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(),
		entry.BuildID, entry.IndexVersion).BuildPrefix()
	if path.Clean(entry.Path) != expected {
		return nil, false
	}
	return manifestIndexFilePathInfo(segment.GetID(), entry)
}

// Bound outstanding reads by both the configured scan concurrency and the
// storage connection budget.
func segmentIndexManifestReadConcurrency() int {
	params := paramtable.Get()
	limit := params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.GetAsInt()
	if connections := params.MinioCfg.MaxConnections.GetAsInt(); connections > 0 {
		limit = min(limit, connections)
	}
	return max(1, limit)
}

// readManifestIndexes shares a read budget across startup, restore and GC.
// Waiting for admission is cancellable and does not enter cgo.
func (m *meta) readManifestIndexes(ctx context.Context, manifestPath string, config *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
	io := packed.NewManifestIOContext(1)
	defer io.Close()
	return m.readManifestIndexesWithIO(ctx, io, manifestPath, config)
}

func (m *meta) readManifestIndexesWithIO(ctx context.Context, io *packed.ManifestIOContext, manifestPath string, config *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
	m.manifestReadOnce.Do(func() { m.manifestReadSlots = semaphore.NewWeighted(int64(segmentIndexManifestReadConcurrency())) })
	if err := m.manifestReadSlots.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer m.manifestReadSlots.Release(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return packed.GetManifestIndexInfosAsync(ctx, io, manifestPath, config)
}

// submitManifestIndexRead shares the read budget with runtime reads. Completion
// releases admission on the executor, so a submitting coordinator never needs
// another goroutine to consume completions just to unblock admission.
func (m *meta) submitManifestIndexRead(ctx context.Context, io *packed.ManifestIOContext, manifestPath string, config *indexpb.StorageConfig, complete func([]packed.ManifestIndexInfo, error)) error {
	m.manifestReadOnce.Do(func() { m.manifestReadSlots = semaphore.NewWeighted(int64(segmentIndexManifestReadConcurrency())) })
	if err := m.manifestReadSlots.Acquire(ctx, 1); err != nil {
		return err
	}
	err := packed.SubmitManifestIndexInfos(ctx, io, manifestPath, config, func(entries []packed.ManifestIndexInfo, err error) {
		defer m.manifestReadSlots.Release(1)
		complete(entries, err)
	})
	if err != nil {
		m.manifestReadSlots.Release(1)
	}
	return err
}

// validateManifestIndexPublishable is the writer-side twin of
// manifestIndexFilePathInfo: an entry the fail-closed readers (GC's
// retraction resolve, the startup reload) would refuse can neither be served
// nor retired, so no writer may commit one. It applies the exact reader
// predicate rather than restating the field checks, so writer and readers
// cannot drift apart.
func validateManifestIndexPublishable(segmentID int64, manifestIndex packed.ManifestIndexInfo) error {
	if _, ok := manifestIndexFilePathInfo(segmentID, manifestIndex); !ok {
		return merr.WrapErrServiceInternalMsg(
			"refusing to publish unusable index entry for segment %d: indexID %d buildID %d indexName %q",
			segmentID, manifestIndex.IndexID, manifestIndex.BuildID, manifestIndex.IndexName)
	}
	return nil
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

// reloadSegmentIndexesFromManifests rebuilds completed index records from
// retained StorageV3 manifests marked manifest_has_index. Dropped compaction
// parents still need their indexes for query fallback and orphan-GC protection.
// Recovery is independent of the current write-mode switch: records published
// while the switch was on must remain visible after it is turned off. Existing
// etcd rows win on buildID conflicts, so active, failed, fake-finished, and
// record-resident results stay authoritative.
//
// A manifest that cannot be read fails startup. Skipping it would
// leave meta silently incomplete, and a silently incomplete indexMeta is not
// merely "that segment looks unindexed": garbage collection treats an absent
// SegmentIndex as proof the artifact is garbage. recycleUnusedIndexFilesV0's
// CheckCleanSegmentIndex miss path deletes the whole buildID prefix with no
// time tolerance, and the default storePathVersion 0 puts index files exactly
// under the index_files/ prefix it walks. One transient object-store error
// would therefore destroy live index files that the manifest still references.
// Failing to start is recoverable; that is not.
func (m *meta) reloadSegmentIndexesFromManifests(ctx context.Context) error {
	record := timerecord.NewTimeRecorder("indexMeta-reloadFromManifests")
	segments := m.SelectSegments(ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return (isSegmentHealthy(segment) || segment.GetState() == commonpb.SegmentState_Dropped) &&
			segment.GetStorageVersion() >= storage.StorageV3 &&
			segment.GetManifestHasIndex() && segment.GetLevel() != datapb.SegmentLevel_L0
	}))
	for _, segment := range segments {
		if segment.GetManifestPath() == "" {
			return merr.Wrapf(merr.ErrDataIntegrity,
				"segment %d is marked manifest_has_index but has no manifest path", segment.GetID())
		}
	}
	if len(segments) == 0 {
		mlog.Info(ctx, "no segment manifests to recover indexes from")
		return nil
	}

	storageConfig := createStorageConfig()
	rootPath := storageConfig.GetRootPath()
	if m.chunkManager != nil {
		rootPath = m.chunkManager.RootPath()
	}
	concurrency := min(len(segments), segmentIndexManifestReadConcurrency())
	reader := m.newManifestIndexReader(ctx, segments, concurrency, storageConfig)
	defer reader.close()
	installed := 0
	var emptyMarkers []UpdateOperator
	flushMarkers := func(markers []UpdateOperator) error {
		if len(markers) == 0 {
			return nil
		}
		return m.UpdateSegmentsInfo(ctx, markers...)
	}
	for {
		result, err := reader.next()
		if err != nil {
			return merr.Wrap(err, "recover segment indexes from manifests")
		}
		if result == nil {
			break
		}
		indexes, removed, err := m.recoverManifestIndexes(ctx, result.request.segment, result.entries, result.err, rootPath)
		if err != nil {
			return merr.Wrap(err, "recover segment indexes from manifests")
		}
		if len(indexes) == 0 && !removed {
			emptyMarkers = append(emptyMarkers, clearEmptyManifestIndexMarker(result.request.segment.GetID(), result.request.segment.GetManifestPath()))
			if len(emptyMarkers) == reader.limit {
				if err := flushMarkers(emptyMarkers); err != nil {
					return merr.Wrap(err, "persist empty manifest index markers")
				}
				emptyMarkers = nil
			}
		}
		for _, segIdx := range indexes {
			if _, ok := m.indexMeta.segmentBuildInfo.Get(segIdx.BuildID); ok {
				continue
			}
			m.indexMeta.updateSegmentIndex(segIdx)
			m.indexMeta.addStoredIndexSizeMetric(segIdx.CollectionID, segIdx.IndexID, float64(segIdx.IndexSerializedSize))
			installed++
		}
	}
	if err := flushMarkers(emptyMarkers); err != nil {
		return merr.Wrap(err, "persist empty manifest index markers")
	}

	mlog.Info(ctx, "recovered segment indexes from manifests",
		mlog.Int("manifestsRead", len(segments)), mlog.Int("recoveredIndexes", installed),
		mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

// recoverManifestIndexes validates a completed read. Missing dropped manifests
// retain their catalog row for GC; any other unreadable manifest fails startup.
func (m *meta) recoverManifestIndexes(ctx context.Context, segment *SegmentInfo, entries []packed.ManifestIndexInfo, err error, rootPath string) ([]*model.SegmentIndex, bool, error) {
	if err != nil {
		if segment.GetState() == commonpb.SegmentState_Dropped && m.chunkManager != nil {
			manifestFile, pathErr := packed.ManifestFilePath(segment.GetManifestPath())
			if pathErr != nil {
				return nil, false, merr.Wrap(pathErr, "resolve dropped segment manifest during recovery")
			}
			exists, existErr := m.chunkManager.Exist(ctx, manifestFile)
			if existErr != nil {
				return nil, false, merr.Wrap(existErr, "check dropped segment manifest during recovery")
			}
			if !exists {
				return nil, true, nil
			}
		}
		return nil, false, retry.Unrecoverable(merr.Wrapf(err, "recover segment %d indexes from manifest %s", segment.GetID(), segment.GetManifestPath()))
	}
	indexes := make([]*model.SegmentIndex, 0, len(entries))
	for _, entry := range entries {
		if _, ok := manifestIndexFilePathInfoForSegment(rootPath, segment.SegmentInfo, entry); !ok {
			return nil, false, merr.Wrapf(merr.ErrDataIntegrity,
				"segment %d manifest holds an unusable index entry: indexID %d buildID %d", segment.GetID(), entry.IndexID, entry.BuildID)
		}
		indexes = append(indexes, segmentIndexFromManifest(segment, entry))
	}
	return indexes, false, nil
}

// segmentIndexFromManifest projects a manifest index entry back into the
// SegmentIndex record the catalog would have held.
//
// The entry carries every field that describes a finished artifact. What it
// cannot carry is the build task's own history - the assigned node, the
// failure reason, and the create/finish timestamps - because a manifest
// records the artifact, not the build that produced it. Those are left zero:
// the state is Finished by construction, so the only consumer of the
// timestamps is the human-readable projection in DescribeIndex.
func segmentIndexFromManifest(segment *SegmentInfo, manifestIndex packed.ManifestIndexInfo) *model.SegmentIndex {
	return &model.SegmentIndex{
		SegmentID:                 segment.GetID(),
		CollectionID:              segment.GetCollectionID(),
		PartitionID:               segment.GetPartitionID(),
		NumRows:                   manifestIndex.NumRows,
		IndexID:                   manifestIndex.IndexID,
		BuildID:                   manifestIndex.BuildID,
		IndexVersion:              manifestIndex.IndexVersion,
		IndexState:                commonpb.IndexState_Finished,
		IndexFileKeys:             manifestIndex.IndexFileKeys,
		IndexSerializedSize:       uint64(manifestIndex.SerializedSize),
		IndexMemSize:              uint64(manifestIndex.MemSize),
		CurrentIndexVersion:       manifestIndex.CurrentIndexVersion,
		CurrentScalarIndexVersion: manifestIndex.CurrentScalarIndexVersion,
		IndexType:                 manifestIndex.IndexType,
		IndexStorePathVersion:     manifestIndex.IndexStorePathVersion,
	}
}
