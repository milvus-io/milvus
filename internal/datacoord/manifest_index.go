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
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
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

// Every read blocks a native thread. Bound it by both the configured scan
// concurrency and the storage connection budget.
func segmentIndexManifestReadConcurrency() int {
	params := paramtable.Get()
	limit := params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.GetAsInt()
	if connections := params.MinioCfg.MaxConnections.GetAsInt(); connections > 0 {
		limit = min(limit, connections)
	}
	return max(1, limit)
}

// readManifestIndexes shares a native-thread budget across startup, restore and GC.
// Waiting for admission is cancellable and does not enter cgo.
func (m *meta) readManifestIndexes(ctx context.Context, manifestPath string, config *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
	m.manifestReadOnce.Do(func() { m.manifestReadSlots = semaphore.NewWeighted(int64(segmentIndexManifestReadConcurrency())) })
	if err := m.manifestReadSlots.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer m.manifestReadSlots.Release(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return packed.GetManifestIndexInfos(manifestPath, config)
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
	pool := conc.NewPool[any](concurrency)
	defer pool.Release()
	installed := 0
	// Retain futures and result slices only for the active batch. Completed
	// records are installed before proceeding to the next batch; newMeta does
	// not expose this metadata until the entire scan succeeds.
	for start := 0; start < len(segments); start += concurrency {
		batch := segments[start:min(start+concurrency, len(segments))]
		recovered := make([][]*model.SegmentIndex, len(batch))
		removedManifests := make([]bool, len(batch))
		futures := make([]*conc.Future[any], 0, len(batch))
		for i, segment := range batch {
			i, segment := i, segment
			futures = append(futures, pool.Submit(func() (any, error) {
				// Retry only this segment. newMeta prevents a failed scan
				// from being replayed by initMeta's metastore retry loop.
				var entries []packed.ManifestIndexInfo
				err := retry.Do(ctx, func() error {
					var readErr error
					entries, readErr = m.readManifestIndexes(ctx, segment.GetManifestPath(), storageConfig)
					return readErr
				}, retry.Attempts(3), retry.Sleep(200*time.Millisecond))
				if err != nil {
					// GC removes files before the catalog row. A restart in that
					// window must let the remaining Dropped row finish GC, but a
					// read error alone cannot prove the manifest was removed.
					if segment.GetState() == commonpb.SegmentState_Dropped && m.chunkManager != nil {
						manifestFile, pathErr := packed.ManifestFilePath(segment.GetManifestPath())
						if pathErr != nil {
							return nil, merr.Wrap(pathErr, "resolve dropped segment manifest during recovery")
						}
						exists, existErr := m.chunkManager.Exist(ctx, manifestFile)
						if existErr != nil {
							return nil, merr.Wrap(existErr, "check dropped segment manifest during recovery")
						}
						if !exists {
							removedManifests[i] = true
							mlog.Info(ctx, "dropped segment manifest already removed before recovery",
								mlog.FieldSegmentID(segment.GetID()))
							return nil, nil
						}
					}
					return nil, merr.Wrapf(err, "recover segment %d indexes from manifest %s",
						segment.GetID(), segment.GetManifestPath())
				}
				for _, entry := range entries {
					if _, ok := manifestIndexFilePathInfoForSegment(rootPath, segment.SegmentInfo, entry); !ok {
						return nil, merr.Wrapf(merr.ErrDataIntegrity,
							"segment %d manifest holds an unusable index entry: indexID %d buildID %d",
							segment.GetID(), entry.IndexID, entry.BuildID)
					}
					recovered[i] = append(recovered[i], segmentIndexFromManifest(segment, entry))
				}
				return nil, nil
			}))
		}
		// Drain all in-flight reads before returning or releasing the pool.
		if err := conc.BlockOnAll(futures...); err != nil {
			return merr.Wrap(err, "recover segment indexes from manifests")
		}
		// Normalize historical sticky markers once. Persist before exposing meta;
		// a catalog failure leaves the conservative true marker for the next start.
		var emptyMarkers []UpdateOperator
		for i, indexes := range recovered {
			if len(indexes) == 0 && !removedManifests[i] {
				emptyMarkers = append(emptyMarkers, clearEmptyManifestIndexMarker(batch[i].GetID(), batch[i].GetManifestPath()))
			}
		}
		if len(emptyMarkers) > 0 {
			if err := m.UpdateSegmentsInfo(ctx, emptyMarkers...); err != nil {
				return merr.Wrap(err, "persist empty manifest index markers")
			}
		}
		for _, indexes := range recovered {
			for _, segIdx := range indexes {
				if _, ok := m.indexMeta.segmentBuildInfo.Get(segIdx.BuildID); ok {
					continue
				}
				// Definitions may have been dropped: keep their records so GC
				// can still find and retire the manifest artifacts.
				m.indexMeta.updateSegmentIndex(segIdx)
				m.indexMeta.addStoredIndexSizeMetric(segIdx.CollectionID, segIdx.IndexID,
					float64(segIdx.IndexSerializedSize))
				installed++
			}
		}
	}
	mlog.Info(ctx, "recovered segment indexes from manifests",
		mlog.Int("manifestsRead", len(segments)), mlog.Int("recoveredIndexes", installed),
		mlog.Duration("duration", record.ElapseSpan()))
	return nil
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
