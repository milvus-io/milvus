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
	"fmt"
	"path"
	"sort"
	"time"

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
// healthy StorageV3 manifests marked manifest_has_index. It runs independently
// of the current write-mode switch: records published while the switch was on
// must remain visible after it is turned off. Existing etcd rows win on buildID
// conflicts, so active, failed, fake-finished, and record-resident results stay
// authoritative.
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
	scanned := 0
	segments := m.SelectSegments(ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		if !isSegmentHealthy(segment) || segment.GetStorageVersion() < storage.StorageV3 {
			return false
		}
		if !segment.GetManifestHasIndex() {
			// False proves no index entry has ever been published for this
			// segment. This keeps an all-etcd cluster free of manifest GETs.
			return false
		}
		if segment.GetLevel() == datapb.SegmentLevel_L0 {
			// L0 carries deltalogs only; it never has an index entry, so
			// reading its manifest is pure cost and pure failure surface.
			return false
		}
		scanned++
		if segment.GetManifestPath() == "" {
			// Load-bearing invariant: a visible V3 segment always carries a
			// manifest pointer, set at registration and never converted in
			// place. If it is ever violated the segment silently routes to the
			// etcd path and bypasses the switch, so say so loudly rather than
			// letting it look like an unindexed segment.
			mlog.Error(ctx, "invariant violated: visible StorageV3 segment has no manifest path",
				mlog.Int64("segmentID", segment.GetID()),
				mlog.Int64("collectionID", segment.GetCollectionID()),
				mlog.Int64("storageVersion", segment.GetStorageVersion()))
			return false
		}
		return true
	}))
	if len(segments) == 0 {
		mlog.Info(ctx, "no segment manifests to recover indexes from", mlog.Int("scannedV3Segments", scanned))
		return nil
	}

	storageConfig := createStorageConfig()
	// Deliberately NOT metastore.readConcurrency. That knob is shared with the
	// querycoord and rootcoord catalogs and defaults to 32, which is a sane
	// etcd fan-out and a badly wrong one here: every task below is an
	// object-storage GET behind a cgo call, and this loop runs once per healthy
	// V3 segment. At 32 in flight a million-segment cluster serializes its boot
	// into hours, and because the scan is fail-closed inside newMeta that time
	// is downtime, not background warmup.
	pool := conc.NewPool[any](paramtable.Get().DataCoordCfg.SegmentIndexManifestLoadConcurrency.GetAsInt())
	defer pool.Release()

	recovered := make([][]*model.SegmentIndex, len(segments))
	futures := make([]*conc.Future[any], 0, len(segments))
	for i, segment := range segments {
		i, segment := i, segment
		futures = append(futures, pool.Submit(func() (any, error) {
			// Absorb a transient object-store error here rather than letting it
			// abort startup: a failure propagates to newMeta, and initMeta
			// replays the whole scan up to connMetaMaxRetryTime times, so one
			// unretried throttle would re-read every segment's manifest again.
			var manifestIndexes []packed.ManifestIndexInfo
			err := retry.Do(ctx, func() error {
				var readErr error
				manifestIndexes, readErr = packed.GetManifestIndexInfos(segment.GetManifestPath(), storageConfig)
				return readErr
			}, retry.Attempts(3), retry.Sleep(200*time.Millisecond))
			if err != nil {
				mlog.Error(ctx, "failed to read segment manifest index metadata during reload",
					mlog.Int64("segmentID", segment.GetID()),
					mlog.String("manifestPath", segment.GetManifestPath()),
					mlog.Err(err))
				return nil, merr.Wrap(err, fmt.Sprintf("recover segment %d indexes from manifest", segment.GetID()))
			}
			for _, manifestIndex := range manifestIndexes {
				// Validate with the SAME predicate every other manifest
				// consumer applies - GC's retraction resolve, dropped-segment
				// GC and snapshot copy all route entries through
				// manifestIndexFilePathInfo. The reload is the one path that
				// promotes a manifest entry into a durable-looking
				// SegmentIndex record, and that record's IndexFileKeys reach
				// removeObjectFiles through BuildFilePath, whose path.Join
				// normalizes "..": an entry we did not write could otherwise
				// aim a delete outside its own buildID prefix.
				//
				// Rejecting at boot rather than skipping is the same trade the
				// unreadable-manifest case makes above, and it also surfaces an
				// entry that resolveManifestIndexRetraction would reject on
				// every GC cycle forever while only logging a warning.
				if _, ok := manifestIndexFilePathInfo(segment.GetID(), manifestIndex); !ok {
					return nil, merr.WrapErrServiceInternalMsg(
						"segment %d manifest holds an unusable index entry: indexID %d buildID %d",
						segment.GetID(), manifestIndex.IndexID, manifestIndex.BuildID)
				}
				recovered[i] = append(recovered[i], segmentIndexFromManifest(segment, manifestIndex))
			}
			return nil, nil
		}))
	}
	// BlockOnAll, not AwaitAll: AwaitAll returns at the first failing future
	// and leaves the rest in flight, so the deferred pool.Release would close
	// the pool under running tasks and initMeta's retry loop - up to
	// connMetaMaxRetryTime attempts - would stack a fresh set of orphaned
	// object-store reads on every attempt.
	if err := conc.BlockOnAll(futures...); err != nil {
		return merr.Wrap(err, "recover segment indexes from manifests")
	}

	installed := 0
	for _, segIdxes := range recovered {
		for _, segIdx := range segIdxes {
			if _, ok := m.indexMeta.segmentBuildInfo.Get(segIdx.BuildID); ok {
				continue
			}
			// Entries for indexes whose definition is already gone are
			// installed too, on purpose. The record is the ONLY thing that
			// re-drives GC's delete-then-retract for that artifact, and
			// consumers gate on the index definition (IsIndexExist), so a
			// record for a dropped index is inert to readers and load-bearing
			// to GC.
			m.indexMeta.updateSegmentIndex(segIdx)
			m.indexMeta.addStoredIndexSizeMetric(segIdx.CollectionID, segIdx.IndexID,
				float64(segIdx.IndexSerializedSize))
			installed++
		}
	}

	// scanned vs candidates vs recovered is the only signal that meta came up
	// complete; without it a partial reload is indistinguishable from a
	// cluster that simply has no indexes.
	mlog.Info(ctx, "recovered segment indexes from manifests",
		mlog.Int("scannedV3Segments", scanned),
		mlog.Int("manifestsRead", len(segments)),
		mlog.Int("recoveredIndexes", installed),
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
