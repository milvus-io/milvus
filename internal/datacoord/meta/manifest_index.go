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

package meta

import (
	"context"
	"path"
	"strings"

	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// buildManifestIndexInfo assembles the manifest entry for a completed index
// build entirely from DataCoord metadata: the segment's manifest base path,
// the collection's index definition, and the task record the worker result was
// already projected onto. Nothing here needs the worker to have touched the
// manifest, which is what keeps manifest publication a DataCoord-only step.
func BuildManifestIndexInfo(m *Meta, segment *SegmentInfo, segIdx *model.SegmentIndex) (packed.ManifestIndexInfo, error) {
	basePath, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return packed.ManifestIndexInfo{}, merr.Wrap(err, "parse segment manifest path for index publication")
	}
	if basePath == "" {
		return packed.ManifestIndexInfo{}, merr.WrapErrServiceInternalMsg(
			"segment %d manifest path has an empty base path", segment.GetID())
	}

	indexPrefix := metautil.NewIndexPathBuilder(
		m.ChunkManager.RootPath(),
		segIdx.IndexStorePathVersion,
		segIdx.CollectionID,
		segIdx.PartitionID,
		segIdx.SegmentID,
		segIdx.BuildID,
		segIdx.IndexVersion,
	).WithManifestPath(segment.GetManifestPath()).BuildPrefix()
	relativePath, err := packed.ManifestIndexRelativePath(basePath, indexPrefix)
	if err != nil {
		return packed.ManifestIndexInfo{}, err
	}

	indexParams := m.IndexCatalog.GetIndexParams(segIdx.CollectionID, segIdx.IndexID)
	indexType := GetIndexType(indexParams)
	// A per-segment override wins: DataCoord may downgrade the index type for
	// one segment (e.g. to a flat index for a tiny segment).
	if segIdx.IndexType != "" {
		indexType = segIdx.IndexType
	}
	// Collection parameters describe the current definition, not this build.
	// Leave Properties empty rather than persisting them as artifact metadata.

	fieldID := m.IndexCatalog.GetFieldIDByIndexID(segIdx.CollectionID, segIdx.IndexID)
	return packed.ManifestIndexInfo{
		ColumnName:                CollectionFieldName(m, segIdx.CollectionID, fieldID),
		IndexName:                 m.IndexCatalog.GetIndexNameByID(segIdx.CollectionID, segIdx.IndexID),
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
	}, nil
}

func CollectionFieldName(m *Meta, collectionID, fieldID int64) string {
	collection := m.GetCollection(collectionID)
	if collection == nil || collection.Schema == nil {
		return ""
	}
	for _, field := range typeutil.GetAllFieldSchemas(collection.Schema) {
		if field.GetFieldID() == fieldID {
			return field.GetName()
		}
	}
	return ""
}

// segmentIndexFilePaths resolves stored index file keys for load and snapshot
// consumers. A v2 build needs its owning segment's actual manifest base: a
// missing row or invalid pointer must not silently select a different namespace.
func SegmentIndexFilePaths(rootPath string, segIdx *model.SegmentIndex, segment *datapb.SegmentInfo) ([]string, error) {
	builder := metautil.NewIndexPathBuilder(rootPath, segIdx.IndexStorePathVersion,
		segIdx.CollectionID, segIdx.PartitionID, segIdx.SegmentID, segIdx.BuildID, segIdx.IndexVersion)
	if metautil.IsSegmentRooted(segIdx.IndexStorePathVersion) && len(segIdx.IndexFileKeys) > 0 {
		if segment == nil {
			return nil, merr.WrapErrSegmentNotFound(segIdx.SegmentID, "cannot resolve segment-rooted index files")
		}
		base, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
		if err != nil {
			return nil, merr.WrapErrDataIntegrity(err, "invalid manifest path for segment %d index files", segIdx.SegmentID)
		}
		if base == "" {
			return nil, merr.WrapErrDataIntegrityMsg("empty manifest base for segment %d index files", segIdx.SegmentID)
		}
		builder.WithManifestPath(segment.GetManifestPath())
	}
	return builder.BuildFilePaths(segIdx.IndexFileKeys), nil
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
		if metautil.IsSegmentRooted(manifestIndex.IndexStorePathVersion) {
			filePaths = append(filePaths, strings.TrimRight(manifestIndex.Path, "/")+"/"+fileKey)
		} else {
			filePaths = append(filePaths, path.Join(manifestIndex.Path, fileKey))
		}
	}

	return &indexpb.IndexFilePathInfo{
		SegmentID:                 segmentID,
		FieldID:                   manifestIndex.FieldID,
		IndexID:                   manifestIndex.IndexID,
		BuildID:                   manifestIndex.BuildID,
		IndexName:                 manifestIndex.IndexName,
		IndexType:                 manifestIndex.IndexType,
		IndexParams:               []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: manifestIndex.IndexType}},
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
func ManifestIndexFilePathInfoForSegment(rootPath string, segment *datapb.SegmentInfo,
	entry packed.ManifestIndexInfo,
) (*indexpb.IndexFilePathInfo, bool) {
	expected := metautil.NewIndexPathBuilder(rootPath, entry.IndexStorePathVersion,
		segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(),
		entry.BuildID, entry.IndexVersion).WithManifestPath(segment.GetManifestPath()).BuildPrefix()
	if metautil.IsSegmentRooted(entry.IndexStorePathVersion) {
		if entry.Path != expected {
			return nil, false
		}
	} else if path.Clean(entry.Path) != expected {
		return nil, false
	}
	return manifestIndexFilePathInfo(segment.GetID(), entry)
}

// Bound outstanding reads by both the configured scan concurrency and the
// storage connection budget.
func SegmentIndexManifestReadConcurrency() int {
	params := paramtable.Get()
	limit := params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.GetAsInt()
	if connections := params.MinioCfg.MaxConnections.GetAsInt(); connections > 0 {
		limit = min(limit, connections)
	}
	return max(1, limit)
}

// readManifestIndexesWithIO shares a read budget across startup, restore and GC.
// Waiting for admission is cancellable and does not enter cgo.
func (m *Meta) ReadManifestIndexesWithIO(ctx context.Context, io *packed.ManifestIOContext, manifestPath string, config *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
	m.manifestReadOnce.Do(func() { m.manifestReadSlots = semaphore.NewWeighted(int64(SegmentIndexManifestReadConcurrency())) })
	if err := m.manifestReadSlots.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer m.manifestReadSlots.Release(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Per-request readers allocate their native context only after admission.
	if io == nil {
		io = packed.NewManifestIOContext(1)
		defer io.Close()
	}
	return packed.GetManifestIndexInfosAsync(ctx, io, manifestPath, config)
}

// submitManifestIndexRead shares the read budget with runtime reads. Completion
// releases admission on the executor, so a submitting coordinator never needs
// another goroutine to consume completions just to unblock admission.
func (m *Meta) SubmitManifestIndexRead(ctx context.Context, io *packed.ManifestIOContext, manifestPath string, config *indexpb.StorageConfig, complete func([]packed.ManifestIndexInfo, error)) error {
	m.manifestReadOnce.Do(func() { m.manifestReadSlots = semaphore.NewWeighted(int64(SegmentIndexManifestReadConcurrency())) })
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
func ValidateManifestIndexPublishable(segmentID int64, manifestIndex packed.ManifestIndexInfo) error {
	if _, ok := manifestIndexFilePathInfo(segmentID, manifestIndex); !ok {
		return merr.WrapErrServiceInternalMsg(
			"refusing to publish unusable index entry for segment %d: indexID %d buildID %d indexName %q",
			segmentID, manifestIndex.IndexID, manifestIndex.BuildID, manifestIndex.IndexName)
	}
	return nil
}

func needsManifestIndexRecovery(segment *SegmentInfo) bool {
	return segment != nil && (IsSegmentHealthy(segment) || segment.GetState() == commonpb.SegmentState_Dropped) &&
		segment.GetStorageVersion() >= storage.StorageV3 && segment.GetManifestHasIndex() &&
		segment.GetLevel() != datapb.SegmentLevel_L0
}

// ReloadSegmentIndexesFromManifests synchronously verifies and installs index
// records for callers that explicitly require an entirely recovered inventory.
func (m *Meta) ReloadSegmentIndexesFromManifests(ctx context.Context) error {
	segments := m.SelectSegments(ctx, SegmentFilterFunc(needsManifestIndexRecovery))
	for _, segment := range segments {
		if segment.GetManifestPath() == "" {
			return merr.Wrapf(merr.ErrDataIntegrity,
				"segment %d is marked manifest_has_index but has no manifest path", segment.GetID())
		}
	}
	return m.readManifestIndexInventory(ctx, segments, func(result manifestIndexReadResult, rootPath string) error {
		return m.installRecoveredManifestIndexes(ctx, result, rootPath)
	})
}

// readManifestIndexInventory owns the shared read window and drains accepted
// reads even when its caller stops at the first recovery failure.
func (m *Meta) readManifestIndexInventory(ctx context.Context, segments []*SegmentInfo, consume func(manifestIndexReadResult, string) error) error {
	if len(segments) == 0 {
		return nil
	}
	config := CreateStorageConfig()
	rootPath := config.GetRootPath()
	if m.ChunkManager != nil {
		rootPath = m.ChunkManager.RootPath()
	}
	reader := m.newManifestIndexReader(ctx, segments, min(len(segments), SegmentIndexManifestReadConcurrency()), config)
	if m.IndexCatalog != nil {
		defer m.IndexCatalog.PublishStoredIndexSizes()
	}
	defer reader.Close()
	for {
		result, err := reader.next()
		if err != nil || result == nil {
			return err
		}
		if err := consume(*result, rootPath); err != nil {
			return err
		}
	}
}

// recoverManifestIndexes validates a completed read. Missing dropped manifests
// retain their catalog row for GC. Other unreadable manifests leave the
// segment pending so callers cannot treat its inventory as complete.
func (m *Meta) recoverManifestIndexes(ctx context.Context, segment *SegmentInfo, entries []packed.ManifestIndexInfo, err error, rootPath string) ([]*model.SegmentIndex, bool, error) {
	if err != nil {
		if segment.GetState() == commonpb.SegmentState_Dropped && m.ChunkManager != nil {
			manifestFile, pathErr := packed.ManifestFilePath(segment.GetManifestPath())
			if pathErr != nil {
				return nil, false, merr.Wrap(pathErr, "resolve dropped segment manifest during recovery")
			}
			exists, existErr := m.ChunkManager.Exist(ctx, manifestFile)
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
		if _, ok := ManifestIndexFilePathInfoForSegment(rootPath, segment.SegmentInfo, entry); !ok {
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
		ManifestPublished:         true,
	}
}
