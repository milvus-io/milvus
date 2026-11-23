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

	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func ManifestIndexFilePathInfoForSegment(rootPath string, segment *datapb.SegmentInfo,
	entry packed.ManifestIndexInfo,
) (*indexpb.IndexFilePathInfo, bool) {
	return packed.ManifestIndexFilePathInfoForSegment(rootPath, segment.GetManifestPath(),
		segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), entry)
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
// manifestIndexFilePathInfo: an entry the fail-closed readers (GC and
// index load) would refuse can neither be served
// nor retired, so no writer may commit one. It applies the exact reader
// predicate rather than restating the field checks, so writer and readers
// cannot drift apart.
func ValidateManifestIndexPublishable(segmentID int64, manifestIndex packed.ManifestIndexInfo) error {
	if _, ok := packed.ManifestIndexFilePathInfo(segmentID, manifestIndex); !ok {
		return merr.WrapErrServiceInternalMsg(
			"refusing to publish unusable index entry for segment %d: indexID %d buildID %d indexName %q",
			segmentID, manifestIndex.IndexID, manifestIndex.BuildID, manifestIndex.IndexName)
	}
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
		ManifestPublished:         true,
	}
}
