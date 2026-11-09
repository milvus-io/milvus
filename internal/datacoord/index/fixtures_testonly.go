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

//go:build test

package index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func NewTestVersionManager() IndexEngineVersionManager {
	return &VersionManagerImpl{}
}

// indexTestMetas holds the domain metas that index-layer tests build together with a meta.
type IndexTestMetas struct {
	IndexMeta *IndexMeta
}

// loadIndexTestMetas returns a DomainMetaLoader that builds only the index meta into out and returns it as the index catalog.
func LoadIndexTestMetas(catalog metastore.DataCoordCatalog, out *IndexTestMetas) meta.DomainMetaLoader {
	return func(ctx context.Context, collectionIDs []int64) (meta.SegmentIndexCatalog, meta.CompactionProtection, error) {
		var err error
		out.IndexMeta, err = NewIndexMeta(ctx, catalog, collectionIDs)
		if err != nil {
			return nil, nil, err
		}
		return out.IndexMeta, nil, nil
	}
}

// withIndexTestMetas returns a factory of loaders that build only the index meta into out.
func withIndexTestMetas(out *IndexTestMetas) func(metastore.DataCoordCatalog, storage.ChunkManager) meta.DomainMetaLoader {
	return func(catalog metastore.DataCoordCatalog, _ storage.ChunkManager) meta.DomainMetaLoader {
		return LoadIndexTestMetas(catalog, out)
	}
}

// newMemoryMetaWithIndex builds a meta over an in-memory catalog together with an index meta over the same catalog.
func NewMemoryMetaWithIndex(t *testing.T) (*meta.Meta, *IndexTestMetas, error) {
	dm := &IndexTestMetas{}
	m, err := meta.NewMemoryMetaWithLoader(t, withIndexTestMetas(dm))
	return m, dm, err
}

// bootMetaForRestart runs the real newMeta against a caller-owned catalog, so
// two calls with the same catalog model a DataCoord restart.
func BootMetaForRestart(t *testing.T, catalog metastore.DataCoordCatalog, collectionID int64) (*meta.Meta, *IndexTestMetas) {
	t.Helper()
	dm := &IndexTestMetas{}
	m := meta.BootMetaWithLoader(t, catalog, collectionID, withIndexTestMetas(dm))
	return m, dm
}

func NewSegmentIndexMeta(catalog metastore.DataCoordCatalog) *IndexMeta {
	return &IndexMeta{
		KeyLock:          lock.NewKeyLock[UniqueID](),
		ctx:              context.Background(),
		Catalog:          catalog,
		Indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
		SegmentBuildInfo: NewSegmentIndexBuildInfo(),
		SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
}

const restartPartID = UniqueID(30)

// seedRestartFixture creates the segment, its index definition and its index
// task, then publishes the finished index exactly the way task_index.go's
// publishIndexToManifest does.
func SeedRestartFixture(t *testing.T, m *meta.Meta, im *IndexMeta) {
	t.Helper()
	SeedRestartTask(t, m, im)
	PublishRestartTask(t, m, im)
}

// seedRestartTask creates the segment, its index definition and its
// in-progress index task, without finishing or publishing it.
func SeedRestartTask(t *testing.T, m *meta.Meta, im *IndexMeta) {
	t.Helper()
	pathVersion := indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED
	if WriteSegmentIndexToManifest() {
		pathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	}
	ctx := context.TODO()
	basePath := "/tmp/test-restart/insert_log/300/30/8001"

	require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             meta.RestartSegID,
		CollectionID:   meta.RestartCollID,
		PartitionID:    restartPartID,
		State:          commonpb.SegmentState_Flushed,
		NumOfRows:      1000,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 1),
	})))
	require.NoError(t, im.CreateIndex(ctx, &model.Index{
		CollectionID: meta.RestartCollID,
		FieldID:      meta.RestartFieldID,
		IndexID:      meta.RestartIndexID,
		IndexName:    "vec_idx",
		TypeParams:   []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
	}))
	require.NoError(t, im.AddSegmentIndex(ctx, &model.SegmentIndex{
		CollectionID:          meta.RestartCollID,
		PartitionID:           restartPartID,
		SegmentID:             meta.RestartSegID,
		NumRows:               1000,
		IndexID:               meta.RestartIndexID,
		BuildID:               meta.RestartBuildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_InProgress,
		IndexStorePathVersion: pathVersion,
	}))
}

// publishRestartTask finishes the seeded task through the real index-task
// result entry point, including its write-to-manifest switch decision.
func PublishRestartTask(t *testing.T, m *meta.Meta, im *IndexMeta) {
	t.Helper()
	segIdx, ok := im.GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)

	result := &workerpb.IndexTaskInfo{
		BuildID:               meta.RestartBuildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0", "1"},
		SerializedSize:        4096,
		MemSize:               8192,
		IndexStorePathVersion: segIdx.IndexStorePathVersion,
	}
	it := NewIndexBuildTask(segIdx, 1, m, im, nil, nil, nil)
	require.NoError(t, it.SetJobInfo(result))
	require.Equal(t, commonpb.IndexState_Finished, it.IndexState)
}

// createTestIndexMeta creates an indexMeta with pre-registered index definitions for testing.
// If catalog is nil, a default mock catalog (CreateSegmentIndex returns nil) is used.
func CreateTestIndexMeta(t *testing.T, collectionID int64, indexes map[int64]*model.Index, catalog ...metastore.DataCoordCatalog) *IndexMeta {
	var cat metastore.DataCoordCatalog
	if len(catalog) > 0 && catalog[0] != nil {
		cat = catalog[0]
	} else {
		mockCat := catalogmocks.NewDataCoordCatalog(t)
		mockCat.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Maybe()
		cat = mockCat
	}

	im := &IndexMeta{
		ctx:              context.Background(),
		Catalog:          cat,
		KeyLock:          lock.NewKeyLock[UniqueID](),
		Indexes:          map[UniqueID]map[UniqueID]*model.Index{collectionID: indexes},
		SegmentBuildInfo: NewSegmentIndexBuildInfo(),
		SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	return im
}

func JSONArrayVersionManager(t *testing.T, version int32) IndexEngineVersionManager {
	t.Helper()
	manager := NewIndexEngineVersionManager()
	manager.AddNode(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{
		ServerID: 1,
		ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{
			CurrentIndexVersion: version,
			MaximumIndexVersion: common.MaximumScalarIndexEngineVersion,
		},
	}})
	require.Equal(t, version, manager.ResolveScalarIndexVersion())
	return manager
}

func ResetJSONVersionTargets(t *testing.T) {
	t.Helper()
	paramtable.Init()
	target := Params.DataCoordCfg.TargetScalarIndexVersion.GetValue()
	force := Params.DataCoordCfg.ForceRebuildScalarSegmentIndex.GetValue()
	require.NoError(t, Params.Save(Params.DataCoordCfg.TargetScalarIndexVersion.Key, "-1"))
	require.NoError(t, Params.Save(Params.DataCoordCfg.ForceRebuildScalarSegmentIndex.Key, "false"))
	t.Cleanup(func() {
		require.NoError(t, Params.Save(Params.DataCoordCfg.TargetScalarIndexVersion.Key, target))
		require.NoError(t, Params.Save(Params.DataCoordCfg.ForceRebuildScalarSegmentIndex.Key, force))
	})
}

func NewTestIndexMeta(collID, segID, indexID int64, indexType string, segIdx *model.SegmentIndex) *IndexMeta {
	im := NewSegmentIndexMeta(nil)
	im.Indexes[collID] = map[UniqueID]*model.Index{
		indexID: {
			CollectionID: collID,
			IndexID:      indexID,
			IndexName:    "test_idx",
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: indexType},
			},
		},
	}
	segIdxMap := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdxMap.Insert(indexID, segIdx)
	im.SegmentIndexes.Insert(segID, segIdxMap)
	return im
}

func (i *ManifestIndexBackfillInspector) RunOnceForTest(ctx context.Context) bool {
	return i.runOnce(ctx)
}

func (i *ManifestIndexBackfillInspector) BackfillIndexesForTest(ctx context.Context, segmentID int64, buildIDs ...int64) error {
	return i.backfillIndexes(ctx, segmentID, buildIDs...)
}

func (i *ManifestIndexBackfillInspector) ScanForTest(ctx context.Context) ([]segmentManifestIndexBackfill, int) {
	return i.scan(ctx)
}

func (i *ManifestIndexRollbackInspector) RunOnceForTest(ctx context.Context) bool {
	return i.runOnce(ctx)
}

const RestartPartID = restartPartID

func BootMigrationMeta(t *testing.T, catalog metastore.DataCoordCatalog, collectionID int64) *meta.Meta {
	m, _ := BootMetaForRestart(t, catalog, collectionID)
	return m
}

func NewMigrationBackfillInspector(ctx context.Context, m *meta.Meta) *ManifestIndexBackfillInspector {
	return NewManifestIndexBackfillInspector(ctx, m, m.IndexCatalog.(*IndexMeta))
}

func NewMigrationRollbackInspector(ctx context.Context, m *meta.Meta) *ManifestIndexRollbackInspector {
	return NewManifestIndexRollbackInspector(ctx, m, m.IndexCatalog.(*IndexMeta))
}

func (m *IndexMeta) AlterSegmentIndexesForTest(records []*model.SegmentIndex) error {
	return m.alterSegmentIndexes(records)
}

func (m *IndexMeta) ManifestIndexBackfillNotifyForTest() <-chan struct{} {
	return m.manifestIndexBackfillNotify
}

func (i *ManifestIndexRollbackInspector) ReadyForTest() bool {
	return i.ready
}

func newMigrationManifestReadIO(t *testing.T) *packed.ManifestIOContext {
	t.Helper()
	io := packed.NewManifestIOContext(1)
	t.Cleanup(io.Close)
	return io
}
