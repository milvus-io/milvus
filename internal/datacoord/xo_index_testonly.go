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

package datacoord

import (
	"context"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func newMockVersionManager() IndexEngineVersionManager {
	return &versionManagerImpl{}
}

// indexTestMetas holds the domain metas that index-layer tests build together with a meta.
type indexTestMetas struct {
	indexMeta *indexMeta
}

// loadIndexTestMetas returns a DomainMetaLoader that builds only the index meta into out and returns it as the index catalog.
func loadIndexTestMetas(catalog metastore.DataCoordCatalog, out *indexTestMetas) DomainMetaLoader {
	return func(ctx context.Context, collectionIDs []int64) (SegmentIndexCatalog, CompactionProtection, error) {
		var err error
		out.indexMeta, err = newIndexMeta(ctx, catalog, collectionIDs)
		if err != nil {
			return nil, nil, err
		}
		return out.indexMeta, nil, nil
	}
}

// withIndexTestMetas returns a factory of loaders that build only the index meta into out.
func withIndexTestMetas(out *indexTestMetas) func(metastore.DataCoordCatalog, storage.ChunkManager) DomainMetaLoader {
	return func(catalog metastore.DataCoordCatalog, _ storage.ChunkManager) DomainMetaLoader {
		return loadIndexTestMetas(catalog, out)
	}
}

// newMemoryMetaWithIndex builds a meta over an in-memory catalog together with an index meta over the same catalog.
func newMemoryMetaWithIndex(t *testing.T) (*meta, *indexTestMetas, error) {
	dm := &indexTestMetas{}
	m, err := newMemoryMetaWithLoader(t, withIndexTestMetas(dm))
	return m, dm, err
}

// bootMetaForRestart runs the real newMeta against a caller-owned catalog, so
// two calls with the same catalog model a DataCoord restart.
func bootMetaForRestart(t *testing.T, catalog metastore.DataCoordCatalog, collectionID int64) (*meta, *indexTestMetas) {
	t.Helper()
	dm := &indexTestMetas{}
	m := bootMetaWithLoader(t, catalog, collectionID, withIndexTestMetas(dm))
	return m, dm
}

func newSegmentIndexMeta(catalog metastore.DataCoordCatalog) *indexMeta {
	return &indexMeta{
		keyLock:          lock.NewKeyLock[UniqueID](),
		ctx:              context.Background(),
		catalog:          catalog,
		indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
		segmentBuildInfo: newSegmentIndexBuildInfo(),
		segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
}

const restartPartID = UniqueID(30)

// seedRestartFixture creates the segment, its index definition and its index
// task, then publishes the finished index exactly the way task_index.go's
// publishIndexToManifest does.
func seedRestartFixture(t *testing.T, m *meta, im *indexMeta) {
	t.Helper()
	seedRestartTask(t, m, im)
	publishRestartTask(t, m, im)
}

// seedRestartTask creates the segment, its index definition and its
// in-progress index task, without finishing or publishing it.
func seedRestartTask(t *testing.T, m *meta, im *indexMeta) {
	t.Helper()
	ctx := context.TODO()
	basePath := "/tmp/test-restart/insert_log/300/30/8001"

	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:             restartSegID,
		CollectionID:   restartCollID,
		PartitionID:    restartPartID,
		State:          commonpb.SegmentState_Flushed,
		NumOfRows:      1000,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 1),
	})))
	require.NoError(t, im.CreateIndex(ctx, &model.Index{
		CollectionID: restartCollID,
		FieldID:      restartFieldID,
		IndexID:      restartIndexID,
		IndexName:    "vec_idx",
		TypeParams:   []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
	}))
	require.NoError(t, im.AddSegmentIndex(ctx, &model.SegmentIndex{
		CollectionID:          restartCollID,
		PartitionID:           restartPartID,
		SegmentID:             restartSegID,
		NumRows:               1000,
		IndexID:               restartIndexID,
		BuildID:               restartBuildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_InProgress,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))
}

// publishRestartTask finishes the seeded task through the real index-task
// result entry point, including its write-to-manifest switch decision.
func publishRestartTask(t *testing.T, m *meta, im *indexMeta) {
	t.Helper()

	result := &workerpb.IndexTaskInfo{
		BuildID:               restartBuildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0", "1"},
		SerializedSize:        4096,
		MemSize:               8192,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	segIdx, ok := im.GetIndexJob(restartBuildID)
	require.True(t, ok)
	it := newIndexBuildTask(segIdx, 1, m, im, nil, nil, nil)
	require.NoError(t, it.setJobInfo(result))
}
