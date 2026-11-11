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

package gc

import (
	"context"
	"path"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSummaryGCProtectsManifestWithoutResidentBuild(t *testing.T) {
	ctx := context.Background()
	mt, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	base := path.Join(root, "insert_log/1/2/3")
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath(base, 1), ManifestHasIndex: true,
		IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 4, BuildId: 5, HasArtifact: true, IndexVersion: 1}},
	})))
	require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
	gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm}))
	entry := packed.ManifestIndexInfo{IndexID: 4, BuildID: 5, IndexVersion: 1, IndexName: "idx", IndexType: "HNSW",
		Path: path.Join(base, "_index/5/1"), IndexFileKeys: []string{"index"}, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED}
	read := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{entry}, nil).Build()
	defer read.UnPatch()
	reclaim := gc.newIndexFileReclaimChecker(ctx)
	require.False(t, reclaim(path.Join(entry.Path, "index"), 1, 3, 5))
	require.True(t, reclaim(path.Join(entry.Path, "obsolete"), 1, 3, 5))
	require.True(t, reclaim(path.Join(base, "_index/6/1/orphan"), 1, 3, 6))
	require.False(t, dm.IndexMeta.HasBuildInfo(5))
	require.True(t, reclaim(path.Join(root, "insert_log/1/2/99/_index/100/1/orphan"), 1, 99, 100))
	read.UnPatch()
	read = mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, merr.ErrServiceUnavailable).Build()
	defer read.UnPatch()
	reclaim = gc.newIndexFileReclaimChecker(ctx)
	require.False(t, reclaim(path.Join(entry.Path, "index"), 1, 3, 5))
	require.False(t, reclaim(path.Join(base, "_index/6/1/orphan"), 1, 3, 6))
}

func TestSummaryGCRefreshesSegmentAfterTaskRetirement(t *testing.T) {
	mt, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	base := path.Join(root, "insert_log/1/2/3")
	old := &datapb.SegmentInfo{ID: 3, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3, State: commonpb.SegmentState_Flushed,
		ManifestPath: packed.MarshalManifestPath(base, 1)}
	require.NoError(t, mt.AddSegment(t.Context(), meta.NewSegmentInfo(old)))
	require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
	published := proto.Clone(old).(*datapb.SegmentInfo)
	published.ManifestPath = packed.MarshalManifestPath(base, 2)
	published.ManifestHasIndex = true
	published.IndexSummaries = []*datapb.SegmentIndexSummary{{IndexId: 4, BuildId: 5, IndexVersion: 1, HasArtifact: true}}
	lookup := mockey.Mock((*index.IndexMeta).CheckCleanSegmentIndex).To(func(_ *index.IndexMeta, _ int64) (bool, *model.SegmentIndex) {
		mt.MetaStore.PutSegment(published)
		return true, nil
	}).Build()
	defer lookup.UnPatch()
	entry := packed.ManifestIndexInfo{IndexID: 4, BuildID: 5, IndexVersion: 1, IndexName: "idx", IndexType: "HNSW",
		Path: path.Join(base, "_index/5/1"), IndexFileKeys: []string{"index"}, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED}
	read := mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(_ context.Context, _ *packed.ManifestIOContext, manifest string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
		require.Equal(t, published.GetManifestPath(), manifest)
		return []packed.ManifestIndexInfo{entry}, nil
	}).Build()
	defer read.UnPatch()
	gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm}))
	require.False(t, gc.newIndexFileReclaimChecker(t.Context())(path.Join(entry.Path, "index"), 1, 3, 5))
}
