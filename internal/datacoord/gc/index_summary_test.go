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
	"os"
	"path"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSummaryGCKeepsPublishedArtifactAcrossRestart(t *testing.T) {
	ctx := t.Context()
	params := paramtable.Get()
	key := params.DataCoordCfg.SegmentIndexSummariesEnabled.Key
	previous := params.DataCoordCfg.SegmentIndexSummariesEnabled.GetValue()
	params.Save(key, "true")
	t.Cleanup(func() { params.Save(key, previous) })

	root := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	cfgMock := mockey.Mock(meta.CreateStorageConfig).Return(cfg).Build()
	defer cfgMock.UnPatch()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), root, "")
	boot := func() (*meta.Meta, *index.IndexMeta) {
		dm := &index.IndexTestMetas{}
		store := meta.NewMetaStoreWithCollections(catalog, 1)
		store.PutCollection(&meta.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "indexed_field"}},
		}})
		mt, err := meta.NewMeta(ctx, catalog, cm, store, broker.NewMockBroker(t), index.LoadIndexTestMetas(catalog, dm))
		require.NoError(t, err)
		t.Cleanup(mt.CloseManifestCommitExecutor)
		return mt, dm.IndexMeta
	}
	mt, im := boot()
	require.True(t, im.UsesSegmentSummaries())
	require.NoError(t, im.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 4, FieldID: 100, IndexName: "idx"}))
	base := path.Join(root, "insert_log/1/2/3")
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath(base, packed.ManifestEarliest), NumOfRows: 10,
	})))
	version := indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	task := &model.SegmentIndex{
		CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, IndexVersion: 1, NumRows: 10,
		IndexState: commonpb.IndexState_InProgress, IndexStorePathVersion: version, IndexType: "FLAT",
	}
	require.NoError(t, im.AddSegmentIndex(ctx, task))
	const tolerance = time.Minute
	live := path.Join(base, "_index/5/1/index.bin")
	orphan := path.Join(base, "_index/6/1/orphan.bin")
	for _, file := range []string{live, orphan} {
		require.NoError(t, cm.Write(ctx, file, []byte("artifact")))
		old := time.Now().Add(-2 * tolerance)
		require.NoError(t, os.Chtimes(file, old, old))
	}
	require.NoError(t, index.NewIndexBuildTask(task, 1, mt, im, nil, nil, nil).SetJobInfo(&workerpb.IndexTaskInfo{
		BuildID: 5, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"index.bin"},
		SerializedSize: 64, MemSize: 256, IndexStorePathVersion: version,
	}))
	require.False(t, im.HasBuildInfo(5))
	pinned := mt.GetSegment(ctx, 3).GetManifestPath()

	gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcOption{IndexMeta: im, Cli: cm, MissingTolerance: tolerance})
	gc.RecycleUnusedBinlogFiles(ctx)
	require.FileExists(t, live)
	require.NoFileExists(t, orphan)
	mt.CloseManifestCommitExecutor()
	recovered, recoveredIndex := boot()
	require.False(t, recoveredIndex.HasBuildInfo(5))
	segment := recovered.GetSegment(ctx, 3)
	require.Equal(t, pinned, segment.GetManifestPath())
	indexes, err := recovered.ReadCompletedIndexes(ctx, segment.SegmentInfo)
	require.NoError(t, err)
	require.Contains(t, indexes, int64(4))
	require.Equal(t, []string{"index.bin"}, indexes[4].IndexFileKeys)
	require.EqualValues(t, 64, indexes[4].IndexSerializedSize)
	require.EqualValues(t, 256, indexes[4].IndexMemSize)
	data, err := cm.Read(ctx, live)
	require.NoError(t, err)
	require.Equal(t, "artifact", string(data))
}

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
	entry := packed.ManifestIndexInfo{
		IndexID: 4, BuildID: 5, IndexVersion: 1, IndexName: "idx", IndexType: "HNSW",
		Path: path.Join(base, "_index/5/1"), IndexFileKeys: []string{"index"}, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
	}
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
	old := &datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3, State: commonpb.SegmentState_Flushed,
		ManifestPath: packed.MarshalManifestPath(base, 1),
	}
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
	entry := packed.ManifestIndexInfo{
		IndexID: 4, BuildID: 5, IndexVersion: 1, IndexName: "idx", IndexType: "HNSW",
		Path: path.Join(base, "_index/5/1"), IndexFileKeys: []string{"index"}, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
	}
	read := mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(_ context.Context, _ *packed.ManifestIOContext, manifest string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
		require.Equal(t, published.GetManifestPath(), manifest)
		return []packed.ManifestIndexInfo{entry}, nil
	}).Build()
	defer read.UnPatch()
	gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{Cli: cm}))
	require.False(t, gc.newIndexFileReclaimChecker(t.Context())(path.Join(entry.Path, "index"), 1, 3, 5))
}

func TestSummaryGCLoadsFileDetailsExplicitly(t *testing.T) {
	for _, scenario := range []string{"read failure", "missing entry", "remove failure", "success", "empty completion"} {
		t.Run(scenario, func(t *testing.T) {
			mt, dm, err := newMemoryMetaWithSnapshot(t)
			require.NoError(t, err)
			root := t.TempDir()
			hasArtifact := scenario != "empty completion"
			base := path.Join(root, "insert_log/1/2/3")
			require.NoError(t, mt.AddSegment(t.Context(), meta.NewSegmentInfo(&datapb.SegmentInfo{
				ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed,
				StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath(base, 1), ManifestHasIndex: hasArtifact,
				IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 4, BuildId: 5, HasArtifact: hasArtifact, IndexVersion: 1}},
			})))
			require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
			gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcTestDomainOption(dm, GcOption{
				Cli: storage.NewLocalChunkManager(objectstorage.RootPath(root)),
			}))
			entry := packed.ManifestIndexInfo{
				IndexID: 4, BuildID: 5, IndexVersion: 1, IndexName: "idx", IndexType: "HNSW",
				Path: path.Join(base, "_index/5/1"), IndexFileKeys: []string{"index"},
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
			}
			reads := 0
			read := mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(_ context.Context, _ *packed.ManifestIOContext, manifest string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				reads++
				require.Equal(t, packed.MarshalManifestPath(base, 1), manifest)
				if scenario == "read failure" {
					return nil, merr.ErrServiceUnavailable
				}
				if scenario == "missing entry" {
					return nil, nil
				}
				return []packed.ManifestIndexInfo{entry}, nil
			}).Build()
			defer read.UnPatch()
			removed := 0
			remove := mockey.Mock((*GarbageCollector).RemoveObjectFiles).To(func(_ *GarbageCollector, _ context.Context, files map[string]struct{}) error {
				removed++
				if hasArtifact {
					require.Equal(t, map[string]struct{}{path.Join(entry.Path, "index"): {}}, files)
				} else {
					require.Empty(t, files)
				}
				if scenario == "remove failure" {
					return merr.ErrServiceUnavailable
				}
				return nil
			}).Build()
			defer remove.UnPatch()
			commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).Return(packed.ManifestUpdateResult{
				ManifestPath: packed.MarshalManifestPath(base, 2), HasIndexes: proto.Bool(false),
			}, nil).Build()
			defer commit.UnPatch()
			meta.MockManifestUpdateSubmissions(t)
			gc.RecycleUnusedSegIndexes(t.Context(), nil)
			if hasArtifact {
				require.Equal(t, 1, reads)
			} else {
				require.Zero(t, reads, "empty completion has no artifact details to load")
			}
			if scenario == "read failure" || scenario == "missing entry" {
				require.Zero(t, removed)
			} else {
				require.Equal(t, 1, removed)
			}
			if scenario == "success" || scenario == "empty completion" {
				require.Empty(t, dm.IndexMeta.GetAllSegmentIndexSummaries(3))
			} else {
				require.Len(t, dm.IndexMeta.GetAllSegmentIndexSummaries(3), 1, "unresolved or undeleted files must retain inventory for retry")
			}
			require.False(t, dm.IndexMeta.HasBuildInfo(5))
		})
	}
}
