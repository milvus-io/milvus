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
	"github.com/milvus-io/milvus/internal/metastore"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSummaryGCKeepsPublishedArtifactAcrossRestart(t *testing.T) {
	ctx := t.Context()

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
	manifest, err := packed.CreateManifestForSegment(base, []string{"100"}, "parquet",
		[]packed.Fragment{{FilePath: path.Join(base, "_data", "old.parquet"), EndRow: 10}}, cfg)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
		ManifestPath: manifest, NumOfRows: 10,
	})))
	version := indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	task := &model.SegmentIndex{
		CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, IndexVersion: 1, NumRows: 10,
		IndexState: commonpb.IndexState_InProgress, IndexStorePathVersion: version, IndexType: "FLAT",
	}
	require.NoError(t, im.AddSegmentIndex(ctx, task))
	const tolerance = 24 * time.Hour
	live := path.Join(base, "_index/5/1/index.bin")
	orphan := path.Join(base, "_index/6/1/orphan.bin")
	for _, file := range []string{live, orphan} {
		require.NoError(t, cm.Write(ctx, file, []byte("artifact")))
		old := time.Now().Add(-3 * tolerance)
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
	indexes, err := recovered.ResolveSegmentIndexes(ctx, segment.SegmentInfo, nil, nil)
	require.NoError(t, err)
	require.Contains(t, indexes, int64(4))
	require.Equal(t, []string{"index.bin"}, indexes[4].IndexFileKeys)
	require.EqualValues(t, 64, indexes[4].IndexSerializedSize)
	require.EqualValues(t, 256, indexes[4].IndexMemSize)
	data, err := cm.Read(ctx, live)
	require.NoError(t, err)
	require.Equal(t, "artifact", string(data))

	// A load can still hold the old metadata while the structured replacement
	// atomically retracts this build and installs a new revision.
	require.NoError(t, recovered.CommitSegmentManifest(ctx, meta.SegmentManifestCommit{
		SegmentID: 3, StorageConfig: cfg, InspectedManifest: pinned,
		Mutation: meta.ManifestMutation{Type: meta.ManifestMutationCommitUpdates, Updates: &packed.ManifestUpdates{
			DropColumns: []string{"100"},
			ColumnGroups: []packed.ColumnGroupEntry{{Columns: []string{"100"}, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{
				Path: path.Join(base, "_data", "replacement.parquet"), StartIndex: 0, EndIndex: 10,
			}}}},
		}},
	}))
	latest := recovered.GetSegment(ctx, 3)
	require.NotEqual(t, pinned, latest.GetManifestPath())
	require.Empty(t, latest.GetIndexSummaries())
	require.False(t, latest.GetManifestHasIndex())
	require.EqualValues(t, 100, recoveredIndex.GetFieldIDByIndexID(1, 4), "the index definition is still live")
	indexes, err = recovered.ResolveSegmentIndexes(ctx, latest.SegmentInfo, nil, nil)
	require.NoError(t, err)
	require.Empty(t, indexes)
	gc = NewGarbageCollector(recovered, meta.NewMockHandler(), GcOption{IndexMeta: recoveredIndex, Cli: cm, MissingTolerance: tolerance})
	gc.RecycleUnusedBinlogFiles(ctx)
	require.NoFileExists(t, live)
	indexes, err = recovered.ResolveSegmentIndexes(ctx, segment.SegmentInfo, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, 5, indexes[4].BuildID, "reading the old manifest still discovers the retired build")
	_, err = cm.Read(ctx, live)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
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
	require.Equal(t, 1, read.Times(), "adjacent builds share the immutable manifest read")
	latest := mt.GetSegment(ctx, 3).Clone()
	latest.ManifestPath = packed.MarshalManifestPath(base, 2)
	mt.MetaStore.PutSegment(latest.SegmentInfo)
	require.False(t, reclaim(path.Join(entry.Path, "index"), 1, 3, 5))
	require.Equal(t, 2, read.Times(), "a new revision requires a fresh read")
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

func TestSummaryGCRechecksPinAfterTaskLookup(t *testing.T) {
	mt, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	ctx := t.Context()
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath("files/insert_log/1/2/3", 1), ManifestHasIndex: true,
	})))
	require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
	lookup := mockey.Mock((*index.IndexMeta).CheckCleanSegmentIndex).To(func(_ *index.IndexMeta, buildID int64) (bool, *model.SegmentIndex) {
		dm.SnapshotMeta.RegisterSnapshotProtection(&datapb.SnapshotInfo{Id: 42, CollectionId: 1}, []int64{3}, []int64{buildID})
		mt.MetaStore.PutSegment(&datapb.SegmentInfo{
			ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
			ManifestPath: packed.MarshalManifestPath("files/insert_log/1/2/3", 2),
		})
		return true, nil
	}).Build()
	defer lookup.UnPatch()

	gc := &GarbageCollector{Meta: mt, Option: GcTestDomainOption(dm, GcOption{})}
	reclaim := gc.newIndexFileReclaimChecker(ctx)
	require.False(t, reclaim("files/insert_log/1/2/3/_index/5/1/index", 1, 3, 5))
	require.False(t, reclaim("files/insert_log/1/2/3/_index/5/1/index", 1, 3, 5))
	require.Equal(t, 1, lookup.Times())
}

func TestSummaryGCRetiresDroppedIndexReadinessBeforeFileRemoval(t *testing.T) {
	mt, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	ctx := t.Context()
	im := dm.IndexMeta
	for i := int64(10); i <= 11; i++ {
		require.NoError(t, im.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: i, FieldID: i + 90}))
	}
	manifest := packed.MarshalManifestPath(path.Join(t.TempDir(), "insert_log/1/2/3"), 1)
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Dropped, StorageVersion: storage.StorageV3,
		ManifestPath: manifest, ManifestHasIndex: true,
		IndexSummaries: []*datapb.SegmentIndexSummary{
			{IndexId: 10, BuildId: 20, FieldId: 100, HasArtifact: true},
			{IndexId: 11, BuildId: 21, FieldId: 101, HasArtifact: true},
		},
	})))
	require.NoError(t, im.UseSegmentSummaries(mt.MetaStore))
	require.Equal(t, []int64{3}, im.GetIndexedSegments(1, []int64{3}, []int64{100, 101}))
	discover := mockey.Mock((*GarbageCollector).GetDroppedSegmentIndexFiles).To(
		func(_ *GarbageCollector, _ context.Context, _ *packed.ManifestIOContext, segmentID int64) ([]*model.SegmentIndexSummary, map[string]struct{}, GcBlockReason) {
			return im.GetAllSegmentIndexSummaries(segmentID), map[string]struct{}{"index": {}}, GcNotBlocked
		}).Build()
	defer discover.UnPatch()
	remove := mockey.Mock((*GarbageCollector).RemoveDroppedSegmentFiles).To(
		func(_ *GarbageCollector, _ context.Context, _ *meta.SegmentInfo, files map[string]struct{}) error {
			require.Contains(t, files, "index")
			require.Empty(t, im.GetIndexedSegments(1, []int64{3}, []int64{100, 101}))
			return merr.ErrServiceUnavailable
		}).Build()
	defer remove.UnPatch()
	gc := &GarbageCollector{Meta: mt, Option: GcTestDomainOption(dm, GcOption{})}
	io := NewGCManifestReadIO(t)
	failedWrite := mockey.Mock((*kvdatacoord.Catalog).Update).To(
		func(_ *kvdatacoord.Catalog, _ context.Context, _ ...metastore.UpdateAction) error {
			return merr.ErrServiceUnavailable
		}).Build()
	gc.RecycleDroppedSegment(ctx, io, 3)
	failedWrite.UnPatch()
	require.Zero(t, remove.Times())
	require.Len(t, mt.GetSegment(ctx, 3).GetIndexSummaries(), 2)

	gc.RecycleDroppedSegment(ctx, io, 3)
	require.Empty(t, mt.GetSegment(ctx, 3).GetIndexSummaries())
	persisted, err := mt.Catalog.ListSegments(ctx, 1)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	require.Empty(t, persisted[0].GetIndexSummaries())
	require.Equal(t, manifest, persisted[0].GetManifestPath())
	gc.RecycleDroppedSegment(ctx, io, 3)
	require.Equal(t, 2, remove.Times(), "the retained manifest allows retrying a failed delete")
}

func TestSummaryGCDeletionGuardRespectsPendingAndCommittedSnapshot(t *testing.T) {
	_, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	sm := dm.SnapshotMeta
	releaseFirst, ok := sm.TryGuardIndexGC(1)
	require.True(t, ok)
	releaseSecond, ok := sm.TryGuardIndexGC(1)
	require.True(t, ok)

	drained := sm.SetSnapshotPending(1)
	t.Cleanup(func() { sm.ClearSnapshotPending(1) })
	require.NotNil(t, drained)
	require.True(t, sm.IsCollectionCompactionBlocked(1))
	_, ok = sm.TryGuardIndexGC(1)
	require.False(t, ok)
	release, ok := sm.TryGuardIndexGC(2)
	require.True(t, ok)
	release()

	releaseFirst()
	select {
	case <-drained:
		t.Fatal("one deletion is still active")
	default:
	}
	releaseSecond()
	select {
	case <-drained:
	default:
		t.Fatal("all admitted deletions have finished")
	}

	sm.RegisterSnapshotProtection(&datapb.SnapshotInfo{Id: 42, CollectionId: 1}, []int64{3}, []int64{5})
	sm.ClearSnapshotPending(1)
	release, ok = sm.TryGuardIndexGC(1)
	require.True(t, ok)
	defer release()
	require.True(t, sm.IsBuildIDGCBlocked(1, 5), "a queued deletion must observe the committed pin")
}
