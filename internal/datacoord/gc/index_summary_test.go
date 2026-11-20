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
	"sync/atomic"
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
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLegacyManifestIndexRecoveryProtectsArtifacts(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	ctx := t.Context()
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), root, "")
	boot := func() (*meta.Meta, *index.IndexMeta) {
		dm := &index.IndexTestMetas{}
		store := meta.NewMetaStoreWithCollections(catalog, 1)
		mt, err := meta.NewMeta(ctx, catalog, cm, store, broker.NewMockBroker(t), index.LoadIndexTestMetas(catalog, dm))
		require.NoError(t, err)
		t.Cleanup(mt.CloseManifestCommitExecutor)
		return mt, dm.IndexMeta
	}
	oldMeta, oldIndex := boot()
	require.False(t, oldIndex.PublishesSegmentSummaries(3))
	for _, segmentID := range []int64{3, 8} {
		base := path.Join(root, "insert_log", metautil.JoinIDPath(1, 2, segmentID))
		require.NoError(t, oldMeta.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: segmentID, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath(base, 1), ManifestHasIndex: true,
		})))
	}
	version := indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED
	livePrefix := metautil.NewIndexPathBuilder(root, version, 1, 2, 3, 5, 1).BuildPrefix()
	live := path.Join(livePrefix, "index.bin")
	orphan := metautil.NewIndexPathBuilder(root, version, 1, 2, 3, 6, 1).BuildFilePath("index.bin")
	for _, file := range []string{live, orphan} {
		require.NoError(t, cm.Write(ctx, file, []byte("index")))
	}
	entry := packed.ManifestIndexInfo{
		IndexID: 4, FieldID: 7, BuildID: 5, IndexVersion: 1, IndexName: "idx",
		IndexType: "FLAT", Path: livePrefix, IndexFileKeys: []string{"index.bin"}, IndexStorePathVersion: version,
	}
	var reads atomic.Int32
	var storageReady atomic.Bool
	config := mockey.Mock(meta.CreateStorageConfig).Return(cfg).Build()
	defer config.UnPatch()
	reader := mockey.Mock(packed.SubmitManifestIndexInfos).To(func(_ context.Context, _ *packed.ManifestIOContext, manifest string, _ *indexpb.StorageConfig, complete func([]packed.ManifestIndexInfo, error)) error {
		reads.Add(1)
		if manifest == oldMeta.GetSegment(ctx, 8).GetManifestPath() {
			complete(nil, nil)
		} else if storageReady.Load() {
			complete([]packed.ManifestIndexInfo{entry}, nil)
		} else {
			complete(nil, merr.ErrIoKeyNotFound)
		}
		return nil
	}).Build()
	defer reader.UnPatch()

	mt, im := boot()
	require.Zero(t, reads.Load(), "startup must not read a marked manifest")
	require.True(t, mt.ManifestIndexesPending(3))
	require.False(t, im.HasBuildInfo(5))
	_, err := mt.LoadableProjection(ctx, 1, nil)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)

	gc := NewGarbageCollector(mt, meta.NewMockHandler(), GcOption{IndexMeta: im, Cli: cm})
	gc.RecycleUnusedIndexFilesV0(ctx)
	require.FileExists(t, live)
	require.FileExists(t, orphan)
	require.False(t, gc.newIndexFileReclaimChecker(ctx)(live, 1, 3, 5))

	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		mt.RecoverPendingManifestIndexes(workerCtx, func(int64) error { return nil })
	}()
	defer func() {
		cancel()
		<-done
	}()
	require.Eventually(t, func() bool {
		return reads.Load() >= 2 && !mt.ManifestIndexesPending(8)
	}, 5*time.Second, 10*time.Millisecond)
	require.True(t, mt.ManifestIndexesPending(3), "an unreadable manifest stays protected")
	require.False(t, mt.GetSegment(ctx, 8).GetManifestHasIndex(), "an empty revision clears the old marker")
	require.False(t, gc.newIndexFileReclaimChecker(ctx)(live, 1, 3, 5))

	storageReady.Store(true)
	require.Eventually(t, func() bool { return !mt.HasPendingManifestIndexes() }, 5*time.Second, 10*time.Millisecond)
	recovered, exists := im.GetIndexJob(5)
	require.True(t, exists)
	require.Equal(t, []string{"index.bin"}, recovered.IndexFileKeys)
	gc.RecycleUnusedIndexFilesV0(ctx)
	require.FileExists(t, live)
	require.NoFileExists(t, orphan)

	meta.WithSegmentIndexManifestWrites(t, true)
	migrating, migratedIndex := boot()
	require.True(t, migrating.ManifestIndexesPending(3))
	migrating.RecoverPendingManifestIndexes(ctx, func(int64) error { return nil })
	published := migrating.GetSegment(ctx, 3)
	require.Equal(t, meta.CurrentIndexSummaryVersion, published.GetIndexSummaryVersion())
	require.Equal(t, int64(5), published.GetIndexSummaries()[0].GetBuildId())
	require.False(t, migratedIndex.HasBuildInfo(5), "manifest-only entries persist as summaries")

	meta.WithSegmentIndexManifestWrites(t, false)
	beforeRestart := reads.Load()
	restarted, restartedIndex := boot()
	require.False(t, restarted.HasPendingManifestIndexes())
	require.Equal(t, beforeRestart, reads.Load())
	require.False(t, restartedIndex.HasBuildInfo(5))
	restartedGC := NewGarbageCollector(restarted, meta.NewMockHandler(), GcOption{IndexMeta: restartedIndex, Cli: cm})
	restartedGC.RecycleUnusedIndexFilesV0(ctx)
	require.FileExists(t, live)
	runtimeRead := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{entry}, nil).Build()
	defer runtimeRead.UnPatch()
	records, err := restarted.ReadCompletedIndexes(ctx, restarted.GetSegment(ctx, 3).SegmentInfo)
	require.NoError(t, err)
	require.Equal(t, version, records[4].IndexStorePathVersion)
}

func TestSummaryGCKeepsPublishedArtifactAcrossRestart(t *testing.T) {
	ctx := t.Context()
	params := paramtable.Get()
	key := params.DataCoordCfg.WriteSegmentIndexToManifest.Key
	previous := params.DataCoordCfg.WriteSegmentIndexToManifest.GetValue()
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
	task := &model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, IndexVersion: 1, NumRows: 10,
		IndexState: commonpb.IndexState_InProgress, IndexStorePathVersion: version, IndexType: "FLAT"}
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
	entry := packed.ManifestIndexInfo{IndexID: 4, BuildID: 5, IndexVersion: 1, IndexName: "idx", IndexType: "HNSW",
		Path: path.Join(base, "_index/5/1"), IndexFileKeys: []string{"index"}, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED}
	read := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{entry}, nil).Build()
	defer read.UnPatch()
	reclaim := gc.newIndexFileReclaimChecker(ctx)
	require.False(t, reclaim(path.Join(entry.Path, "index"), 1, 3, 5))
	legacyAlias := metautil.NewIndexPathBuilder(root, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, 1, 2, 3, 5, 1).BuildFilePath("index")
	require.False(t, reclaim(legacyAlias, 1, 3, 5))
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
			entry := packed.ManifestIndexInfo{IndexID: 4, BuildID: 5, IndexVersion: 1, IndexName: "idx", IndexType: "HNSW",
				Path: path.Join(base, "_index/5/1"), IndexFileKeys: []string{"index"},
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED}
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
