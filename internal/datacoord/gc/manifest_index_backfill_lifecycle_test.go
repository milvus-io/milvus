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

package gc

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestManifestIndexBackfillGCRechecksLegacyObservation(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	store := meta.NewFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m := index.BootMigrationMeta(t, catalog, meta.RestartCollID)
	index.SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
	observed := m.GetSegment(ctx, meta.RestartSegID)
	record, _ := m.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
	gc := NewGarbageCollector(m, nil, GcOption{Cli: m.ChunkManager, IndexMeta: m.IndexCatalog.(*index.IndexMeta)})
	t.Cleanup(gc.Close)
	item := segmentIndexGCItem{observed: observed, segIdx: record, files: gc.GetAllIndexFilesOfIndex(record)}
	for file := range item.files {
		require.NoError(t, m.ChunkManager.Write(ctx, file, []byte("artifact")))
		t.Cleanup(func() { _ = m.ChunkManager.Remove(ctx, file) })
	}
	// Backfill passed its live-definition check before DDL retired the index.
	require.NoError(t, index.NewMigrationBackfillInspector(ctx, m).BackfillIndexesForTest(ctx, meta.RestartSegID, meta.RestartBuildID))
	require.NoError(t, m.IndexCatalog.(*index.IndexMeta).MarkIndexAsDeleted(ctx, meta.RestartCollID, []int64{meta.RestartIndexID}))
	gc.recycleRecordOnlySegmentIndexes(ctx, nil, []segmentIndexGCItem{item})
	_, exists := m.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
	require.True(t, exists, "stale GC must preserve the driver of manifest retraction")
	for file := range item.files {
		present, err := m.ChunkManager.Exist(ctx, file)
		require.NoError(t, err)
		assert.True(t, present, "recheck precedes deletion")
	}
	gc.RecycleUnusedSegIndexes(ctx, nil)
	_, exists = m.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
	assert.False(t, exists)
	published := m.GetSegment(ctx, meta.RestartSegID)
	assert.Empty(t, store.BackfillEntriesAt(published.GetManifestPath()))
	assert.False(t, published.GetManifestHasIndex(), "merged marker clearing stays effective")
}

func TestManifestIndexBackfillRealManifestRestart(t *testing.T) {
	for _, layout := range []indexpb.IndexStorePathVersion{
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	} {
		t.Run(layout.String(), func(t *testing.T) {
			meta.WithSegmentIndexManifestWrites(t, false)
			index.WithManifestIndexBackfillEnabled(t, true)
			ctx := context.TODO()
			root := t.TempDir()
			for item, value := range map[*paramtable.ParamItem]string{
				&Params.CommonCfg.StorageType: "local",
				&Params.LocalStorageCfg.Path:  root,
			} {
				previous := item.GetValue()
				Params.Save(item.Key, value)
				t.Cleanup(func() { Params.Save(item.Key, previous) })
			}
			catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			boot := func() *meta.Meta {
				b := broker.NewMockBroker(t)
				m, err := meta.NewMeta(ctx, catalog, index.MigrationServerCopyFixture(t, root), meta.NewMetaStoreWithCollections(catalog, meta.RestartCollID), b, index.LoadIndexTestMetas(catalog, &index.IndexTestMetas{}))
				require.NoError(t, err)
				t.Cleanup(m.CloseManifestCommitExecutor)
				return m
			}
			m := boot()
			index.SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
			m.AddCollection(&meta.CollectionInfo{ID: meta.RestartCollID, Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{FieldID: meta.RestartFieldID, Name: "vec", DataType: schemapb.DataType_FloatVector}},
			}})
			basePath := path.Join(root, "insert_log/300/30/8001")
			cfg := meta.CreateStorageConfig()
			statPath := path.Join(basePath, "_stats/bloom_filter.100/1")
			require.NoError(t, packed.WriteFile(cfg, statPath, []byte("stats")))
			initial, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(basePath, packed.ManifestEarliest), cfg, &packed.ManifestUpdates{
				Stats: []packed.StatEntry{{Key: "bloom_filter.100", Files: []string{statPath}}},
			})
			require.NoError(t, err)
			require.NoError(t, m.UpdateSegmentsInfo(ctx, meta.UpdateManifest(meta.RestartSegID, initial)))
			first, _ := m.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
			first.IndexStorePathVersion = layout
			require.NoError(t, m.IndexCatalog.(*index.IndexMeta).AlterSegmentIndexesForTest([]*model.SegmentIndex{first}))
			definition := model.CloneIndex(m.IndexCatalog.(*index.IndexMeta).GetIndexesForCollection(meta.RestartCollID, "")[0])
			definition.IndexID++
			definition.IndexName = "second_idx"
			require.NoError(t, m.IndexCatalog.(*index.IndexMeta).CreateIndex(ctx, definition))
			second := model.CloneSegmentIndex(first)
			second.IndexID++
			second.BuildID++
			require.NoError(t, m.IndexCatalog.(*index.IndexMeta).AddSegmentIndex(ctx, second))
			gc := NewGarbageCollector(m, nil, GcOption{Cli: m.ChunkManager, IndexMeta: m.IndexCatalog.(*index.IndexMeta)})
			t.Cleanup(gc.Close)
			for _, record := range []*model.SegmentIndex{first, second} {
				for file := range gc.GetAllIndexFilesOfIndex(record) {
					require.NoError(t, m.ChunkManager.Write(ctx, file, []byte("artifact")))
				}
			}
			meta.WithSegmentIndexManifestWrites(t, true)
			inspector := index.NewMigrationBackfillInspector(ctx, m)
			inspector.RunOnceForTest(ctx)
			published := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
			require.NotEqual(t, initial, published)
			_, initialVersion, err := packed.UnmarshalManifestPath(initial)
			require.NoError(t, err)
			_, publishedVersion, err := packed.UnmarshalManifestPath(published)
			require.NoError(t, err)
			assert.Equal(t, initialVersion+1, publishedVersion, "both indexes must share one manifest revision")
			entries, err := packed.GetManifestIndexInfos(published, cfg)
			require.NoError(t, err)
			require.Len(t, entries, 2)
			for _, entry := range entries {
				assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED, entry.IndexStorePathVersion)
				assert.Equal(t, "vec", entry.ColumnName)
				for _, key := range entry.IndexFileKeys {
					data, err := m.ChunkManager.Read(ctx, path.Join(entry.Path, key))
					require.NoError(t, err)
					assert.Equal(t, []byte("artifact"), data)
				}
			}
			stats, err := packed.GetManifestStats(published, cfg)
			require.NoError(t, err)
			assert.Contains(t, stats, "bloom_filter.100", "migration must preserve unrelated manifest sections")
			rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, err)
			require.Empty(t, rows)
			inspector.RunOnceForTest(ctx)
			assert.Equal(t, published, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
			meta.WithSegmentIndexManifestWrites(t, false)
			restarted := boot()
			for _, record := range []*model.SegmentIndex{first, second} {
				recovered, ok := restarted.IndexCatalog.(*index.IndexMeta).GetIndexJob(record.BuildID)
				require.True(t, ok)
				assert.Equal(t, record.IndexFileKeys, recovered.IndexFileKeys)
				assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED, recovered.IndexStorePathVersion)
				assert.True(t, index.IndexManifestPublished(t, restarted.IndexCatalog.(*index.IndexMeta), record.BuildID))
			}
			_, pending := index.NewMigrationBackfillInspector(ctx, restarted).ScanForTest(ctx)
			assert.Zero(t, pending)
		})
	}
}
