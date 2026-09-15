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

package datacoord

import (
	"context"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestManifestIndexBackfillRevalidatesSelectedRecord(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(context.Context, *meta) error
	}{
		{"deleted definition", func(ctx context.Context, m *meta) error {
			return m.indexMeta.MarkIndexAsDeleted(ctx, restartCollID, []int64{restartIndexID})
		}},
		{"replaced build", func(ctx context.Context, m *meta) error {
			old, _ := m.indexMeta.GetIndexJob(restartBuildID)
			replacement := model.CloneSegmentIndex(old)
			replacement.BuildID++
			return m.indexMeta.AddSegmentIndex(ctx, replacement)
		}},
		{"changed artifact version", func(_ context.Context, m *meta) error {
			return m.indexMeta.UpdateVersion(restartBuildID, 10)
		}},
		{"changed level", func(ctx context.Context, m *meta) error {
			return m.UpdateSegmentsInfo(ctx, UpdateSegmentLevelOperator(restartSegID, datapb.SegmentLevel_L0))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			withSegmentIndexManifestWrites(t, false)
			store := newFakeManifestStore(t)
			ctx := context.TODO()
			catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
			m := bootMetaForRestart(t, catalog, restartCollID)
			seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
			base := m.GetSegment(ctx, restartSegID).GetManifestPath()
			// The inspector has constructed its entry, but the framework has
			// not acquired the locks that authorize publishing it yet.
			var original func(*meta, context.Context, SegmentManifestCommit) error
			patch := mockey.Mock((*meta).CommitSegmentManifest).Origin(&original).To(
				func(receiver *meta, ctx context.Context, commit SegmentManifestCommit) error {
					if err := test.change(ctx, receiver); err != nil {
						return err
					}
					return original(receiver, ctx, commit)
				}).Build()
			t.Cleanup(patch.UnPatch)
			err := newManifestIndexBackfillInspector(ctx, m).backfillIndex(ctx, restartSegID, restartBuildID)
			require.Error(t, err)
			assert.Zero(t, store.commitCount, "stale candidate must fail before manifest I/O")
			assert.Equal(t, base, m.GetSegment(ctx, restartSegID).GetManifestPath())
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			assert.NotEmpty(t, rows)
			assert.False(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))
		})
	}
}

func TestManifestIndexBackfillFailureDuringManifestIO(t *testing.T) {
	for _, scenario := range []string{"write fails", "pointer advances", "segment drops"} {
		t.Run(scenario, func(t *testing.T) {
			withSegmentIndexManifestWrites(t, false)
			withManifestIndexBackfillEnabled(t, true)
			store := newFakeManifestStore(t)
			ctx := context.TODO()
			catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
			m := bootMetaForRestart(t, catalog, restartCollID)
			seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
			base := m.GetSegment(ctx, restartSegID).GetManifestPath()
			withSegmentIndexManifestWrites(t, true)
			var original func(string, SegmentManifestCommit) (string, error)
			inject := true
			patch := mockey.Mock(commitManifestMutation).Origin(&original).To(
				func(base string, commit SegmentManifestCommit) (string, error) {
					if inject && scenario == "write fails" {
						return "", merr.ErrIoTooManyRequests
					}
					result, err := original(base, commit)
					if err != nil || !inject {
						return result, err
					}
					if scenario == "pointer advances" {
						err = m.UpdateSegmentsInfo(ctx, UpdateManifestVersion(restartSegID, 3))
					} else {
						err = m.SetState(ctx, restartSegID, commonpb.SegmentState_Dropped)
					}
					return result, err
				}).Build()
			t.Cleanup(patch.UnPatch)
			inspector := newManifestIndexBackfillInspector(ctx, m)
			err := inspector.backfillIndex(ctx, restartSegID, restartBuildID)
			require.Error(t, err)
			switch scenario {
			case "write fails":
				assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
			case "pointer advances":
				assert.ErrorIs(t, err, errSegmentManifestStale)
			case "segment drops":
				assert.ErrorIs(t, err, merr.ErrSegmentNotFound)
			}
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.False(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))
			assert.False(t, m.GetSegment(ctx, restartSegID).GetManifestHasIndex())
			if scenario == "write fails" {
				assert.Equal(t, base, m.GetSegment(ctx, restartSegID).GetManifestPath())
			}
			inject = false
			inspector.runOnce(ctx)
			rows, err = catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			if scenario == "segment drops" {
				assert.Len(t, rows, 1, "dropped segment remains owned by GC")
				assert.Zero(t, inspector.lastPending)
			} else {
				assert.Empty(t, rows)
				assert.Len(t, store.backfillEntriesAt(m.GetSegment(ctx, restartSegID).GetManifestPath()), 1)
			}
		})
	}
}

func TestManifestIndexBackfillGCRechecksLegacyObservation(t *testing.T) {
	withSegmentIndexManifestWrites(t, false)
	store := newFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
	observed := m.GetSegment(ctx, restartSegID)
	record, _ := m.indexMeta.GetIndexJob(restartBuildID)
	gc := newGarbageCollector(m, nil, GcOption{cli: m.chunkManager})
	t.Cleanup(gc.close)
	item := segmentIndexGCItem{segIdx: record, files: gc.getAllIndexFilesOfIndex(record)}
	for file := range item.files {
		require.NoError(t, m.chunkManager.Write(ctx, file, []byte("artifact")))
		t.Cleanup(func() { _ = m.chunkManager.Remove(ctx, file) })
	}
	// Backfill passed its live-definition check before DDL retired the index.
	require.NoError(t, newManifestIndexBackfillInspector(ctx, m).backfillIndex(ctx, restartSegID, restartBuildID))
	require.NoError(t, m.indexMeta.MarkIndexAsDeleted(ctx, restartCollID, []int64{restartIndexID}))
	gc.recycleRecordOnlySegmentIndex(ctx, observed, item)
	_, exists := m.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, exists, "stale GC must preserve the driver of manifest retraction")
	for file := range item.files {
		present, err := m.chunkManager.Exist(ctx, file)
		require.NoError(t, err)
		assert.True(t, present, "recheck precedes deletion")
	}
	gc.recycleUnusedSegIndexes(ctx, nil)
	_, exists = m.indexMeta.GetIndexJob(restartBuildID)
	assert.False(t, exists)
	published := m.GetSegment(ctx, restartSegID)
	assert.Empty(t, store.backfillEntriesAt(published.GetManifestPath()))
	assert.False(t, published.GetManifestHasIndex(), "merged marker clearing stays effective")
}

func TestManifestIndexBackfillConcurrencyAndShutdown(t *testing.T) {
	withSegmentIndexManifestWrites(t, false)
	withManifestIndexBackfillEnabled(t, true)
	newFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	for offset := int64(0); offset < 4; offset++ {
		seedLegacyBackfillRecord(t, m, restartSegID+offset, restartBuildID+offset)
	}
	key := Params.DataCoordCfg.ManifestIndexBackfillConcurrency.Key
	previous := Params.DataCoordCfg.ManifestIndexBackfillConcurrency.GetValue()
	Params.Save(key, "2")
	t.Cleanup(func() { Params.Save(key, previous) })
	withSegmentIndexManifestWrites(t, true)
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	var once sync.Once
	var original func(string, SegmentManifestCommit) (string, error)
	patch := mockey.Mock(commitManifestMutation).Origin(&original).To(func(base string, commit SegmentManifestCommit) (string, error) {
		entered <- struct{}{}
		<-release
		return original(base, commit)
	}).Build()
	inspector := newManifestIndexBackfillInspector(ctx, m)
	t.Cleanup(func() {
		inspector.cancel()
		once.Do(func() { close(release) })
		inspector.Stop()
		patch.UnPatch()
	})
	inspector.Start()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("backfill did not fill its two worker slots")
		}
	}
	select {
	case <-entered:
		t.Fatal("backfill exceeded configured concurrency")
	default:
	}
	inspector.cancel()
	stopped := make(chan struct{})
	go func() { inspector.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned before in-flight manifest I/O drained")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("backfill did not stop after draining")
	}
	assert.Empty(t, entered, "queued segment groups must not enter manifest I/O after cancellation")
	rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(rows), 2, "unstarted work must remain durable for the next leader")
}

// Exercise the real packed FFI and durable catalog protocol together. The
// artifact bytes are fixtures: this checks metadata/path recovery, not search.
func TestManifestIndexBackfillRealManifestRestart(t *testing.T) {
	for _, layout := range []indexpb.IndexStorePathVersion{
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	} {
		t.Run(layout.String(), func(t *testing.T) {
			withSegmentIndexManifestWrites(t, false)
			withManifestIndexBackfillEnabled(t, true)
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
			catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
			boot := func() *meta {
				b := broker.NewMockBroker(t)
				b.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
					Status:        merr.Success(),
					DbCollections: []*rootcoordpb.DBCollections{{DbName: "default", CollectionIDs: []int64{restartCollID}}},
				}, nil)
				m, err := newMeta(ctx, catalog, storage.NewLocalChunkManager(objectstorage.RootPath(root)), b)
				require.NoError(t, err)
				return m
			}
			m := boot()
			seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
			m.AddCollection(&collectionInfo{ID: restartCollID, Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{FieldID: restartFieldID, Name: "vec", DataType: schemapb.DataType_FloatVector}},
			}})
			basePath := path.Join(root, "insert_log/300/30/8001")
			cfg := createStorageConfig()
			statPath := path.Join(basePath, "_stats/bloom_filter.100/1")
			require.NoError(t, packed.WriteFile(cfg, statPath, []byte("stats")))
			initial, err := packed.CommitManifestUpdates(basePath, packed.ManifestEarliest, cfg, &packed.ManifestUpdates{
				Stats: []packed.StatEntry{{Key: "bloom_filter.100", Files: []string{statPath}}},
			})
			require.NoError(t, err)
			require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateManifest(restartSegID, initial)))
			first, _ := m.indexMeta.GetIndexJob(restartBuildID)
			first.IndexStorePathVersion = layout
			require.NoError(t, m.indexMeta.alterSegmentIndexes([]*model.SegmentIndex{first}))
			definition := model.CloneIndex(m.indexMeta.GetIndexesForCollection(restartCollID, "")[0])
			definition.IndexID++
			definition.IndexName = "second_idx"
			require.NoError(t, m.indexMeta.CreateIndex(ctx, definition))
			second := model.CloneSegmentIndex(first)
			second.IndexID++
			second.BuildID++
			require.NoError(t, m.indexMeta.AddSegmentIndex(ctx, second))
			gc := newGarbageCollector(m, nil, GcOption{cli: m.chunkManager})
			t.Cleanup(gc.close)
			for _, record := range []*model.SegmentIndex{first, second} {
				for file := range gc.getAllIndexFilesOfIndex(record) {
					require.NoError(t, m.chunkManager.Write(ctx, file, []byte("artifact")))
				}
			}
			withSegmentIndexManifestWrites(t, true)
			inspector := newManifestIndexBackfillInspector(ctx, m)
			inspector.runOnce(ctx)
			published := m.GetSegment(ctx, restartSegID).GetManifestPath()
			require.NotEqual(t, initial, published)
			entries, err := packed.GetManifestIndexInfos(published, cfg)
			require.NoError(t, err)
			require.Len(t, entries, 2)
			for _, entry := range entries {
				assert.Equal(t, layout, entry.IndexStorePathVersion)
				assert.Equal(t, "vec", entry.ColumnName)
				for _, key := range entry.IndexFileKeys {
					data, err := m.chunkManager.Read(ctx, path.Join(entry.Path, key))
					require.NoError(t, err)
					assert.Equal(t, []byte("artifact"), data)
				}
			}
			stats, err := packed.GetManifestStats(published, cfg)
			require.NoError(t, err)
			assert.Contains(t, stats, "bloom_filter.100", "migration must preserve unrelated manifest sections")
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			require.Empty(t, rows)
			inspector.runOnce(ctx)
			assert.Equal(t, published, m.GetSegment(ctx, restartSegID).GetManifestPath())
			withSegmentIndexManifestWrites(t, false)
			restarted := boot()
			for _, record := range []*model.SegmentIndex{first, second} {
				recovered, ok := restarted.indexMeta.GetIndexJob(record.BuildID)
				require.True(t, ok)
				assert.Equal(t, record.IndexFileKeys, recovered.IndexFileKeys)
				assert.Equal(t, layout, recovered.IndexStorePathVersion)
				assert.True(t, restarted.indexMeta.isSegmentIndexCatalogAbsent(record.BuildID))
			}
			_, pending := newManifestIndexBackfillInspector(ctx, restarted).scan(ctx)
			assert.Zero(t, pending)
		})
	}
}
