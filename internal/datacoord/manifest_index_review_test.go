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

package datacoord

import (
	"context"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// Exercise the actual initMeta loop: healthy manifests are read once, a
// transient read retries locally, and later recovery phases never replay newMeta.
func TestInitMetaDoesNotReplayManifestScan(t *testing.T) {
	for _, mode := range []string{"transient", "persistent", "invalid", "dataview-reload", "dataview-failure"} {
		t.Run(mode, func(t *testing.T) {
			meta.MockManifestIndexSubmissions(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			kv := meta.NewMetaMemoryKV()
			catalog := datacoord.NewCatalog(kv, "", "")
			m, _ := index.BootMetaForRestart(t, catalog, 100)
			cm := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-restart"))
			for _, id := range []int64{5001, 5002} {
				require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
					CollectionID: 100, PartitionID: 10, ID: id, State: commonpb.SegmentState_Flushed,
					StorageVersion: storage.StorageV3, ManifestHasIndex: true,
					ManifestPath: packed.MarshalManifestPath(path.Join("/tmp/test-restart", "insert_log", metautil.JoinIDPath(100, 10, id)), 1),
				})))
			}
			var mu sync.Mutex
			reads := make(map[string]int)
			good := packed.MarshalManifestPath(path.Join("/tmp/test-restart", "insert_log", metautil.JoinIDPath(100, 10, 5001)), 1)
			bad := packed.MarshalManifestPath(path.Join("/tmp/test-restart", "insert_log", metautil.JoinIDPath(100, 10, 5002)), 1)
			reader := mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(_ context.Context, _ *packed.ManifestIOContext, pointer string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				mu.Lock()
				defer mu.Unlock()
				reads[pointer]++
				if pointer == bad {
					if mode == "persistent" || (mode == "transient" && reads[pointer] == 1) {
						return nil, merr.WrapErrIoFailedReason("throttled")
					}
					if mode == "invalid" {
						return []packed.ManifestIndexInfo{{IndexID: 1, BuildID: 2, Path: "wrong"}}, nil
					}
				}
				return nil, nil
			}).Build()
			defer reader.UnPatch()
			b := broker.NewCoordinatorBroker(nil)
			viewLoads := 0
			listViews := mockey.Mock((*datacoord.Catalog).ListAllDataViews).To(func(*datacoord.Catalog, context.Context) ([]*viewpb.DataViewOfCollection, error) {
				viewLoads++
				if mode == "dataview-failure" {
					return nil, retry.Unrecoverable(merr.WrapErrDataIntegrityMsg("invalid DataView catalog"))
				}
				if mode == "dataview-reload" && viewLoads == 1 {
					return nil, merr.WrapErrServiceUnavailable("DataView reload failed")
				}
				return nil, nil
			}).Build()
			defer listViews.UnPatch()
			// The catalog initMeta would build, with collection 100 in the view.
			svrCatalog := datacoord.NewCatalog(kv, cm.RootPath(), "")
			server := &Server{
				ctx: ctx, kv: kv, broker: b, catalog: svrCatalog,
				metaStore: meta.NewMetaStoreWithCollections(svrCatalog, 100),
			}
			server.SetDataViewCollectionRecoveryValidator(func(context.Context, int64) (bool, error) {
				return true, nil
			})
			err := server.initMeta(cm)
			if mode == "dataview-failure" {
				// A later phase that fails first cancels the manifest scan, which
				// may not have read every manifest yet; none is read twice.
				require.LessOrEqual(t, reads[good], 1)
			} else {
				require.Equal(t, 1, reads[good])
			}
			if mode == "transient" || mode == "dataview-reload" {
				require.NoError(t, err)
				require.NotNil(t, server.meta)
				require.NotNil(t, server.dataViewManager)
				wantReads, wantViews := 1, 1
				switch mode {
				case "transient":
					wantReads = 2
				case "dataview-reload":
					wantViews = 2
				}
				require.Equal(t, wantReads, reads[bad])
				require.Equal(t, wantViews, viewLoads)
			} else {
				require.Error(t, err)
				require.Nil(t, server.meta)
				require.Nil(t, server.dataViewManager)
				require.False(t, retry.IsRecoverable(err))
				switch mode {
				case "invalid":
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
					require.Equal(t, 1, reads[bad])
				case "dataview-failure":
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
					require.LessOrEqual(t, reads[bad], 1)
				default:
					require.Equal(t, 3, reads[bad])
				}
			}
		})
	}
}

// The manifest scan may finish after DataView recovery projected a collection.
// A compaction output whose index only its manifest records must still replace
// its parent in the recovered DataView.
func TestInitMetaRechecksDataViewsAfterManifestScan(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, true)
	meta.NewFakeManifestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv := meta.NewMetaMemoryKV()
	catalog := datacoord.NewCatalog(kv, "", "")
	m, metas := index.BootMetaForRestart(t, catalog, meta.RestartCollID)
	index.SeedRestartFixture(t, m, metas.IndexMeta)
	parentID := meta.RestartSegID - 1
	output := m.GetSegment(ctx, meta.RestartSegID).Clone()
	require.True(t, output.GetManifestHasIndex())
	output.InsertChannel = "v1"
	output.CompactionFrom = []int64{parentID}
	output.CreatedByCompaction = true
	output.IsSorted = true
	output.TransformStartAfterTimetick = 50
	parentManifest := packed.MarshalManifestPath(storage.SegmentManifestBasePath("/tmp/test-restart", meta.RestartCollID, output.GetPartitionID(), parentID), 1)
	parent := &datapb.SegmentInfo{
		ID: parentID, CollectionID: meta.RestartCollID, PartitionID: output.GetPartitionID(), InsertChannel: "v1",
		State: commonpb.SegmentState_Dropped, Compacted: true, NumOfRows: 1000, TransformStartAfterTimetick: 50,
		StorageVersion: storage.StorageV3, ManifestPath: parentManifest,
	}
	require.NoError(t, catalog.AlterSegments(ctx, []*datapb.SegmentInfo{parent, output.SegmentInfo}))
	require.NoError(t, catalog.SaveChannelCheckpoint(ctx, "v1", &msgpb.MsgPosition{ChannelName: "v1", MsgID: []byte{1}, Timestamp: 100}))

	// Start the scan only after the first projection returned.
	var (
		project   func(*meta.Meta, context.Context, int64, *viewpb.DataViewOfCollection) ([]dataview.LoadableSegment, error)
		reload    func(*meta.Meta, context.Context) error
		projected = make(chan struct{})
		once      sync.Once
		first     []int64
	)
	projection := mockey.Mock((*meta.Meta).LoadableProjection).Origin(&project).To(func(m *meta.Meta, ctx context.Context, collectionID int64, previous *viewpb.DataViewOfCollection) ([]dataview.LoadableSegment, error) {
		segments, err := project(m, ctx, collectionID, previous)
		once.Do(func() {
			first = lo.Map(segments, func(segment dataview.LoadableSegment, _ int) int64 { return segment.SegmentID })
			close(projected)
		})
		return segments, err
	}).Build()
	defer projection.UnPatch()
	scan := mockey.Mock((*meta.Meta).ReloadSegmentIndexesFromManifests).Origin(&reload).To(func(m *meta.Meta, ctx context.Context) error {
		select {
		case <-projected:
		case <-ctx.Done():
			return ctx.Err()
		}
		return reload(m, ctx)
	}).Build()
	defer scan.UnPatch()

	cm := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-restart"))
	svrCatalog := datacoord.NewCatalog(kv, cm.RootPath(), "")
	store := meta.NewMetaStoreWithCollections(svrCatalog)
	store.PutCollection(&meta.CollectionInfo{ID: meta.RestartCollID, VChannelNames: []string{"v1"}})
	server := &Server{ctx: ctx, kv: kv, broker: broker.NewCoordinatorBroker(nil), catalog: svrCatalog, metaStore: store}
	server.SetDataViewCollectionRecoveryValidator(func(context.Context, int64) (bool, error) {
		return true, nil
	})
	require.NoError(t, server.initMeta(cm))
	require.Equal(t, []int64{parentID}, first, "the first projection ran before the scan installed the index")
	selected, _ := admissionView(t, server.meta, meta.RestartCollID)
	require.Equal(t, []int64{meta.RestartSegID}, selected)
}

// A later phase that fails must cancel the running manifest scan and wait for it:
// the scan writes segment markers through the shared MetaStore, so it must not
// outlive a failed initMeta.
func TestInitMetaCancelsAndJoinsManifestScanOnFailure(t *testing.T) {
	stopped := false
	scan := mockey.Mock((*meta.Meta).ReloadSegmentIndexesFromManifests).To(func(_ *meta.Meta, ctx context.Context) error {
		<-ctx.Done()
		// Finish well after the cancel, so that only a join sees it stop.
		time.Sleep(100 * time.Millisecond)
		stopped = true
		return ctx.Err()
	}).Build()
	defer scan.UnPatch()
	listViews := mockey.Mock((*datacoord.Catalog).ListAllDataViews).Return(nil, retry.Unrecoverable(merr.WrapErrDataIntegrityMsg("invalid DataView catalog"))).Build()
	defer listViews.UnPatch()
	server := &Server{ctx: t.Context(), kv: meta.NewMetaMemoryKV(), broker: broker.NewCoordinatorBroker(nil)}

	err := server.initMeta(storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-restart")))

	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.True(t, stopped)
}
