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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func TestManifestIndexPathMatchesSegmentIdentity(t *testing.T) {
	segment := &datapb.SegmentInfo{CollectionID: 1, PartitionID: 2, ID: 3}
	for _, layout := range []indexpb.IndexStorePathVersion{0, 1} {
		entry := packed.ManifestIndexInfo{
			IndexID: 4, BuildID: 5, IndexVersion: 6, IndexName: "idx", IndexType: "HNSW",
			IndexStorePathVersion: layout, IndexFileKeys: []string{"data"},
		}
		prefix := metautil.NewIndexPathBuilder("root", layout, 1, 2, 3, 5, 6).BuildPrefix()
		entry.Path = prefix
		info, ok := manifestIndexFilePathInfoForSegment("root", segment, entry)
		require.True(t, ok)
		require.Equal(t, []string{path.Join(prefix, "data")}, info.GetIndexFilePaths())
		for _, badPath := range []string{
			"other/root", prefix + "/child", path.Dir(prefix),
			metautil.NewIndexPathBuilder("root", layout, 1, 2, 99, 5, 6).BuildPrefix(),
		} {
			entry.Path = badPath
			_, ok = manifestIndexFilePathInfoForSegment("root", segment, entry)
			require.False(t, ok, badPath)
		}
		entry.Path = prefix
		entry.IndexFileKeys = []string{".."}
		_, ok = manifestIndexFilePathInfoForSegment("root", segment, entry)
		require.False(t, ok)
	}
}

func TestManifestReloadMissingPointerFailsClosed(t *testing.T) {
	m := setupManifestReloadMeta(t)
	segment := m.GetSegment(context.Background(), 5001).Clone()
	segment.ManifestPath = ""
	m.segments.SetSegment(segment.GetID(), segment)
	err := m.reloadSegmentIndexesFromManifests(context.Background())
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestManifestReadConcurrencyUsesStorageBudget(t *testing.T) {
	params := paramtable.Get()
	scan := params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("64")
	defer params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(scan)
	connections := params.MinioCfg.MaxConnections.SwapTempValue("7")
	defer params.MinioCfg.MaxConnections.SwapTempValue(connections)
	require.Equal(t, 7, segmentIndexManifestReadConcurrency())
	params.MinioCfg.MaxConnections.SwapTempValue("100")
	require.Equal(t, 64, segmentIndexManifestReadConcurrency())
}

// Exercise the actual initMeta loop: healthy manifests are read once, a
// transient read retries locally, and persistent failures never replay newMeta.
func TestInitMetaDoesNotReplayManifestScan(t *testing.T) {
	for _, mode := range []string{"transient", "persistent", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			kv := NewMetaMemoryKV()
			catalog := datacoord.NewCatalog(kv, "", "")
			m := bootMetaForRestart(t, catalog, 100)
			cm := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-restart"))
			for _, id := range []int64{5001, 5002} {
				require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
					CollectionID: 100, PartitionID: 10, ID: id, State: commonpb.SegmentState_Flushed,
					StorageVersion: storage.StorageV3, ManifestHasIndex: true,
					ManifestPath: packed.MarshalManifestPath(metautil.JoinIDPath(id), 1),
				})))
			}
			var mu sync.Mutex
			reads := make(map[string]int)
			bad := packed.MarshalManifestPath("5002", 1)
			reader := mockey.Mock(packed.GetManifestIndexInfos).To(func(pointer string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
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
			b := broker.NewMockBroker(t)
			b.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
				Status: merr.Success(), DbCollections: []*rootcoordpb.DBCollections{{DbName: "default", CollectionIDs: []int64{100}}},
			}, nil).Once()
			collectionsLoaded := make(chan struct{})
			b.EXPECT().ListDatabases(mock.Anything).Run(func(context.Context) { close(collectionsLoaded) }).Return(nil, nil).Maybe()
			server := &Server{ctx: ctx, kv: kv, broker: b}
			err := server.initMeta(cm)
			require.Equal(t, 1, reads[packed.MarshalManifestPath("5001", 1)])
			if mode == "transient" {
				require.NoError(t, err)
				<-collectionsLoaded
				require.NotNil(t, server.meta)
				require.Equal(t, 2, reads[bad])
			} else {
				require.Error(t, err)
				require.Nil(t, server.meta)
				require.False(t, retry.IsRecoverable(err))
				if mode == "invalid" {
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
					require.Equal(t, 1, reads[bad])
				} else {
					require.Equal(t, 3, reads[bad])
				}
			}
		})
	}
}

func TestManifestGCRejectsForeignArtifactDirectory(t *testing.T) {
	ctx := context.Background()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	foreign := path.Join(root, "unrelated", "index.bin")
	require.NoError(t, cm.Write(ctx, foreign, []byte("keep")))
	segment := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Dropped,
		StorageVersion: storage.StorageV3, ManifestHasIndex: true,
		ManifestPath: packed.MarshalManifestPath(path.Join(root, "insert_log/1/2/3"), 1),
	})
	require.NoError(t, m.AddSegment(ctx, segment))
	entry := packed.ManifestIndexInfo{
		IndexID: 4, BuildID: 5, IndexName: "idx", IndexType: "HNSW",
		Path: path.Dir(foreign), IndexFileKeys: []string{"index.bin"},
	}
	reader := mockey.Mock(packed.GetManifestIndexInfos).Return([]packed.ManifestIndexInfo{entry}, nil).Build()
	defer reader.UnPatch()
	gc := newGarbageCollector(m, newMockHandler(), GcOption{cli: cm})
	gc.recycleDroppedSegment(ctx, segment.GetID(), segment)
	require.NotNil(t, m.GetSegment(ctx, segment.GetID()))
	content, err := cm.Read(ctx, foreign)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), content)
}
