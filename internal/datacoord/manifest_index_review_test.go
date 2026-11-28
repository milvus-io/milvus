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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func TestInitMetaRecoversIndexInventoryWithoutManifestReads(t *testing.T) {
	for _, mode := range []string{"writes-off", "writes-on", "dataview-reload", "dataview-failure"} {
		t.Run(mode, func(t *testing.T) {
			meta.MockManifestIndexSubmissions(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			meta.WithSegmentIndexManifestWrites(t, mode != "writes-off")
			kv := tikv.NewTiKV(globalTestTikv, "manifest-index-restart-"+uuid.NewString())
			t.Cleanup(func() { require.NoError(t, kv.RemoveWithPrefix(context.Background(), "")) })
			catalog := datacoord.NewCatalog(kv, "", "")
			m, _ := index.BootMetaForRestart(t, catalog, 100)
			cm := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-restart"))
			for _, id := range []int64{5001, 5002} {
				require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
					CollectionID: 100, PartitionID: 10, ID: id, State: commonpb.SegmentState_Flushed,
					StorageVersion: storage.StorageV3, ManifestHasIndex: true,
					ManifestPath:   packed.MarshalManifestPath(path.Join("/tmp/test-restart", "insert_log", metautil.JoinIDPath(100, 10, id)), 1),
					IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 1, FieldId: 2, BuildId: id + 100, HasArtifact: true}},
				})))
			}
			reader := mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, merr.WrapErrIoFailedReason("index storage unavailable")).Build()
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
			require.Zero(t, reader.Times(), "initMeta returned %v", err)
			if mode == "dataview-failure" {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				require.False(t, retry.IsRecoverable(err))
				require.Nil(t, server.meta)
				require.Nil(t, server.dataViewManager)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, server.dataViewManager)
			if mode == "dataview-reload" {
				require.Equal(t, 2, viewLoads)
			} else {
				require.Equal(t, 1, viewLoads)
			}
			summary, ok := server.indexMeta.GetSegmentIndexSummary(5001, 5101)
			require.True(t, ok)
			require.True(t, summary.HasArtifact)
			require.False(t, server.indexMeta.HasBuildInfo(5101))
		})
	}
}
