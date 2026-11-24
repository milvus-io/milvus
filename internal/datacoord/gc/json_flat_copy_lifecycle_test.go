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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// A pre-upgrade copy already published both flat and vector artifacts. Exercise
// real manifest I/O and orphan deletion, not just the ownership predicate.
func TestCopiedJSONFlatOwnershipSurvivesGCAndManifestReload(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(strconv.FormatBool(acknowledged), func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			for item, value := range map[*paramtable.ParamItem]string{
				&Params.CommonCfg.StorageType:                     "local",
				&Params.LocalStorageCfg.Path:                      root,
				&Params.DataCoordCfg.SegmentIndexSummariesEnabled: "false",
			} {
				old := item.GetValue()
				require.NoError(t, Params.Save(item.Key, value))
				t.Cleanup(func() { require.NoError(t, Params.Save(item.Key, old)) })
			}
			meta.DrainBuildIndexChForTest()
			defer meta.DrainBuildIndexChForTest()
			meta.DrainStatsBuildChForTest()
			defer meta.DrainStatsBuildChForTest()
			catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
				{FieldID: 102, Name: "json", DataType: schemapb.DataType_JSON},
			}}
			boot := func() (*meta.Meta, *index.IndexMeta) {
				dm := &index.IndexTestMetas{}
				m, err := meta.NewMeta(ctx, catalog, cm, meta.NewMetaStoreWithCollections(catalog, 100), broker.NewMockBroker(t), index.LoadIndexTestMetas(catalog, dm))
				require.NoError(t, err)
				t.Cleanup(m.CloseManifestCommitExecutor)
				m.AddCollection(&meta.CollectionInfo{ID: 100, Schema: schema})
				return m, dm.IndexMeta
			}
			m, im := boot()
			for _, def := range []*model.Index{
				{CollectionID: 100, FieldID: 101, IndexID: 300, IndexName: "vec_idx", IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}}},
				{CollectionID: 100, FieldID: 102, IndexID: 301, IndexName: "json_flat", IndexParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: "INVERTED"}, {Key: common.JSONCastTypeKey, Value: "JSON"},
				}},
			} {
				require.NoError(t, im.CreateIndex(ctx, def))
			}
			segment := copysegment.NewTestCopySegment(2001)
			segment.StorageVersion = storage.StorageV3
			segment.Level = datapb.SegmentLevel_L1
			require.NoError(t, m.AddSegment(ctx, segment))
			task := copysegment.CreateTestCopyTask(100, 2001)
			copies, err := copysegment.NewCopySegmentMeta(ctx, catalog, m, im, nil, nil)
			require.NoError(t, err)
			require.NoError(t, copies.AddTask(ctx, task))
			require.NoError(t, copies.AddJob(ctx, copysegment.NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))
			base := path.Join(root, "insert_log/100/10/2001")
			entries := []packed.ManifestIndexInfo{
				{ColumnName: "vec", FieldID: 101, IndexID: 300, BuildID: 9001, IndexName: "vec_idx", IndexType: "HNSW", Path: "9001/0", IndexFileKeys: []string{"vector.bin"}, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED, NumRows: 100},
				{ColumnName: "json", FieldID: 102, IndexID: 301, BuildID: 9002, IndexName: "json_flat", IndexType: "INVERTED", Path: "9002/0", IndexFileKeys: []string{"flat.bin"}, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED, NumRows: 100},
			}
			result := &datapb.CopySegmentResult{SegmentId: 2001, ImportedRows: 100, IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{}}
			files := map[string][]byte{}
			for _, entry := range entries {
				file := path.Join(base, "_index", entry.Path, entry.IndexFileKeys[0])
				payload := []byte("copied artifact " + entry.IndexName)
				files[file] = payload
				require.NoError(t, cm.Write(ctx, file, payload))
				result.IndexInfos[entry.BuildID] = &datapb.VectorScalarIndexInfo{
					BuildId: entry.BuildID, FieldId: entry.FieldID, IndexName: entry.IndexName, IndexType: entry.IndexType,
					IndexFilePaths: entry.IndexFileKeys, IndexStorePathVersion: entry.IndexStorePathVersion,
				}
			}
			manifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(base, packed.ManifestEarliest), meta.CreateStorageConfig(), &packed.ManifestUpdates{Indexes: entries})
			require.NoError(t, err)
			result.ManifestPath = manifest
			if acknowledged {
				result.ManifestIndexRewritten = proto.Bool(true)
				result.ManifestIndexBuildIds = []int64{9001, 9002}
			}
			response := &datapb.QueryCopySegmentResponse{State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted, SegmentResults: []*datapb.CopySegmentResult{result}}
			require.NoError(t, copysegment.SyncCopySegmentTask(ctx, task, response, copies, m, im))
			require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, task.GetState())
			require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 2001).GetState())
			// Collection restore can keep this flag until job completion. Explicitly
			// clear it so neither import protection nor a snapshot can hide lost ownership.
			require.NoError(t, m.UpdateSegmentsInfo(ctx, meta.UpdateIsImporting(2001, false)))
			rows, err := catalog.ListSegmentIndexes(ctx, 100)
			require.NoError(t, err)
			require.Empty(t, rows, "the manifest is the only durable artifact record")
			check := func(m *meta.Meta, im *index.IndexMeta) {
				gc := NewGarbageCollector(m, nil, GcOption{Cli: cm, IndexMeta: im, MissingTolerance: 0})
				defer gc.Close()
				// A control orphan proves GC actually traversed this live segment.
				orphan := path.Join(base, "_index/9999/0/orphan.bin")
				require.NoError(t, cm.Write(ctx, orphan, []byte("unowned")))
				gc.RecycleUnusedBinlogFiles(ctx)
				exists, err := cm.Exist(ctx, orphan)
				require.NoError(t, err)
				require.False(t, exists)
				for file, want := range files {
					got, err := cm.Read(ctx, file)
					require.NoError(t, err, "published artifact must remain readable: %s", file)
					require.Equal(t, want, got)
				}
				for _, buildID := range []int64{9001, 9002} {
					record, ok := im.GetIndexJob(buildID)
					require.True(t, ok)
					require.True(t, record.ManifestPublished)
					require.Equal(t, commonpb.IndexState_Finished, record.IndexState)
				}
				physical := im.GetPhysicalIndexesForCollection(100)
				require.Len(t, physical, 1)
				require.EqualValues(t, 300, physical[0].IndexID)
			}
			check(m, im)
			restarted, reloaded := boot()
			check(restarted, reloaded)
			require.NoError(t, copysegment.SyncCopySegmentTask(ctx, task, response, copies, restarted, reloaded), "completed result replay must retain ownership")
		})
	}
}
