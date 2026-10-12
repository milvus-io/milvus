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
	"fmt"
	"path"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Manifest readback rejects every result before installation, including legacy
// siblings in the same task. Their completed-worker output needs the durable
// cleanup plan even though those segments never adopt a manifest pointer.
func TestRejectedCopyCleanupIncludesLegacySibling(t *testing.T) {
	for _, legacyVersion := range []int64{storage.StorageV1, storage.StorageV2} {
		t.Run(fmt.Sprintf("storage_%d", legacyVersion), func(t *testing.T) {
			ctx := context.Background()
			catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
			m := bootMetaForRestart(t, catalog, 100)
			root := t.TempDir()
			m.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(root))
			task := createTestCopyTask(100, 2001).(*copySegmentTask)
			task.task.Load().IdMappings = append(task.task.Load().IdMappings, &datapb.CopySegmentIDMapping{SourceSegmentId: 2, TargetSegmentId: 2002, PartitionId: 10})
			copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
			require.NoError(t, err)
			require.NoError(t, copies.AddTask(ctx, task))
			require.NoError(t, copies.AddJob(ctx, newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))
			for id, version := range map[int64]int64{2001: storage.StorageV3, 2002: legacyVersion} {
				segment := newTestCopySegment(id)
				segment.StorageVersion = version
				require.NoError(t, m.AddSegment(ctx, segment))
			}
			require.NoError(t, task.prepareCopyCleanup(ctx, &datapb.CopySegmentRequest{
				StorageConfig: &indexpb.StorageConfig{RootPath: root},
				Sources: []*datapb.CopySegmentSource{
					{StorageVersion: storage.StorageV3},
					{StorageVersion: legacyVersion, IndexFiles: []*indexpb.IndexFilePathInfo{{BuildID: 5, IndexVersion: 1, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED}}},
				},
				Targets: []*datapb.CopySegmentTarget{
					{CollectionId: 100, PartitionId: 10, SegmentId: 2001},
					{CollectionId: 100, PartitionId: 10, SegmentId: 2002, NewBuildIds: map[int64]int64{5: 6}},
				},
			}))
			require.Empty(t, m.GetSegment(ctx, 2002).GetManifestPath(), "legacy targets must not receive a V3 placeholder")
			require.NoError(t, m.chunkManager.Write(ctx, path.Join(root, "insert_log/100/10/2001/_data/data.parquet"), []byte("V3 output")))
			artifact := path.Join(root, "index_v1/100/10/2002/6/1/index.bin")
			require.NoError(t, m.chunkManager.Write(ctx, artifact, []byte("completed V2 copy artifact")))
			patch := mockey.Mock(packed.GetManifestIndexInfos).Return(nil, merr.ErrServiceUnavailable).Build()
			defer patch.UnPatch()
			err = SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
				State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
				SegmentResults: []*datapb.CopySegmentResult{
					{SegmentId: 2002, ImportedRows: 100, Binlogs: makeTestCopySegmentBinlogs(), IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{6: {BuildId: 6, Version: 1, IndexName: "vec_idx", FieldId: 101, IndexFilePaths: []string{"index.bin"}, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED}}},
					{SegmentId: 2001, ManifestPath: packed.MarshalManifestPath(path.Join(root, "insert_log/100/10/2001"), 3)},
				},
			}, copies, m)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.Empty(t, m.indexMeta.GetAllSegmentIndexes(2002))
			persisted, err := catalog.ListSegmentIndexes(ctx, 100)
			require.NoError(t, err)
			require.Empty(t, persisted)
			restarted, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
			require.NoError(t, err)
			task = restarted.GetTask(ctx, task.GetTaskId()).(*copySegmentTask)
			inspector := &copySegmentInspector{ctx: ctx, meta: m, copyMeta: restarted}
			inspector.processFailed(task)
			gc := newGarbageCollector(m, newMockHandler(), GcOption{cli: m.chunkManager})
			defer gc.option.removeObjectPool.Release()
			gc.recycleUnusedIndexFilesV1(ctx)
			gc.recycleDroppedSegment(ctx, 2001, m.GetSegment(ctx, 2001))
			require.Nil(t, m.GetSegment(ctx, 2001))
			gc.recycleDroppedSegment(ctx, 2002, m.GetSegment(ctx, 2002))
			require.Nil(t, m.GetSegment(ctx, 2002))
			rebooted := bootMetaForRestart(t, catalog, 100)
			require.Nil(t, rebooted.GetSegment(ctx, 2002))
			require.Empty(t, rebooted.indexMeta.GetAllSegmentIndexes(2002))
			// Ordinary GC has no SegmentIndex row for this rejected result. The
			// persisted task plan must retain ownership after both segment rows retire.
			exists, err := m.chunkManager.Exist(ctx, artifact)
			require.NoError(t, err)
			require.True(t, exists)
			require.NoError(t, cleanupRejectedCopy(ctx, task, m, restarted))
			require.False(t, task.GetCleanupRequired())
			exists, err = m.chunkManager.Exist(ctx, artifact)
			require.NoError(t, err)
			require.False(t, exists, "rejected legacy sibling files must be reclaimed from the persisted cleanup plan")
		})
	}
}
