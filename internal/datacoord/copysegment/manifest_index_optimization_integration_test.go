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

package copysegment

import (
	"context"
	"path"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRejectedCopyCleanupSurvivesRestartAndDeleteFailure(t *testing.T) {
	ctx := context.Background()
	catalog := catalogkv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m, mDomains := index.BootMetaForRestart(t, catalog, 300)
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	m.ChunkManager = cm
	require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 8001, CollectionID: 300, PartitionID: 30, State: commonpb.SegmentState_Importing, StorageVersion: storage.StorageV3})))
	copies, err := NewCopySegmentMeta(ctx, catalog, m, mDomains.IndexMeta, nil, nil)
	require.NoError(t, err)
	task := &Task{Ctx: ctx, Meta: m, CopyMeta: copies}
	task.Task.Store(&datapb.CopySegmentTask{TaskId: 99, CollectionId: 300, State: datapb.CopySegmentTaskState_CopySegmentTaskPending})
	require.NoError(t, copies.AddTask(ctx, task))
	request := &datapb.CopySegmentRequest{StorageConfig: &indexpb.StorageConfig{RootPath: root}, Sources: []*datapb.CopySegmentSource{{StorageVersion: storage.StorageV3, IndexFiles: []*indexpb.IndexFilePathInfo{{BuildID: 5, IndexVersion: 1, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED}}}}, Targets: []*datapb.CopySegmentTarget{{CollectionId: 300, PartitionId: 30, SegmentId: 8001, NewBuildIds: map[int64]int64{5: 6}}}}
	require.NoError(t, task.PrepareCopyCleanup(ctx, request))
	require.NotEmpty(t, m.GetSegment(ctx, 8001).GetManifestPath(), "GC needs a base even if the result is rejected")
	for _, prefix := range task.GetCleanupPrefixes() {
		require.NoError(t, cm.Write(ctx, prefix+"artifact", []byte("owned")))
	}
	foreign := path.Join(root, "insert_log/300/30/80010/keep")
	require.NoError(t, cm.Write(ctx, foreign, []byte("keep")))
	require.NoError(t, copies.UpdateTask(ctx, 99, UpdateCopyTaskCleanup(true), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed)))
	recovered, err := NewCopySegmentMeta(ctx, catalog, m, mDomains.IndexMeta, nil, nil)
	require.NoError(t, err)
	recoveredTask := recovered.GetTask(ctx, 99)
	fail := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).Return(merr.ErrServiceUnavailable).Build()
	t.Cleanup(func() { fail.UnPatch() })
	require.ErrorIs(t, CleanupRejectedCopy(ctx, recoveredTask, m, recovered), merr.ErrServiceUnavailable)
	fail.UnPatch()
	require.True(t, recovered.GetTask(ctx, 99).GetCleanupRequired())
	require.NoError(t, CleanupRejectedCopy(ctx, recoveredTask, m, recovered))
	require.False(t, recovered.GetTask(ctx, 99).GetCleanupRequired())
	for _, prefix := range recoveredTask.GetCleanupPrefixes() {
		exists, err := cm.Exist(ctx, prefix+"artifact")
		require.NoError(t, err)
		require.False(t, exists)
	}
	data, err := cm.Read(ctx, foreign)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
}
