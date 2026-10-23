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
	"io/fs"
	"path"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type copyCleanupLoadedHandler struct {
	*meta.MockHandler
	loaded []int64
}

func (h *copyCleanupLoadedHandler) ListLoadedSegments(context.Context) ([]int64, error) {
	return h.loaded, nil
}

func TestRejectedCopyCleanupWaitsForOrdinaryGC(t *testing.T) {
	for _, protection := range []string{"snapshot", "loaded reader", "paused collection", "retention"} {
		t.Run(protection, func(t *testing.T) {
			ctx := context.Background()
			task, copies, m, dm := snapshot.NewCopyCleanupLifecycle(t)
			root := m.ChunkManager.RootPath()
			base := path.Join(root, "insert_log/100/10/2001")
			data := path.Join(base, "_data/data.parquet")
			orphanIndex := path.Join(root, "index_v1/100/10/2001/6/1/index.bin")
			require.NoError(t, m.ChunkManager.Write(ctx, data, []byte("published segment")))
			require.NoError(t, m.ChunkManager.Write(ctx, orphanIndex, []byte("uninstalled index")))
			// The target publication succeeds, then the final task save fails.
			var original func(*kvdatacoord.Catalog, context.Context, *datapb.CopySegmentTask) error
			patch := mockey.Mock((*kvdatacoord.Catalog).SaveCopySegmentTask).Origin(&original).To(func(c *kvdatacoord.Catalog, ctx context.Context, record *datapb.CopySegmentTask) error {
				if record.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskCompleted {
					return merr.ErrServiceUnavailable
				}
				return original(c, ctx, record)
			}).Build()
			cluster := session.NewMockCluster(t)
			rewritten := true
			cluster.EXPECT().QueryCopySegment(int64(42), mock.Anything).Return(&datapb.QueryCopySegmentResponse{
				State:          datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
				SegmentResults: []*datapb.CopySegmentResult{{SegmentId: 2001, ManifestPath: packed.MarshalManifestPath(base, 3), ManifestIndexRewritten: &rewritten}},
			}, nil)
			task.QueryTaskOnWorker(cluster)
			patch.UnPatch()
			require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
			require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 2001).GetState())
			require.False(t, m.GetSegment(ctx, 2001).GetIsImporting())
			// This is the same visible-segment shape selected by GenSnapshot.
			handler := &copyCleanupLoadedHandler{MockHandler: meta.NewMockHandler()}
			gc := NewGarbageCollector(m, handler, GcOption{Cli: m.ChunkManager, IndexMeta: dm.IndexMeta, AnalyzeMeta: dm.AnalyzeMeta, SnapshotMeta: dm.SnapshotMeta})
			defer gc.Option.RemoveObjectPool.Release()
			release := func() {}
			switch protection {
			case "snapshot":
				snapshotMeta := snapshot.CreateTestSnapshotMeta(t)
				gc.Option.SnapshotMeta = snapshotMeta
				m.CompactionProtection = snapshotMeta
				info := &datapb.SnapshotInfo{Id: 900, CollectionId: 100, Name: "retained"}
				snapshot.InsertTestSnapshot(snapshotMeta, info, []int64{2001}, nil)
				snapshotMeta.RegisterSnapshotProtection(info, []int64{2001}, nil)
				require.True(t, snapshotMeta.IsSegmentGCBlocked(100, 2001))
				release = func() { snapshotMeta.SnapshotID2Info.Remove(900); snapshotMeta.RebuildAllSegmentProtection() }
			case "loaded reader":
				handler.loaded = []int64{2001}
				release = func() { handler.loaded = nil }
			case "paused collection":
				records := NewGCPauseRecords()
				_, err := records.Insert("copy test", time.Now().Add(time.Hour))
				require.NoError(t, err)
				gc.PausedCollection.Insert(100, records)
				release = func() { gc.PausedCollection.Remove(100) }
			case "retention":
				gc.Option.DropTolerance = time.Hour
				release = func() { gc.Option.DropTolerance = 0 }
			}
			(&copysegment.Inspector{Ctx: ctx, Meta: m, CopyMeta: copies}).ProcessFailed(task)
			gc.RecycleDroppedSegments(ctx, nil)
			require.NotNil(t, m.GetSegment(ctx, 2001), "ordinary GC must preserve protected metadata")
			require.NoError(t, copysegment.CleanupRejectedCopy(ctx, task, m, copies))
			for _, file := range []string{data, orphanIndex} {
				exists, err := m.ChunkManager.Exist(ctx, file)
				require.NoError(t, err)
				require.True(t, exists, "copy cleanup must preserve protected bytes")
			}
			require.True(t, task.GetCleanupRequired())
			release()
			gc.RecycleDroppedSegments(ctx, nil)
			require.Nil(t, m.GetSegment(ctx, 2001), "ordinary GC must durably retire the target after protection ends")
			restarted, _ := snapshot.BootMetaForRestartWithSnapshot(t, m.Catalog, 100)
			require.Nil(t, restarted.GetSegment(ctx, 2001))
			require.NoError(t, copysegment.CleanupRejectedCopy(ctx, task, m, copies))
			exists, err := m.ChunkManager.Exist(ctx, orphanIndex)
			require.NoError(t, err)
			require.False(t, exists)
			require.False(t, task.GetCleanupRequired())
		})
	}
}

func TestRejectedCopyCleanupNeverCreatedManifestBase(t *testing.T) {
	for _, failure := range []string{"absent root", "missing child", "permission", "remote failure"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			task, copies, m, dm := snapshot.NewCopyCleanupLifecycle(t)
			require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), copysegment.UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed), copysegment.UpdateCopyTaskCleanup(true)))
			(&copysegment.Inspector{Ctx: ctx, Meta: m, CopyMeta: copies}).ProcessFailed(task)
			base, version, err := packed.UnmarshalManifestPath(m.GetSegment(ctx, 2001).GetManifestPath())
			require.NoError(t, err)
			require.Zero(t, version)
			sibling := base + "0/_data/sibling.parquet"
			require.NoError(t, m.ChunkManager.Write(ctx, sibling, []byte("another segment")))
			gc := NewGarbageCollector(m, meta.NewMockHandler(), GcOption{Cli: m.ChunkManager, IndexMeta: dm.IndexMeta, AnalyzeMeta: dm.AnalyzeMeta, SnapshotMeta: dm.SnapshotMeta})
			defer gc.Option.RemoveObjectPool.Release()
			if failure != "absent root" {
				var failureErr error
				switch failure {
				case "missing child":
					failureErr = &fs.PathError{Op: "lstat", Path: path.Join(base, "child"), Err: fs.ErrNotExist}
				case "permission":
					failureErr = &fs.PathError{Op: "lstat", Path: base, Err: fs.ErrPermission}
				default:
					failureErr = merr.ErrServiceUnavailable
				}
				patch := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).Return(failureErr).Build()
				gc.RecycleDroppedSegments(ctx, nil)
				require.NotNil(t, m.GetSegment(ctx, 2001), "failed walks must preserve the GC marker")
				require.NoError(t, copysegment.CleanupRejectedCopy(ctx, task, m, copies))
				require.True(t, task.GetCleanupRequired())
				patch.UnPatch()
			}
			gc.RecycleDroppedSegments(ctx, nil)
			require.Nil(t, m.GetSegment(ctx, 2001), "a never-created version-zero base is an idempotent GC no-op")
			restarted, _ := snapshot.BootMetaForRestartWithSnapshot(t, m.Catalog, 100)
			require.Nil(t, restarted.GetSegment(ctx, 2001))
			exists, err := m.ChunkManager.Exist(ctx, sibling)
			require.NoError(t, err)
			require.True(t, exists, "GC must not match a neighboring segment ID prefix")
			require.NoError(t, copysegment.CleanupRejectedCopy(ctx, task, m, copies))
			require.False(t, task.GetCleanupRequired())
		})
	}
}
