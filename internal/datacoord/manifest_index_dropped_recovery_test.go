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
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestManifestDroppedParentSurvivesRestart(t *testing.T) {
	for _, layout := range []indexpb.IndexStorePathVersion{0, 1} {
		for _, manifest := range []bool{false, true} {
			name := "etcd_control"
			if manifest {
				name = "manifest"
			}
			t.Run(fmt.Sprintf("%s_layout_%d", name, layout), func(t *testing.T) {
				ctx := context.Background()
				withSegmentIndexManifestWrites(t, manifest)
				newFakeManifestStore(t)
				catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
				m := bootMetaForRestart(t, catalog, restartCollID)
				seedRestartTask(t, m)
				job, ok := m.indexMeta.GetIndexJob(restartBuildID)
				require.True(t, ok)
				task := newIndexBuildTask(job, 1, m, nil, nil, nil)
				require.NoError(t, task.setJobInfo(&workerpb.IndexTaskInfo{
					BuildID: restartBuildID, State: commonpb.IndexState_Finished,
					IndexFileKeys: []string{"0", "1"}, IndexStorePathVersion: layout,
				}))
				job, ok = m.indexMeta.GetIndexJob(restartBuildID)
				require.True(t, ok)
				files := (&garbageCollector{meta: m, option: GcOption{cli: m.chunkManager}}).getAllIndexFilesOfIndex(job)
				for file := range files {
					require.NoError(t, m.chunkManager.Write(ctx, file, []byte("index")))
				}
				require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateStatusOperator(restartSegID, commonpb.SegmentState_Dropped)))
				// Disabling publication must not hide an existing manifest artifact.
				withSegmentIndexManifestWrites(t, false)
				restarted := bootMetaForRestart(t, catalog, restartCollID)
				ready := func(id int64) bool {
					return len(restarted.indexMeta.GetIndexedSegments(restartCollID, []int64{id}, []int64{restartFieldID})) > 0
				}
				parent := restarted.GetSegment(ctx, restartSegID)
				require.NotNil(t, parent)
				childID := restartSegID + 1
				child := NewSegmentInfo(&datapb.SegmentInfo{ID: childID, State: commonpb.SegmentState_Flushed, CompactionFrom: []int64{restartSegID}})
				frontier, _ := retrieveSegment(map[int64]*SegmentInfo{restartSegID: parent, childID: child}, typeutil.NewUniqueSet(childID), typeutil.NewUniqueSet(restartSegID), ready, ready)
				require.True(t, ready(restartSegID))
				gc := newGarbageCollector(restarted, newMockHandler(), GcOption{cli: restarted.chunkManager})
				defer gc.option.removeObjectPool.Release()
				gc.recycleUnusedIndexFilesV0(ctx)
				for file := range files {
					exists, err := restarted.chunkManager.Exist(ctx, file)
					require.NoError(t, err)
					if !exists {
						t.Errorf("retained dropped parent's referenced index file was deleted: %s", file)
					}
				}
				require.True(t, frontier.Contain(restartSegID), "indexed compaction parent must remain eligible as fallback until child is indexed")
			})
		}
	}
}

// A failed read can mean absent, corrupt, or temporarily unavailable. Only a
// definitively absent Dropped manifest permits startup to omit its records.
type recoveryManifestExistence struct {
	storage.ChunkManager
	exists bool
	err    error
	calls  int
}

func (c *recoveryManifestExistence) Exist(context.Context, string) (bool, error) {
	c.calls++
	return c.exists, c.err
}

func TestManifestDroppedRecoveryReadFailures(t *testing.T) {
	for _, tc := range []struct {
		name         string
		state        commonpb.SegmentState
		exists       bool
		existenceErr error
		wantErr      error
		wantProbe    bool
	}{
		{"dropped_absent", commonpb.SegmentState_Dropped, false, nil, nil, true},
		{"dropped_present_transient_read", commonpb.SegmentState_Dropped, true, nil, merr.ErrIoFailed, true},
		{"dropped_existence_error", commonpb.SegmentState_Dropped, false, merr.ErrIoTooManyRequests, merr.ErrIoTooManyRequests, true},
		{"healthy_absent", commonpb.SegmentState_Flushed, false, nil, merr.ErrIoFailed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := setupManifestReloadMeta(t)
			segment := m.GetSegment(context.Background(), 5001).Clone()
			segment.State = tc.state
			m.segments.SetSegment(segment.GetID(), segment)
			cm := &recoveryManifestExistence{ChunkManager: m.chunkManager, exists: tc.exists, err: tc.existenceErr}
			m.chunkManager = cm
			reader := mockey.Mock(packed.GetManifestIndexInfos).Return(nil, merr.WrapErrIoFailedReason("manifest read unavailable")).Build()
			defer reader.UnPatch()
			err := m.reloadSegmentIndexesFromManifests(context.Background())
			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
			require.Equal(t, tc.wantProbe, cm.calls > 0)
			require.Empty(t, m.indexMeta.GetAllSegIndexes())
			require.True(t, m.GetSegment(context.Background(), 5001).GetManifestHasIndex(), "absence is not proof of an empty manifest section")
		})
	}
}

func TestManifestDroppedRecoveryRejectsInvalidEntry(t *testing.T) {
	m := setupManifestReloadMeta(t)
	segment := m.GetSegment(context.Background(), 5001).Clone()
	segment.State = commonpb.SegmentState_Dropped
	m.segments.SetSegment(segment.GetID(), segment)
	cm := &recoveryManifestExistence{ChunkManager: m.chunkManager}
	m.chunkManager = cm
	reader := mockey.Mock(packed.GetManifestIndexInfos).Return([]packed.ManifestIndexInfo{{BuildID: 5100}}, nil).Build()
	defer reader.UnPatch()
	require.ErrorIs(t, m.reloadSegmentIndexesFromManifests(context.Background()), merr.ErrDataIntegrity)
	require.Zero(t, cm.calls, "a decoded invalid entry must not use the missing-manifest exception")
}

func TestManifestDroppedRecoveryAfterFilesDeleted(t *testing.T) {
	ctx := context.Background()
	withSegmentIndexManifestWrites(t, true)
	store := newFakeManifestStore(t)
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartFixture(t, m)
	require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateStatusOperator(restartSegID, commonpb.SegmentState_Dropped)))
	segment := m.GetSegment(ctx, restartSegID)
	gc := newGarbageCollector(m, newMockHandler(), GcOption{cli: m.chunkManager})
	defer gc.option.removeObjectPool.Release()
	job, ok := m.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	files := gc.getAllIndexFilesOfIndex(job)
	for file := range files {
		require.NoError(t, m.chunkManager.Write(ctx, file, []byte("index")))
	}
	manifestFile, err := packed.ManifestFilePath(segment.GetManifestPath())
	require.NoError(t, err)
	require.NoError(t, m.chunkManager.Write(ctx, manifestFile, []byte("manifest")))
	// Crash after file removal but before either index/segment catalog removal.
	require.NoError(t, gc.removeDroppedSegmentFiles(ctx, segment.Clone(), files))
	store.failReadsFrom()
	restarted := bootMetaForRestart(t, catalog, restartCollID)
	retained := restarted.GetSegment(ctx, restartSegID)
	require.NotNil(t, retained)
	require.True(t, retained.GetManifestHasIndex())
	require.Empty(t, restarted.indexMeta.GetAllSegIndexes())
	retryGC := newGarbageCollector(restarted, newMockHandler(), GcOption{cli: restarted.chunkManager})
	defer retryGC.option.removeObjectPool.Release()
	retryGC.recycleDroppedSegment(ctx, restartSegID, retained)
	require.Nil(t, restarted.GetSegment(ctx, restartSegID), "pending GC must remove the remaining catalog row")
	again := bootMetaForRestart(t, catalog, restartCollID)
	require.Nil(t, again.GetSegment(ctx, restartSegID))
}
