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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
)

func TestGarbageCollector_RefreshManifestAfterTaskCompletion(t *testing.T) {
	for _, previouslyMarked := range []bool{false, true} {
		t.Run(fmt.Sprintf("previously_marked_%v", previouslyMarked), func(t *testing.T) {
			ctx := context.Background()
			withSegmentIndexManifestWrites(t, true)
			store := newFakeManifestStore(t)
			catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
			m := bootMetaForRestart(t, catalog, restartCollID)
			seedRestartTask(t, m)
			if previouslyMarked {
				// The old marked revision contains another index, but not this build.
				pointer := m.GetSegment(ctx, restartSegID).GetManifestPath()
				store.revisions[pointer] = []packed.ManifestIndexInfo{{
					IndexID: restartIndexID + 1, BuildID: restartBuildID + 1,
					FieldID: restartFieldID, IndexName: "other_idx", IndexType: "HNSW",
					IndexVersion: 1, NumRows: 1000, IndexFileKeys: []string{"0"},
					Path: "/tmp/test-restart/index_files/8101/1/30/8001",
				}}
				require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateManifestHasIndex(restartSegID)))
			}
			gc := newGarbageCollector(m, newMockHandler(), GcOption{cli: m.chunkManager})
			defer gc.option.removeObjectPool.Release()
			var original func(*indexMeta, int64) (*model.SegmentIndex, bool)
			injected := false
			var files map[string]struct{}
			hook := mockey.Mock((*indexMeta).GetIndexJob).Origin(&original).To(func(im *indexMeta, id int64) (*model.SegmentIndex, bool) {
				if !injected {
					injected = true
					// GC captured placement before this task completion and index drop.
					publishRestartTask(t, m)
					job, ok := original(im, id)
					require.True(t, ok)
					files = gc.getAllIndexFilesOfIndex(job)
					for file := range files {
						require.NoError(t, m.chunkManager.Write(ctx, file, []byte("index")))
					}
					require.NoError(t, m.indexMeta.RemoveIndex(ctx, restartCollID, restartIndexID))
				}
				return original(im, id)
			}).Build()
			defer hook.UnPatch()
			gc.recycleUnusedSegIndexes(ctx, nil)
			hook.UnPatch()
			require.True(t, injected)
			pointer := m.GetSegment(ctx, restartSegID).GetManifestPath()
			_, recordExists := m.indexMeta.GetIndexJob(restartBuildID)
			require.False(t, recordExists, "GC removed its only driving record")
			entries := store.revisions[pointer]
			expectedEntries := 0
			if previouslyMarked {
				expectedEntries = 1
			}
			require.Len(t, entries, expectedEntries, "GC must retract the manifest entry together with deleting its record")
			if previouslyMarked {
				require.Equal(t, restartIndexID+1, entries[0].IndexID)
			}
			for file := range files {
				exists, err := m.chunkManager.Exist(ctx, file)
				require.NoError(t, err)
				require.False(t, exists, "eligible artifacts should be removed before retraction")
			}
			// Once the driving record is gone, another ordinary cycle cannot repair a
			// stranded entry. Assert the first cycle completed both metadata removals.
			gc.recycleUnusedSegIndexes(ctx, nil)
			require.Equal(t, previouslyMarked, m.GetSegment(ctx, restartSegID).GetManifestHasIndex())
		})
	}
}
