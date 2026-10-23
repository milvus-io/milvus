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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/gc"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
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
				meta.WithSegmentIndexManifestWrites(t, manifest)
				meta.NewFakeManifestStore(t)
				catalog := datacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
				m, mDomains := snapshot.BootMetaForRestartWithSnapshot(t, catalog, meta.RestartCollID)
				index.SeedRestartTask(t, m, mDomains.IndexMeta)
				job, ok := mDomains.IndexMeta.GetIndexJob(meta.RestartBuildID)
				require.True(t, ok)
				task := index.NewIndexBuildTask(job, 1, m, mDomains.IndexMeta, nil, nil, nil)
				require.NoError(t, task.SetJobInfo(&workerpb.IndexTaskInfo{
					BuildID: meta.RestartBuildID, State: commonpb.IndexState_Finished,
					IndexFileKeys: []string{"0", "1"}, IndexStorePathVersion: layout,
				}))
				job, ok = mDomains.IndexMeta.GetIndexJob(meta.RestartBuildID)
				require.True(t, ok)
				files := (&gc.GarbageCollector{Meta: m, Option: gc.GcTestDomainOption(mDomains, gc.GcOption{Cli: m.ChunkManager})}).GetAllIndexFilesOfIndex(job)
				for file := range files {
					require.NoError(t, m.ChunkManager.Write(ctx, file, []byte("index")))
				}
				require.NoError(t, m.UpdateSegmentsInfo(ctx, meta.UpdateStatusOperator(meta.RestartSegID, commonpb.SegmentState_Dropped)))
				// Disabling publication must not hide an existing manifest artifact.
				meta.WithSegmentIndexManifestWrites(t, false)
				restarted, restartedDomains := snapshot.BootMetaForRestartWithSnapshot(t, catalog, meta.RestartCollID)
				ready := func(id int64) bool {
					return len(restartedDomains.IndexMeta.GetIndexedSegments(meta.RestartCollID, []int64{id}, []int64{meta.RestartFieldID})) > 0
				}
				parent := restarted.GetSegment(ctx, meta.RestartSegID)
				require.NotNil(t, parent)
				childID := meta.RestartSegID + 1
				child := meta.NewSegmentInfo(&datapb.SegmentInfo{ID: childID, State: commonpb.SegmentState_Flushed, CompactionFrom: []int64{meta.RestartSegID}})
				frontier, _ := retrieveSegment(map[int64]*meta.SegmentInfo{meta.RestartSegID: parent, childID: child}, typeutil.NewUniqueSet(childID), typeutil.NewUniqueSet(meta.RestartSegID), ready, ready)
				require.True(t, ready(meta.RestartSegID))
				gcx := gc.NewGarbageCollector(restarted, meta.NewMockHandler(), gc.GcTestDomainOption(restartedDomains, gc.GcOption{Cli: restarted.ChunkManager}))
				defer gcx.Option.RemoveObjectPool.Release()
				gcx.RecycleUnusedIndexFilesV0(ctx)
				for file := range files {
					exists, err := restarted.ChunkManager.Exist(ctx, file)
					require.NoError(t, err)
					if !exists {
						t.Errorf("retained dropped parent's referenced index file was deleted: %s", file)
					}
				}
				require.True(t, frontier.Contain(meta.RestartSegID), "indexed compaction parent must remain eligible as fallback until child is indexed")
			})
		}
	}
}
