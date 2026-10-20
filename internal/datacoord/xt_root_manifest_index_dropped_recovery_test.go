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
				withSegmentIndexManifestWrites(t, manifest)
				newFakeManifestStore(t)
				catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
				m, mDomains := bootMetaForRestartWithSnapshot(t, catalog, restartCollID)
				seedRestartTask(t, m, mDomains.indexMeta)
				job, ok := mDomains.indexMeta.GetIndexJob(restartBuildID)
				require.True(t, ok)
				task := newIndexBuildTask(job, 1, m, mDomains.indexMeta, nil, nil, nil)
				require.NoError(t, task.setJobInfo(&workerpb.IndexTaskInfo{
					BuildID: restartBuildID, State: commonpb.IndexState_Finished,
					IndexFileKeys: []string{"0", "1"}, IndexStorePathVersion: layout,
				}))
				job, ok = mDomains.indexMeta.GetIndexJob(restartBuildID)
				require.True(t, ok)
				files := (&garbageCollector{meta: m, option: gcTestDomainOption(mDomains, GcOption{cli: m.chunkManager})}).getAllIndexFilesOfIndex(job)
				for file := range files {
					require.NoError(t, m.chunkManager.Write(ctx, file, []byte("index")))
				}
				require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateStatusOperator(restartSegID, commonpb.SegmentState_Dropped)))
				// Disabling publication must not hide an existing manifest artifact.
				withSegmentIndexManifestWrites(t, false)
				restarted, restartedDomains := bootMetaForRestartWithSnapshot(t, catalog, restartCollID)
				ready := func(id int64) bool {
					return len(restartedDomains.indexMeta.GetIndexedSegments(restartCollID, []int64{id}, []int64{restartFieldID})) > 0
				}
				parent := restarted.GetSegment(ctx, restartSegID)
				require.NotNil(t, parent)
				childID := restartSegID + 1
				child := NewSegmentInfo(&datapb.SegmentInfo{ID: childID, State: commonpb.SegmentState_Flushed, CompactionFrom: []int64{restartSegID}})
				frontier, _ := retrieveSegment(map[int64]*SegmentInfo{restartSegID: parent, childID: child}, typeutil.NewUniqueSet(childID), typeutil.NewUniqueSet(restartSegID), ready, ready)
				require.True(t, ready(restartSegID))
				gc := newGarbageCollector(restarted, newMockHandler(), gcTestDomainOption(restartedDomains, GcOption{cli: restarted.chunkManager}))
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
