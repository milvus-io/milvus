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

	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
)

func TestManifestDroppedRecoveryAfterFilesDeleted(t *testing.T) {
	ctx := context.Background()
	withSegmentIndexManifestWrites(t, true)
	store := newFakeManifestStore(t)
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m, mDomains := bootMetaForRestartWithSnapshot(t, catalog, restartCollID)
	seedRestartFixture(t, m, mDomains.indexMeta)
	require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateStatusOperator(restartSegID, commonpb.SegmentState_Dropped)))
	segment := m.GetSegment(ctx, restartSegID)
	gc := newGarbageCollector(m, newMockHandler(), gcTestDomainOption(mDomains, GcOption{cli: m.chunkManager}))
	defer gc.option.removeObjectPool.Release()
	job, ok := mDomains.indexMeta.GetIndexJob(restartBuildID)
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
	restarted, restartedDomains := bootMetaForRestartWithSnapshot(t, catalog, restartCollID)
	retained := restarted.GetSegment(ctx, restartSegID)
	require.NotNil(t, retained)
	require.True(t, retained.GetManifestHasIndex())
	require.Empty(t, restartedDomains.indexMeta.GetAllSegIndexes())
	retryGC := newGarbageCollector(restarted, newMockHandler(), gcTestDomainOption(restartedDomains, GcOption{cli: restarted.chunkManager}))
	defer retryGC.option.removeObjectPool.Release()
	retryGC.recycleDroppedSegment(ctx, newGCManifestReadIO(t), restartSegID, retained)
	require.Nil(t, restarted.GetSegment(ctx, restartSegID), "pending GC must remove the remaining catalog row")
	again, _ := bootMetaForRestartWithSnapshot(t, catalog, restartCollID)
	require.Nil(t, again.GetSegment(ctx, restartSegID))
}
