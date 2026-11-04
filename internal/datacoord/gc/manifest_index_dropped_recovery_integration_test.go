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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
)

func TestManifestDroppedRecoveryAfterFilesDeleted(t *testing.T) {
	ctx := context.Background()
	meta.WithSegmentIndexManifestWrites(t, true)
	store := meta.NewFakeManifestStore(t)
	catalog := datacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m, mDomains := snapshot.BootMetaForRestartWithSnapshot(t, catalog, meta.RestartCollID)
	index.SeedRestartFixture(t, m, mDomains.IndexMeta)
	require.NoError(t, m.UpdateSegmentsInfo(ctx, meta.UpdateStatusOperator(meta.RestartSegID, commonpb.SegmentState_Dropped)))
	segment := m.GetSegment(ctx, meta.RestartSegID)
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcTestDomainOption(mDomains, GcOption{Cli: m.ChunkManager}))
	defer gc.Option.RemoveObjectPool.Release()
	job, ok := mDomains.IndexMeta.GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	files := gc.GetAllIndexFilesOfIndex(job)
	for file := range files {
		require.NoError(t, m.ChunkManager.Write(ctx, file, []byte("index")))
	}
	manifestFile, err := packed.ManifestFilePath(segment.GetManifestPath())
	require.NoError(t, err)
	require.NoError(t, m.ChunkManager.Write(ctx, manifestFile, []byte("manifest")))
	// Crash after file removal but before either index/segment catalog removal.
	require.NoError(t, gc.RemoveDroppedSegmentFiles(ctx, segment.Clone(), files))
	store.FailReadsFrom()
	restarted, restartedDomains := snapshot.BootMetaForRestartWithSnapshot(t, catalog, meta.RestartCollID)
	retained := restarted.GetSegment(ctx, meta.RestartSegID)
	require.NotNil(t, retained)
	require.True(t, retained.GetManifestHasIndex())
	require.Empty(t, restartedDomains.IndexMeta.GetAllSegIndexes())
	retryGC := NewGarbageCollector(restarted, meta.NewMockHandler(), GcTestDomainOption(restartedDomains, GcOption{Cli: restarted.ChunkManager}))
	defer retryGC.Option.RemoveObjectPool.Release()
	retryGC.RecycleDroppedSegment(ctx, NewGCManifestReadIO(t), meta.RestartSegID, retained)
	require.Nil(t, restarted.GetSegment(ctx, meta.RestartSegID), "pending GC must remove the remaining catalog row")
	again, _ := snapshot.BootMetaForRestartWithSnapshot(t, catalog, meta.RestartCollID)
	require.Nil(t, again.GetSegment(ctx, meta.RestartSegID))
}
