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

	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestManifestIndexMigrationRetainsLayoutAliases(t *testing.T) {
	meta.NewFakeManifestStore(t)
	catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m := index.BootMigrationMeta(t, catalog, meta.RestartCollID)
	index.SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
	ctx := context.Background()
	segment := m.GetSegment(ctx, meta.RestartSegID)
	record, _ := m.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
	gc := NewGarbageCollector(m, nil, GcOption{Cli: m.ChunkManager, IndexMeta: m.IndexCatalog.(*index.IndexMeta)})
	defer gc.Close()
	canReclaim := gc.newIndexFileReclaimChecker(ctx)
	var files []string
	for _, version := range []indexpb.IndexStorePathVersion{0, 1, 2} {
		candidate := model.CloneSegmentIndex(record)
		candidate.IndexStorePathVersion = version
		files = append(files, meta.MigrationIndexPaths(m.ChunkManager.RootPath(), segment.GetManifestPath(), candidate)...)
	}
	for _, file := range files {
		require.False(t, canReclaim(file, meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID), file)
	}
	require.NoError(t, index.NewMigrationBackfillInspector(ctx, m).BackfillIndexesForTest(ctx, meta.RestartSegID, meta.RestartBuildID))
	// Also exercise the decision cached before the placement change.
	for _, check := range []func(string, int64, int64, int64) bool{canReclaim, gc.newIndexFileReclaimChecker(ctx)} {
		for _, file := range files {
			require.False(t, check(file, meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID), file)
		}
	}
	require.True(t, gc.newIndexFileReclaimChecker(ctx)(files[0]+"-unrecorded", meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID))
	require.NoError(t, m.IndexCatalog.(*index.IndexMeta).MarkIndexAsDeleted(ctx, meta.RestartCollID, []int64{meta.RestartIndexID}))
	gc.RecycleUnusedSegIndexes(ctx, nil)
	for _, file := range files {
		require.True(t, gc.newIndexFileReclaimChecker(ctx)(file, meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID), file)
	}
}
