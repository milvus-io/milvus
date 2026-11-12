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
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestManifestIndexMigrationRetainsLayoutAliases(t *testing.T) {
	m, _, _, _ := index.RollbackFixture(t)
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
	_, err := m.RollbackSegmentIndexes(ctx, NewGCManifestReadIO(t), meta.RestartSegID, meta.RestartBuildID)
	require.NoError(t, err)
	// Also exercise the decision cached before the placement change.
	for _, check := range []func(string, int64, int64, int64) bool{canReclaim, gc.newIndexFileReclaimChecker(ctx)} {
		for _, file := range files {
			require.False(t, check(file, meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID), file)
		}
	}
	require.True(t, gc.newIndexFileReclaimChecker(ctx)(files[0]+"-unrecorded", meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID))
	require.NoError(t, m.IndexCatalog.(*index.IndexMeta).RemoveSegmentIndex(ctx, meta.RestartBuildID))
	for _, file := range files {
		require.True(t, gc.newIndexFileReclaimChecker(ctx)(file, meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID), file)
	}
}

func TestManifestIndexMigrationWaitsForGCRetirement(t *testing.T) {
	m, _, _, _ := index.RollbackFixture(t)
	ctx := context.Background()
	record, _ := m.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
	gc := NewGarbageCollector(m, nil, GcOption{Cli: m.ChunkManager, IndexMeta: m.IndexCatalog.(*index.IndexMeta)})
	defer gc.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock((*GarbageCollector).RemoveObjectFiles).To(func(*GarbageCollector, context.Context, map[string]struct{}) error {
		close(entered)
		<-release
		return nil
	}).Build()
	defer patch.UnPatch()
	retired := make(chan struct{})
	go func() {
		defer close(retired)
		gc.recycleManifestSegmentIndexes(ctx, []segmentIndexGCItem{{segIdx: record, files: gc.getRetainedIndexFilesOfIndex(record)}})
	}()
	<-entered
	restored := make(chan int, 1)
	restoreErr := make(chan error, 1)
	io := NewGCManifestReadIO(t)
	go func() {
		n, err := m.RollbackSegmentIndexes(ctx, io, meta.RestartSegID, meta.RestartBuildID)
		restored <- n
		restoreErr <- err
	}()
	select {
	case <-restored:
		close(release)
		<-retired
		t.Fatal("rollback overtook source-file retirement")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	<-retired
	assert.Zero(t, <-restored)
	require.NoError(t, <-restoreErr)
	_, present := m.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
	assert.False(t, present, "rollback must not resurrect a retired build")
}
