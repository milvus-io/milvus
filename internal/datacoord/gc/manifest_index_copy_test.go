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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func indexRecordWithFiles(t *testing.T, m *meta.Meta, record *model.SegmentIndex) *model.SegmentIndex {
	t.Helper()
	keys, err := m.ResolveIndexFileKeys(t.Context(), nil, record)
	require.NoError(t, err)
	copy := *record
	copy.IndexFileKeys = keys
	copy.IndexFileCount = 0
	return &copy
}

func recordedIndexFiles(t *testing.T, gc *GarbageCollector, record *model.SegmentIndex) map[string]struct{} {
	t.Helper()
	return gc.GetAllIndexFilesOfIndex(indexRecordWithFiles(t, gc.Meta, record))
}

func TestManifestIndexMigrationRetainsLayoutAliases(t *testing.T) {
	m, _, store, _ := index.RollbackFixture(t)
	ctx := context.Background()
	segment := m.GetSegment(ctx, meta.RestartSegID)
	record, _ := m.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
	gc := NewGarbageCollector(m, nil, GcOption{Cli: m.ChunkManager, IndexMeta: m.IndexCatalog.(*index.IndexMeta)})
	defer gc.Close()
	canReclaim := gc.newIndexFileReclaimChecker(ctx)
	record = model.CloneSegmentIndex(record)
	keys, err := m.ResolveIndexFileKeys(ctx, nil, record)
	require.NoError(t, err)
	require.NotEmpty(t, keys)
	record.IndexFileKeys = keys
	var files []string
	for _, version := range []indexpb.IndexStorePathVersion{0, 1, 2} {
		candidate := model.CloneSegmentIndex(record)
		candidate.IndexStorePathVersion = version
		files = append(files, meta.MigrationIndexPaths(m.ChunkManager.RootPath(), segment.GetManifestPath(), candidate)...)
	}
	beforeReads := store.ReadCount
	for _, file := range files {
		require.False(t, canReclaim(file, meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID), file)
	}
	require.Equal(t, beforeReads, store.ReadCount)
	otherVersion := model.CloneSegmentIndex(record)
	otherVersion.IndexVersion = record.IndexVersion * 10
	for _, layout := range []indexpb.IndexStorePathVersion{0, 1, 2} {
		otherVersion.IndexStorePathVersion = layout
		for _, file := range meta.MigrationIndexPaths(m.ChunkManager.RootPath(), segment.GetManifestPath(), otherVersion) {
			require.True(t, canReclaim(file, meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID), file)
		}
	}
	_, err = m.RollbackSegmentIndexes(ctx, NewGCManifestReadIO(t), meta.RestartSegID, meta.RestartBuildID)
	require.NoError(t, err)
	// Also exercise the decision cached before the placement change.
	for _, check := range []func(string, int64, int64, int64) bool{canReclaim, gc.newIndexFileReclaimChecker(ctx)} {
		for _, file := range files {
			require.False(t, check(file, meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID), file)
		}
	}
	require.False(t, gc.newIndexFileReclaimChecker(ctx)(files[0]+"-unrecorded", meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID))
	require.NoError(t, m.IndexCatalog.(*index.IndexMeta).RemoveSegmentIndex(ctx, meta.RestartBuildID))
	for _, file := range files {
		require.True(t, gc.newIndexFileReclaimChecker(ctx)(file, meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID), file)
	}
}

func TestManifestIndexRetirementAfterSegmentRemoval(t *testing.T) {
	ctx := t.Context()
	m, _, store, _ := index.RollbackFixture(t)
	record, ok := m.IndexCatalog.GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	require.Empty(t, record.IndexFileKeys)
	chunks := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	gc := NewGarbageCollector(m, nil, GcOption{Cli: chunks, IndexMeta: m.IndexCatalog.(*index.IndexMeta)})
	t.Cleanup(gc.Close)

	want := make(map[string]struct{})
	for _, layout := range []indexpb.IndexStorePathVersion{
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	} {
		artifact := *record
		artifact.IndexStorePathVersion = layout
		artifact.IndexFileKeys = []string{"orphan"}
		for _, version := range []int64{record.IndexVersion, record.IndexVersion * 10} {
			artifact.IndexVersion = version
			file := meta.MigrationIndexPaths(chunks.RootPath(), "", &artifact)[0]
			require.NoError(t, chunks.Write(ctx, file, []byte("artifact")))
			if version == record.IndexVersion {
				want[file] = struct{}{}
			}
		}
	}
	require.NoError(t, m.DropSegment(ctx, record.SegmentID))
	reads := store.ReadCount
	_, err := m.SegmentIndexFilePaths(ctx, record, nil)
	require.ErrorIs(t, err, merr.ErrSegmentNotFound)
	files, err := gc.resolveRetainedIndexFiles(ctx, nil, record)
	require.NoError(t, err)
	require.Equal(t, want, files)
	require.Equal(t, reads, store.ReadCount)
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
	files, err := gc.resolveRetainedIndexFiles(ctx, nil, record)
	require.NoError(t, err)
	go func() {
		defer close(retired)
		gc.recycleManifestSegmentIndexes(ctx, []segmentIndexGCItem{{segIdx: record, files: files}})
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
