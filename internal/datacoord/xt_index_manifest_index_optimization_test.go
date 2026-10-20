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
	"path"

	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

func TestEmptyManifestMarkerNormalizedAcrossRestarts(t *testing.T) {
	mockManifestIndexSubmissions(t)
	ctx := context.Background()
	catalog := catalogkv.NewCatalog(NewMetaMemoryKV(), "", "")
	m, _ := bootMetaForRestart(t, catalog, 300)
	manifest := packed.MarshalManifestPath("/tmp/test-restart/insert_log/300/30/8001", 1)
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 8001, CollectionID: 300, PartitionID: 30, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3, ManifestPath: manifest, ManifestHasIndex: true,
	})))
	reads := 0
	reader := mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
		reads++
		return nil, nil
	}).Build()
	defer reader.UnPatch()
	failure := mockey.Mock((*catalogkv.Catalog).AlterSegments).Return(merr.ErrServiceUnavailable).Build()
	t.Cleanup(func() { failure.UnPatch() })
	require.ErrorIs(t, m.reloadSegmentIndexesFromManifests(ctx), merr.ErrServiceUnavailable)
	failure.UnPatch()
	require.True(t, m.GetSegment(ctx, 8001).GetManifestHasIndex())
	reads = 0
	first, _ := bootMetaForRestart(t, catalog, 300)
	require.False(t, first.GetSegment(ctx, 8001).GetManifestHasIndex())
	require.Equal(t, 1, reads)
	second, _ := bootMetaForRestart(t, catalog, 300)
	require.False(t, second.GetSegment(ctx, 8001).GetManifestHasIndex())
	require.Equal(t, 1, reads, "subsequent startup must not read historical empty manifests")
	// A stale normalization may not clear a replacement pointer's marker.
	require.NoError(t, second.UpdateSegmentsInfo(ctx, UpdateManifest(8001, packed.MarshalManifestPath("/tmp/test-restart/insert_log/300/30/8001", 2)), UpdateManifestHasIndex(8001)))
	require.NoError(t, second.UpdateSegmentsInfo(ctx, clearEmptyManifestIndexMarker(8001, manifest)))
	require.True(t, second.GetSegment(ctx, 8001).GetManifestHasIndex())
}

func TestManifestLastIndexDropClearsMarkerAtomically(t *testing.T) {
	ctx := context.Background()
	catalog := catalogkv.NewCatalog(NewMetaMemoryKV(), "", "")
	m, _ := bootMetaForRestart(t, catalog, 300)
	root := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	base := path.Join(root, "insert_log/300/30/8001")
	entry := func(id int64) packed.ManifestIndexInfo {
		prefix := metautil.NewIndexPathBuilder(root, 0, 300, 30, 8001, id, 1).BuildPrefix()
		relative, err := packed.ManifestIndexRelativePath(base, prefix)
		require.NoError(t, err)
		return packed.ManifestIndexInfo{ColumnName: "vector", IndexName: "idx", IndexType: "HNSW", Path: relative, FieldID: 101, IndexID: id, BuildID: id, IndexVersion: 1, NumRows: 100, IndexFileKeys: []string{"index.bin"}}
	}
	manifest, err := packed.CommitManifestUpdates(base, 0, cfg, &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{entry(1), entry(2)}})
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{ID: 8001, CollectionID: 300, PartitionID: 30, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3, ManifestPath: manifest, ManifestHasIndex: true})))
	drop := func(id int64) error {
		return m.CommitSegmentManifest(ctx, SegmentManifestCommit{SegmentID: 8001, StorageConfig: cfg, Mutation: ManifestMutation{Type: ManifestMutationCommitUpdates, Updates: &packed.ManifestUpdates{DropIndexes: []packed.DropIndexEntry{{IndexID: id, ExpectedBuildID: id}}}}})
	}
	require.NoError(t, drop(1))
	require.True(t, m.GetSegment(ctx, 8001).GetManifestHasIndex(), "one index remains")
	previous := m.GetSegment(ctx, 8001).GetManifestPath()
	readFailure := mockey.Mock((*meta).readManifestIndexesWithIO).Return([]packed.ManifestIndexInfo(nil), merr.ErrServiceUnavailable).Build()
	t.Cleanup(func() { readFailure.UnPatch() })
	failure := mockey.Mock((*catalogkv.Catalog).Update).Return(merr.ErrServiceUnavailable).Build()
	t.Cleanup(func() { failure.UnPatch() })
	require.ErrorIs(t, drop(2), merr.ErrServiceUnavailable)
	failure.UnPatch()
	require.Equal(t, previous, m.GetSegment(ctx, 8001).GetManifestPath())
	require.True(t, m.GetSegment(ctx, 8001).GetManifestHasIndex())
	require.NoError(t, drop(2))
	require.Zero(t, readFailure.Times(), "derive the marker from the commit input, without a final-revision read")
	readFailure.UnPatch()
	current := m.GetSegment(ctx, 8001)
	require.False(t, current.GetManifestHasIndex())
	entries, err := packed.GetManifestIndexInfos(current.GetManifestPath(), cfg)
	require.NoError(t, err)
	require.Empty(t, entries)
	reader := mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, merr.ErrServiceUnavailable).Build()
	defer reader.UnPatch()
	restarted, _ := bootMetaForRestart(t, catalog, 300)
	require.False(t, restarted.GetSegment(ctx, 8001).GetManifestHasIndex(), "restart must skip the verified empty revision")
}
