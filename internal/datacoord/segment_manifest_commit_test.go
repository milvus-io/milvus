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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Exercise the actual 3.0 catalog and both in-memory projections when the
// pointer changes during preparation or a replace exceeds the atomic KV limit.
func TestBackfillPublicationFences(t *testing.T) {
	ctx := context.Background()
	m, dm, err := index.NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	s := &Server{meta: m, domainMetas: domainMetas{indexMeta: dm.IndexMeta}}
	v1 := packed.MarshalManifestPath("/segment", 1)
	v2 := packed.MarshalManifestPath("/segment", 2)
	v3 := packed.MarshalManifestPath("/segment", 3)
	require.NoError(t, m.AddSegment(ctx, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 7, CollectionID: 1, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3, ManifestPath: v1}}))
	require.NoError(t, dm.IndexMeta.CreateIndex(ctx, &model.Index{CollectionID: 1, FieldID: 100, IndexID: 10}))
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(ctx, &model.SegmentIndex{CollectionID: 1, SegmentID: 7, IndexID: 10, BuildID: 11, IndexState: commonpb.IndexState_Finished}))

	require.NoError(t, m.UpdateSegmentsInfo(ctx, meta.UpdateManifest(7, v2)))
	p := preparedBackfillManifest{segmentID: 7, base: v1, next: v3, droppedFields: map[int64]struct{}{100: {}}}
	err = s.publishPreparedReplacement(ctx, p)
	require.ErrorContains(t, err, "stale segment manifest")

	key := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.Key
	old := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetValue()
	t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(key, old)) })
	require.NoError(t, paramtable.Get().Save(key, "1"))
	p.base = v2
	err = s.publishPreparedReplacement(ctx, p)
	require.ErrorContains(t, err, "atomic composite update")
	persisted, err := m.Catalog.ListSegments(ctx, 1)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	require.Equal(t, v2, persisted[0].GetManifestPath())
	require.Equal(t, v2, m.GetSegment(ctx, 7).GetManifestPath())
	records, err := m.Catalog.ListSegmentIndexes(ctx, 1)
	require.NoError(t, err)
	require.Len(t, records, 1)
	_, exists := dm.IndexMeta.GetIndexJob(11)
	require.True(t, exists)
}

// A revision published after classification can carry unrelated column groups.
// Verify the complete native read/modify/write and the durable catalog pointer.
func TestBackfillDeltaRebasesOnPublishedRevision(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: base}
	group := func(field string) packed.ColumnGroupEntry {
		return packed.ColumnGroupEntry{Columns: []string{field}, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{Path: path.Join(base, "_data", field+".parquet"), EndIndex: 5}}}
	}
	initial, err := packed.CommitManifestUpdates(base, 0, cfg, &packed.ManifestUpdates{ColumnGroups: []packed.ColumnGroupEntry{group("100")}})
	require.NoError(t, err)
	m, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(ctx, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 7, CollectionID: 1, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3, ManifestPath: initial}}))

	next, err := packed.CommitManifestUpdates(base, 1, cfg, &packed.ManifestUpdates{ColumnGroups: []packed.ColumnGroupEntry{group("102")}})
	require.NoError(t, err)
	require.NoError(t, m.UpdateSegmentsInfo(ctx, meta.UpdateManifest(7, next)))
	entry := &BackfillSegment{Ops: []BackfillManifestOp{{Type: "add", Columns: []string{"101"}, Format: "parquet", RowCount: 5, Files: []BackfillManifestFile{{Path: path.Join(base, "_data", "101.parquet"), EndIndex: 5}}}}}
	updates, err := opsToManifestUpdates(entry, base)
	require.NoError(t, err)
	server := &Server{meta: m}
	commit := SegmentManifestCommit{SegmentID: 7, StorageConfig: cfg, classifiedManifest: initial, entry: entry, Mutation: ManifestMutation{Type: ManifestMutationCommitUpdates, Updates: updates}}
	require.NoError(t, server.commitSegmentManifests(ctx, []SegmentManifestCommit{commit}))
	pathV3 := m.GetSegment(ctx, 7).GetManifestPath()
	persisted, err := m.Catalog.ListSegments(ctx, 1)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	require.Equal(t, pathV3, persisted[0].GetManifestPath())
	groups, err := packed.ReadManifestColumnGroups(pathV3, cfg)
	require.NoError(t, err)
	require.ElementsMatch(t, []packed.ColumnGroupEntry{group("100"), group("101"), group("102")}, groups)

	require.NoError(t, server.commitSegmentManifests(ctx, []SegmentManifestCommit{commit}))
	require.Equal(t, pathV3, m.GetSegment(ctx, 7).GetManifestPath())
}
