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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/gc"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	datacatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func compactionAdmissionMeta(t *testing.T) *meta.Meta {
	t.Helper()
	p := paramtable.Get()
	old := p.DataCoordCfg.EnableSortCompaction.GetValue()
	require.NoError(t, p.Save(p.DataCoordCfg.EnableSortCompaction.Key, "true"))
	t.Cleanup(func() { require.NoError(t, p.Save(p.DataCoordCfg.EnableSortCompaction.Key, old)) })
	catalog := datacatalog.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m := meta.NewEmptyTestMeta()
	m.Catalog = catalog
	m.Ctx = t.Context()
	m.AddCollection(&meta.CollectionInfo{ID: 1, Schema: readinessVectorSchema()})
	m.IndexCatalog = newReadinessIndexMeta(1, map[int64]*model.Index{
		10: {CollectionID: 1, FieldID: 2, IndexID: 10},
		11: {CollectionID: 1, FieldID: 3, IndexID: 11},
		12: {CollectionID: 1, FieldID: 3, IndexID: 12},
	}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	manager, err := dataview.RecoverManager(ctx, catalog, func(context.Context, int64) (bool, error) { return true, nil }, m.LoadableProjection, nil, nil)
	require.NoError(t, err)
	m.DataViewManager = manager
	_, err = manager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v1"}, TransformStarts: map[string]uint64{"v1": 50}})
	require.NoError(t, err)
	return m
}

func admissionSegment(id int64, parents ...int64) *meta.SegmentInfo {
	return meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: id, CollectionID: 1, PartitionID: 1, InsertChannel: "v1", Level: datapb.SegmentLevel_L1,
		State: commonpb.SegmentState_Flushed, NumOfRows: 2000, CompactionFrom: parents,
		CreatedByCompaction: len(parents) > 0, IsSorted: true, TransformStartAfterTimetick: 50,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: id}}}},
	})
}

func admissionRetire(m *meta.Meta, ids ...int64) {
	for _, id := range ids {
		segment := m.GetSegment(context.Background(), id).Clone()
		segment.State = commonpb.SegmentState_Dropped
		segment.Compacted = true
		m.Segments.SetSegment(id, segment)
	}
}

func admissionFinishIndexes(m *meta.Meta, id int64, indexes ...int64) {
	states, ok := m.IndexCatalog.(*index.IndexMeta).SegmentIndexes.Get(id)
	if !ok {
		states = typeutil.NewConcurrentMap[int64, *model.SegmentIndex]()
		m.IndexCatalog.(*index.IndexMeta).SegmentIndexes.Insert(id, states)
	}
	for _, index := range indexes {
		states.Insert(index, &model.SegmentIndex{CollectionID: 1, SegmentID: id, IndexID: index, IndexState: commonpb.IndexState_Finished})
	}
}

func admissionView(t *testing.T, m *meta.Meta) ([]int64, *viewpb.DataVersion) {
	t.Helper()
	ref, err := m.DataViewManager.Latest(t.Context(), 1)
	require.NoError(t, err)
	defer ref.Deref()
	var ids []int64
	for _, shard := range ref.DataView().GetShards() {
		for _, part := range shard.GetPartitions() {
			ids = append(ids, part.GetSegmentIds()...)
		}
	}
	return ids, ref.Version()
}

func admissionRecompute(t *testing.T, m *meta.Meta, want ...int64) *viewpb.DataVersion {
	t.Helper()
	_, err := m.DataViewManager.RecomputeNow(t.Context(), 1, m.LoadableProjection)
	require.NoError(t, err)
	ids, version := admissionView(t, m)
	require.ElementsMatch(t, want, ids)
	return version
}

func TestDataViewCompactionAdmissionAllOutputsAndAllIndexes(t *testing.T) {
	m := compactionAdmissionMeta(t)
	for _, id := range []int64{1, 2} {
		m.Segments.SetSegment(id, admissionSegment(id))
	}
	before := admissionRecompute(t, m, 1, 2)
	for _, id := range []int64{3, 4} {
		m.Segments.SetSegment(id, admissionSegment(id, 1, 2))
	}
	// The output-before-input retirement recovery window cannot expose both.
	require.Equal(t, before, admissionRecompute(t, m, 1, 2))
	admissionRetire(m, 1, 2)
	small := m.GetSegment(t.Context(), 4).Clone()
	small.NumOfRows = 1
	m.Segments.SetSegment(4, small)
	admissionFinishIndexes(m, 3, 10, 11, 12)
	admissionFinishIndexes(m, 4, 10, 11)
	require.Equal(t, before, admissionRecompute(t, m, 1, 2), "the second JSON-path index is still missing")
	admissionFinishIndexes(m, 4, 12)
	after := admissionRecompute(t, m, 3, 4)
	require.Equal(t, before.GetCompactVersion()+1, after.GetCompactVersion())
	// Index DDL after admission must not resurrect the compacted ancestors.
	m.IndexCatalog.(*index.IndexMeta).Indexes[1][13] = &model.Index{CollectionID: 1, FieldID: 3, IndexID: 13}
	require.Equal(t, after, admissionRecompute(t, m, 3, 4))
	// Recovery receives the persisted admission frontier, too.
	recovered, err := dataview.RecoverManager(t.Context(), m.Catalog.(dataview.Catalog), func(context.Context, int64) (bool, error) { return true, nil }, m.LoadableProjection, []int64{1}, map[int64][]string{1: {"v1"}})
	require.NoError(t, err)
	m.DataViewManager = recovered
	require.Equal(t, after, admissionRecompute(t, m, 3, 4))
}

func TestDataViewCompactionAdmissionFlattensUnsortedChain(t *testing.T) {
	m := compactionAdmissionMeta(t)
	for _, id := range []int64{1, 2} {
		m.Segments.SetSegment(id, admissionSegment(id))
	}
	before := admissionRecompute(t, m, 1, 2)
	held, err := m.DataViewManager.Latest(t.Context(), 1)
	require.NoError(t, err)
	for _, id := range []int64{3, 4} {
		s := admissionSegment(id, 1, 2)
		s.IsSorted = false
		m.Segments.SetSegment(id, s)
		admissionFinishIndexes(m, id, 10, 11, 12)
	}
	admissionRetire(m, 1, 2)
	require.Equal(t, before, admissionRecompute(t, m, 1, 2), "even indexed unsorted outputs cannot be admitted")
	m.Segments.SetSegment(5, admissionSegment(5, 3))
	m.Segments.SetSegment(6, admissionSegment(6, 4))
	admissionRetire(m, 3, 4)
	admissionFinishIndexes(m, 5, 10, 11, 12)
	require.Equal(t, before, admissionRecompute(t, m, 1, 2), "one sorted branch is insufficient for the M:N replacement")
	collector := &gc.GarbageCollector{Meta: m, Option: gc.GcOption{DataViewGC: m.DataViewManager}}
	protected := collector.DataViewCompactionProtectionForTest(m.SelectSegments(t.Context(), meta.WithCollection(1)))
	for _, id := range []int64{1, 2, 3, 4, 5, 6} {
		require.Contains(t, protected, id)
	}
	admissionFinishIndexes(m, 6, 10, 11, 12)
	after := admissionRecompute(t, m, 5, 6)
	require.Equal(t, before.GetCompactVersion()+1, after.GetCompactVersion())
	require.NoError(t, m.DataViewManager.GarbageCollect(t.Context(), 1, 1))
	protected = collector.DataViewCompactionProtectionForTest(m.SelectSegments(t.Context(), meta.WithCollection(1)))
	require.Contains(t, protected, int64(1), "old QueryViews still retain the parent frontier")
	held.Deref()
	require.NoError(t, m.DataViewManager.GarbageCollect(t.Context(), 1, 1))
	protected = collector.DataViewCompactionProtectionForTest(m.SelectSegments(t.Context(), meta.WithCollection(1)))
	for _, id := range []int64{1, 2, 3, 4} {
		require.NotContains(t, protected, id)
	}
}

func TestDataViewCompactionAdmissionMissingParentFailsClosed(t *testing.T) {
	m := compactionAdmissionMeta(t)
	m.Segments.SetSegment(1, admissionSegment(1))
	before := admissionRecompute(t, m, 1)
	m.Segments.SetSegment(3, admissionSegment(3, 1, 2))
	admissionRetire(m, 1)
	_, err := m.DataViewManager.RecomputeNow(t.Context(), 1, m.LoadableProjection)
	require.ErrorContains(t, err, "no complete ready ancestor frontier")
	ids, after := admissionView(t, m)
	require.Equal(t, []int64{1}, ids)
	require.Equal(t, before, after)
}

func TestDataViewCompactionAdmissionNoIndexAndEmptyOutput(t *testing.T) {
	m := compactionAdmissionMeta(t)
	m.IndexCatalog.(*index.IndexMeta).Indexes[1] = nil
	m.Segments.SetSegment(1, admissionSegment(1))
	before := admissionRecompute(t, m, 1)
	child := admissionSegment(2, 1)
	child.IsSorted = false
	m.Segments.SetSegment(2, child)
	admissionRetire(m, 1)
	require.Equal(t, before, admissionRecompute(t, m, 1))
	sorted := child.Clone()
	sorted.IsSorted = true
	m.Segments.SetSegment(2, sorted)
	admissionRecompute(t, m, 2)
	// All rows removed by the next compaction: no output needs index/sort.
	admissionRetire(m, 2)
	admissionRecompute(t, m)
}
