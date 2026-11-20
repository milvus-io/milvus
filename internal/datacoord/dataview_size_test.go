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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metacache"
	datacatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewCollectionSizeBytes(t *testing.T) {
	const id int64 = 9020
	ctx := t.Context()
	catalog := datacatalog.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	store := metacache.NewMetaStore(nil)
	indexes := index.NewSegmentIndexMeta(nil)
	mt := &meta.Meta{Ctx: ctx, Catalog: catalog, MetaStore: store, Segments: meta.NewSegmentsInfo(store), IndexCatalog: indexes}
	options := dataview.WithCollectionSizeProjector(mt.DataViewSizeBytes, func() bool { return true })
	manager := dataview.NewManager(catalog, nil, options)
	shards := []string{"a", "b"}
	create, err := manager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: id, VChannels: shards})
	require.NoError(t, err)
	check := func(m dataview.Manager, v *viewpb.DataVersion, want int64) {
		t.Helper()
		ref, err := m.Get(ctx, id, v)
		require.NoError(t, err)
		require.NotNil(t, ref)
		defer ref.Deref()
		require.Equal(t, proto.Int64(want), ref.CollectionSizeBytes())
		// A consumer owns the returned scalar.
		*ref.CollectionSizeBytes() = -1
		require.Equal(t, proto.Int64(want), ref.DataView().CollectionSizeBytes)
	}
	check(manager, create, 0)
	store.PutCollection(&meta.CollectionInfo{ID: id, Partitions: []int64{1, 2}})
	put := func(seg *datapb.SegmentInfo, size int64) {
		seg.CollectionID = id
		seg.Stats = &datapb.Statistics{InsertBinlogSize: size}
		mt.Segments.SetSegment(seg.ID, meta.NewSegmentInfo(seg))
	}
	a := dataview.LoadableSegment{SegmentID: 10, PartitionID: 1, VChannel: "a"}
	a2 := dataview.LoadableSegment{SegmentID: 12, PartitionID: 1, VChannel: "a"}
	b := dataview.LoadableSegment{SegmentID: 20, PartitionID: 2, VChannel: "b"}
	c := dataview.LoadableSegment{SegmentID: 30, PartitionID: 1, VChannel: "a"}
	put(&datapb.SegmentInfo{ID: 10, PartitionID: 1, State: commonpb.SegmentState_Growing, TextStatsLogs: map[int64]*datapb.TextIndexStats{3: {LogSize: 6}}}, 1)
	put(&datapb.SegmentInfo{ID: 12, PartitionID: 1, State: commonpb.SegmentState_Growing}, 1)
	put(&datapb.SegmentInfo{ID: 20, PartitionID: 2, State: commonpb.SegmentState_Growing, JsonKeyStats: map[int64]*datapb.JsonKeyStats{4: {LogSize: 7}}}, 2)
	put(&datapb.SegmentInfo{ID: 40, PartitionID: common.AllPartitionsID, Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed}, 9)
	put(&datapb.SegmentInfo{ID: 50, PartitionID: 2, State: commonpb.SegmentState_Growing}, 5)
	// The replacement already exists in SegmentMeta. It must first enter a DataView.
	put(&datapb.SegmentInfo{ID: 30, PartitionID: 1, State: commonpb.SegmentState_Flushed, IsInvisible: true}, 11)
	for _, extra := range []*datapb.SegmentInfo{
		{ID: 60, PartitionID: 1, State: commonpb.SegmentState_Flushed},
		{ID: 61, PartitionID: 3, State: commonpb.SegmentState_Growing},
		{ID: 62, PartitionID: 1, State: commonpb.SegmentState_Growing, IsImporting: true},
		{ID: 63, PartitionID: 1, State: commonpb.SegmentState_Growing, IsFake: true},
		{ID: 64, PartitionID: 1, State: commonpb.SegmentState_Flushing, CompactionFrom: []int64{10}},
	} {
		put(extra, 1000)
	}
	indexes.Indexes[id] = map[int64]*model.Index{1: {CollectionID: id, IndexID: 1}}
	indexes.UpdateSegmentIndex(&model.SegmentIndex{CollectionID: id, SegmentID: 10, IndexID: 1, BuildID: 7, IndexState: commonpb.IndexState_Finished, IndexSerializedSize: 18})

	flush, commit, abort, err := manager.PrepareFlush(ctx, dataview.FlushDataViewEvent{CollectionID: id, Segments: []dataview.LoadableSegment{a, a2, b}})
	require.NoError(t, err)
	defer abort()
	stats := &datapb.Statistics{InsertBinlogSize: 20, StatsBinlogSize: 3, DeltaBinlogSize: 2}
	ok, err := mt.UpdateSegmentsInfoAndDataView(ctx, flush,
		meta.UpdateSegmentStats(10, stats), meta.UpdateSegmentStats(20, stats),
		meta.UpdateSegmentStats(12, &datapb.Statistics{InsertBinlogSize: 8}), meta.UpdateStatusOperator(12, commonpb.SegmentState_Flushed),
		meta.UpdateStatusOperator(10, commonpb.SegmentState_Flushed), meta.UpdateStatusOperator(20, commonpb.SegmentState_Flushed))
	require.NoError(t, err)
	require.True(t, ok)
	commit()
	old := flush.Version()
	// Three flushed segments of 25, 25 and 8 bytes, text 6, JSON 7, field index 18, L0 9 and growing 5.
	check(manager, old, 103)

	// A Dropped parent still belongs to this snapshot. Rebuilding the field
	// index changes what new versions may sample, never the persisted value.
	put(&datapb.SegmentInfo{ID: 10, PartitionID: 1, State: commonpb.SegmentState_Dropped, TextStatsLogs: map[int64]*datapb.TextIndexStats{3: {LogSize: 6}}}, 25)
	indexes.UpdateSegmentIndex(&model.SegmentIndex{CollectionID: id, SegmentID: 10, IndexID: 1, BuildID: 7, IndexState: commonpb.IndexState_Finished, IndexSerializedSize: 38})
	schema, err := manager.AdvanceSchema(ctx, id, 1, nil)
	require.NoError(t, err)
	check(manager, schema, 123)
	check(manager, old, 103)

	change := dataview.ChangeGroupDataViewEvent{CollectionID: id, NewSegments: []dataview.LoadableSegment{c}, SupersededSegmentIDs: []int64{10, 12, 40}}
	next, publish, cancel, err := manager.PublishChange(ctx, change)
	require.NoError(t, err)
	defer cancel()
	require.NoError(t, catalog.SaveDataView(ctx, next))
	publish()
	check(manager, next.DataVersion, 48) // replacement 11, other shard 32, and growing 5
	check(manager, old, 103)

	// Metadata has moved on before another shard or a restarted coordinator pins the old version.
	mt.Segments.DropSegment(10)
	mt.Segments.DropSegment(12)
	mt.Segments.DropSegment(40)
	put(&datapb.SegmentInfo{ID: 50, PartitionID: 2, State: commonpb.SegmentState_Growing}, 80)
	replayed, _, release, err := manager.PublishChange(ctx, change)
	require.NoError(t, err)
	require.True(t, proto.Equal(next, replayed))
	release()
	project := func(context.Context, int64, *viewpb.DataViewOfCollection) ([]dataview.LoadableSegment, error) {
		return []dataview.LoadableSegment{c, b}, nil
	}
	recovered, err := dataview.RecoverManager(ctx, catalog, func(context.Context, int64) (bool, error) { return true, nil },
		project, []int64{id}, map[int64][]string{id: shards}, options)
	require.NoError(t, err)
	check(recovered, old, 103)
	check(recovered, next.DataVersion, 48)

	// A missing selected segment leaves the previous version serving.
	mt.Segments.DropSegment(30)
	_, err = recovered.AdvanceSchema(ctx, id, 2, nil)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	latest, err := recovered.Latest(ctx, id)
	require.NoError(t, err)
	defer latest.Deref()
	require.True(t, proto.Equal(next.DataVersion, latest.Version()))
	require.Equal(t, proto.Int64(48), latest.CollectionSizeBytes())
}
