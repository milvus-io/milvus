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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSegmentChangeEmptyOutputs(t *testing.T) {
	for _, shape := range []string{"mixed", "all_empty", "dropped_empty", "no_segments", "no_op"} {
		t.Run(shape, func(t *testing.T) {
			ctx := t.Context()
			m, err := newMemoryMeta(t)
			require.NoError(t, err)
			schema := readinessStatsSchema()
			schema.Version = 2
			m.AddCollection(&collectionInfo{ID: 10, Schema: schema, VChannelNames: []string{"ch-1"}})
			manager := dataview.NewManager(m.catalog.(dataview.Catalog), nil)
			m.dataViewManager = manager
			_, err = manager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: 10, VChannels: []string{"ch-1"}})
			require.NoError(t, err)
			g := newTestGroup()
			g.NewSegmentIDs = nil
			if shape == "no_op" {
				g.SupersededSegmentIDs = nil
			} else {
				require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
					ID: 2001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
					State: commonpb.SegmentState_Flushed, NumOfRows: 100,
					Binlogs: []*datapb.FieldBinlog{{FieldID: 1}},
				})))
				_, err = manager.RecomputeNow(ctx, 10, m.loadableProjection)
				require.NoError(t, err)
			}
			if shape != "no_segments" && shape != "no_op" {
				state := commonpb.SegmentState_Flushed
				if shape == "dropped_empty" {
					state = commonpb.SegmentState_Dropped
				}
				require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
					ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
					State: state, NumOfRows: 0, IsInvisible: state != commonpb.SegmentState_Dropped,
					// Empty outputs need neither a readable manifest nor stats,
					// sorting or a current schema: they contain no rows to load.
					ManifestPath: "unused-empty-manifest",
				})))
				g.NewSegmentIDs = append(g.NewSegmentIDs, 1001)
			}
			if shape == "mixed" {
				require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
					ID: 1002, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
					State: commonpb.SegmentState_Flushed, NumOfRows: 42, IsInvisible: true,
					IsSorted: true, SchemaVersion: 2, Binlogs: []*datapb.FieldBinlog{{FieldID: 1}},
					TextStatsLogs: map[int64]*datapb.TextIndexStats{2: {}},
					JsonKeyStats:  map[int64]*datapb.JsonKeyStats{3: {JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion}},
				})))
				g.NewSegmentIDs = append(g.NewSegmentIDs, 1002)
			}
			require.NoError(t, m.AddSegmentChangeGroup(ctx, g))
			inspector := newSegmentChangeInspector(ctx, m, newMockAllocator(t))
			inspector.checkAllGroups(ctx)
			require.Equal(t, model.SegmentChangeStateReady, m.GetSegmentChangeGroup(ctx, 10, 1).State)
			if shape != "no_op" {
				require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 2001).GetState(), "parent remains live until publication")
			}
			inspector.checkAllGroups(ctx)
			committed := m.GetSegmentChangeGroup(ctx, 10, 1)
			require.Equal(t, model.SegmentChangeStateCommitted, committed.State)
			require.Equal(t, g.NewSegmentIDs, committed.NewSegmentIDs, "task output identity stays immutable")
			ref, err := manager.Latest(ctx, 10)
			require.NoError(t, err)
			defer ref.Deref()
			var ids []int64
			for _, shard := range ref.DataView().GetShards() {
				for _, partition := range shard.GetPartitions() {
					ids = append(ids, partition.GetSegmentIds()...)
				}
			}
			if shape == "mixed" {
				require.Equal(t, []int64{1002}, ids)
			} else {
				require.Empty(t, ids)
			}
			if empty := m.GetSegment(ctx, 1001); empty != nil {
				require.Equal(t, commonpb.SegmentState_Dropped, empty.GetState())
				require.Zero(t, empty.GetCommitTimestamp())
				require.NoError(t, m.DropSegment(ctx, 1001), "empty metadata is reclaimable after publication")
			}
			if shape == "no_op" {
				require.Equal(t, int64(1), ref.Version().GetStreamingVersion())
				require.Zero(t, ref.Version().GetCompactVersion(), "no output and no retirement burns no view version")
			} else {
				require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 2001).GetState())
			}
		})
	}
}

func TestSegmentChangeEmptyOutputRecovery(t *testing.T) {
	ctx := t.Context()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	empty := m.GetSegment(ctx, 1001).Clone()
	empty.NumOfRows = 0
	empty.State = commonpb.SegmentState_Dropped
	empty.Binlogs = nil
	require.NoError(t, m.catalog.AlterSegments(ctx, []*datapb.SegmentInfo{empty.SegmentInfo}))
	m.segments.SetSegment(1001, empty)
	require.ErrorIs(t, m.DropSegment(ctx, 1001), merr.ErrServiceUnavailable, "GC must retain the no-output evidence while its group is ALIVE")
	recovered := reloadSegmentChangeMeta(t, m)
	require.NoError(t, recovered.reconcileSegmentChangeGroups(ctx, nil, liveSegmentChangeCollection))
	require.Equal(t, model.SegmentChangeStateCommitted, recovered.GetSegmentChangeGroup(ctx, 10, 1).State)
	require.Equal(t, commonpb.SegmentState_Dropped, recovered.GetSegment(ctx, 2001).GetState())
	require.NoError(t, recovered.DropSegment(ctx, 1001))
	// Replay after empty-output GC must keep the completed change committed.
	recovered = reloadSegmentChangeMeta(t, recovered)
	require.NoError(t, recovered.reconcileSegmentChangeGroups(ctx, nil, liveSegmentChangeCollection))
	require.Equal(t, model.SegmentChangeStateCommitted, recovered.GetSegmentChangeGroup(ctx, 10, 1).State)

	// A caller cannot smuggle a zero-row output into the committed visible set.
	m, err = newMemoryMeta(t)
	require.NoError(t, err)
	g := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	require.NoError(t, m.UpdateSegmentsInfo(ctx, func(pack *updateSegmentPack) bool { pack.Get(1001).NumOfRows = 0; return true }))
	g.State = model.SegmentChangeStateCommitted
	require.ErrorIs(t, m.UpdateSegmentsInfoAndChangeGroups(ctx, []metastore.UpdateAction{metastore.SaveSegmentChangeGroup(g)},
		SetSegmentIsInvisible(1001, false), UpdateStatusOperator(2001, commonpb.SegmentState_Dropped)), merr.ErrDataIntegrity)
}

func TestSegmentChangeProvisionalZeroRowsWaitForFinalization(t *testing.T) {
	for _, state := range []commonpb.SegmentState{commonpb.SegmentState_Growing, commonpb.SegmentState_Flushing, commonpb.SegmentState_Importing} {
		t.Run(state.String(), func(t *testing.T) {
			ctx := t.Context()
			m, g := seedStagedInspectorMeta(t, func(seg *SegmentInfo) {
				seg.State = state
				seg.NumOfRows = 0
				seg.Binlogs = nil
			})
			g.CreateTS = time.Now().Unix()
			inspector := newSegmentChangeInspector(ctx, m, newMockAllocator(t))
			inspector.evaluateStaged(ctx, g)
			require.Equal(t, model.SegmentChangeStateStaged, m.GetSegmentChangeGroup(ctx, 10, g.GroupID).State)
			require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateStatusOperator(1001, commonpb.SegmentState_Flushed)))
			inspector.evaluateStaged(ctx, g)
			require.Equal(t, model.SegmentChangeStateReady, m.GetSegmentChangeGroup(ctx, 10, g.GroupID).State)
		})
	}
}

func TestPublishGroupRechecksAllReadinessTerms(t *testing.T) {
	for _, regression := range []string{"sort", "text", "json_format", "data_state"} {
		t.Run(regression, func(t *testing.T) {
			ctx := t.Context()
			m, err := newMemoryMeta(t)
			require.NoError(t, err)
			g := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
			m.GetCollection(10).Schema = readinessStatsSchema()
			makeReady := func(pack *updateSegmentPack) bool {
				seg := pack.Get(1001)
				seg.State = commonpb.SegmentState_Flushed
				seg.IsSorted = true
				seg.TextStatsLogs = map[int64]*datapb.TextIndexStats{2: {}}
				seg.JsonKeyStats = map[int64]*datapb.JsonKeyStats{3: {JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion}}
				return true
			}
			require.NoError(t, m.UpdateSegmentsInfo(ctx, makeReady, func(pack *updateSegmentPack) bool {
				seg := pack.Get(1001)
				switch regression {
				case "sort":
					seg.IsSorted = false
				case "text":
					seg.TextStatsLogs = nil
				case "json_format":
					seg.JsonKeyStats[3].JsonKeyStatsDataFormat = common.JSONStatsDataFormatVersion - 1
				case "data_state":
					seg.State = commonpb.SegmentState_Growing
				}
				return true
			}))
			require.ErrorIs(t, m.publishGroup(ctx, g, newMockAllocator(t)), merr.ErrServiceUnavailable)
			require.True(t, m.GetSegment(ctx, 1001).GetIsInvisible())
			require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 2001).GetState())
			inspector := newSegmentChangeInspector(ctx, m, newMockAllocator(t))
			inspector.tryPublish(ctx, g)
			require.Equal(t, model.SegmentChangeStateStaged, m.GetSegmentChangeGroup(ctx, 10, 1).State)
			require.NoError(t, m.UpdateSegmentsInfo(ctx, makeReady))
			inspector.checkAllGroups(ctx)
			inspector.checkAllGroups(ctx)
			require.Equal(t, model.SegmentChangeStateCommitted, m.GetSegmentChangeGroup(ctx, 10, 1).State)
		})
	}
}
