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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// seedStagedInspectorMeta builds a meta with one STAGED group whose member
// (1001) is Flushed, sorted, invisible and has binlogs — ready under the
// default config (no index on the collection, no text/JSON stats). The member
// can be mutated via opts to force a specific not-ready condition.
func seedStagedInspectorMeta(t *testing.T, memberOpts ...func(*SegmentInfo)) (*meta, *model.SegmentChangeGroup) {
	t.Helper()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()
	m.AddCollection(&collectionInfo{ID: 10, Schema: readinessVectorSchema()})

	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, IsInvisible: true,
		NumOfRows: 42, IsSorted: true,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
	})
	for _, opt := range memberOpts {
		opt(seg)
	}
	require.NoError(t, m.AddSegment(ctx, seg))

	group := &model.SegmentChangeGroup{
		GroupID: 1, Source: model.SegmentChangeSourceMixCompaction, CollectionID: 10, PartitionID: 100,
		State: model.SegmentChangeStateStaged, NewSegmentIDs: []int64{1001}, CreateTS: time.Now().Unix(),
	}
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))
	return m, group
}

func TestSegmentChangeInspector_StagedToReady(t *testing.T) {
	m, group := seedStagedInspectorMeta(t)
	i := newSegmentChangeInspector(context.Background(), m, newMockAllocator(t))
	i.evaluateStaged(context.Background(), group)

	got := m.GetSegmentChangeGroup(context.Background(), 10, 1)
	require.NotNil(t, got)
	require.Equal(t, model.SegmentChangeStateReady, got.State)
	require.NotZero(t, got.ReadyTS)
}

func TestSegmentChangeInspector_StagedNotReady(t *testing.T) {
	m, group := seedStagedInspectorMeta(t, func(seg *SegmentInfo) { seg.IsSorted = false })
	i := newSegmentChangeInspector(context.Background(), m, newMockAllocator(t))
	i.evaluateStaged(context.Background(), group)

	got := m.GetSegmentChangeGroup(context.Background(), 10, 1)
	require.Equal(t, model.SegmentChangeStateStaged, got.State, "unsorted member stays staged while sort is enabled")
}

func TestSegmentChangeInspector_StagedTimeoutAborts(t *testing.T) {
	Params.Save(Params.DataCoordCfg.SegmentChangeStagingTimeout.Key, "1")
	defer Params.Reset(Params.DataCoordCfg.SegmentChangeStagingTimeout.Key)

	m, group := seedStagedInspectorMeta(t, func(seg *SegmentInfo) { seg.IsSorted = false })
	group.CreateTS = time.Now().Unix() - 100
	// Persist the old CreateTS so the transition merge does not zero it.
	require.NoError(t, m.UpdateSegmentChangeGroup(context.Background(), group))

	i := newSegmentChangeInspector(context.Background(), m, newMockAllocator(t))
	i.evaluateStaged(context.Background(), group)

	got := m.GetSegmentChangeGroup(context.Background(), 10, 1)
	require.Equal(t, model.SegmentChangeStateAborted, got.State)
	require.Contains(t, got.FailReason, "staging timeout")
}

func TestSegmentChangeInspector_MemberDroppedFails(t *testing.T) {
	m, group := seedStagedInspectorMeta(t)
	require.NoError(t, m.UpdateSegmentsInfo(context.Background(), UpdateStatusOperator(1001, commonpb.SegmentState_Dropped)))

	i := newSegmentChangeInspector(context.Background(), m, newMockAllocator(t))
	i.evaluateStaged(context.Background(), group)

	got := m.GetSegmentChangeGroup(context.Background(), 10, 1)
	require.Equal(t, model.SegmentChangeStateFailed, got.State)
	require.Contains(t, got.FailReason, "externally dropped")
}

func TestSegmentChangeInspector_SchemaFenceFails(t *testing.T) {
	// C23: a schema bump while staged is unconvergeable — fail immediately
	// instead of waiting out the staging timeout.
	m, group := seedStagedInspectorMeta(t, func(seg *SegmentInfo) { seg.SchemaVersion = 1 })
	m.GetCollection(10).Schema.Version = 2

	i := newSegmentChangeInspector(context.Background(), m, newMockAllocator(t))
	i.evaluateStaged(context.Background(), group)

	got := m.GetSegmentChangeGroup(context.Background(), 10, 1)
	require.Equal(t, model.SegmentChangeStateFailed, got.State)
	require.Contains(t, got.FailReason, "schema version behind")
}

func TestSegmentChangeInspector_ReadyPublishes(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)

	i := newSegmentChangeInspector(context.Background(), m, newMockAllocator(t))
	i.tryPublish(context.Background(), ready)

	require.Equal(t, model.SegmentChangeStateCommitted, m.GetSegmentChangeGroup(context.Background(), 10, 1).State)
	member := m.GetSegment(context.Background(), 1001)
	require.False(t, member.GetIsInvisible())
	parent := m.GetSegment(context.Background(), 2001)
	require.Equal(t, commonpb.SegmentState_Dropped, parent.GetState())
}

func TestSegmentChangeInspector_ReadyMemberDroppedFails(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	require.NoError(t, m.UpdateSegmentsInfo(context.Background(), UpdateStatusOperator(1001, commonpb.SegmentState_Dropped)))

	i := newSegmentChangeInspector(context.Background(), m, newMockAllocator(t))
	i.tryPublish(context.Background(), ready)

	got := m.GetSegmentChangeGroup(context.Background(), 10, 1)
	require.Equal(t, model.SegmentChangeStateFailed, got.State)
	require.Contains(t, got.FailReason, "publish failed")
}

func TestSegmentChangeInspector_CollectionDroppedAborts(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	dataViewCatalog, ok := m.catalog.(dataview.Catalog)
	require.True(t, ok)
	dv := dataview.NewManager(dataViewCatalog, nil)
	m.dataViewManager = dv
	_, err = dv.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: 10, VChannels: []string{"ch-1"}})
	require.NoError(t, err)

	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	_, err = dv.OnDropCollection(ctx, 10)
	require.NoError(t, err)

	i := newSegmentChangeInspector(ctx, m, newMockAllocator(t))
	i.tryPublish(ctx, ready)

	got := m.GetSegmentChangeGroup(ctx, 10, 1)
	require.Equal(t, model.SegmentChangeStateAborted, got.State)
}

func TestSegmentChangeInspector_NilMetaNoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	i := newSegmentChangeInspector(ctx, nil, nil)
	require.NotPanics(t, i.Start)
	i.Stop()
	cancel()
}

func TestSegmentChangeInspector_CheckAllGroups(t *testing.T) {
	m, group := seedStagedInspectorMeta(t)
	i := newSegmentChangeInspector(context.Background(), m, newMockAllocator(t))
	i.checkAllGroups(context.Background())
	require.Equal(t, model.SegmentChangeStateReady, m.GetSegmentChangeGroup(context.Background(), 10, 1).State)
	_ = group
}

func TestSegmentChangeInspector_FailFastSkipsWaitingMembers(t *testing.T) {
	// F4: the unconvergeable scan must run before the wait scan. Member 1001 is
	// merely converging (unsorted, waits on the sort term) while member 1002 is
	// schema-behind; the C23 fail-fast must fire THIS round, not after 1001
	// converges.
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()
	m.AddCollection(&collectionInfo{ID: 10, Schema: readinessVectorSchema()})

	for _, seg := range []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, IsInvisible: true,
			NumOfRows: 42, IsSorted: false,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
		}),
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1002, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, IsInvisible: true,
			NumOfRows: 42, IsSorted: true,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: 2}}}},
		}),
	} {
		require.NoError(t, m.AddSegment(ctx, seg))
	}
	m.GetCollection(10).Schema.Version = 2 // member 1002's SchemaVersion (0) is behind

	group := &model.SegmentChangeGroup{
		GroupID: 1, Source: model.SegmentChangeSourceMixCompaction, CollectionID: 10, PartitionID: 100,
		State: model.SegmentChangeStateStaged, NewSegmentIDs: []int64{1001, 1002}, CreateTS: time.Now().Unix(),
	}
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

	i := newSegmentChangeInspector(ctx, m, newMockAllocator(t))
	i.evaluateStaged(ctx, group)

	got := m.GetSegmentChangeGroup(ctx, 10, 1)
	require.Equal(t, model.SegmentChangeStateFailed, got.State, "C23 must fail-fast despite an earlier waiting member")
	require.Contains(t, got.FailReason, "schema version behind")
}

func TestSegmentChangeInspector_UnconvergeableNoFootprint(t *testing.T) {
	// F3: a member with no data footprint can never be published — the publish
	// is rejected in Phase 2, and the group must be FAILED (not retried).
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()
	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)

	member := m.GetSegment(ctx, 1001).Clone()
	member.Binlogs = nil
	member.ManifestPath = ""
	m.segments.SetSegment(1001, member)

	i := newSegmentChangeInspector(ctx, m, newMockAllocator(t))
	i.tryPublish(ctx, ready)

	got := m.GetSegmentChangeGroup(ctx, 10, 1)
	require.Equal(t, model.SegmentChangeStateFailed, got.State)
	require.Contains(t, got.FailReason, "publish failed")
}
