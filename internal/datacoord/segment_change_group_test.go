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
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// seedPublishMeta builds a meta with one staged member (1001, IsInvisible=true,
// with binlogs) and one visible superseded parent (2001), plus the group in
// READY state. It returns the ready group clone.
func seedPublishMeta(t *testing.T, m *meta, source model.SegmentChangeSource) *model.SegmentChangeGroup {
	t.Helper()
	ctx := context.Background()
	m.AddCollection(&collectionInfo{ID: 10, Schema: readinessVectorSchema()})

	for _, seg := range []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, IsInvisible: true,
			NumOfRows: 42,
			Stats:     &datapb.Statistics{InsertBinlogSize: 131073, StatsBinlogSize: 65536, DeltaBinlogSize: 65536},
			Binlogs:   []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
		}),
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 2001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: 2}}}},
		}),
	} {
		require.NoError(t, m.AddSegment(ctx, seg))
	}

	group := newTestGroup()
	group.Source = source
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))
	ready := group.Clone()
	ready.State = model.SegmentChangeStateReady
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready))
	return ready
}

func TestPublishGroup_Dataviewless(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	// No dataViewManager: the dataview-less (3.0-compatible) publish path, a
	// pure SegmentMeta composite write.
	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	require.NoError(t, m.publishGroup(ctx, ready, newMockAllocator(t)))

	member := m.GetSegment(ctx, 1001)
	require.NotNil(t, member)
	require.False(t, member.GetIsInvisible(), "member must become visible at publish")
	require.Zero(t, member.GetCommitTimestamp(), "compaction output must keep CommitTimestamp=0 (C5)")

	parent := m.GetSegment(ctx, 2001)
	require.NotNil(t, parent)
	require.Equal(t, commonpb.SegmentState_Dropped, parent.GetState(), "superseded parent must be retired at publish")

	group := m.GetSegmentChangeGroup(ctx, 10, 1)
	require.NotNil(t, group)
	require.Equal(t, model.SegmentChangeStateCommitted, group.State)
	require.False(t, m.HasStagedSegment(ctx, 10, 1001), "COMMITTED group releases its member claim")
}

func TestPublishGroup_WithDataView(t *testing.T) {
	p := paramtable.Get()
	for _, setting := range []struct {
		item  *paramtable.ParamItem
		value string
	}{
		{&p.QueryViewCfg.LogicalSizeSource, "insertBytes"},
		{&p.QueryViewCfg.LogicalSizeBytesPerUnit, "64KB"},
		{&p.QueryViewCfg.LogicalSizeSegmentBaseCost, "16"},
	} {
		original := setting.item.GetValue()
		require.NoError(t, p.Save(setting.item.Key, setting.value))
		t.Cleanup(func() { require.NoError(t, p.Save(setting.item.Key, original)) })
	}
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	dataViewCatalog, ok := m.catalog.(dataview.Catalog)
	require.True(t, ok, "kv catalog must implement the DataView Catalog interface")
	dv := dataview.NewManager(dataViewCatalog, nil)
	m.dataViewManager = dv
	_, err = dv.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: 10, VChannels: []string{"ch-1"}})
	require.NoError(t, err)

	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	require.NoError(t, m.publishGroup(ctx, ready, newMockAllocator(t)))

	// DataView side: one snapshot with the member in, the parent out, and only
	// compact_version advanced (streaming_version untouched by staging).
	ref, err := dv.Latest(ctx, 10)
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	latest := ref.DataView()
	require.Equal(t, &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}, latest.GetDataVersion())
	partition := latest.GetShards()[0].GetPartitions()[0]
	require.Equal(t, []int64{1001}, partition.GetSegmentIds())
	require.Equal(t, []int64{0}, partition.GetSegmentManifestVersions())
	stats, ok := ref.Stats(1001)
	require.True(t, ok, "published member carries its RowNum footprint")
	require.Equal(t, int64(42), stats.RowNum)
	require.Equal(t, int64(131073), stats.Size)
	require.Equal(t, int64(19), stats.LogicalSize, "change-group publication uses insert bytes, not row fallback or stats/delta bytes")
}

func TestPublishGroup_ImportSetsCommitTS(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	ready := seedPublishMeta(t, m, model.SegmentChangeSourceImportJob)
	alloc := newMockAllocator(t)
	require.NoError(t, m.publishGroup(ctx, ready, alloc))

	member := m.GetSegment(ctx, 1001)
	require.NotNil(t, member)
	require.False(t, member.GetIsInvisible())
	// newMockAllocator's AllocTimestamp returns the counter value (>= 1); the
	// member binlogs carry TimestampTo=0 so the commit fence passes.
	require.NotZero(t, member.GetCommitTimestamp(), "import rows become officially present at commit_ts")
	require.Equal(t, uint64(1), member.GetCommitTimestamp())
}

func TestPublishGroup_RejectsNonReady(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	staged := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	staged.State = model.SegmentChangeStateStaged
	err = m.publishGroup(ctx, staged, newMockAllocator(t))
	require.Error(t, err)
	require.ErrorContains(t, err, "requires READY")
}

func TestPublishGroup_MemberMissing(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	// Externally drop the staged member (truncate / drop-by-time), so the
	// publish cannot flip it visible.
	require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateStatusOperator(1001, commonpb.SegmentState_Dropped)))

	err = m.publishGroup(ctx, ready, newMockAllocator(t))
	require.Error(t, err)
	require.ErrorContains(t, err, "externally dropped")
	// The group stays READY for the inspector to fail it (unconvergeable).
	group := m.GetSegmentChangeGroup(ctx, 10, 1)
	require.Equal(t, model.SegmentChangeStateReady, group.State)
}

func TestPublishGroup_CollectionDropped(t *testing.T) {
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

	err = m.publishGroup(ctx, ready, newMockAllocator(t))
	require.ErrorIs(t, err, errSegmentChangeGroupCollectionDropped)
}

func TestPublishGroup_IdempotentReplay(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)

	// Simulate the recovery probe outcome "members visible but no COMMITTED
	// evidence" (design C7): the members were flipped by an earlier chunked
	// fallback flush while the commit marker did not land. The group is still
	// ALIVE (READY), so a replayed publish must converge idempotently.
	require.NoError(t, m.UpdateSegmentsInfo(ctx, SetSegmentIsInvisible(1001, false)))

	require.NoError(t, m.publishGroup(ctx, ready, newMockAllocator(t)))

	member := m.GetSegment(ctx, 1001)
	require.False(t, member.GetIsInvisible())
	parent := m.GetSegment(ctx, 2001)
	require.Equal(t, commonpb.SegmentState_Dropped, parent.GetState())
	require.Equal(t, model.SegmentChangeStateCommitted, m.GetSegmentChangeGroup(ctx, 10, 1).State)
}

func TestGroupUsesPublishCommitTS(t *testing.T) {
	cases := []struct {
		source model.SegmentChangeSource
		want   bool
	}{
		{model.SegmentChangeSourceImportJob, true},
		{model.SegmentChangeSourceCopySegment, true},
		{model.SegmentChangeSourceCDCReplicated, true},
		{model.SegmentChangeSourceMixCompaction, false},
		{model.SegmentChangeSourceSortCompaction, false},
		{model.SegmentChangeSourceClustering, false},
		{model.SegmentChangeSourceForceMerge, false},
		{model.SegmentChangeSourceStorageVersion, false},
		{model.SegmentChangeSourceBumpSchema, false},
		{model.SegmentChangeSourceExternalRefresh, false},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, groupUsesPublishCommitTS(tc.source), "source %s", tc.source)
	}
}

func TestPublishGroup_IndexRegressionRejected(t *testing.T) {
	// F1/R5: a member's index state can regress between the READY decision and
	// the publish (a rebuild retry / index DDL in flight). publishGroup's
	// Phase-2 re-check must reject the publish instead of exposing an
	// indexed-out member while its superseded parents are retired.
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()
	m.indexMeta = newReadinessIndexMeta(10, map[int64]*model.Index{10: vectorIndex(2, 10)}, nil)

	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	err = m.publishGroup(ctx, ready, newMockAllocator(t))
	require.Error(t, err)
	require.ErrorContains(t, err, "index state regressed")

	// The inspector regresses the group to STAGED for re-evaluation instead of
	// retrying the impossible publish forever.
	i := newSegmentChangeInspector(ctx, m, newMockAllocator(t))
	i.tryPublish(ctx, ready)
	got := m.GetSegmentChangeGroup(ctx, 10, 1)
	require.Equal(t, model.SegmentChangeStateStaged, got.State)
	require.Zero(t, got.ReadyTS, "regression clears the stale ready timestamp")
}

func TestPublishGroup_SchemaRegressionRejected(t *testing.T) {
	// F1/C23 at publish: a schema bump landing between READY and publish makes
	// the member's schema version behind; publishGroup rejects it and the
	// inspector regresses to STAGED where the C23 fail-fast fails the group.
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()
	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	m.GetCollection(10).Schema.Version = 2 // member SchemaVersion is 0

	err = m.publishGroup(ctx, ready, newMockAllocator(t))
	require.Error(t, err)
	require.ErrorContains(t, err, "schema version behind")

	i := newSegmentChangeInspector(ctx, m, newMockAllocator(t))
	i.tryPublish(ctx, ready)
	require.Equal(t, model.SegmentChangeStateStaged, m.GetSegmentChangeGroup(ctx, 10, 1).State)
}
