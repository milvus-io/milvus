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
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/gc"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	kvcatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newStreamingFlushTest(t *testing.T) (*Server, *datapb.SaveBinlogPathsRequest) {
	return newStreamingFlushTestWithRows(t, 2000)
}

func newStreamingFlushTestWithRows(t *testing.T, rows int64) (*Server, *datapb.SaveBinlogPathsRequest) {
	t.Helper()
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	parent := newIndexLineageSegment(1, rows, commonpb.SegmentState_Growing)
	s, scalar := newIndexReadinessTestServer([]*datapb.SegmentInfo{parent}, nil)
	scalar.FieldID, scalar.IndexName = 20, "scalar_index"
	scalar.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: "STL_SORT"}}
	collection := s.meta.GetCollection(1)
	collection.VChannelNames = []string{"lineage_channel"}
	for _, field := range parent.Binlogs {
		for _, binlog := range field.Binlogs {
			binlog.LogPath = ""
		}
	}
	m, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	m.AddCollection(collection)
	m.IndexCatalog = s.indexMeta
	require.NoError(t, m.AddSegment(t.Context(), meta.NewSegmentInfo(parent)))
	manager := dataview.NewManager(m.Catalog.(dataview.Catalog), nil)
	m.DataViewManager = manager
	_, err = manager.OnCreateCollection(t.Context(), dataview.CreateCollectionDataViewEvent{CollectionID: 1, VChannels: collection.VChannelNames})
	require.NoError(t, err)
	s.meta, s.dataViewManager = m, manager
	owner := mockey.Mock((*snmanager.StreamingNodeManager).GetLatestWALLocated).To(func(*snmanager.StreamingNodeManager, context.Context, string) (int64, error) { return 1, nil }).Build()
	t.Cleanup(func() { owner.UnPatch() })
	return s, &datapb.SaveBinlogPathsRequest{
		Base:         &commonpb.MsgBase{SourceID: 1, Properties: map[string]string{dataview.FlushGroupProtocol: "1"}},
		CollectionID: 1, PartitionID: 1, SegmentID: 1, Channel: "lineage_channel", SegLevel: datapb.SegmentLevel_L1,
		Flushed: true, WithFullBinlogs: true, Field2BinlogPaths: parent.Binlogs,
		CheckPoints: []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: rows, Position: &msgpb.MsgPosition{ChannelName: "lineage_channel", Timestamp: 100}}},
	}
}

func TestStreamingFlushGroupSortIndexAndHandoff(t *testing.T) {
	s, req := newStreamingFlushTest(t)
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.EnableCompaction, "true")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.EnableSortCompaction, "true")
	ctx := t.Context()
	before, err := s.dataViewManager.Latest(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(func() {
		if before != nil {
			before.Deref()
		}
	})
	status, err := s.SaveBinlogPaths(ctx, proto.CloneOf(req))
	require.NoError(t, merr.CheckRPCCall(status, err))
	through, pending, err := dataview.ParseStagedFlushResult(status)
	require.NoError(t, err)
	require.True(t, pending)
	legacy := proto.CloneOf(req)
	legacy.Base.Properties = nil
	legacyStatus, legacyErr := s.SaveBinlogPaths(ctx, legacy)
	require.ErrorIs(t, merr.CheckRPCCall(legacyStatus, legacyErr), merr.ErrServiceUnavailable, "an old writer cannot treat a pending proof as a completed handoff")

	require.True(t, proto.Equal(before.Version(), through))
	require.Nil(t, s.meta.GetSegment(ctx, 1).GetSealedAtDataVersion())
	require.True(t, s.meta.GetSegment(ctx, 1).GetIsInvisible())
	latest, err := s.dataViewManager.Latest(ctx, 1)
	require.NoError(t, err)
	require.True(t, proto.Equal(before.DataView(), latest.DataView()), "registration cannot publish the raw flush")
	latest.Deref()
	_, err = s.meta.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
		PlanID: 1000, CollectionID: 1, PartitionID: 1, Channel: "lineage_channel", InputSegments: []int64{1},
		Type: datapb.CompactionType_SortCompaction, Schema: s.meta.GetCollection(1).Schema,
	}, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 2, NumOfRows: 2000, IsSorted: true, InsertLogs: req.Field2BinlogPaths}}})
	require.NoError(t, err)
	require.Equal(t, []int64{2}, s.meta.GetSegmentChangeGroup(ctx, 1, 1).NewSegmentIDs)
	require.Equal(t, commonpb.SegmentState_Flushed, s.meta.GetSegment(ctx, 1).GetState(), "source stays recoverable while growing still serves")
	require.Equal(t, int64(1), s.meta.SnapshotCandidateSegments(1)[0].GetID(), "snapshot pins the durable source instead of the private output")
	scalar := s.indexMeta.Indexes[1][10]
	assertIndexServingState(t, s, scalar, commonpb.IndexState_InProgress)
	addIndexServingTask(s, 2, 10, commonpb.IndexState_Finished)
	assertIndexServingState(t, s, scalar, commonpb.IndexState_Finished)
	inspector := newSegmentChangeInspector(ctx, s.meta, meta.NewMockAllocator(t))
	defer inspector.Stop()
	inspector.checkAllGroups(ctx)
	require.Equal(t, model.SegmentChangeStateStaged, s.meta.GetSegmentChangeGroup(ctx, 1, 1).State, "scalar completion does not waive vector readiness")
	s.indexMeta.Indexes[1][20] = &model.Index{CollectionID: 1, FieldID: 10, IndexID: 20, IndexName: "vector_index"}
	addIndexServingTask(s, 2, 20, commonpb.IndexState_Finished)
	inspector.checkAllGroups(ctx)
	failure := mockey.Mock((*kvcatalog.Catalog).Update).Return(context.DeadlineExceeded).Build()
	t.Cleanup(func() { failure.UnPatch() })
	failedCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	inspector.checkAllGroups(failedCtx)
	cancel()
	failure.UnPatch()
	require.Equal(t, model.SegmentChangeStateReady, s.meta.GetSegmentChangeGroup(ctx, 1, 1).State)
	require.Nil(t, s.meta.GetSegment(ctx, 1).GetSealedAtDataVersion(), "failed publication cannot hand off growing data")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "false")
	inspector.checkAllGroups(ctx)
	g := s.meta.GetSegmentChangeGroup(ctx, 1, 1)
	require.Equal(t, model.SegmentChangeStateCommitted, g.State)
	require.EqualValues(t, before.Version().GetStreamingVersion()+1, g.StreamingDataVersion.GetStreamingVersion())
	require.EqualValues(t, before.Version().GetCompactVersion(), g.StreamingDataVersion.GetCompactVersion())
	require.True(t, proto.Equal(g.StreamingDataVersion, s.meta.GetSegment(ctx, 1).GetSealedAtDataVersion()))
	status, err = s.SaveBinlogPaths(ctx, proto.CloneOf(req))
	require.NoError(t, merr.CheckRPCCall(status, err))
	version, pending, err := dataview.ParseStagedFlushResult(status)
	require.NoError(t, err)
	require.False(t, pending)
	require.True(t, proto.Equal(g.StreamingDataVersion, version))
	collector := &gc.GarbageCollector{Meta: s.meta}
	_, protected := collector.DataViewCompactionProtectionForTest(nil)[1]
	require.True(t, protected)
	req.Base.Properties[dataview.FlushHandoffAck] = "wrong/version"
	status, err = s.SaveBinlogPaths(ctx, proto.CloneOf(req))
	require.ErrorIs(t, merr.CheckRPCCall(status, err), merr.ErrDataIntegrity)
	require.False(t, s.meta.GetSegmentChangeGroup(ctx, 1, 1).StreamingHandoffAcknowledged)
	req.Base.Properties[dataview.FlushHandoffAck] = dataview.FlushHandoffToken(version)
	status, err = s.SaveBinlogPaths(ctx, proto.CloneOf(req))
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.True(t, s.meta.GetSegmentChangeGroup(ctx, 1, 1).StreamingHandoffAcknowledged)
	_, protected = collector.DataViewCompactionProtectionForTest(nil)[1]
	require.True(t, protected, "old retained views still need the growing source")
	before.Deref()
	before = nil
	require.NoError(t, s.dataViewManager.GarbageCollect(ctx, 1, 1))
	_, protected = collector.DataViewCompactionProtectionForTest(nil)[1]
	require.False(t, protected, "durable SN acknowledgment and version GC release the old source")
}

func TestStreamingFlushGroupWithoutSortAndEmptyFlush(t *testing.T) {
	for _, rows := range []int64{0, 2000} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			s, req := newStreamingFlushTestWithRows(t, rows)
			setSegmentChangeTestParam(t, &Params.DataCoordCfg.EnableSortCompaction, "false")
			req.CheckPoints[0].NumOfRows = rows
			if rows == 0 {
				req.Field2BinlogPaths = nil
			}
			status, err := s.SaveBinlogPaths(t.Context(), proto.CloneOf(req))
			require.NoError(t, merr.CheckRPCCall(status, err))
			_, pending, err := dataview.ParseStagedFlushResult(status)
			require.NoError(t, err)
			if rows == 0 {
				require.False(t, pending)
				require.Nil(t, s.meta.GetSegmentChangeGroup(t.Context(), 1, 1))
				return
			}
			require.True(t, pending)
			addIndexServingTask(s, 1, 10, commonpb.IndexState_Finished)
			s.indexMeta.Indexes[1][20] = &model.Index{CollectionID: 1, FieldID: 10, IndexID: 20}
			addIndexServingTask(s, 1, 20, commonpb.IndexState_Finished)
			inspector := newSegmentChangeInspector(t.Context(), s.meta, meta.NewMockAllocator(t))
			defer inspector.Stop()
			inspector.checkAllGroups(t.Context())
			inspector.checkAllGroups(t.Context())
			g := s.meta.GetSegmentChangeGroup(t.Context(), 1, 1)
			require.Equal(t, model.SegmentChangeStateCommitted, g.State)
			require.Equal(t, []int64{1}, g.NewSegmentIDs)
			require.Empty(t, g.SupersededSegmentIDs)
			require.True(t, proto.Equal(g.StreamingDataVersion, s.meta.GetSegment(t.Context(), 1).GetSealedAtDataVersion()))
		})
	}
}

func TestStreamingFlushInterruptedRegistrationRecovery(t *testing.T) {
	s, req := newStreamingFlushTest(t)
	ctx := t.Context()
	g := &model.SegmentChangeGroup{
		GroupID: 1, SourceJobID: 1, StreamingSegmentID: 1,
		Source: model.SegmentChangeSourceStreamingFlush, CollectionID: 1, PartitionID: 1,
		VChannel: req.Channel, State: model.SegmentChangeStateStaged, NewSegmentIDs: []int64{1}, InitialSegmentIDs: []int64{1},
	}
	// Simulate the durable first chunk: the anchor exists, source registration
	// (the commit-marked record) has not completed yet.
	require.NoError(t, s.meta.AddSegmentChangeGroup(ctx, g))
	manager := s.meta.DataViewManager
	s.meta.DataViewManager = nil
	require.NoError(t, s.meta.ReconcileSegmentChangeGroups(ctx, nil, func(context.Context, int64) (bool, error) { return true, nil }))
	s.meta.DataViewManager = manager
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "false")
	status, err := s.SaveBinlogPaths(ctx, proto.CloneOf(req))
	require.NoError(t, merr.CheckRPCCall(status, err))
	_, pending, err := dataview.ParseStagedFlushResult(status)
	require.NoError(t, err)
	require.True(t, pending)
	require.True(t, s.meta.GetSegment(ctx, 1).GetIsInvisible())
	require.Equal(t, commonpb.SegmentState_Flushed, s.meta.GetSegment(ctx, 1).GetState())
}

func TestStreamingFlushInterruptedPublishRecovery(t *testing.T) {
	s, req := newStreamingFlushTest(t)
	ctx := t.Context()
	status, err := s.SaveBinlogPaths(ctx, proto.CloneOf(req))
	require.NoError(t, merr.CheckRPCCall(status, err))
	_, err = s.meta.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
		PlanID: 1000, CollectionID: 1, PartitionID: 1, Channel: req.Channel,
		InputSegments: []int64{1}, Type: datapb.CompactionType_SortCompaction, Schema: s.meta.GetCollection(1).Schema,
	},
		&datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 2, NumOfRows: 2000, IsSorted: true, InsertLogs: req.Field2BinlogPaths}}})
	require.NoError(t, err)
	g := s.meta.GetSegmentChangeGroup(ctx, 1, 1)
	g.State = model.SegmentChangeStateReady
	require.NoError(t, s.meta.UpdateSegmentChangeGroup(ctx, g))
	// An output visibility record can precede the terminal atomic marker.
	output := s.meta.GetSegment(ctx, 2).Clone()
	output.IsInvisible = false
	s.meta.Segments.SetSegment(2, output)
	manager := s.meta.DataViewManager
	s.meta.DataViewManager = nil
	require.NoError(t, s.meta.ReconcileSegmentChangeGroups(ctx, nil, func(context.Context, int64) (bool, error) { return true, nil }))
	s.meta.DataViewManager = manager
	require.True(t, s.meta.GetSegment(ctx, 2).GetIsInvisible())
	require.Nil(t, s.meta.GetSegment(ctx, 1).GetSealedAtDataVersion())
	require.Equal(t, model.SegmentChangeStateReady, s.meta.GetSegmentChangeGroup(ctx, 1, 1).State)
}

func TestStreamingFlushPrivateSchemaPreparation(t *testing.T) {
	s, req := newStreamingFlushTest(t)
	ctx := t.Context()
	status, err := s.SaveBinlogPaths(ctx, proto.CloneOf(req))
	require.NoError(t, merr.CheckRPCCall(status, err))
	source := s.meta.GetSegment(ctx, 1).Clone()
	source.StorageVersion = storage.StorageV3
	source.ManifestPath = packed.MarshalManifestPath("/tmp/flush-group/insert_log/1/1/1", 1)
	s.meta.Segments.SetSegment(1, source)
	collection := *s.meta.GetCollection(1)
	collection.Schema = proto.CloneOf(collection.Schema)
	collection.Schema.Version = 1
	s.meta.AddCollection(&collection)
	_, err = s.dataViewManager.AdvanceSchema(ctx, 1, 1, s.meta.LoadableProjection)
	require.NoError(t, err)
	inspector := newSegmentChangeInspector(ctx, s.meta, meta.NewMockAllocator(t))
	defer inspector.Stop()
	inspector.checkAllGroups(ctx)
	require.Equal(t, model.SegmentChangeStateStaged, s.meta.GetSegmentChangeGroup(ctx, 1, 1).State, "accepted rows cannot fail on schema drift")
	require.True(t, s.meta.CanPrepareStreamingSchema(1))
	task := &datapb.CompactionTask{
		PlanID: 1000, CollectionID: 1, PartitionID: 1, Channel: req.Channel,
		InputSegments: []int64{1}, Type: datapb.CompactionType_BumpSchemaVersionCompaction, Schema: collection.Schema,
	}
	require.True(t, s.meta.ReserveStreamingSchema(task))
	_, err = s.meta.CompleteCompactionMutation(ctx, task, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{
		SegmentID: 1, NumOfRows: 2000, StorageVersion: storage.StorageV3,
		BaseManifest: source.ManifestPath, Manifest: packed.MarshalManifestPath("/tmp/flush-group/insert_log/1/1/1", 2),
	}}})
	require.NoError(t, err)
	require.EqualValues(t, 1, s.meta.GetSegment(ctx, 1).GetSchemaVersion())
	require.True(t, s.meta.GetSegment(ctx, 1).GetIsInvisible())
	require.Nil(t, s.meta.GetSegment(ctx, 1).GetSealedAtDataVersion())
	require.Equal(t, []int64{1}, s.meta.GetSegmentChangeGroup(ctx, 1, 1).NewSegmentIDs)
}

func TestStreamingFlushRetiredByDDL(t *testing.T) {
	for _, kind := range []string{"partition", "truncate"} {
		t.Run(kind, func(t *testing.T) {
			s, req := newStreamingFlushTest(t)
			ctx := t.Context()
			status, err := s.SaveBinlogPaths(ctx, proto.CloneOf(req))
			require.NoError(t, merr.CheckRPCCall(status, err))
			_, err = s.meta.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
				PlanID: 1000, CollectionID: 1, PartitionID: 1, Channel: req.Channel,
				InputSegments: []int64{1}, Type: datapb.CompactionType_SortCompaction, Schema: s.meta.GetCollection(1).Schema,
			},
				&datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 2, NumOfRows: 2000, IsSorted: true, InsertLogs: req.Field2BinlogPaths}}})
			require.NoError(t, err)
			if kind == "partition" {
				require.NoError(t, s.NotifyDropPartition(ctx, req.Channel, 1, []int64{1}))
			} else {
				// A private physical rewrite's checkpoint must not evade the
				// source's logical truncate boundary.
				output := s.meta.GetSegment(ctx, 2).Clone()
				output.DmlPosition = &msgpb.MsgPosition{ChannelName: req.Channel, Timestamp: 1000}
				s.meta.Segments.SetSegment(2, output)
				require.NoError(t, s.DropSegmentsByTime(ctx, 1, map[string]uint64{req.Channel: 200}))
			}
			g := s.meta.GetSegmentChangeGroup(ctx, 1, 1)
			require.Equal(t, model.SegmentChangeStateAborted, g.State)
			require.True(t, g.StreamingDataRetired)
			require.Equal(t, commonpb.SegmentState_Dropped, s.meta.GetSegment(ctx, 1).GetState())
			require.Equal(t, commonpb.SegmentState_Dropped, s.meta.GetSegment(ctx, 2).GetState())
			status, err = s.SaveBinlogPaths(ctx, proto.CloneOf(req))
			require.NoError(t, merr.CheckRPCCall(status, err))
			version, pending, err := dataview.ParseStagedFlushResult(status)
			require.NoError(t, err)
			require.False(t, pending)
			require.Nil(t, version)
			s.meta.DataViewManager = nil
			require.NoError(t, s.meta.ReconcileSegmentChangeGroups(ctx, nil, func(context.Context, int64) (bool, error) { return true, nil }))
		})
	}
}

func TestStreamingFlushPrivateSchemaRewrite(t *testing.T) {
	s, req := newStreamingFlushTest(t)
	ctx := t.Context()
	status, err := s.SaveBinlogPaths(ctx, proto.CloneOf(req))
	require.NoError(t, merr.CheckRPCCall(status, err))
	source := s.meta.GetSegment(ctx, 1).Clone()
	source.StorageVersion = storage.StorageV3
	source.ManifestPath = packed.MarshalManifestPath("/tmp/flush-group/insert_log/1/1/1", 1)
	s.meta.Segments.SetSegment(1, source)
	collection := *s.meta.GetCollection(1)
	collection.Schema = proto.CloneOf(collection.Schema)
	collection.Schema.Version = 1
	s.meta.AddCollection(&collection)
	_, err = s.dataViewManager.AdvanceSchema(ctx, 1, 1, nil)
	require.NoError(t, err)
	task := &datapb.CompactionTask{
		PlanID: 1000, CollectionID: 1, PartitionID: 1, Channel: req.Channel,
		InputSegments: []int64{1}, Type: datapb.CompactionType_BumpSchemaVersionCompaction, Schema: collection.Schema,
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 2, End: 3},
	}
	require.True(t, s.meta.ReserveStreamingSchema(task))
	worker := ctask.NewBumpSchemaVersionTask(task, nil, s.meta, newSegmentChangeTaskMeta(t, s.meta), nil)
	require.NoError(t, worker.SaveSegmentMeta(&datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{
		SegmentID: 2, NumOfRows: 2000, StorageVersion: storage.StorageV3,
		Manifest: packed.MarshalManifestPath("/tmp/flush-group/insert_log/1/1/2", 1), InsertLogs: req.Field2BinlogPaths,
	}}}))
	g := s.meta.GetSegmentChangeGroup(ctx, 1, 1)
	require.Equal(t, []int64{2}, g.NewSegmentIDs)
	require.Equal(t, []int64{1}, g.SupersededSegmentIDs)
	require.Equal(t, commonpb.SegmentState_Flushed, s.meta.GetSegment(ctx, 1).GetState())
	require.True(t, s.meta.GetSegment(ctx, 2).GetIsInvisible())
	require.EqualValues(t, 1, s.meta.GetSegment(ctx, 2).GetSchemaVersion())
	require.True(t, worker.Clean())
	require.Equal(t, model.SegmentChangeStateStaged, s.meta.GetSegmentChangeGroup(ctx, 1, 1).State, "cleaning a private preparation task cannot abort the accepted producer")
}

func TestStreamingFlushDDLRetiresManyGroups(t *testing.T) {
	s, req := newStreamingFlushTest(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	status, err := s.SaveBinlogPaths(ctx, proto.CloneOf(req))
	require.NoError(t, merr.CheckRPCCall(status, err))
	for id := int64(10); id < 90; id++ {
		segment := proto.CloneOf(s.meta.GetSegment(ctx, 1).SegmentInfo)
		segment.ID = id
		require.NoError(t, s.meta.AddSegment(ctx, meta.NewSegmentInfo(segment)))
		_, _, err := s.meta.StageAdmittedChange(ctx, &model.SegmentChangeGroup{
			GroupID: id, SourceJobID: id, StreamingSegmentID: id,
			Source: model.SegmentChangeSourceStreamingFlush, CollectionID: 1, PartitionID: 1, VChannel: req.Channel, NewSegmentIDs: []int64{id},
		}, true)
		require.NoError(t, err)
	}
	// 81 source records + terminal groups exceed one 128-op final transaction.
	require.NoError(t, s.NotifyDropPartition(ctx, req.Channel, 1, []int64{1}))
	for _, g := range s.meta.GetSegmentChangeGroupsByCollection(ctx, 1) {
		require.Equal(t, model.SegmentChangeStateAborted, g.State)
		require.True(t, g.StreamingDataRetired)
	}
}

func TestStreamingFlushDoesNotRestageUnboundLegacySource(t *testing.T) {
	s, req := newStreamingFlushTest(t)
	ctx := t.Context()
	source := s.meta.GetSegment(ctx, 1).Clone()
	source.State = commonpb.SegmentState_Flushed
	source.IsSorted = true
	s.meta.Segments.SetSegment(1, source)
	addIndexServingTask(s, 1, 10, commonpb.IndexState_Finished)
	// A historical source can already be in a sealed snapshot without the
	// newer per-source version binding. It must not become a growing group.
	view, commit, abort, err := s.dataViewManager.PrepareFlush(ctx, dataview.FlushDataViewEvent{
		CollectionID: 1, Segments: []dataview.LoadableSegment{{SegmentID: 1, PartitionID: 1, VChannel: req.Channel, RowNum: 2000}},
	})
	require.NoError(t, err)
	if abort != nil {
		defer abort()
	}
	commit()
	require.False(t, s.streamingFlushAdmitted(ctx, 1))
	status, err := s.SaveBinlogPaths(ctx, proto.CloneOf(req))
	require.ErrorIs(t, merr.CheckRPCCall(status, err), merr.ErrServiceInternal, "preserve the existing unbound Flushed integrity guard")
	require.Nil(t, s.meta.GetSegmentChangeGroup(ctx, 1, 1))
	latest, err := s.dataViewManager.Latest(ctx, 1)
	require.NoError(t, err)
	defer latest.Deref()
	require.True(t, proto.Equal(view.GetDataVersion(), latest.Version()))
}
