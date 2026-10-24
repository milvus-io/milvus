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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metacache"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func newImportCommitCallbackTest(t *testing.T) (*DDLCallbacks, *datacoordkv.Catalog, message.BroadcastResultCommitImportMessageV2) {
	t.Helper()
	ctx := context.Background()
	catalog := &datacoordkv.Catalog{MetaKv: NewMetaMemoryKV()}
	imports, err := NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	require.NoError(t, imports.AddJob(ctx, &importJob{
		ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 100, Vchannels: []string{"v1", "v2"}, State: internalpb.ImportJobState_Uncommitted, CommitByCoordinator: true},
		tr:        timerecord.NewTimeRecorder("import-commit"),
	}))
	segments := NewSegmentsInfo(metacache.NewMetaStore(nil))
	for id, channel := range map[int64]string{10: "v1", 11: "v1", 20: "v2"} {
		segments.SetSegment(id, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: id, CollectionID: 100, PartitionID: 1, InsertChannel: channel,
			State: commonpb.SegmentState_Flushed, IsImporting: true,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: id, TimestampTo: 100}}}},
		}})
	}
	callbacks := &DDLCallbacks{Server: &Server{importMeta: imports, meta: &meta{catalog: catalog, segments: segments}}}
	manager, err := dataview.RecoverManager(t.Context(), catalog, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil,
		dataview.WithPendingTransformProjector(func(ctx context.Context, view *viewpb.DataViewOfCollection) (map[string]uint64, error) {
			return pendingTransformBounds(ctx, callbacks.meta, callbacks.importMeta, view)
		}))
	require.NoError(t, err)
	_, err = manager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{
		CollectionID: 100, VChannels: []string{"v1", "v2"}, TransformStarts: map[string]uint64{"v1": 50, "v2": 60},
	})
	require.NoError(t, err)
	callbacks.dataViewManager = manager
	callbacks.meta.dataViewManager = manager
	callbacks.UpdateStateCode(commonpb.StateCode_Healthy)
	patch := mockey.Mock((*Server).getImportSegmentIDsByVchannel).To(func(_ *Server, _ context.Context, jobID int64, vchannel string) []int64 {
		require.Equal(t, int64(1), jobID)
		switch vchannel {
		case "v1":
			return []int64{10, 11} // Include original and sorted output.
		case "v2":
			return []int64{20}
		default:
			t.Fatalf("unexpected data channel %q", vchannel)
			return nil
		}
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	result := buildCommitImportBroadcastResult(1)
	result.Results = map[string]*message.AppendResult{
		"v1": {TimeTick: 200}, "v2": {TimeTick: 300}, funcutil.GetControlChannel("test"): {TimeTick: 900},
	}
	return callbacks, catalog, result
}

func TestImportCommitCallbackPublishesPerChannelVisibility(t *testing.T) {
	callbacks, catalog, result := newImportCommitCallbackTest(t)
	ctx := context.Background()
	require.NoError(t, callbacks.commitImportV2AckCallback(ctx, result))
	for id, tick := range map[int64]uint64{10: 200, 11: 200, 20: 300} {
		segment := callbacks.meta.GetSegment(ctx, id)
		require.False(t, segment.GetIsImporting())
		require.Equal(t, tick, segment.GetCommitTimestamp())
		require.Equal(t, tick, segment.GetTransformStartAfterTimetick())
	}
	// Completion is durable and no per-vchannel RPC/counter is needed.
	restored, err := NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	job := restored.GetJob(ctx, 1)
	require.Equal(t, internalpb.ImportJobState_Completed, job.GetState())
	require.NotEmpty(t, job.GetCompleteTime())
	require.Empty(t, job.GetCommittedVchannels())
	patch := mockey.Mock((*meta).UpdateSegmentsInfo).Return(context.DeadlineExceeded).Build()
	defer patch.UnPatch()
	callbacks.importMeta = restored
	require.NoError(t, callbacks.commitImportV2AckCallback(ctx, result), "completed replay must not publish again")
}

func TestImportCommitCallbackRetriesSegmentPersistence(t *testing.T) {
	callbacks, _, result := newImportCommitCallbackTest(t)
	ctx := context.Background()
	patch := mockey.Mock((*meta).UpdateSegmentsInfo).Return(context.DeadlineExceeded).Build()
	require.ErrorIs(t, callbacks.commitImportV2AckCallback(ctx, result), context.DeadlineExceeded)
	patch.UnPatch()
	require.Equal(t, internalpb.ImportJobState_Committing, callbacks.importMeta.GetJob(ctx, 1).GetState())
	require.True(t, callbacks.meta.GetSegment(ctx, 10).GetIsImporting())
	require.NoError(t, callbacks.commitImportV2AckCallback(ctx, result))
	require.Equal(t, internalpb.ImportJobState_Completed, callbacks.importMeta.GetJob(ctx, 1).GetState())
}

func TestImportCommitCallbackPublishesBeforeCompleted(t *testing.T) {
	callbacks, catalog, result := newImportCommitCallbackTest(t)
	ctx := t.Context()
	// Another import is still being prepared. It must not be visible or block
	// this commit while the collection's broadcast lock excludes its commit.
	require.NoError(t, callbacks.importMeta.AddJob(ctx, &importJob{
		ImportJob: &datapb.ImportJob{JobID: 2, CollectionID: 100, Vchannels: []string{"v1", "v2"}, State: internalpb.ImportJobState_Importing, CommitByCoordinator: true},
	}))
	callbacks.meta.segments.SetSegment(30, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 30, CollectionID: 100, PartitionID: 1, InsertChannel: "v1", State: commonpb.SegmentState_Flushed, IsImporting: true,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 100}},
	}))
	failure := mockey.Mock((*datacoordkv.Catalog).SaveDataView).Return(context.DeadlineExceeded).Build()
	require.ErrorIs(t, callbacks.commitImportV2AckCallback(ctx, result), context.DeadlineExceeded)
	failure.UnPatch()
	require.Equal(t, internalpb.ImportJobState_Committing, callbacks.importMeta.GetJob(ctx, 1).GetState())
	ref, err := callbacks.dataViewManager.Latest(ctx, 100)
	require.NoError(t, err)
	for _, shard := range ref.DataView().Shards {
		require.Empty(t, shard.Partitions, "failed publication must retain the old view")
	}
	ref.Deref()
	// The durable callback can resume after a coordinator restart. No QueryView
	// infrastructure is installed: import completion does not wait for QNs.
	callbacks.importMeta, err = NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	require.NoError(t, callbacks.commitImportV2AckCallback(ctx, result))
	require.Equal(t, internalpb.ImportJobState_Completed, callbacks.importMeta.GetJob(ctx, 1).GetState())
	require.Equal(t, internalpb.ImportJobState_Importing, callbacks.importMeta.GetJob(ctx, 2).GetState())
	ref, err = callbacks.dataViewManager.Latest(ctx, 100)
	require.NoError(t, err)
	defer ref.Deref()
	ids := []int64{}
	for _, shard := range ref.DataView().Shards {
		for _, partition := range shard.Partitions {
			ids = append(ids, partition.SegmentIds...)
		}
	}
	require.ElementsMatch(t, []int64{10, 11, 20}, ids)
	version := ref.DataView().GetDataVersion()
	require.NoError(t, callbacks.commitImportV2AckCallback(ctx, result))
	replay, err := callbacks.dataViewManager.Latest(ctx, 100)
	require.NoError(t, err)
	defer replay.Deref()
	require.Equal(t, version, replay.DataView().GetDataVersion(), "completed replay is idempotent")
}

func TestImportCommitCallbackRetriesCompletionAfterRestart(t *testing.T) {
	callbacks, catalog, result := newImportCommitCallbackTest(t)
	ctx := context.Background()
	// All segment metadata succeeds, but the final job write fails.
	patch := mockey.Mock((*datacoordkv.Catalog).SaveImportJob).
		When(func(_ *datacoordkv.Catalog, _ context.Context, job *datapb.ImportJob) bool {
			return job.GetState() == internalpb.ImportJobState_Completed
		}).
		Return(context.DeadlineExceeded).Build()
	require.ErrorIs(t, callbacks.commitImportV2AckCallback(ctx, result), context.DeadlineExceeded)
	patch.UnPatch()
	require.False(t, callbacks.meta.GetSegment(ctx, 10).GetIsImporting())
	restored, err := NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	require.Equal(t, internalpb.ImportJobState_Committing, restored.GetJob(ctx, 1).GetState())
	callbacks.importMeta = restored
	// A stale timeout snapshot must not fail a partially published commit.
	checker := &importChecker{ctx: ctx, importMeta: restored}
	stale := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, State: internalpb.ImportJobState_Uncommitted, TimeoutTs: 1}}
	checker.tryTimeoutJob(stale)
	require.Equal(t, internalpb.ImportJobState_Committing, restored.GetJob(ctx, 1).GetState())
	require.NoError(t, callbacks.commitImportV2AckCallback(ctx, result))
	require.Equal(t, internalpb.ImportJobState_Completed, restored.GetJob(ctx, 1).GetState())
	require.Equal(t, uint64(200), callbacks.meta.GetSegment(ctx, 10).GetCommitTimestamp())
}

func TestImportCommitCallbackRejectsTimestampBeforeRows(t *testing.T) {
	callbacks, _, result := newImportCommitCallbackTest(t)
	result.Results["v1"].TimeTick = 99
	err := callbacks.commitImportV2AckCallback(context.Background(), result)
	require.ErrorIs(t, err, merr.ErrImportSysFailed)
	require.Equal(t, internalpb.ImportJobState_Committing, callbacks.importMeta.GetJob(context.Background(), 1).GetState())
	for _, id := range []int64{10, 11, 20} {
		segment := callbacks.meta.GetSegment(context.Background(), id)
		require.True(t, segment.GetIsImporting())
		require.Zero(t, segment.GetCommitTimestamp())
	}
}

func TestImportCommitCallbackRetriesCommitPhasePersistence(t *testing.T) {
	callbacks, _, result := newImportCommitCallbackTest(t)
	ctx := context.Background()
	patch := mockey.Mock((*datacoordkv.Catalog).SaveImportJob).Return(context.DeadlineExceeded).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, callbacks.commitImportV2AckCallback(ctx, result), context.DeadlineExceeded)
	require.Equal(t, internalpb.ImportJobState_Uncommitted, callbacks.importMeta.GetJob(ctx, 1).GetState())
	require.True(t, callbacks.meta.GetSegment(ctx, 10).GetIsImporting())
	patch.UnPatch()
	require.NoError(t, callbacks.commitImportV2AckCallback(ctx, result))
	require.Equal(t, internalpb.ImportJobState_Completed, callbacks.importMeta.GetJob(ctx, 1).GetState())
}

func TestImportCallbackExcludesControlFromJobChannels(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	type ackAllocator struct{ allocator.Allocator }
	type ackHandler struct{ Handler }
	allocate := mockey.Mock((*ackAllocator).AllocN).Return(int64(10), int64(11), nil).Build()
	defer allocate.UnPatch()
	collection := mockey.Mock((*ackHandler).GetCollection).Return(
		&collectionInfo{ID: 100, VChannelNames: []string{"v1"}}, nil).Build()
	defer collection.UnPatch()
	catalog := &datacoordkv.Catalog{MetaKv: NewMetaMemoryKV()}
	imports, err := NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	callbacks := &DDLCallbacks{Server: &Server{
		allocator: &ackAllocator{}, handler: &ackHandler{}, importMeta: imports,
	}}
	callbacks.stateCode.Store(commonpb.StateCode_Healthy)

	control := funcutil.GetControlChannel("test")
	msg := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{}).
		WithBody(&message.ImportMsg{JobID: 1, CollectionID: 100}).
		WithBroadcast([]string{"v1", control}).MustBuildBroadcast()
	// Routing belongs to job creation; do not mock away the ACK-to-job path.
	err = callbacks.importV1AckCallback(ctx, message.BroadcastResultImportMessageV1{
		Message: message.MustAsSpecializedBroadcastMessage[*message.ImportMessageHeader, *message.ImportMsg](msg),
		Results: map[string]*message.AppendResult{"v1": {TimeTick: 100}, control: {TimeTick: 900}},
	})
	require.NoError(t, err)
	job := imports.GetJob(ctx, 1)
	require.NotNil(t, job)
	require.False(t, job.GetCommitByCoordinator())
	require.Equal(t, []string{"v1"}, job.GetReadyVchannels())
	require.Equal(t, int64(1), job.GetJobID())
	require.EqualValues(t, 900, job.GetDataTs())
}

func TestLegacyImportCommitSurvivesRetiredBroadcast(t *testing.T) {
	ctx := context.Background()
	callbacks, catalog, result := newImportCommitCallbackTest(t)
	callbacks.stateCode.Store(commonpb.StateCode_Healthy)
	require.NoError(t, callbacks.importMeta.UpdateJob(ctx, 1, func(job ImportJob) {
		job.(*importJob).CommitByCoordinator = false
	}))
	msg := message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{JobId: 1}).
		WithBody(&message.CommitImportMessageBody{}).
		WithBroadcast([]string{"v1", "v2"}).MustBuildBroadcast()
	result.Message = message.MustAsSpecializedBroadcastMessage[*message.CommitImportMessageHeader, *message.CommitImportMessageBody](msg)
	require.NoError(t, callbacks.commitImportV2AckCallback(ctx, result))
	require.True(t, callbacks.meta.GetSegment(ctx, 10).GetIsImporting())
	require.Equal(t, internalpb.ImportJobState_Committing, callbacks.importMeta.GetJob(ctx, 1).GetState())
	// The old broadcaster can now be TOMBSTONE. Recovery needs only the
	// original per-channel WAL message, not its broadcast task.
	restored, err := NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	callbacks.importMeta = restored
	checker := &importChecker{ctx: ctx, importMeta: restored}
	request := &datapb.HandleCommitVchannelRequest{JobId: 1, Vchannel: "v1", CommitTimestamp: 200}
	failure := mockey.Mock((*meta).UpdateSegmentsInfo).Return(context.DeadlineExceeded).Build()
	status, err := callbacks.HandleCommitVchannel(ctx, request)
	require.Error(t, merr.CheckRPCCall(status, err))
	failure.UnPatch()
	require.Empty(t, restored.GetJob(ctx, 1).GetCommittedVchannels())
	status, err = callbacks.HandleCommitVchannel(ctx, request)
	require.NoError(t, merr.CheckRPCCall(status, err))
	checker.checkCommittingJob(restored.GetJob(ctx, 1))
	require.Equal(t, internalpb.ImportJobState_Committing, restored.GetJob(ctx, 1).GetState())
	require.False(t, callbacks.meta.GetSegment(ctx, 10).GetIsImporting())
	require.True(t, callbacks.meta.GetSegment(ctx, 20).GetIsImporting())
	request.CommitTimestamp = 999
	status, err = callbacks.HandleCommitVchannel(ctx, request)
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.EqualValues(t, 200, callbacks.meta.GetSegment(ctx, 10).GetCommitTimestamp(), "duplicate RPC must retain original visibility")
	status, err = callbacks.HandleCommitVchannel(ctx, &datapb.HandleCommitVchannelRequest{JobId: 1, Vchannel: "v2", CommitTimestamp: 300})
	require.NoError(t, merr.CheckRPCCall(status, err))
	checker.checkCommittingJob(restored.GetJob(ctx, 1))
	require.Equal(t, internalpb.ImportJobState_Completed, restored.GetJob(ctx, 1).GetState())
}

func TestCoordinatorImportIgnoresLegacyCompletion(t *testing.T) {
	ctx := context.Background()
	callbacks, catalog, _ := newImportCommitCallbackTest(t)
	callbacks.stateCode.Store(commonpb.StateCode_Healthy)
	for _, state := range []internalpb.ImportJobState{internalpb.ImportJobState_Importing, internalpb.ImportJobState_Uncommitted, internalpb.ImportJobState_Committing} {
		require.NoError(t, callbacks.importMeta.UpdateJob(ctx, 1, UpdateJobState(state)))
		restored, err := NewImportMeta(ctx, catalog, nil, nil)
		require.NoError(t, err)
		callbacks.importMeta = restored
		require.True(t, restored.GetJob(ctx, 1).GetCommitByCoordinator())
		status, err := callbacks.HandleCommitVchannel(ctx, &datapb.HandleCommitVchannelRequest{JobId: 1, Vchannel: "v1", CommitTimestamp: 200})
		require.NoError(t, merr.CheckRPCCall(status, err))
		require.True(t, callbacks.meta.GetSegment(ctx, 10).GetIsImporting())
		require.Empty(t, restored.GetJob(ctx, 1).GetCommittedVchannels())
		checker := &importChecker{ctx: ctx, importMeta: restored}
		checker.checkCommittingJob(restored.GetJob(ctx, 1))
		require.Equal(t, state, restored.GetJob(ctx, 1).GetState())
	}
}

// TestImportCommitCallbackV3JobPublishesVisibility pins the commit path for an
// Import V3 job: the segment ownership is resolved through the completed
// ImportTaskV3 records, and the same visibility fence (per-channel commit
// timestamp + is_importing clear) and Completed transition apply.
func TestImportCommitCallbackV3JobPublishesVisibility(t *testing.T) {
	ctx := context.Background()
	catalog := &datacoordkv.Catalog{MetaKv: NewMetaMemoryKV()}
	imports, err := NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	require.NoError(t, imports.AddJob(ctx, &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 1, CollectionID: 100, Vchannels: []string{"v1"},
			State: internalpb.ImportJobState_Uncommitted, CommitByCoordinator: true,
			Version: internalpb.ImportVersion_ImportVersionV3,
		},
		tr: timerecord.NewTimeRecorder("import-commit"),
	}))
	segments := NewSegmentsInfo(metacache.NewMetaStore(nil))
	segments.SetSegment(10, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 10, CollectionID: 100, PartitionID: 1, InsertChannel: "v1",
		State: commonpb.SegmentState_Flushed, IsImporting: true, NumOfRows: 10,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 10, TimestampTo: 100}}}},
	}})
	// The V3 segment is owned by a Completed ImportTaskV3; the commit callback
	// resolves it through getImportSegmentIDsByVchannel's V3 branch.
	v3task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 41, CollectionId: 100, State: datapb.ImportTaskStateV2_Completed,
		SegmentId: 10, Vchannel: "v1",
	}, imports, nil, nil)
	require.NoError(t, imports.AddTask(ctx, v3task))

	callbacks := &DDLCallbacks{Server: &Server{importMeta: imports, meta: &meta{catalog: catalog, segments: segments}}}
	manager, err := dataview.RecoverManager(ctx, catalog, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil,
		dataview.WithPendingTransformProjector(func(ctx context.Context, view *viewpb.DataViewOfCollection) (map[string]uint64, error) {
			return pendingTransformBounds(ctx, callbacks.meta, callbacks.importMeta, view)
		}))
	require.NoError(t, err)
	_, err = manager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{
		CollectionID: 100, VChannels: []string{"v1"}, TransformStarts: map[string]uint64{"v1": 50},
	})
	require.NoError(t, err)
	callbacks.dataViewManager = manager
	callbacks.meta.dataViewManager = manager
	callbacks.UpdateStateCode(commonpb.StateCode_Healthy)
	result := buildCommitImportBroadcastResult(1)
	result.Results = map[string]*message.AppendResult{"v1": {TimeTick: 200}}

	require.NoError(t, callbacks.commitImportV2AckCallback(ctx, result))

	seg := callbacks.meta.GetSegment(ctx, 10)
	require.False(t, seg.GetIsImporting(), "the commit fence must clear is_importing")
	require.Equal(t, uint64(200), seg.GetCommitTimestamp(), "the visibility fence must stamp the channel's timetick")
	job := imports.GetJob(ctx, 1)
	require.Equal(t, internalpb.ImportJobState_Completed, job.GetState())
	require.NotEmpty(t, job.GetCompleteTime())
}

func TestImportCommitTransformFenceSurvivesRecovery(t *testing.T) {
	callbacks, catalog, _ := newImportCommitCallbackTest(t)
	ctx := context.Background()
	raw := message.NewCommitImportMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.CommitImportMessageHeader{CollectionId: 100, JobId: 1, CommitByCoordinator: true}).
		WithBody(&message.CommitImportMessageBody{}).MustBuildMutable().WithTimeTick(200).
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	result := message.AckResultCommitImportMessageV2{Message: message.MustAsImmutableCommitImportMessageV2(raw)}
	failure := mockey.Mock((*datacoordkv.Catalog).SaveImportJob).Return(context.DeadlineExceeded).Build()
	require.ErrorIs(t, callbacks.commitImportV2AckOnceCallback(ctx, result), context.DeadlineExceeded)
	failure.UnPatch()
	require.Empty(t, callbacks.importMeta.GetJob(ctx, 1).(*importJob).GetTransformCommitTimeticks())
	require.NoError(t, callbacks.commitImportV2AckOnceCallback(ctx, result))
	require.NoError(t, callbacks.commitImportV2AckOnceCallback(ctx, result), "ack retry is idempotent")
	recovered, err := NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"v1": 200}, recovered.GetJob(ctx, 1).(*importJob).GetTransformCommitTimeticks())
	require.Equal(t, internalpb.ImportJobState_Uncommitted, recovered.GetJob(ctx, 1).GetState(), "registration alone must not publish imported rows")
}

func TestImportPendingJobCapsFlushCertificate(t *testing.T) {
	callbacks, _, _ := newImportCommitCallbackTest(t)
	ctx := t.Context()
	check := func(expected map[string]uint64) {
		view, _, abort, err := callbacks.dataViewManager.PrepareFlush(ctx, dataview.FlushDataViewEvent{
			CollectionID:           100,
			GrowingTransformStarts: map[string]uint64{"v1": 1000, "v2": 1000},
			Segments:               []dataview.LoadableSegment{{SegmentID: 40, VChannel: "v1", PartitionID: 1, TransformStartAfterTimetick: 70}},
		})
		require.NoError(t, err)
		defer abort()
		for _, shard := range view.GetShards() {
			require.Equal(t, expected[shard.GetVchannel()], shard.GetGrowingTransformStartAfterTimetick())
		}
	}
	// A certificate observed past a not-yet-acknowledged CommitImport may
	// publish its L1, but must not advance W across that unknown commit.
	check(map[string]uint64{"v1": 50, "v2": 60})
	require.NoError(t, callbacks.importMeta.UpdateJob(ctx, 1, func(job ImportJob) {
		job.(*importJob).TransformCommitTimeticks = map[string]uint64{"v1": 200, "v2": 300}
	}))
	check(map[string]uint64{"v1": 200, "v2": 300})
}
