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
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	segments := NewSegmentsInfo()
	for id, channel := range map[int64]string{10: "v1", 11: "v1", 20: "v2"} {
		segments.SetSegment(id, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: id, CollectionID: 100, PartitionID: 1, InsertChannel: channel,
			State: commonpb.SegmentState_Flushed, IsImporting: true,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: id, TimestampTo: 100}}}},
		}})
	}
	callbacks := &DDLCallbacks{Server: &Server{importMeta: imports, meta: &meta{catalog: catalog, segments: segments}}}
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

func TestImportCommitCallbackRecomputesDataView(t *testing.T) {
	callbacks, _, result := newImportCommitCallbackTest(t)
	ctx := context.Background()
	callbacks.meta.dataViewManager = &recordingDataViewManager{}
	recompute := mockey.Mock((*recordingDataViewManager).Recompute).To(func(_ *recordingDataViewManager, ctx context.Context, collectionID int64) error {
		require.EqualValues(t, 100, collectionID)
		// Reading ImportMeta here also checks that reconciliation is requested
		// outside its write lock. Segment visibility must already be persisted.
		require.NotNil(t, callbacks.importMeta.GetJob(ctx, 1))
		projection, err := callbacks.meta.loadableProjection(ctx, collectionID)
		require.NoError(t, err)
		require.Len(t, projection, 3)
		for _, id := range []int64{10, 11, 20} {
			require.False(t, callbacks.meta.GetSegment(ctx, id).GetIsImporting())
		}
		return nil
	}).Build()
	defer recompute.UnPatch()
	failSave := mockey.Mock((*meta).UpdateSegmentsInfo).Return(context.DeadlineExceeded).Build()
	defer failSave.UnPatch()
	require.ErrorIs(t, callbacks.commitImportV2AckCallback(ctx, result), context.DeadlineExceeded)
	require.Zero(t, recompute.Times())
	failSave.UnPatch()
	require.NoError(t, callbacks.commitImportV2AckCallback(ctx, result))
	require.Equal(t, 1, recompute.Times())
	// A replay of an already-completed job can retry a lost reconciliation.
	require.NoError(t, callbacks.commitImportV2AckCallback(ctx, result))
	require.Equal(t, 2, recompute.Times())
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
	control := funcutil.GetControlChannel("test")
	msg := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{}).
		WithBody(&message.ImportMsg{JobID: 1, CollectionID: 100}).
		WithBroadcast([]string{"v1", control}).MustBuildBroadcast()
	patch := mockey.Mock((*Server).createImportJobFromAck).To(func(_ *Server, _ context.Context, req *internalpb.ImportRequestInternal, commitByCoordinator bool) (*internalpb.ImportResponse, error) {
		require.False(t, commitByCoordinator)
		require.Equal(t, []string{"v1"}, req.GetChannelNames())
		require.Equal(t, int64(1), req.GetJobID())
		return &internalpb.ImportResponse{Status: merr.Success()}, nil
	}).Build()
	defer patch.UnPatch()
	callbacks := &DDLCallbacks{Server: &Server{}}
	err := callbacks.importV1AckCallback(context.Background(), message.BroadcastResultImportMessageV1{
		Message: message.MustAsSpecializedBroadcastMessage[*message.ImportMessageHeader, *message.ImportMsg](msg),
		Results: map[string]*message.AppendResult{"v1": {TimeTick: 100}, control: {TimeTick: 900}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, patch.Times())
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
