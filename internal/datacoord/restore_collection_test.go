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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/rootcoord"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRestoreCollectionUsesOneBroadcast(t *testing.T) {
	mockey.PatchConvey("one durable creation recipe", t, func() {
		body := &messagespb.RestoreSnapshotMessageBody{
			CollectionHeader: &message.CreateCollectionMessageHeader{CollectionId: 100, DbId: 1, PartitionIds: []int64{201, 202}},
			Collection: &message.CreateCollectionRequest{
				CollectionID: 100, PartitionIDs: []int64{201, 202}, PartitionNames: []string{"_default", "p1"},
				CollectionSchema: &schemapb.CollectionSchema{Name: "target"}, VirtualChannelNames: []string{"v1", "v2"},
			},
		}
		cleaned := false
		mockey.Mock((*rootcoord.Core).CheckRestoreTargetAbsent).Return(merr.WrapErrCollectionNotFound("target")).Build()
		mockey.Mock((*rootcoord.Core).PrepareRestoreCollection).To(func(_ *rootcoord.Core, _ context.Context, request *milvuspb.CreateCollectionRequest, partitions []string) (*messagespb.RestoreSnapshotMessageBody, func(), error) {
			require.Equal(t, "target", request.GetCollectionName())
			require.ElementsMatch(t, []string{"_default", "p1"}, partitions)
			return body, func() { cleaned = true }, nil
		}).Build()
		mockey.Mock((*restoreAllocatorTarget).AllocID).Return(int64(77), nil).Build()
		capture := &captureBroadcastAPI{}
		mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(capture, nil).Build()
		calls := 0
		mockey.Mock((*captureBroadcastAPI).Broadcast).To(func(_ *captureBroadcastAPI, _ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			calls++
			require.Equal(t, message.MessageTypeRestoreSnapshot, msg.MessageType())
			restore := message.MustAsBroadcastRestoreSnapshotMessageV2(msg)
			require.Equal(t, int64(100), restore.Header().GetCollectionId())
			require.Equal(t, int64(77), restore.Header().GetJobId())
			require.True(t, proto.Equal(body, restore.MustBody()))
			require.ElementsMatch(t, []string{"v1", "v2"}, msg.BroadcastHeader().VChannels)
			return &types.BroadcastAppendResult{BroadcastID: 1}, nil
		}).Build()
		legacy := mockey.Mock((*snapshotManager).RestoreCollection).Return(int64(0), merr.ErrServiceInternal).Build()
		sm := &snapshotManager{collectionRestorer: &rootcoord.Core{}, allocator: &restoreAllocatorTarget{}}
		job, err := sm.finishRestoreSnapshot(t.Context(), nil, &snapshotstorage.SnapshotData{
			Collection: &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "source"}, Partitions: map[string]int64{"_default": 1, "p1": 2}},
		}, "snapshot", 10, "target", "default", 5, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, int64(77), job)
		require.Equal(t, 1, calls)
		require.Zero(t, legacy.Times())
		require.False(t, cleaned, "durable broadcast owns creation resources")
	})
}

func TestRestorePublicationRetriesWithoutNewState(t *testing.T) {
	lookup := mockey.Mock((*rootcoord.Core).IsCollectionDropped).Return(false, nil).Build()
	defer lookup.UnPatch()
	ctx := t.Context()
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copies, meta := newCopySegmentTaskTestMeta(t, task)
	job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
	job.CreatesCollection = true
	job.IdMappings = task.GetIdMappings()
	job.TimeoutTs = CopyJobTimeoutTs(-time.Hour)
	require.NoError(t, copies.AddJob(ctx, job))
	require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted)))
	// A restart reconstructs the publication prerequisite from existing records.
	recovered, err := NewCopySegmentMeta(ctx, meta.catalog, meta, nil, nil)
	require.NoError(t, err)
	checker := &copySegmentChecker{ctx: ctx, copyMeta: recovered, meta: meta, collectionRestorer: &rootcoord.Core{}}
	checker.tryTimeoutJob(job)
	require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, recovered.GetJob(ctx, 100).GetState())
	patch := mockey.Mock((*rootcoord.Core).CompleteRestoreCollection).Return(merr.ErrServiceUnavailable).Build()
	checker.publishRestoredCollection(job, 10)
	patch.UnPatch()
	require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, recovered.GetJob(ctx, 100).GetState())
	// Stale task failure and timeout cannot revoke copied data while publication retries.
	require.NoError(t, recovered.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed)))
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, recovered.GetTask(ctx, task.GetTaskId()).GetState())
	require.NoError(t, recovered.UpdateJobStateAndReleaseRef(ctx, 100, UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed)))
	require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, recovered.GetJob(ctx, 100).GetState())
	patch = mockey.Mock((*rootcoord.Core).CompleteRestoreCollection).Return(nil).Build()
	checker.publishRestoredCollection(job, 10)
	patch.UnPatch()
	checker.tryTimeoutJob(job) // Snapshot from before completion.
	require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobCompleted, recovered.GetJob(ctx, 100).GetState())
}

func TestRestoreCopyCoverageRequiresEveryExpectedSegment(t *testing.T) {
	job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
	job.IdMappings = []*datapb.CopySegmentIDMapping{{SourceSegmentId: 1, TargetSegmentId: 2001}, {SourceSegmentId: 2, TargetSegmentId: 2002}}
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted)(task)
	require.False(t, allRestoreSegmentsCopied(job, []CopySegmentTask{task}))
	task.task.Load().IdMappings = job.IdMappings
	require.True(t, allRestoreSegmentsCopied(job, []CopySegmentTask{task}))
	job.IdMappings = nil
	require.True(t, allRestoreSegmentsCopied(job, nil))
}

func TestRestoreSegmentRegistrationResumesFromJob(t *testing.T) {
	ctx := t.Context()
	catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	store := metacache.NewMetaStore(nil)
	m := &meta{metaStore: store, catalog: catalog, segments: NewSegmentsInfo(store), snapshotMeta: &snapshotMeta{}}
	copies, err := NewCopySegmentMeta(ctx, catalog, m, m.snapshotMeta, nil)
	require.NoError(t, err)
	job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobPending).(*copySegmentJob)
	job.CreatesCollection = true
	job.IdMappings = []*datapb.CopySegmentIDMapping{{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 10}, {SourceSegmentId: 2, TargetSegmentId: 2002, PartitionId: 10}}
	job.ChannelMapping = map[string]string{"source": "target"}
	job.TransformStartAfterTimeticks = map[string]uint64{"target": 123}
	require.NoError(t, copies.AddJob(ctx, job))
	snapshot := &snapshotstorage.SnapshotData{Segments: []*datapb.SegmentDescription{
		{SegmentId: 1, ChannelName: "source", SegmentLevel: datapb.SegmentLevel_L1},
		{SegmentId: 2, ChannelName: "source", SegmentLevel: datapb.SegmentLevel_L1},
	}}
	patch := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshot, nil).Build()
	defer patch.UnPatch()
	// Crash after Job persistence: no target segment has been registered yet.
	copies, err = NewCopySegmentMeta(ctx, catalog, m, m.snapshotMeta, nil)
	require.NoError(t, err)
	checker := &copySegmentChecker{ctx: ctx, meta: m, copyMeta: copies}
	var original func(*meta, context.Context, *SegmentInfo) error
	fail := mockey.Mock((*meta).AddSegment).Origin(&original).To(func(meta *meta, ctx context.Context, segment *SegmentInfo) error {
		if segment.GetID() == 2002 {
			return merr.ErrServiceUnavailable
		}
		return original(meta, ctx, segment)
	}).Build()
	require.ErrorIs(t, checker.registerRestoreSegments(copies.GetJob(ctx, 100)), merr.ErrServiceUnavailable)
	fail.UnPatch()
	require.NotNil(t, m.GetSegment(ctx, 2001))
	require.Nil(t, m.GetSegment(ctx, 2002))
	// Another restart after partial registration reuses both original IDs.
	copies, err = NewCopySegmentMeta(ctx, catalog, m, m.snapshotMeta, nil)
	require.NoError(t, err)
	checker.copyMeta = copies
	require.NoError(t, checker.registerRestoreSegments(copies.GetJob(ctx, 100)))
	require.NoError(t, checker.registerRestoreSegments(copies.GetJob(ctx, 100)))
	for _, id := range []int64{2001, 2002} {
		segment := m.GetSegment(ctx, id)
		require.Equal(t, "target", segment.GetInsertChannel())
		require.EqualValues(t, 123, segment.GetTransformStartAfterTimetick())
	}
}

func TestRestoreCallbackPersistsJobBeforeSegments(t *testing.T) {
	mockey.PatchConvey("callback owns segment allocation; retries reuse durable Job", t, func() {
		ctx := t.Context()
		catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
		store := metacache.NewMetaStore(nil)
		m := &meta{metaStore: store, catalog: catalog, segments: NewSegmentsInfo(store)}
		copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
		require.NoError(t, err)
		allocated := mockey.Mock((*restoreAllocatorTarget).AllocN).Return(int64(2001), int64(2002), nil).Build()
		mockey.Mock((*ServerHandler).GetCollection).Return(&collectionInfo{}, nil).Build()
		mockey.Mock((*meta).UpdateChannelCheckpoint).Return(nil).Build()
		sm := &snapshotManager{meta: m, copySegmentMeta: copies, allocator: &restoreAllocatorTarget{}, handler: &ServerHandler{}}
		snapshot := &snapshotstorage.SnapshotData{
			SnapshotInfo: &datapb.SnapshotInfo{Name: "snapshot", CollectionId: 1},
			Segments:     []*datapb.SegmentDescription{{SegmentId: 1, PartitionId: 1, ChannelName: "source", SegmentLevel: datapb.SegmentLevel_L1}},
		}
		require.NoError(t, sm.createRestoreJob(ctx, 100, map[string]string{"source": "target"}, map[int64]int64{1: 10}, snapshot, 77, 0, false, "", "", "", &restorePublication{starts: map[string]uint64{"target": 123}, createsCollection: true}))
		require.Nil(t, m.GetSegment(ctx, 2001), "registration must follow durable Job creation")
		job := copies.GetJob(ctx, 77)
		require.EqualValues(t, 2001, job.GetIdMappings()[0].GetTargetSegmentId())
		// WAL callback replay after restart resolves the original JobID first.
		sm.copySegmentMeta, err = NewCopySegmentMeta(ctx, catalog, m, nil, nil)
		require.NoError(t, err)
		id, err := sm.restoreData(ctx, 1, "snapshot", "", 100, 77, 0, false, "", "")
		require.NoError(t, err)
		require.EqualValues(t, 77, id)
		require.Equal(t, 1, allocated.Times())
	})
}

func TestRestoreBroadcastResourceOwnership(t *testing.T) {
	for _, tc := range []struct {
		name                                                                   string
		startErr, checkErr, prepareErr, allocErr, fingerprintErr, broadcastErr error
		external, accepted                                                     bool
	}{
		{name: "start failure", startErr: merr.ErrServiceUnavailable},
		{name: "reserved name", checkErr: merr.ErrParameterInvalid},
		{name: "prepare failure", prepareErr: merr.ErrServiceUnavailable},
		{name: "job allocation failure", allocErr: merr.ErrServiceUnavailable},
		{name: "external fingerprint failure", external: true, fingerprintErr: merr.ErrDataIntegrity},
		{name: "broadcast rejected", broadcastErr: errors.Mark(merr.ErrServiceUnavailable, broadcaster.ErrBroadcastTaskNotCreated)},
		{name: "durable broadcast owns canceled wait", broadcastErr: context.Canceled, accepted: true},
		{name: "external durable recipe", external: true, accepted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				checkErr := tc.checkErr
				if checkErr == nil {
					checkErr = merr.ErrCollectionNotFound
				}
				mockey.Mock((*rootcoord.Core).CheckRestoreTargetAbsent).Return(checkErr).Build()
				released := 0
				body := &messagespb.RestoreSnapshotMessageBody{CollectionHeader: &message.CreateCollectionMessageHeader{CollectionId: 10}, Collection: &message.CreateCollectionRequest{VirtualChannelNames: []string{"target"}}}
				prepared := mockey.Mock((*rootcoord.Core).PrepareRestoreCollection).Return(body, func() { released++ }, tc.prepareErr).Build()
				mockey.Mock((*restoreAllocatorTarget).AllocID).Return(int64(77), tc.allocErr).Build()
				mockey.Mock(snapshotstorage.SnapshotFingerprint).Return("fingerprint", tc.fingerprintErr).Build()
				api := &captureBroadcastAPI{}
				mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(api, tc.startErr).Build()
				sent := mockey.Mock((*captureBroadcastAPI).Broadcast).To(func(_ *captureBroadcastAPI, _ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
					require.True(t, msg.IsUnreplicable())
					recipe := message.MustAsBroadcastRestoreSnapshotMessageV2(msg)
					require.EqualValues(t, 10, recipe.Header().GetCollectionId())
					if tc.external {
						require.Equal(t, "fingerprint", recipe.Header().GetSnapshotFingerprint())
					}
					return nil, tc.broadcastErr
				}).Build()
				manager := &snapshotManager{collectionRestorer: &rootcoord.Core{}, allocator: &restoreAllocatorTarget{}}
				id, err := manager.broadcastRestoreCollection(t.Context(), &snapshotstorage.SnapshotData{Collection: &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "source"}}}, "snapshot", 1, "target", "default", 5, tc.external, "location", "")
				if tc.accepted {
					require.NoError(t, err)
					require.EqualValues(t, 77, id)
					require.Equal(t, 0, released)
					require.Equal(t, 1, sent.Times())
					return
				}
				require.Error(t, err)
				require.Zero(t, id)
				wantRelease := tc.startErr == nil && tc.checkErr == nil && tc.prepareErr == nil
				require.Equal(t, wantRelease, released == 1)
				if tc.startErr != nil || tc.checkErr != nil {
					require.Zero(t, prepared.Times())
				}
			})
		})
	}
}

func TestRestoreCallbackPreservesBusinessChannelFences(t *testing.T) {
	mockey.PatchConvey("indexes precede job, and CChannel cannot replace the target fence", t, func() {
		core := &rootcoord.Core{}
		apply := mockey.Mock((*rootcoord.Core).ApplyRestoreCollection).Return(merr.ErrServiceUnavailable).Build()
		body := &messagespb.RestoreSnapshotMessageBody{Collection: &message.CreateCollectionRequest{CollectionID: 10}}
		msg := message.NewRestoreSnapshotMessageBuilderV2().WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 10, JobId: 77, PinId: 5}).WithBody(body).WithBroadcast([]string{"target"}).MustBuildBroadcast()
		result := message.BroadcastResultRestoreSnapshotMessageV2{Message: message.MustAsBroadcastRestoreSnapshotMessageV2(msg), Results: map[string]*message.AppendResult{"target": {TimeTick: 123}, "p1_vcchan": {TimeTick: 999}}}
		manager := &snapshotManager{collectionRestorer: core}
		restore := mockey.Mock((*snapshotManager).restoreData).To(func(_ *snapshotManager, _ context.Context, _ int64, _ string, _ string, collectionID, jobID, pinID int64, _ bool, _ string, _ string, publication ...*restorePublication) (int64, error) {
			require.EqualValues(t, 10, collectionID)
			require.EqualValues(t, 77, jobID)
			require.EqualValues(t, 5, pinID)
			require.Len(t, publication, 1)
			require.True(t, publication[0].createsCollection)
			require.Equal(t, map[string]uint64{"target": 123}, publication[0].starts)
			return 77, nil
		}).Build()
		require.ErrorIs(t, manager.applyRestoreCollection(t.Context(), result), merr.ErrServiceUnavailable)
		require.Zero(t, restore.Times())
		apply.Return(nil)
		require.NoError(t, manager.applyRestoreCollection(t.Context(), result))
		require.Equal(t, 1, restore.Times())
	})
}

func TestRestoreRegistrationRejectsIncompleteSnapshotMapping(t *testing.T) {
	mockey.PatchConvey("registration fails closed while durable Job remains retryable", t, func() {
		job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobPending).(*copySegmentJob)
		job.IdMappings = []*datapb.CopySegmentIDMapping{{SourceSegmentId: 1, TargetSegmentId: 2}}
		snapshot := &snapshotstorage.SnapshotData{}
		patch := mockey.Mock(loadCopySnapshot).Return(nil, merr.ErrServiceUnavailable).Build()
		checker := &copySegmentChecker{ctx: t.Context(), meta: &meta{}}
		require.ErrorIs(t, checker.registerRestoreSegments(job), merr.ErrServiceUnavailable)
		patch.Return(snapshot, nil)
		require.ErrorIs(t, checker.registerRestoreSegments(job), merr.ErrDataIntegrity)
		snapshot.Segments = []*datapb.SegmentDescription{{SegmentId: 1, ChannelName: "source"}}
		require.ErrorIs(t, checker.registerRestoreSegments(job), merr.ErrDataIntegrity)
	})
}

func TestRestoreIndexRecipeValidationAndIdentity(t *testing.T) {
	for _, failure := range []string{"allocate index", "validate parameters", "json version", "scalar version", ""} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("validate the complete index recipe before durable ownership transfer", t, func() {
				mockey.Mock((*rootcoord.Core).CheckRestoreTargetAbsent).Return(merr.ErrCollectionNotFound).Build()
				body := &messagespb.RestoreSnapshotMessageBody{CollectionHeader: &message.CreateCollectionMessageHeader{CollectionId: 10}, Collection: &message.CreateCollectionRequest{VirtualChannelNames: []string{"target"}}}
				cleaned := 0
				mockey.Mock((*rootcoord.Core).PrepareRestoreCollection).Return(body, func() { cleaned++ }, nil).Build()
				id := int64(76)
				mockey.Mock((*restoreAllocatorTarget).AllocID).To(func(*restoreAllocatorTarget, context.Context) (int64, error) {
					if failure == "allocate index" {
						return 0, merr.ErrServiceUnavailable
					}
					id++
					return id, nil
				}).Build()
				var validation, jsonVersion, scalarVersion error
				switch failure {
				case "validate parameters":
					validation = merr.ErrParameterInvalid
				case "json version":
					jsonVersion = merr.ErrServiceUnavailable
				case "scalar version":
					scalarVersion = merr.ErrServiceUnavailable
				}
				mockey.Mock(indexparamcheck.ValidateIndexParams).Return(validation).Build()
				mockey.Mock((*snapshotManager).checkJSONPathIndexVersion).Return(jsonVersion).Build()
				mockey.Mock(checkFMIndexEngineVersion).To(func(_ []*commonpb.KeyValuePair, version int32) error {
					require.EqualValues(t, 5, version)
					return scalarVersion
				}).Build()
				mockey.Mock((*versionManagerImpl).ResolveScalarIndexVersion).Return(int32(5)).Build()
				api := &captureBroadcastAPI{}
				mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(api, nil).Build()
				sent := mockey.Mock((*captureBroadcastAPI).Broadcast).To(func(_ *captureBroadcastAPI, _ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
					recipe := message.MustAsBroadcastRestoreSnapshotMessageV2(msg).MustBody()
					require.Len(t, recipe.Indexes, 1)
					index := model.UnmarshalIndexModel(recipe.Indexes[0])
					require.EqualValues(t, 10, index.CollectionID)
					require.EqualValues(t, 77, index.IndexID)
					require.EqualValues(t, 101, index.FieldID)
					require.Equal(t, "vector_index", index.IndexName)
					require.Empty(t, index.TypeParams, "source mmap property is not copied into the native index recipe")
					return &types.BroadcastAppendResult{}, nil
				}).Build()
				manager := &snapshotManager{collectionRestorer: &rootcoord.Core{}, allocator: &restoreAllocatorTarget{}, indexEngineVersionManager: &versionManagerImpl{}}
				snapshot := &snapshotstorage.SnapshotData{Collection: &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "source", Properties: []*commonpb.KeyValuePair{{Key: "property", Value: "value"}}}}, Indexes: []*indexpb.IndexInfo{{FieldID: 101, IndexName: "vector_index", TypeParams: []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: "true"}}}}}
				jobID, err := manager.broadcastRestoreCollection(t.Context(), snapshot, "snapshot", 1, "target", "default", 0, false, "", "")
				if failure != "" {
					require.Error(t, err)
					require.Zero(t, jobID)
					require.Equal(t, 1, cleaned)
					require.Zero(t, sent.Times())
					return
				}
				require.NoError(t, err)
				require.EqualValues(t, 78, jobID)
				require.Equal(t, 0, cleaned)
				require.Equal(t, 1, sent.Times())
			})
		})
	}
}

func TestRestoreBroadcastChecksOrdinaryArraySortGate(t *testing.T) {
	for _, tc := range []struct {
		name, enabled, fieldName string
		dataType                 schemapb.DataType
		version                  int32
		expected                 error
	}{
		{"gate disabled", "false", "values", schemapb.DataType_Array, 5, merr.ErrServiceNotReady},
		{"version too old", "true", "values", schemapb.DataType_Array, 2, merr.ErrParameterInvalid},
		{"supported", "true", "values", schemapb.DataType_Array, 3, nil},
		{"existing scalar", "false", "value", schemapb.DataType_Int64, 2, nil},
		{"existing nested array", "false", "items[value]", schemapb.DataType_Array, 2, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				require.NoError(t, Params.Save(Params.DataCoordCfg.EnableArraySortIndex.Key, tc.enabled))
				defer Params.Reset(Params.DataCoordCfg.EnableArraySortIndex.Key)
				mockey.Mock((*versionManagerImpl).ResolveScalarIndexVersion).Return(tc.version).Build()
				schema := &schemapb.CollectionSchema{Name: "source", Fields: []*schemapb.FieldSchema{
					{FieldID: 101, Name: tc.fieldName, DataType: tc.dataType, ElementType: schemapb.DataType_Int64},
				}}
				body := &messagespb.RestoreSnapshotMessageBody{
					CollectionHeader: &message.CreateCollectionMessageHeader{CollectionId: 10},
					Collection:       &message.CreateCollectionRequest{CollectionID: 10, CollectionSchema: schema, VirtualChannelNames: []string{"v1"}},
				}
				mockey.Mock((*rootcoord.Core).CheckRestoreTargetAbsent).Return(merr.ErrCollectionNotFound).Build()
				cleaned := 0
				mockey.Mock((*rootcoord.Core).PrepareRestoreCollection).Return(body, func() { cleaned++ }, nil).Build()
				allocated := mockey.Mock((*restoreAllocatorTarget).AllocID).Return(int64(77), nil).Build()
				api := &captureBroadcastAPI{}
				mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(api, nil).Build()
				snapshot := &snapshotstorage.SnapshotData{
					Collection: &datapb.CollectionDescription{Schema: schema},
					Indexes: []*indexpb.IndexInfo{{FieldID: 101, IndexName: "sort", IndexParams: []*commonpb.KeyValuePair{
						{Key: common.IndexTypeKey, Value: indexparamcheck.IndexSTLSORT},
					}}},
				}
				sm := &snapshotManager{collectionRestorer: &rootcoord.Core{}, allocator: &restoreAllocatorTarget{}, indexEngineVersionManager: &versionManagerImpl{}}
				job, err := sm.broadcastRestoreCollection(t.Context(), snapshot, "snapshot", 1, "target", "default", 0, false, "", "")
				if tc.expected != nil {
					require.ErrorIs(t, err, tc.expected)
					require.Zero(t, job)
					require.Nil(t, api.captured, "reject before creating the durable Restore broadcast or Job")
					require.Equal(t, 1, allocated.Times(), "only the prospective index ID was allocated")
					require.Equal(t, 1, cleaned, "release the prepared collection's file resources")
				} else {
					require.NoError(t, err)
					require.EqualValues(t, 77, job)
					require.NotNil(t, api.captured)
					require.Equal(t, 2, allocated.Times())
					require.Zero(t, cleaned, "the durable broadcast now owns the resources")
				}
			})
		})
	}
}

func TestRestoreIndexPersistencePrecedesCopyJob(t *testing.T) {
	mockey.PatchConvey("index persistence failure cannot start data copying", t, func() {
		mockey.Mock((*rootcoord.Core).ApplyRestoreCollection).Return(nil).Build()
		create := mockey.Mock((*indexMeta).CreateIndex).Return(merr.ErrServiceUnavailable).Build()
		restore := mockey.Mock((*snapshotManager).restoreData).Return(int64(77), nil).Build()
		body := &messagespb.RestoreSnapshotMessageBody{Collection: &message.CreateCollectionRequest{CollectionID: 10}, Indexes: []*indexpb.FieldIndex{model.MarshalIndexModel(&model.Index{CollectionID: 10, IndexID: 11})}}
		msg := message.NewRestoreSnapshotMessageBuilderV2().WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 10}).WithBody(body).WithBroadcast([]string{"target"}).MustBuildBroadcast()
		manager := &snapshotManager{collectionRestorer: &rootcoord.Core{}, meta: &meta{indexMeta: &indexMeta{}}}
		result := message.BroadcastResultRestoreSnapshotMessageV2{Message: message.MustAsBroadcastRestoreSnapshotMessageV2(msg)}
		require.ErrorIs(t, manager.applyRestoreCollection(t.Context(), result), merr.ErrServiceUnavailable)
		require.Zero(t, restore.Times())
		create.Return(nil)
		require.NoError(t, manager.applyRestoreCollection(t.Context(), result))
		require.Equal(t, 1, restore.Times())
	})
}
