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
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/rootcoord"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
	m := &meta{catalog: catalog, segments: NewSegmentsInfo(), snapshotMeta: &snapshotMeta{}}
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
		m := &meta{catalog: catalog, segments: NewSegmentsInfo()}
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
