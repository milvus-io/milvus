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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type cutoffTestAllocator struct {
	ids    []int64
	nStart int64
	nEnd   int64
	nErr   error
}

func (a *cutoffTestAllocator) AllocTimestamp(context.Context) (typeutil.Timestamp, error) {
	return 0, nil
}

func (a *cutoffTestAllocator) AllocID(context.Context) (typeutil.UniqueID, error) {
	if len(a.ids) == 0 {
		return 0, nil
	}
	id := a.ids[0]
	a.ids = a.ids[1:]
	return id, nil
}

func (a *cutoffTestAllocator) AllocN(n int64) (typeutil.UniqueID, typeutil.UniqueID, error) {
	return a.nStart, a.nEnd, a.nErr
}

type cutoffTestHandler struct {
	Handler
	collection *collectionInfo
	err        error
}

func (h *cutoffTestHandler) GetCollection(context.Context, UniqueID) (*collectionInfo, error) {
	return h.collection, h.err
}

type cutoffBroadcastAPI struct {
	closeFn     func()
	broadcastFn func(message.BroadcastMutableMessage)
}

func (b *cutoffBroadcastAPI) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	if b.broadcastFn != nil {
		b.broadcastFn(msg)
	}
	return &types.BroadcastAppendResult{}, nil
}

func (b *cutoffBroadcastAPI) Close() {
	if b.closeFn != nil {
		b.closeFn()
	}
}

var _ broadcaster.BroadcastAPI = (*cutoffBroadcastAPI)(nil)

func TestServer_RestoreSnapshotDoesNotRequireCutoffTs(t *testing.T) {
	ctx := context.Background()

	mockRestore := mockey.Mock((*snapshotManager).RestoreSnapshot).To(
		func(
			sm *snapshotManager,
			ctx context.Context,
			sourceCollectionID int64,
			snapshotName string,
			targetCollectionName string,
			targetDbName string,
			startRestoreLock StartRestoreLockFunc,
			startBroadcaster StartBroadcasterFunc,
			rollback RollbackFunc,
			validateResources ValidateResourcesFunc,
		) (int64, error) {
			assert.Equal(t, int64(100), sourceCollectionID)
			assert.Equal(t, "snapshot_a", snapshotName)
			assert.Equal(t, "target_a", targetCollectionName)
			assert.Equal(t, "default", targetDbName)
			return 99, nil
		}).Build()
	defer mockRestore.UnPatch()

	server := &Server{
		snapshotManager: NewSnapshotManager(nil, nil, nil, nil, nil, nil, nil, nil),
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	resp, err := server.RestoreSnapshot(ctx, &datapb.RestoreSnapshotRequest{
		Name:                 "snapshot_a",
		SourceCollectionId:   100,
		TargetDbName:         "default",
		TargetCollectionName: "target_a",
	})

	assert.NoError(t, err)
	assert.NoError(t, merr.Error(resp.GetStatus()))
	assert.Equal(t, int64(99), resp.GetJobId())
}

func TestDDLCallbacks_RestoreSnapshotV2AckCallbackWithoutCutoff(t *testing.T) {
	ctx := context.Background()

	mockRestoreData := mockey.Mock((*snapshotManager).RestoreData).To(
		func(
			sm *snapshotManager,
			ctx context.Context,
			sourceCollectionID int64,
			snapshotName string,
			collectionID int64,
			jobID int64,
			pinID int64,
		) (int64, error) {
			assert.Equal(t, int64(100), sourceCollectionID)
			assert.Equal(t, "snapshot_a", snapshotName)
			assert.Equal(t, int64(200), collectionID)
			assert.Equal(t, int64(300), jobID)
			assert.Equal(t, int64(400), pinID)
			return jobID, nil
		}).Build()
	defer mockRestoreData.UnPatch()

	callbacks := &DDLCallbacks{Server: &Server{snapshotManager: &snapshotManager{}}}
	broadcastMsg := message.NewRestoreSnapshotMessageBuilderV2().
		WithHeader(&message.RestoreSnapshotMessageHeader{
			SnapshotName:       "snapshot_a",
			SourceCollectionId: 100,
			CollectionId:       200,
			JobId:              300,
			PinId:              400,
		}).
		WithBody(&message.RestoreSnapshotMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()

	err := callbacks.restoreSnapshotV2AckCallback(ctx, message.BroadcastResultRestoreSnapshotMessageV2{
		Message: message.MustAsBroadcastRestoreSnapshotMessageV2(broadcastMsg),
	})

	assert.NoError(t, err)
}

func TestSnapshotManager_RestoreSnapshotBroadcastsWithoutCutoffTs(t *testing.T) {
	ctx := context.Background()

	mPin := mockey.Mock((*snapshotMeta).PinSnapshot).Return(int64(42), 1, nil).Build()
	defer mPin.UnPatch()
	mUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).Return(int64(0), "", 0, nil).Build()
	defer mUnpin.UnPatch()
	mRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(&SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snapshot_a"},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "source"},
		},
	}, nil).Build()
	defer mRead.UnPatch()
	mValidateCMEK := mockey.Mock((*snapshotManager).validateCMEKCompatibility).Return(nil).Build()
	defer mValidateCMEK.UnPatch()
	mRestoreCollection := mockey.Mock((*snapshotManager).RestoreCollection).Return(int64(200), nil).Build()
	defer mRestoreCollection.UnPatch()
	mRestoreIndexes := mockey.Mock((*snapshotManager).RestoreIndexes).Return(nil).Build()
	defer mRestoreIndexes.UnPatch()

	streaming.SetupNoopWALForTest()

	var captured message.BroadcastMutableMessage
	sm := &snapshotManager{
		allocator:       &cutoffTestAllocator{ids: []int64{300}},
		snapshotMeta:    &snapshotMeta{},
		copySegmentMeta: &copySegmentMeta{},
	}
	startRestoreLock := func(context.Context, int64, string, string, string) (broadcaster.BroadcastAPI, error) {
		return &cutoffBroadcastAPI{}, nil
	}
	startBroadcaster := func(context.Context, int64, string) (broadcaster.BroadcastAPI, error) {
		return &cutoffBroadcastAPI{broadcastFn: func(msg message.BroadcastMutableMessage) {
			captured = msg
		}}, nil
	}
	rollback := func(context.Context, string, string) error {
		t.Fatal("rollback should not be called")
		return nil
	}
	validateResources := func(context.Context, int64, *SnapshotData) error {
		return nil
	}

	jobID, err := sm.RestoreSnapshot(
		ctx, 100, "snapshot_a", "target_a", "default",
		startRestoreLock, startBroadcaster, rollback, validateResources)

	assert.NoError(t, err)
	assert.Equal(t, int64(300), jobID)
	require.NotNil(t, captured)
	restoreMsg := message.MustAsBroadcastRestoreSnapshotMessageV2(captured)
	assert.Equal(t, "snapshot_a", restoreMsg.Header().GetSnapshotName())
	assert.Equal(t, int64(100), restoreMsg.Header().GetSourceCollectionId())
	assert.Equal(t, int64(200), restoreMsg.Header().GetCollectionId())
	assert.Equal(t, int64(300), restoreMsg.Header().GetJobId())
	assert.Equal(t, int64(42), restoreMsg.Header().GetPinId())
}

func TestSnapshotManager_RestoreDataCreatesJobWithCutoff(t *testing.T) {
	ctx := context.Background()
	const cutoffTs uint64 = 12345

	mRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(&SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snapshot_a", CollectionId: 100, CreateTs: int64(cutoffTs)},
		Collection: &datapb.CollectionDescription{
			Partitions: map[string]int64{"_default": 1},
		},
	}, nil).Build()
	defer mRead.UnPatch()
	mGetJob := mockey.Mock((*copySegmentMeta).GetJob).Return(CopySegmentJob(nil)).Build()
	defer mGetJob.UnPatch()
	mBuildPartition := mockey.Mock((*snapshotManager).buildPartitionMapping).Return(map[int64]int64{1: 10}, nil).Build()
	defer mBuildPartition.UnPatch()
	mBuildChannel := mockey.Mock((*snapshotManager).buildChannelMapping).Return(map[string]string{"ch1": "ch2"}, nil).Build()
	defer mBuildChannel.UnPatch()

	var capturedCutoffTs uint64
	mCreateJob := mockey.Mock((*snapshotManager).createRestoreJob).To(
		func(
			sm *snapshotManager,
			ctx context.Context,
			collectionID int64,
			channelMapping map[string]string,
			partitionMapping map[int64]int64,
			snapshotData *SnapshotData,
			jobID int64,
			pinID int64,
			cutoffTs uint64,
		) error {
			capturedCutoffTs = cutoffTs
			assert.Equal(t, int64(200), collectionID)
			assert.Equal(t, int64(300), jobID)
			assert.Equal(t, int64(400), pinID)
			return nil
		}).Build()
	defer mCreateJob.UnPatch()

	sm := &snapshotManager{copySegmentMeta: &copySegmentMeta{}}
	jobID, err := sm.RestoreData(ctx, 100, "snapshot_a", 200, 300, 400)

	assert.NoError(t, err)
	assert.Equal(t, int64(300), jobID)
	assert.Equal(t, cutoffTs, capturedCutoffTs)
}

func TestSnapshotManager_RestoreDataRejectsNegativeSnapshotCreateTs(t *testing.T) {
	ctx := context.Background()

	mRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(&SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snapshot_a", CollectionId: 100, CreateTs: -1},
	}, nil).Build()
	defer mRead.UnPatch()
	mGetJob := mockey.Mock((*copySegmentMeta).GetJob).Return(CopySegmentJob(nil)).Build()
	defer mGetJob.UnPatch()

	createJobCalled := false
	mCreateJob := mockey.Mock((*snapshotManager).createRestoreJob).To(
		func(
			sm *snapshotManager,
			ctx context.Context,
			collectionID int64,
			channelMapping map[string]string,
			partitionMapping map[int64]int64,
			snapshotData *SnapshotData,
			jobID int64,
			pinID int64,
			cutoffTs uint64,
		) error {
			createJobCalled = true
			return nil
		}).Build()
	defer mCreateJob.UnPatch()

	sm := &snapshotManager{copySegmentMeta: &copySegmentMeta{}}
	jobID, err := sm.RestoreData(ctx, 100, "snapshot_a", 200, 300, 400)

	assert.Error(t, err)
	assert.Equal(t, int64(0), jobID)
	assert.Contains(t, err.Error(), "snapshot create_ts is negative")
	assert.False(t, createJobCalled)
}

func TestSnapshotManager_CreateRestoreJobWithCutoffPersistsCutoffTs(t *testing.T) {
	ctx := context.Background()
	const cutoffTs uint64 = 12345

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snapshot_a", CollectionId: 100},
		Segments:     []*datapb.SegmentDescription{},
	}

	var captured *datapb.CopySegmentJob
	mAddJob := mockey.Mock((*copySegmentMeta).AddJob).To(
		func(_ *copySegmentMeta, _ context.Context, job CopySegmentJob) error {
			captured = job.(*copySegmentJob).CopySegmentJob
			return nil
		}).Build()
	defer mAddJob.UnPatch()

	sm := &snapshotManager{
		meta:            &meta{},
		allocator:       &cutoffTestAllocator{},
		handler:         &cutoffTestHandler{collection: &collectionInfo{}},
		copySegmentMeta: &copySegmentMeta{},
	}

	err := sm.createRestoreJob(ctx, 200, map[string]string{}, map[int64]int64{}, snapshotData, 300, 400, cutoffTs)

	assert.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, cutoffTs, captured.GetCutoffTs())
	assert.Equal(t, int64(400), captured.GetPinId())
}

func TestAssembleCopySegmentRequestWithCutoff(t *testing.T) {
	const cutoffTs uint64 = 12345
	schema := &schemapb.CollectionSchema{
		Name: "source",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}
	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{CollectionId: 100, Name: "snapshot_a"},
		Collection:   &datapb.CollectionDescription{Schema: schema},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1,
				PartitionId: 10,
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{BuildID: 3001, FieldID: 100, IndexID: 1001},
				},
				TextIndexFiles: map[int64]*datapb.TextIndexStats{
					200: {FieldID: 200, BuildID: 4001},
				},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
					300: {FieldID: 300, BuildID: 5001},
				},
			},
		},
	}

	mRead := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build()
	defer mRead.UnPatch()
	mStorageConfig := mockey.Mock(createStorageConfig).Return(nil).Build()
	defer mStorageConfig.UnPatch()

	task := &copySegmentTask{
		snapshotMeta: &snapshotMeta{},
		alloc:        &cutoffTestAllocator{ids: []int64{9001, 9002, 9003}},
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 10},
		},
	})
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              100,
			CollectionId:       100,
			SnapshotName:       "snapshot_a",
			SourceCollectionId: 100,
			CutoffTs:           cutoffTs,
		},
		tr: timerecord.NewTimeRecorder("test_job"),
	}

	req, err := AssembleCopySegmentRequest(task, job)

	assert.NoError(t, err)
	require.Len(t, req.GetSources(), 1)
	require.Len(t, req.GetTargets(), 1)
	assert.Equal(t, cutoffTs, req.GetSources()[0].GetCutoffTs())
	assert.Equal(t, int64(2001), req.GetTargets()[0].GetSegmentId())
	assert.Equal(t, schema, req.GetSchema())
}
