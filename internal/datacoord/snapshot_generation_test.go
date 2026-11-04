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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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

func TestSnapshotGenerationDropProducer(t *testing.T) {
	mockey.PatchConvey("broadcast carries the identity resolved under the name lock", t, func() {
		lookups := 0
		mockey.Mock((*snapshotManager).GetSnapshot).To(func(_ *snapshotManager, _ context.Context, _ int64, _ string) (*datapb.SnapshotInfo, error) {
			lookups++
			return &datapb.SnapshotInfo{Id: 9000 + int64(lookups)}, nil
		}).Build()
		mockey.Mock((*embeddedHandler).GetCollection).Return(&collectionInfo{
			ID: 100, DatabaseName: "default", Schema: &schemapb.CollectionSchema{Name: "source"},
		}, nil).Build()
		mockey.Mock((*snapshotManager).HasActivePins).Return(false, nil).Build()
		mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(&captureBroadcastAPI{}, nil).Build()
		mockey.Mock((*captureBroadcastAPI).Broadcast).To(func(_ *captureBroadcastAPI, _ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			header := message.MustAsBroadcastDropSnapshotMessageV2(msg).Header()
			require.EqualValues(t, 9002, header.GetSnapshotId())
			return &types.BroadcastAppendResult{}, nil
		}).Build()
		server := &Server{handler: &embeddedHandler{}, snapshotManager: &snapshotManager{}}
		server.stateCode.Store(commonpb.StateCode_Healthy)
		status, err := server.DropSnapshot(t.Context(), &datapb.DropSnapshotRequest{CollectionId: 100, Name: "snapshot"})
		require.NoError(t, err)
		require.NoError(t, merr.Error(status))
		require.Equal(t, 2, lookups)
	})
}

func snapshotGenerationDropResult(id int64) message.BroadcastResultDropSnapshotMessageV2 {
	msg := message.NewDropSnapshotMessageBuilderV2().WithHeader(&message.DropSnapshotMessageHeader{
		SnapshotId: id, CollectionId: 100, Name: "snapshot",
	}).WithBody(&message.DropSnapshotMessageBody{}).WithControlChannelBroadcast().MustBuildBroadcast()
	return message.BroadcastResultDropSnapshotMessageV2{Message: message.MustAsBroadcastDropSnapshotMessageV2(msg)}
}

func TestSnapshotGenerationDropReplay(t *testing.T) {
	m := newReplicaSnapshotTestMeta(t)
	data := replicaSnapshotTestData()
	require.NoError(t, m.SaveSnapshot(t.Context(), data))
	cb := &DDLCallbacks{Server: &Server{snapshotManager: &snapshotManager{snapshotMeta: m}}}
	old := snapshotGenerationDropResult(9001)
	require.NoError(t, cb.dropSnapshotV2AckCallback(t.Context(), old))
	require.NoError(t, cb.dropSnapshotV2AckCallback(t.Context(), old), "missing generation must not block DDL")
	data.SnapshotInfo.Id = 9002
	require.NoError(t, m.SaveSnapshot(t.Context(), data))
	require.NoError(t, cb.dropSnapshotV2AckCallback(t.Context(), old), "replay must not drop the new namesake")
	info, err := m.GetSnapshot(t.Context(), 100, "snapshot")
	require.NoError(t, err)
	require.EqualValues(t, 9002, info.GetId())
	require.NoError(t, cb.dropSnapshotV2AckCallback(t.Context(), snapshotGenerationDropResult(9002)))
	_, err = m.GetSnapshot(t.Context(), 100, "snapshot")
	require.ErrorIs(t, err, merr.ErrSnapshotNotFound)
}

func TestSnapshotGenerationDropRetry(t *testing.T) {
	mockey.PatchConvey("only a missing identity is ignored", t, func() {
		m := newReplicaSnapshotTestMeta(t)
		require.NoError(t, m.SaveSnapshot(t.Context(), replicaSnapshotTestData()))
		cb := &DDLCallbacks{Server: &Server{snapshotManager: &snapshotManager{snapshotMeta: m}}}
		_, err := m.ensureRestorePinByID(t.Context(), 100, "snapshot", 77, 9001)
		require.NoError(t, err)
		require.ErrorIs(t, cb.dropSnapshotV2AckCallback(t.Context(), snapshotGenerationDropResult(9001)), merr.ErrSnapshotPinned)
		_, _, _, err = m.UnpinSnapshot(t.Context(), 77)
		require.NoError(t, err)
		patch := mockey.Mock((*kvdatacoord.Catalog).SaveSnapshot).Return(merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, cb.dropSnapshotV2AckCallback(t.Context(), snapshotGenerationDropResult(9001)), merr.ErrServiceUnavailable)
		patch.UnPatch()
		require.NoError(t, cb.dropSnapshotV2AckCallback(t.Context(), snapshotGenerationDropResult(9001)))
	})
}

func snapshotGenerationRestoreResult(id int64, createsCollection bool) message.BroadcastResultRestoreSnapshotMessageV2 {
	body := &message.RestoreSnapshotMessageBody{}
	if createsCollection {
		body.Collection = &message.CreateCollectionRequest{CollectionID: 200}
	}
	msg := message.NewRestoreSnapshotMessageBuilderV2().WithHeader(&message.RestoreSnapshotMessageHeader{
		SnapshotId: id, SnapshotName: "snapshot", SourceCollectionId: 100, CollectionId: 200, JobId: 101, PinId: 77,
	}).WithBody(body).WithControlChannelBroadcast().MustBuildBroadcast()
	return message.BroadcastResultRestoreSnapshotMessageV2{Message: message.MustAsBroadcastRestoreSnapshotMessageV2(msg)}
}

func TestSnapshotGenerationRestoreMissing(t *testing.T) {
	for _, createsCollection := range []bool{false, true} {
		for _, namesake := range []bool{false, true} {
			t.Run(fmt.Sprintf("creates_collection=%t/namesake=%t", createsCollection, namesake), func(t *testing.T) {
				mockey.PatchConvey("missing source must not restore a reused name or block DDL", t, func() {
					m := newReplicaSnapshotTestMeta(t)
					if namesake {
						data := replicaSnapshotTestData()
						data.SnapshotInfo.Id = 9002
						require.NoError(t, m.SaveSnapshot(t.Context(), data))
					}
					copies, _ := newCopySegmentTaskTestMeta(t, createTestCopyTask(101, 2).(*copySegmentTask))
					mockey.Mock((*rootcoord.Core).ApplyRestoreCollection).Return(nil).Build()
					readByName := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(nil, merr.ErrServiceInternal).Build()
					sm := &snapshotManager{snapshotMeta: m, copySegmentMeta: copies, collectionRestorer: &rootcoord.Core{}}
					cb := &DDLCallbacks{Server: &Server{snapshotManager: sm}}
					result := snapshotGenerationRestoreResult(9001, createsCollection)
					for range 2 {
						require.NoError(t, cb.restoreSnapshotV2AckCallback(t.Context(), result))
					}
					job := copies.GetJob(t.Context(), 101)
					require.NotNil(t, job)
					require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, job.GetState())
					require.EqualValues(t, 9001, job.GetSnapshotId())
					require.Empty(t, job.GetIdMappings())
					require.Zero(t, readByName.Times())
					if namesake {
						info, err := m.GetSnapshot(t.Context(), 100, "snapshot")
						require.NoError(t, err)
						require.Empty(t, info.GetPinIds())
					}
				})
			})
		}
	}
}

func TestSnapshotGenerationRestoreAdmissionAndRecovery(t *testing.T) {
	mockey.PatchConvey("persist the source ID and use it after job recovery", t, func() {
		m := newReplicaSnapshotTestMeta(t)
		data := replicaSnapshotTestData()
		require.NoError(t, m.SaveSnapshot(t.Context(), data))
		copies, segmentMeta := newCopySegmentTaskTestMeta(t, createTestCopyTask(101, 2).(*copySegmentTask))
		sm := &snapshotManager{snapshotMeta: m, copySegmentMeta: copies, meta: segmentMeta, handler: &embeddedHandler{}}
		mockey.Mock((*embeddedHandler).GetCollection).Return(&collectionInfo{}, nil).Build()
		mockey.Mock((*snapshotManager).buildPartitionMapping).Return(map[int64]int64{}, nil).Build()
		mockey.Mock((*snapshotManager).buildChannelMapping).Return(map[string]string{}, nil).Build()
		id, err := sm.restoreData(t.Context(), 100, "snapshot", "", 200, 101, 77, false, "", "",
			&restorePublication{createsCollection: true, replicated: true, snapshotID: 9001})
		require.NoError(t, err)
		require.EqualValues(t, 101, id)
		_, err = m.getSnapshotByID(t.Context(), 101, "snapshot", 9001)
		require.ErrorIs(t, err, merr.ErrSnapshotNotFound, "ID must still belong to the declared source collection")
		_, _, _, err = m.UnpinSnapshot(t.Context(), 77)
		require.NoError(t, err)
		require.NoError(t, m.DropSnapshot(t.Context(), 100, "snapshot"))
		data.SnapshotInfo.Id = 9002
		require.NoError(t, m.SaveSnapshot(t.Context(), data))
		recovered, err := NewCopySegmentMeta(t.Context(), segmentMeta.catalog, segmentMeta, nil, nil)
		require.NoError(t, err)
		job := recovered.GetJob(t.Context(), 101)
		require.EqualValues(t, 9001, job.GetSnapshotId())
		_, err = loadCopySnapshot(t.Context(), job, m)
		require.ErrorIs(t, err, merr.ErrSnapshotNotFound, "restart must not acquire the new namesake")
		_, err = m.ensureRestorePinByID(t.Context(), 100, "snapshot", 78, 9001)
		require.ErrorIs(t, err, merr.ErrSnapshotNotFound)
		info, err := m.GetSnapshot(t.Context(), 100, "snapshot")
		require.NoError(t, err)
		require.Empty(t, info.GetPinIds())
	})
}

func TestSnapshotGenerationRestoreTransientRead(t *testing.T) {
	mockey.PatchConvey("a storage outage is retried instead of ignored", t, func() {
		copies, _ := newCopySegmentTaskTestMeta(t, createTestCopyTask(101, 2).(*copySegmentTask))
		sm := &snapshotManager{snapshotMeta: newReplicaSnapshotTestMeta(t), copySegmentMeta: copies}
		mockey.Mock((*snapshotMeta).readSnapshotData).Return(nil, merr.ErrServiceUnavailable).Build()
		cb := &DDLCallbacks{Server: &Server{snapshotManager: sm}}
		require.ErrorIs(t, cb.restoreSnapshotV2AckCallback(t.Context(), snapshotGenerationRestoreResult(9001, false)), merr.ErrServiceUnavailable)
		require.Nil(t, copies.GetJob(t.Context(), 101))
	})
}

func TestSnapshotGenerationReadRejectsChangedBundle(t *testing.T) {
	mockey.PatchConvey("catalog identity cannot mask a replaced metadata bundle", t, func() {
		m := newReplicaSnapshotTestMeta(t)
		require.NoError(t, m.SaveSnapshot(t.Context(), replicaSnapshotTestData()))
		data := replicaSnapshotTestData()
		data.SnapshotInfo.Id = 9002
		mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).Return(data, nil).Build()
		_, err := m.readSnapshotData(t.Context(), 100, "snapshot", true, 9001)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	})
}

func TestSnapshotGenerationHeaderRoundTrip(t *testing.T) {
	for _, header := range []proto.Message{
		&messagespb.DropSnapshotMessageHeader{SnapshotId: 9001},
		&messagespb.RestoreSnapshotMessageHeader{SnapshotId: 9001},
	} {
		encoded, err := proto.Marshal(header)
		require.NoError(t, err)
		decoded := header.ProtoReflect().New().Interface()
		require.NoError(t, proto.Unmarshal(encoded, decoded))
		require.True(t, proto.Equal(header, decoded))
	}
}
