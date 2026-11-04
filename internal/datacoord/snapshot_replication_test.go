// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newReplicaSnapshotTestMeta(t *testing.T) *snapshotMeta {
	m := createTestSnapshotMetaLoaded(t)
	t.Cleanup(m.loaderCancel)
	m.catalog = kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m.ensurePinMaps()
	return m
}

func replicaSnapshotTestData() *snapshotstorage.SnapshotData {
	return &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 9001, CollectionId: 100, Name: "snapshot"},
		Collection:   &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "source"}},
	}
}

func TestReplicatedSnapshotIdentityAndReadiness(t *testing.T) {
	mockey.PatchConvey("stable identity and restore completion fence", t, func() {
		m := newReplicaSnapshotTestMeta(t)
		available := false
		mockey.Mock((*restoreBrokerTarget).HasCollection).To(func(_ *restoreBrokerTarget, _ context.Context, _ int64) (bool, error) {
			return available, nil
		}).Build()
		allocated := mockey.Mock((*restoreAllocatorTarget).AllocID).Return(int64(0), merr.ErrServiceInternal).Build()
		generated := mockey.Mock((*embeddedHandler).GenSnapshot).To(func(_ *embeddedHandler, _ context.Context, _ int64, positions []*msgpb.MsgPosition) (*snapshotstorage.SnapshotData, error) {
			require.EqualValues(t, 22, positions[0].GetTimestamp())
			return replicaSnapshotTestData(), nil
		}).Build()
		sm := &snapshotManager{snapshotMeta: m, broker: &restoreBrokerTarget{}, handler: &embeddedHandler{}, allocator: &restoreAllocatorTarget{}}
		positions := []*msgpb.MsgPosition{{ChannelName: "secondary_100v0", Timestamp: 22}}
		for range 2 {
			_, err := sm.createSnapshot(t.Context(), 100, "snapshot", "", 0, positions, 9001)
			require.ErrorIs(t, err, merr.ErrServiceNotReady)
		}
		require.Zero(t, generated.Times(), "pending Restore data must not be omitted from a new snapshot")
		available = true
		for range 3 {
			id, err := sm.createSnapshot(t.Context(), 100, "snapshot", "", 0, positions, 9001)
			require.NoError(t, err)
			require.EqualValues(t, 9001, id)
		}
		require.Zero(t, allocated.Times())
		require.Equal(t, 1, generated.Times(), "replay must reuse the committed snapshot")
		persisted, err := m.catalog.ListSnapshots(t.Context())
		require.NoError(t, err)
		require.Len(t, persisted, 1)
		require.EqualValues(t, 9001, persisted[0].GetId())
		require.Equal(t, datapb.SnapshotState_SnapshotStateCommitted, persisted[0].GetState())
		_, err = sm.createSnapshot(t.Context(), 100, "snapshot", "", 0, positions, 9002)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	})
}

func TestReplicatedSnapshotCallbackUsesLocalCutsAndPrimaryID(t *testing.T) {
	for _, role := range []string{"primary", "secondary"} {
		t.Run(role, func(t *testing.T) {
			mockey.PatchConvey("each receiver keeps its own WAL cuts", t, func() {
				channel := role + "_100v0"
				cut := uint64(22)
				if role == "secondary" {
					cut = 44
				}
				mockey.Mock((*restoreBrokerTarget).HasCollection).Return(true, nil).Build()
				allocated := mockey.Mock((*restoreAllocatorTarget).AllocID).Return(int64(0), merr.ErrServiceInternal).Build()
				generated := mockey.Mock((*embeddedHandler).GenSnapshot).To(func(_ *embeddedHandler, _ context.Context, _ int64, positions []*msgpb.MsgPosition) (*snapshotstorage.SnapshotData, error) {
					require.Len(t, positions, 1, "CChannel must not enter the snapshot cut")
					require.Equal(t, channel, positions[0].GetChannelName())
					require.Equal(t, cut, positions[0].GetTimestamp())
					return replicaSnapshotTestData(), nil
				}).Build()
				sm := &snapshotManager{snapshotMeta: newReplicaSnapshotTestMeta(t), broker: &restoreBrokerTarget{}, handler: &embeddedHandler{}, allocator: &restoreAllocatorTarget{}}
				cb := &DDLCallbacks{Server: &Server{snapshotManager: sm}}
				bc := message.NewCreateSnapshotMessageBuilderV2().WithHeader(&message.CreateSnapshotMessageHeader{
					SnapshotId: 9001, CollectionId: 100, Name: "snapshot",
				}).WithBody(&message.CreateSnapshotMessageBody{}).WithBroadcast([]string{channel}).MustBuildBroadcast()
				result := message.BroadcastResultCreateSnapshotMessageV2{Message: message.MustAsBroadcastCreateSnapshotMessageV2(bc), Results: map[string]*message.AppendResult{
					channel:                          {TimeTick: cut, LastConfirmedMessageID: rmq.NewRmqID(10)},
					funcutil.GetControlChannel(role): {TimeTick: 999, LastConfirmedMessageID: rmq.NewRmqID(20)},
				}}
				for range 2 {
					require.NoError(t, cb.createSnapshotV2AckCallback(t.Context(), result))
				}
				info, err := sm.GetSnapshot(t.Context(), 100, "snapshot")
				require.NoError(t, err)
				require.EqualValues(t, 9001, info.GetId())
				require.Zero(t, allocated.Times())
				require.Equal(t, 1, generated.Times())
			})
		})
	}
}

func TestReplicatedSnapshotTransientAdmissionErrors(t *testing.T) {
	mockey.PatchConvey("lookup and collection readiness failures remain retriable", t, func() {
		sm := &snapshotManager{snapshotMeta: newReplicaSnapshotTestMeta(t), broker: &restoreBrokerTarget{}}
		lookup := mockey.Mock((*snapshotMeta).GetSnapshot).Return(nil, merr.ErrServiceUnavailable).Build()
		_, err := sm.createSnapshot(t.Context(), 100, "snapshot", "", 0, nil, 9001)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		lookup.UnPatch()
		mockey.Mock((*restoreBrokerTarget).HasCollection).Return(false, merr.ErrServiceUnavailable).Build()
		_, err = sm.createSnapshot(t.Context(), 100, "snapshot", "", 0, nil, 9001)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})
}

func TestReplicatedRestorePinPersistence(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "secondary", true: "existing expiring pin"}[existing], func(t *testing.T) {
			m := newReplicaSnapshotTestMeta(t)
			data := replicaSnapshotTestData()
			if existing {
				data.SnapshotInfo.PinIds = []int64{77}
				data.SnapshotInfo.PinExpireAtMs = map[int64]int64{77: time.Now().Add(time.Hour).UnixMilli()}
			}
			require.NoError(t, m.SaveSnapshot(t.Context(), data))
			for range 3 {
				active, err := m.ensureRestorePin(t.Context(), 100, "snapshot", 77)
				require.NoError(t, err)
				require.Equal(t, 1, active)
			}
			persisted, err := m.catalog.ListSnapshots(t.Context())
			require.NoError(t, err)
			require.Equal(t, []int64{77}, persisted[0].GetPinIds())
			require.Empty(t, persisted[0].GetPinExpireAtMs())
			recovered := newReplicaSnapshotTestMeta(t)
			recovered.catalog = m.catalog
			require.NoError(t, recovered.reload(t.Context()))
			require.ErrorIs(t, recovered.DropSnapshot(t.Context(), 100, "snapshot"), merr.ErrSnapshotPinned)
			_, _, active, err := recovered.UnpinSnapshot(t.Context(), 77)
			require.NoError(t, err)
			require.Zero(t, active)
			pinned, err := recovered.HasActivePins(t.Context(), 100, "snapshot")
			require.NoError(t, err)
			require.False(t, pinned)
		})
	}
}

func TestReplicatedRestorePinSaveFailure(t *testing.T) {
	mockey.PatchConvey("failed persistence cannot publish or lose a pin", t, func() {
		m := newReplicaSnapshotTestMeta(t)
		require.NoError(t, m.SaveSnapshot(t.Context(), replicaSnapshotTestData()))
		patch := mockey.Mock((*kvdatacoord.Catalog).SaveSnapshot).Return(merr.ErrServiceUnavailable).Build()
		_, err := m.ensureRestorePin(t.Context(), 100, "snapshot", 77)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		pinned, err := m.HasActivePins(t.Context(), 100, "snapshot")
		require.NoError(t, err)
		require.False(t, pinned)
		patch.UnPatch()
		active, err := m.ensureRestorePin(t.Context(), 100, "snapshot", 77)
		require.NoError(t, err)
		require.Equal(t, 1, active)
	})
}

func TestReplicatedRestoreReplayDoesNotRepin(t *testing.T) {
	mockey.PatchConvey("job admission owns the pin exactly once", t, func() {
		copies, _ := newCopySegmentTaskTestMeta(t, createTestCopyTask(100, 2).(*copySegmentTask))
		require.NoError(t, copies.AddJob(t.Context(), &copySegmentJob{CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 100, CollectionId: 200, PinId: 77, CreatesCollection: true,
			State: datapb.CopySegmentJobState_CopySegmentJobCompleted,
		}}))
		pin := mockey.Mock((*snapshotMeta).ensureRestorePin).Return(1, nil).Build()
		sm := &snapshotManager{copySegmentMeta: copies, snapshotMeta: newReplicaSnapshotTestMeta(t)}
		id, err := sm.restoreData(t.Context(), 100, "snapshot", "", 200, 100, 77, false, "", "",
			&restorePublication{createsCollection: true, replicated: true})
		require.NoError(t, err)
		require.EqualValues(t, 100, id)
		require.Zero(t, pin.Times(), "completed replay must not recreate a released source pin")
	})
}

func TestReplicatedRestorePinsBeforeJobAdmission(t *testing.T) {
	mockey.PatchConvey("retain source protection across job persistence retries", t, func() {
		m := newReplicaSnapshotTestMeta(t)
		require.NoError(t, m.SaveSnapshot(t.Context(), replicaSnapshotTestData()))
		copies, _ := newCopySegmentTaskTestMeta(t, createTestCopyTask(100, 2).(*copySegmentTask))
		mockey.Mock((*snapshotManager).buildPartitionMapping).Return(map[int64]int64{10: 20}, nil).Build()
		mockey.Mock((*snapshotManager).buildChannelMapping).Return(map[string]string{"source": "target"}, nil).Build()
		attempts := 0
		mockey.Mock((*snapshotManager).createRestoreJob).To(func(_ *snapshotManager, ctx context.Context,
			collectionID int64, _ map[string]string, _ map[int64]int64, _ *snapshotstorage.SnapshotData,
			jobID, pinID int64, _ bool, _, _, _ string, _ ...*restorePublication,
		) error {
			info, err := m.GetSnapshot(ctx, 100, "snapshot")
			require.NoError(t, err)
			require.Equal(t, []int64{77}, info.GetPinIds(), "pin is durable before job admission")
			attempts++
			if attempts == 1 {
				return merr.ErrServiceUnavailable
			}
			return copies.AddJob(ctx, &copySegmentJob{CopySegmentJob: &datapb.CopySegmentJob{
				JobId: jobID, CollectionId: collectionID, PinId: pinID, CreatesCollection: true,
				State: datapb.CopySegmentJobState_CopySegmentJobPending,
			}})
		}).Build()
		sm := &snapshotManager{snapshotMeta: m, copySegmentMeta: copies}
		apply := func() (int64, error) {
			return sm.restoreData(t.Context(), 100, "snapshot", "", 200, 100, 77, false, "", "",
				&restorePublication{createsCollection: true, replicated: true})
		}
		_, err := apply()
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.Nil(t, copies.GetJob(t.Context(), 100))
		for range 2 {
			id, err := apply()
			require.NoError(t, err)
			require.EqualValues(t, 100, id)
		}
		require.Equal(t, 2, attempts, "existing job replay must skip re-admission")
	})
}

func TestReplicatedRestorePinFailureDefersJobAdmission(t *testing.T) {
	mockey.PatchConvey("a transient pin failure must be retried before creating a job", t, func() {
		copies, _ := newCopySegmentTaskTestMeta(t, createTestCopyTask(100, 2).(*copySegmentTask))
		sm := &snapshotManager{snapshotMeta: newReplicaSnapshotTestMeta(t), copySegmentMeta: copies}
		mockey.Mock((*snapshotMeta).ensureRestorePin).Return(0, merr.ErrServiceUnavailable).Build()
		_, err := sm.restoreData(t.Context(), 100, "snapshot", "", 200, 100, 77, false, "", "",
			&restorePublication{createsCollection: true, replicated: true})
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.Nil(t, copies.GetJob(t.Context(), 100))
	})
}

func TestReplicatedSnapshotPendingGCDoesNotDeletePublishedRetry(t *testing.T) {
	m := newReplicaSnapshotTestMeta(t)
	stale := &datapb.SnapshotInfo{Id: 9001, CollectionId: 100, Name: "snapshot", State: datapb.SnapshotState_SnapshotStatePending}
	require.NoError(t, m.catalog.SaveSnapshot(t.Context(), stale))
	// The GC list captured stale before the callback retried and committed.
	require.NoError(t, m.SaveSnapshot(t.Context(), replicaSnapshotTestData()))
	gc := &garbageCollector{meta: &meta{snapshotMeta: m}}
	cleaned, err := gc.recyclePendingSnapshot(t.Context(), stale)
	require.NoError(t, err)
	require.False(t, cleaned)
	data, err := m.ReadSnapshotData(t.Context(), 100, "snapshot", false)
	require.NoError(t, err)
	require.EqualValues(t, 9001, data.SnapshotInfo.GetId())
	persisted, err := m.catalog.ListSnapshots(t.Context())
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	require.Equal(t, datapb.SnapshotState_SnapshotStateCommitted, persisted[0].GetState())
}

func TestReplicatedRestorePinMissingAndDeleting(t *testing.T) {
	m := newReplicaSnapshotTestMeta(t)
	_, err := m.ensureRestorePin(t.Context(), 100, "snapshot", 77)
	require.ErrorIs(t, err, merr.ErrSnapshotNotFound)
	info := replicaSnapshotTestData().SnapshotInfo
	info.State = datapb.SnapshotState_SnapshotStateDeleting
	m.snapshotID2Info.Insert(info.GetId(), info)
	m.addToSecondaryIndexes(info)
	_, err = m.ensureRestorePin(t.Context(), 100, "snapshot", 77)
	require.ErrorIs(t, err, merr.ErrSnapshotNotFound)
}

func TestReplicatedSnapshotPendingGCRetry(t *testing.T) {
	for _, failure := range []string{"", "manifests", "metadata", "catalog"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("pending cleanup preserves retryability", t, func() {
				m := newReplicaSnapshotTestMeta(t)
				cli := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
				pending := &datapb.SnapshotInfo{Id: 9001, CollectionId: 100, Name: "snapshot", State: datapb.SnapshotState_SnapshotStatePending}
				require.NoError(t, m.catalog.SaveSnapshot(t.Context(), pending))
				manifestDir, metadataPath := snapshotstorage.GetSnapshotPaths(cli.RootPath(), 100, 9001)
				require.NoError(t, cli.Write(t.Context(), manifestDir+"segment.avro", []byte("manifest")))
				require.NoError(t, cli.Write(t.Context(), metadataPath, []byte("metadata")))
				var patch *mockey.Mocker
				switch failure {
				case "manifests":
					patch = mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).Return(merr.ErrServiceUnavailable).Build()
				case "metadata":
					patch = mockey.Mock((*storage.LocalChunkManager).Remove).Return(merr.ErrServiceUnavailable).Build()
				case "catalog":
					patch = mockey.Mock((*kvdatacoord.Catalog).DropSnapshot).Return(merr.ErrServiceUnavailable).Build()
				}
				gc := &garbageCollector{meta: &meta{snapshotMeta: m}, option: GcOption{cli: cli}}
				// Exercise the real sweep as well as retrying the failed entry.
				mockey.Mock((*snapshotMeta).GetPendingSnapshots).Return([]*datapb.SnapshotInfo{pending}, nil).Build()
				gc.recycleSnapshots(t.Context(), nil)
				persisted, err := m.catalog.ListSnapshots(t.Context())
				require.NoError(t, err)
				if patch != nil {
					require.Len(t, persisted, 1, "failed cleanup must retain its durable retry record")
					patch.UnPatch()
					cleaned, err := gc.recyclePendingSnapshot(t.Context(), pending)
					require.NoError(t, err)
					require.True(t, cleaned)
				} else {
					require.Empty(t, persisted)
				}
				persisted, err = m.catalog.ListSnapshots(t.Context())
				require.NoError(t, err)
				require.Empty(t, persisted)
				exists, err := cli.Exist(t.Context(), metadataPath)
				require.NoError(t, err)
				require.False(t, exists)
			})
		})
	}
}
