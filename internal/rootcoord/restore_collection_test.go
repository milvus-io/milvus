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

package rootcoord

import (
	"context"
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRestoreCollectionVisibilityAfterDurableCompletion(t *testing.T) {
	collection := &model.Collection{CollectionID: 10, DBID: 1, DBName: "default", Name: "restored", State: etcdpb.CollectionState_CollectionRestoring}
	mt := &MetaTable{catalog: &kvrootcoord.Catalog{}, collID2Meta: map[int64]*model.Collection{10: collection}}
	_, err := mt.GetCollectionByID(t.Context(), "default", 10, typeutil.MaxTimestamp, false)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	internal, err := mt.GetCollectionByID(t.Context(), "default", 10, typeutil.MaxTimestamp, true)
	require.NoError(t, err)
	require.Equal(t, etcdpb.CollectionState_CollectionRestoring, internal.State)
	patch := mockey.Mock((*kvrootcoord.Catalog).AlterCollection).Return(merr.ErrServiceUnavailable).Build()
	require.ErrorIs(t, mt.CompleteRestoreCollection(t.Context(), 10), merr.ErrServiceUnavailable)
	require.False(t, mt.collID2Meta[10].Available())
	patch.UnPatch()
	patch = mockey.Mock((*kvrootcoord.Catalog).AlterCollection).Return(nil).Build()
	defer patch.UnPatch()
	require.NoError(t, mt.CompleteRestoreCollection(t.Context(), 10))
	require.NoError(t, mt.CompleteRestoreCollection(t.Context(), 10))
	require.Equal(t, 1, mt.availableCollectionCount)
	require.Equal(t, etcdpb.CollectionState_CollectionRestoring, collection.State, "published metadata is immutable")
	_, err = mt.GetCollectionByID(t.Context(), "default", 10, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
}

func TestRestoreCollectionLifecycleChecks(t *testing.T) {
	for _, state := range []etcdpb.CollectionState{
		etcdpb.CollectionState_CollectionCreating, etcdpb.CollectionState_CollectionCreated,
		etcdpb.CollectionState_CollectionRestoring, etcdpb.CollectionState_CollectionDropping,
		etcdpb.CollectionState_CollectionDropped, etcdpb.CollectionState(100),
	} {
		t.Run(state.String(), func(t *testing.T) {
			mt := &MetaTable{collID2Meta: map[int64]*model.Collection{10: {CollectionID: 10, State: state}}}
			core := &Core{meta: mt}
			dropped, err := core.IsCollectionDropped(t.Context(), 10)
			if state == etcdpb.CollectionState(100) {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				return
			}
			require.NoError(t, err)
			require.Equal(t, state == etcdpb.CollectionState_CollectionDropping || state == etcdpb.CollectionState_CollectionDropped, dropped)
			recoverable, err := core.ValidateDataViewCollectionForRecovery(t.Context(), 10)
			require.NoError(t, err)
			require.Equal(t, state == etcdpb.CollectionState_CollectionCreated || state == etcdpb.CollectionState_CollectionRestoring, recoverable)
		})
	}
	mt := &MetaTable{collID2Meta: map[int64]*model.Collection{}}
	core := &Core{meta: mt}
	dropped, err := core.IsCollectionDropped(t.Context(), 10)
	require.NoError(t, err)
	require.True(t, dropped)
	require.ErrorIs(t, mt.CompleteRestoreCollection(t.Context(), 10), merr.ErrCollectionNotFound)
	mt.collID2Meta[10] = &model.Collection{CollectionID: 10, State: etcdpb.CollectionState_CollectionDropping}
	require.ErrorIs(t, mt.CompleteRestoreCollection(t.Context(), 10), merr.ErrServiceNotReady)
}

func TestRestorePreparationOwnsPartitionsAndResources(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		partitionKey              bool
		prepareErr, allocationErr error
		maxPartitions             int
	}{
		{name: "user partitions"},
		{name: "partition key", partitionKey: true},
		{name: "prepare fails", prepareErr: merr.ErrServiceUnavailable},
		{name: "allocation fails", allocationErr: merr.ErrServiceUnavailable},
		{name: "partition limit", maxPartitions: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				if tc.maxPartitions > 0 {
					require.NoError(t, Params.Save(Params.RootCoordCfg.MaxPartitionNum.Key, strconv.Itoa(tc.maxPartitions)))
					defer Params.Reset(Params.RootCoordCfg.MaxPartitionNum.Key)
				}
				schema := &schemapb.CollectionSchema{Name: "target", Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
				if tc.partitionKey {
					schema.Fields[0].IsPartitionKey = true
				}
				data, err := proto.Marshal(schema)
				require.NoError(t, err)
				req := &milvuspb.CreateCollectionRequest{Schema: data, DbName: "default", CollectionName: "target"}
				released := 0
				mockey.Mock((*createCollectionTask).releaseFileResources).To(func(*createCollectionTask) { released++ }).Build()
				mockey.Mock((*createCollectionTask).Prepare).To(func(task *createCollectionTask, _ context.Context) error {
					require.True(t, task.preserveFieldID)
					require.EqualValues(t, 100, task.body.GetCollectionSchema().GetFields()[0].GetFieldID())
					require.Positive(t, task.Req.GetShardsNum())
					if tc.partitionKey {
						require.EqualValues(t, common.DefaultPartitionsWithPartitionKey, task.Req.NumPartitions)
					} else {
						require.EqualValues(t, 1, task.Req.NumPartitions)
					}
					task.header.CollectionId = 10
					task.header.PartitionIds = []int64{11}
					task.body.PartitionIDs = []int64{11}
					task.body.PartitionNames = []string{"_default"}
					return tc.prepareErr
				}).Build()
				allocation := mockey.Mock((*allocator.GlobalIDAllocator).AllocOne).Return(int64(12), tc.allocationErr).Build()
				core := &Core{idAllocator: &allocator.GlobalIDAllocator{}}
				body, cleanup, err := core.PrepareRestoreCollection(t.Context(), req, []string{"p1", "_default"})
				if tc.prepareErr != nil || tc.allocationErr != nil || tc.maxPartitions > 0 {
					require.Error(t, err)
					require.Nil(t, body)
					require.Nil(t, cleanup)
					require.Equal(t, 1, released)
					return
				}
				require.NoError(t, err)
				require.Equal(t, 0, released)
				require.Equal(t, []string{"_default", "p1"}, body.Collection.PartitionNames)
				require.Equal(t, []int64{11, 12}, body.Collection.PartitionIDs)
				require.Equal(t, body.Collection.PartitionIDs, body.CollectionHeader.PartitionIds)
				require.Equal(t, 1, allocation.Times())
				cleanup()
				require.Equal(t, 1, released)
			})
		})
	}
	_, _, err := (&Core{}).PrepareRestoreCollection(t.Context(), &milvuspb.CreateCollectionRequest{Schema: []byte{0xff}}, nil)
	require.Error(t, err)
}

func TestRestoreApplyUsesRecipeWithoutAnotherBroadcast(t *testing.T) {
	mockey.PatchConvey("creation callback reuses target WAL positions", t, func() {
		body := &messagespb.RestoreSnapshotMessageBody{
			CollectionHeader: &message.CreateCollectionMessageHeader{CollectionId: 10},
			Collection:       &message.CreateCollectionRequest{CollectionID: 10, VirtualChannelNames: []string{"target"}},
		}
		msg := message.NewRestoreSnapshotMessageBuilderV2().WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 10}).WithBody(body).WithBroadcast([]string{"target"}).MustBuildBroadcast()
		results := map[string]*message.AppendResult{"target": {TimeTick: 123}}
		patch := mockey.Mock((*DDLCallback).applyCollectionCreation).To(func(_ *DDLCallback, _ context.Context, result message.BroadcastResultCreateCollectionMessageV1, state etcdpb.CollectionState) error {
			require.Equal(t, etcdpb.CollectionState_CollectionRestoring, state)
			require.True(t, proto.Equal(body.Collection, result.Message.MustBody()))
			require.Equal(t, results, result.Results)
			return merr.ErrServiceUnavailable
		}).Build()
		err := (&Core{}).ApplyRestoreCollection(t.Context(), message.BroadcastResultRestoreSnapshotMessageV2{Message: message.MustAsBroadcastRestoreSnapshotMessageV2(msg), Results: results})
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.Equal(t, 1, patch.Times())
	})
}

func TestRestoreAdmissionIncludesUnavailableNames(t *testing.T) {
	mockey.PatchConvey("restoring names remain reserved", t, func() {
		patch := mockey.Mock((*MetaTable).GetCollectionByName).To(func(_ *MetaTable, _ context.Context, db, name string, ts Timestamp, allow bool) (*model.Collection, error) {
			require.True(t, allow)
			require.Equal(t, "default", db)
			require.Equal(t, "target", name)
			require.Equal(t, typeutil.MaxTimestamp, ts)
			return &model.Collection{State: etcdpb.CollectionState_CollectionRestoring}, nil
		}).Build()
		core := &Core{meta: &MetaTable{}}
		require.ErrorIs(t, core.CheckRestoreTargetAbsent(t.Context(), "default", "target"), merr.ErrParameterInvalid)
		patch.Return(nil, merr.ErrCollectionNotFound)
		require.ErrorIs(t, core.CheckRestoreTargetAbsent(t.Context(), "default", "target"), merr.ErrCollectionNotFound)
		patch.Return(nil, merr.ErrServiceUnavailable)
		require.ErrorIs(t, core.CheckRestoreTargetAbsent(t.Context(), "default", "target"), merr.ErrServiceUnavailable)
	})
	core := &Core{meta: &MetaTable{collID2Meta: map[int64]*model.Collection{10: {CollectionID: 10, State: etcdpb.CollectionState_CollectionCreated}}}}
	require.NoError(t, core.CompleteRestoreCollection(t.Context(), 10))
}
