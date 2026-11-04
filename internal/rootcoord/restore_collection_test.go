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
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRestoreCollectionVisibilityAfterDurableCompletion(t *testing.T) {
	collection := &model.Collection{CollectionID: 10, DBID: 1, DBName: "default", Name: "restored", State: etcdpb.CollectionState_CollectionRestoring}
	mt := &MetaTable{catalog: &kvrootcoord.Catalog{}, collID2Meta: map[int64]*model.Collection{10: collection}}
	mt.rebuildAvailableCollectionCountLocked()
	require.Equal(t, 1, mt.availableCollectionCount, "restoring collection reserves capacity before publication")
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

func TestRestoreInternalDescribeRetriesUntilAvailable(t *testing.T) {
	for _, state := range []etcdpb.CollectionState{
		etcdpb.CollectionState_CollectionRestoring, etcdpb.CollectionState_CollectionCreated,
		etcdpb.CollectionState_CollectionDropping, etcdpb.CollectionState_CollectionDropped,
	} {
		t.Run(state.String(), func(t *testing.T) {
			mt := &MetaTable{collID2Meta: map[int64]*model.Collection{10: {CollectionID: 10, State: state}}}
			core := &Core{meta: mt}
			req := &milvuspb.DescribeCollectionRequest{CollectionID: 10}
			_, err := core.describeCollection(t.Context(), req, false)
			switch state {
			case etcdpb.CollectionState_CollectionRestoring:
				require.ErrorIs(t, err, merr.ErrServiceNotReady, "replicated Load must keep retrying")
			case etcdpb.CollectionState_CollectionCreated:
				require.NoError(t, err)
			default:
				require.ErrorIs(t, err, merr.ErrCollectionNotFound, "a real drop remains terminal for Load")
			}
			user := metadata.NewIncomingContext(t.Context(), metadata.Pairs(util.HeaderAuthorize, "user request"))
			_, err = core.describeCollection(user, req, false)
			if state == etcdpb.CollectionState_CollectionCreated {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, merr.ErrCollectionNotFound, "unavailable collection remains hidden from users")
			}
			info, err := core.describeCollection(t.Context(), req, true)
			require.NoError(t, err)
			require.Equal(t, state, info.State, "restore/recovery internal lookup still returns the full recipe")
		})
	}
	core := &Core{meta: &MetaTable{collID2Meta: map[int64]*model.Collection{}}}
	_, err := core.describeCollection(t.Context(), &milvuspb.DescribeCollectionRequest{CollectionID: 10}, false)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
}

func TestRestoreInternalDescribePublicationRace(t *testing.T) {
	for _, lookupErr := range []error{nil, merr.ErrServiceUnavailable} {
		mockey.PatchConvey("publication can race the availability lookup", t, func() {
			mockey.Mock((*MetaTable).GetCollectionByID).To(func(_ *MetaTable, _ context.Context, _ string, _ int64, _ uint64, allowUnavailable bool) (*model.Collection, error) {
				if !allowUnavailable {
					return nil, merr.WrapErrCollectionNotFound(10)
				}
				return &model.Collection{CollectionID: 10, State: etcdpb.CollectionState_CollectionCreated}, lookupErr
			}).Build()
			core := &Core{meta: &MetaTable{}}
			info, err := core.describeCollection(t.Context(), &milvuspb.DescribeCollectionRequest{CollectionID: 10}, false)
			if lookupErr != nil {
				require.ErrorIs(t, err, lookupErr, "a metadata failure must not be acknowledged as a drop")
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 10, info.CollectionID)
			}
		})
	}
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
				core := &Core{meta: &MetaTable{}, idAllocator: &allocator.GlobalIDAllocator{}}
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

func TestRestorePreparationChecksCompletePartitionCapacity(t *testing.T) {
	for _, tc := range []struct {
		name         string
		partitions   []string
		partitionKey bool
		limit        int
		wantNames    []string
		wantError    bool
	}{
		{
			name: "user partitions exceed remaining capacity", limit: 512, wantError: true,
			partitions: []string{"p2", "_default", "p1"},
		},
		{
			name: "user partitions exactly fit", limit: 514,
			partitions: []string{"p2", "_default", "p1", "p1"}, wantNames: []string{"_default", "p1", "p2"},
		},
		{
			name: "default partition counted once", limit: 512,
			partitions: []string{"_default", "_default"}, wantNames: []string{"_default"},
		},
		{
			name: "partition key partitions exactly fit", limit: 512, partitionKey: true,
			partitions: []string{"_default_1", "_default_0"}, wantNames: []string{"_default_0", "_default_1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				require.NoError(t, Params.Save(Params.RootCoordCfg.MaxGeneralCapacity.Key, strconv.Itoa(tc.limit)))
				defer Params.Reset(Params.RootCoordCfg.MaxGeneralCapacity.Key)
				mt := &MetaTable{
					generalCnt: 508, fileResourceRefCnt: map[int64]int{99: 0},
					dbName2Meta: map[string]*model.Database{util.DefaultDBName: {ID: util.DefaultDBID, Name: util.DefaultDBName}},
					collID2Meta: make(map[int64]*model.Collection), names: newNameDb(), aliases: newNameDb(),
				}
				mt.names.createDbIfNotExist(util.DefaultDBName)
				mt.aliases.createDbIfNotExist(util.DefaultDBName)
				mt.rebuildAvailableCollectionCountLocked()
				schema := &schemapb.CollectionSchema{Name: "target", Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, IsPartitionKey: tc.partitionKey},
				}}
				encoded, err := proto.Marshal(schema)
				require.NoError(t, err)
				req := &milvuspb.CreateCollectionRequest{
					DbName: util.DefaultDBName, CollectionName: "target", Schema: encoded, ShardsNum: 2, NumPartitions: 2,
				}
				// Keep Prepare, admission and partition allocation real. Schema validation
				// holds a file resource; channel allocation does not need a live SN here.
				mockey.Mock((*createCollectionTask).prepareSchema).To(func(task *createCollectionTask, _ context.Context) error {
					task.heldFileResourceIds = []int64{99}
					mt.fileResourceRefCnt[99]++
					return nil
				}).Build()
				mockey.Mock((*createCollectionTask).assignChannels).Return(nil).Build()
				nextID := int64(10)
				mockey.Mock((*allocator.GlobalIDAllocator).AllocOne).To(func(*allocator.GlobalIDAllocator) (int64, error) {
					nextID++
					return nextID, nil
				}).Build()
				mockey.Mock((*allocator.GlobalIDAllocator).Alloc).To(func(_ *allocator.GlobalIDAllocator, count uint32) (int64, int64, error) {
					start := nextID + 1
					nextID += int64(count)
					return start, nextID + 1, nil
				}).Build()
				core := &Core{meta: mt, idAllocator: &allocator.GlobalIDAllocator{}}
				body, cleanup, err := core.PrepareRestoreCollection(t.Context(), req, tc.partitions)
				if tc.wantError {
					require.ErrorIs(t, err, merr.ErrGeneralCapacityExceeded)
					require.Nil(t, body)
					require.Nil(t, cleanup)
					require.Zero(t, mt.fileResourceRefCnt[99], "rejected recipe releases held resources")
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.wantNames, body.Collection.PartitionNames)
					require.Len(t, body.Collection.PartitionIDs, len(tc.wantNames))
					require.Equal(t, body.Collection.PartitionIDs, body.CollectionHeader.PartitionIds)
					require.Equal(t, 1, mt.fileResourceRefCnt[99])
					cleanup()
					cleanup()
					require.Zero(t, mt.fileResourceRefCnt[99])
				}
				require.Equal(t, 508, mt.GetGeneralCount(t.Context()), "preparation must not publish or reserve durable metadata")
				require.Empty(t, mt.collID2Meta)
			})
		})
	}
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

func TestRestoringCollectionReservesAdmissionCapacity(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(strconv.FormatBool(complete), func(t *testing.T) {
			mockey.PatchConvey("reserve until drop, including recovery and completion retries", t, func() {
				channel.ResetStaticPChannelStatsManager()
				channel.RecoverPChannelStatsManager(nil)
				t.Cleanup(channel.ResetStaticPChannelStatsManager)
				mockey.Mock((*kvrootcoord.Catalog).CreateCollection).Return(nil).Build()
				alter := mockey.Mock((*kvrootcoord.Catalog).AlterCollection).Return(nil).Build()
				mockey.Mock((*kvrootcoord.Catalog).DeleteGrantByCollectionName).Return(nil).Build()
				mt := &MetaTable{
					catalog:     &kvrootcoord.Catalog{},
					dbName2Meta: map[string]*model.Database{util.DefaultDBName: {ID: util.DefaultDBID, Name: util.DefaultDBName}},
					collID2Meta: make(map[int64]*model.Collection), partitionName2ID: make(map[int64]map[string]int64),
					fileResourceRefCnt: make(map[int64]int), names: newNameDb(), aliases: newNameDb(),
				}
				mt.names.createDbIfNotExist(util.DefaultDBName)
				mt.rebuildAvailableCollectionCountLocked()
				for i, state := range []etcdpb.CollectionState{etcdpb.CollectionState_CollectionCreated, etcdpb.CollectionState_CollectionRestoring} {
					coll := &model.Collection{
						CollectionID: int64(i + 1), DBID: util.DefaultDBID, DBName: util.DefaultDBName,
						Name: strconv.Itoa(i), State: state, ShardsNum: 1,
						Partitions: []*model.Partition{{PartitionID: int64(i + 10), State: etcdpb.PartitionState_PartitionCreated}},
					}
					require.NoError(t, mt.AddCollection(t.Context(), coll))
					require.NoError(t, mt.AddCollection(t.Context(), coll), "replayed creation reserves only once")
				}
				count := func(want int) {
					t.Helper()
					db, total, exists := mt.GetAvailableCollectionCount(t.Context(), util.DefaultDBID)
					require.True(t, exists)
					require.Equal(t, want, db)
					require.Equal(t, want, total)
				}
				count(2)
				mt.rebuildAvailableCollectionCountLocked()
				count(2)
				_, err := mt.GetCollectionByID(t.Context(), util.DefaultDBName, 2, typeutil.MaxTimestamp, false)
				require.ErrorIs(t, err, merr.ErrCollectionNotFound, "quota reservation must not make restoring collections public")
				task := &createCollectionTask{
					Core: &Core{meta: mt}, header: &message.CreateCollectionMessageHeader{DbId: util.DefaultDBID},
					Req: &milvuspb.CreateCollectionRequest{DbName: util.DefaultDBName, CollectionName: "next", ShardsNum: 1},
				}
				for _, limit := range []struct{ db, total string }{{"2", "100"}, {"100", "2"}} {
					require.NoError(t, Params.Save(Params.QuotaConfig.MaxCollectionNumPerDB.Key, limit.db))
					require.NoError(t, Params.Save(Params.QuotaConfig.MaxCollectionNum.Key, limit.total))
					require.ErrorIs(t, task.validate(t.Context()), merr.ErrCollectionNumLimitExceeded)
				}
				t.Cleanup(func() {
					Params.Reset(Params.QuotaConfig.MaxCollectionNumPerDB.Key)
					Params.Reset(Params.QuotaConfig.MaxCollectionNum.Key)
				})
				if complete {
					alter.Return(merr.ErrServiceUnavailable)
					require.ErrorIs(t, mt.CompleteRestoreCollection(t.Context(), 2), merr.ErrServiceUnavailable)
					count(2)
					alter.Return(nil)
					require.NoError(t, mt.CompleteRestoreCollection(t.Context(), 2))
					require.NoError(t, mt.CompleteRestoreCollection(t.Context(), 2))
					count(2)
				} else {
					require.ErrorIs(t, mt.DropCollection(t.Context(), 2, 300), merr.ErrServiceNotReady)
					count(2)
					require.NoError(t, mt.CompleteRestoreCollection(t.Context(), 2))
				}
				require.NoError(t, mt.DropCollection(t.Context(), 2, 300))
				require.NoError(t, mt.DropCollection(t.Context(), 2, 300))
				count(1)
				require.NoError(t, task.validate(t.Context()), "dropping a completed restore releases capacity")
			})
		})
	}
}
