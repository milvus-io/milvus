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
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/dataview"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/types"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type creationDataViewTestCoord struct{ types.MixCoord }

func (*creationDataViewTestCoord) CreateCollectionDataViewWithTransformStarts(context.Context, message.BroadcastResultCreateCollectionMessageV1) error {
	panic("mock with mockey")
}

func collectionCreationWithSchemaVersion(version int32) message.BroadcastResultCreateCollectionMessageV1 {
	header := &message.CreateCollectionMessageHeader{DbId: 1, CollectionId: 100, PartitionIds: []int64{101}}
	body := &message.CreateCollectionRequest{
		DbName: "default", CollectionName: "wal_schema", CollectionID: 100,
		PartitionIDs: []int64{101}, PartitionNames: []string{"_default"},
		VirtualChannelNames:  []string{"schema-dml_0_100v0", "schema-dml_1_100v1"},
		PhysicalChannelNames: []string{"schema-dml_0", "schema-dml_1"},
		CollectionSchema: &schemapb.CollectionSchema{
			Name: "wal_schema", Version: version,
			Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
			Properties: []*commonpb.KeyValuePair{
				{Key: common.ConsistencyLevel, Value: strconv.Itoa(int(commonpb.ConsistencyLevel_Bounded))},
			},
		},
	}
	msg := message.NewCreateCollectionMessageBuilderV1().WithHeader(header).WithBody(body).
		WithBroadcast(body.VirtualChannelNames).MustBuildBroadcast().OverwriteBroadcastHeader(1)
	msg = message.WithBroadcastControlChannel(msg, funcutil.GetControlChannel("schema-dml_0"))
	results := make(map[string]*message.AppendResult)
	for i, name := range msg.BroadcastHeader().VChannels {
		results[name] = &message.AppendResult{
			MessageID: rmq.NewRmqID(int64(i + 1)), LastConfirmedMessageID: rmq.NewRmqID(0), TimeTick: uint64(100 + i),
		}
	}
	return message.BroadcastResultCreateCollectionMessageV1{Message: message.MustAsBroadcastCreateCollectionMessageV1(msg), Results: results}
}

func TestNewCollectionModelPreservesSchemaVersion(t *testing.T) {
	for _, version := range []int32{0, 7} {
		t.Run(strconv.Itoa(int(version)), func(t *testing.T) {
			result := collectionCreationWithSchemaVersion(version)
			// MustBody decodes the actual protobuf payload, including old schemas
			// with no version field (the protobuf default is zero).
			body := result.Message.MustBody()
			before := proto.Clone(body)
			coll := newCollectionModelWithMessage(result.Message.Header(), body, result)
			require.Equal(t, version, coll.SchemaVersion)
			require.Equal(t, result.GetControlChannelResult().TimeTick, coll.CreateTime)
			require.Len(t, coll.StartPositions, 2)
			encoded, err := proto.Marshal(model.MarshalCollectionModel(coll))
			require.NoError(t, err)
			persisted := &etcdpb.CollectionInfo{}
			require.NoError(t, proto.Unmarshal(encoded, persisted))
			require.Equal(t, version, persisted.GetSchema().GetVersion())
			require.Equal(t, version, model.UnmarshalCollectionModel(persisted).SchemaVersion)
			require.True(t, proto.Equal(before, body), "materialization must not mutate the WAL schema")
		})
	}
}

func TestCreateCollectionProducerInitializesSchemaVersion(t *testing.T) {
	result := collectionCreationWithSchemaVersion(7)
	task := &createCollectionTask{
		Req:  &milvuspb.CreateCollectionRequest{CollectionName: "wal_schema"},
		body: result.Message.MustBody(),
	}
	require.NoError(t, task.prepareSchema(t.Context()))
	require.Zero(t, task.body.CollectionSchema.GetVersion(), "the producer still chooses the initial schema version")
	require.Equal(t, int32(7), result.Message.MustBody().CollectionSchema.GetVersion(), "an already-built WAL payload is unchanged")
}

func TestCollectionCreationPreservesSchemaVersionOnReplay(t *testing.T) {
	for _, restore := range []bool{false, true} {
		mode := "create"
		if restore {
			mode = "restore"
		}
		for _, version := range []int32{0, 7} {
			t.Run(mode+"/"+strconv.Itoa(int(version)), func(t *testing.T) {
				ctx := t.Context()
				client, _ := kvfactory.GetEtcdAndPath()
				store := etcdkv.NewEtcdKV(client, "55265-schema-"+funcutil.RandomString(12))
				t.Cleanup(func() { require.NoError(t, store.RemoveWithPrefix(context.Background(), "")) })
				catalog := kvrootcoord.NewCatalog(store)
				mt := &MetaTable{
					ctx: ctx, catalog: catalog, names: newNameDb(), aliases: newNameDb(),
					dbName2Meta: map[string]*model.Database{"default": {ID: 1, Name: "default"}},
					collID2Meta: map[int64]*model.Collection{}, partitionName2ID: map[int64]map[string]int64{},
				}
				channel.ResetStaticPChannelStatsManager()
				channel.RecoverPChannelStatsManager(nil)
				t.Cleanup(channel.ResetStaticPChannelStatsManager)
				// Keep RPC/cache side effects outside this metadata regression.
				var shardErr error
				shards := mockey.Mock((*DDLCallback).createCollectionShard).To(func(*DDLCallback, context.Context, *message.CreateCollectionMessageHeader, *message.CreateCollectionRequest, string, *message.AppendResult) error {
					return shardErr
				}).Build()
				defer shards.UnPatch()
				caches := mockey.Mock((*DDLCallback).ExpireCaches).Return(nil).Build()
				defer caches.UnPatch()
				manager, err := dataview.RecoverManager(ctx, datacoordkv.NewCatalog(store, "", ""), func(context.Context, int64) (bool, error) {
					return true, nil
				}, nil, nil, nil)
				require.NoError(t, err)
				var dataViewErr error
				dataViews := mockey.Mock((*creationDataViewTestCoord).CreateCollectionDataViewWithTransformStarts).To(func(_ *creationDataViewTestCoord, actualCtx context.Context, result message.BroadcastResultCreateCollectionMessageV1) error {
					body := result.Message.MustBody()
					require.Equal(t, version, body.CollectionSchema.GetVersion(), "callback must pass the WAL schema version")
					if dataViewErr != nil {
						return dataViewErr
					}
					starts := make(map[string]uint64, len(body.VirtualChannelNames))
					for _, name := range body.VirtualChannelNames {
						starts[name] = result.Results[name].TimeTick
					}
					_, err := manager.OnCreateCollection(actualCtx, dataview.CreateCollectionDataViewEvent{
						CollectionID: result.Message.Header().CollectionId, VChannels: body.VirtualChannelNames, TransformStarts: starts, SchemaVersion: body.CollectionSchema.GetVersion(),
					})
					return err
				}).Build()
				defer dataViews.UnPatch()
				core := &Core{meta: mt, mixCoord: &creationDataViewTestCoord{}}
				result := collectionCreationWithSchemaVersion(version)
				apply := func() error { return (&DDLCallback{Core: core}).createCollectionV1AckCallback(ctx, result) }
				state := etcdpb.CollectionState_CollectionCreated
				if restore {
					msg := message.NewRestoreSnapshotMessageBuilderV2().WithHeader(&message.RestoreSnapshotMessageHeader{}).
						WithBody(&messagespb.RestoreSnapshotMessageBody{CollectionHeader: result.Message.Header(), Collection: result.Message.MustBody()}).
						WithBroadcast(result.Message.MustBody().VirtualChannelNames).MustBuildBroadcast()
					apply = func() error {
						return core.ApplyRestoreCollection(ctx, message.BroadcastResultRestoreSnapshotMessageV2{
							Message: message.MustAsBroadcastRestoreSnapshotMessageV2(msg), Results: result.Results,
						})
					}
					state = etcdpb.CollectionState_CollectionRestoring
				}
				shardErr = merr.ErrServiceUnavailable
				require.ErrorIs(t, apply(), shardErr)
				shardErr = nil
				dataViewErr = merr.ErrServiceUnavailable
				require.ErrorIs(t, apply(), dataViewErr)
				require.Empty(t, mt.collID2Meta, "failed DataView creation must leave collection creation retryable")
				dataViewErr = nil
				addCollection := mockey.Mock((*MetaTable).AddCollection).Return(merr.ErrServiceUnavailable).Build()
				require.ErrorIs(t, apply(), merr.ErrServiceUnavailable, "a persisted DataView must survive retry after RC persistence failure")
				addCollection.UnPatch()
				require.NoError(t, apply())
				initialView, err := manager.Latest(ctx, 100)
				require.NoError(t, err)
				defer initialView.Deref()
				require.Equal(t, version, initialView.DataView().GetSchemaVersion())
				readPersisted := func() *model.Collection {
					// Read through a fresh catalog and inspect the actual ETCD bytes.
					value, err := store.Load(ctx, kvrootcoord.BuildCollectionKey(1, 100))
					require.NoError(t, err)
					info := &etcdpb.CollectionInfo{}
					require.NoError(t, proto.Unmarshal([]byte(value), info))
					coll, err := kvrootcoord.NewCatalog(store).GetCollectionByID(ctx, 1, typeutil.MaxTimestamp, 100)
					require.NoError(t, err)
					require.Equal(t, info.GetSchema().GetVersion(), coll.SchemaVersion)
					return coll
				}
				require.Equal(t, version, readPersisted().SchemaVersion)
				require.Equal(t, state, mt.collID2Meta[100].State)
				require.NoError(t, apply(), "same-message replay is idempotent")
				require.Equal(t, version, readPersisted().SchemaVersion)
				if restore {
					require.NoError(t, core.CompleteRestoreCollection(ctx, 100))
				}

				// A later schema has already committed when an old creation callback replays.
				schema := mt.collID2Meta[100].ToCollectionSchemaPB()
				schema.Version = version + 1
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 101, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true})
				alter := message.NewAlterCollectionMessageBuilderV2().
					WithHeader(&message.AlterCollectionMessageHeader{CollectionId: 100, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}}}).
					WithBody(&message.AlterCollectionMessageBody{Updates: &messagespb.AlterCollectionMessageUpdates{Schema: schema}}).
					WithControlChannelBroadcast().MustBuildBroadcast()
				require.NoError(t, mt.AlterCollection(ctx, message.BroadcastResultAlterCollectionMessageV2{
					Message: message.MustAsBroadcastAlterCollectionMessageV2(alter),
					Results: map[string]*message.AppendResult{funcutil.GetControlChannel("schema-dml_0"): {TimeTick: 200}},
				}))
				_, err = manager.AdvanceSchema(ctx, 100, version+1, nil)
				require.NoError(t, err)
				require.NoError(t, apply())
				require.Equal(t, version+1, mt.collID2Meta[100].SchemaVersion)
				persisted := readPersisted()
				require.Equal(t, version+1, persisted.SchemaVersion, "old callback must not regress ETCD metadata")
				require.Equal(t, uint64(200), persisted.UpdateTimestamp)
				require.Len(t, persisted.Fields, 2)
				require.Equal(t, etcdpb.CollectionState_CollectionCreated, persisted.State, "restore replay must not undo completion")
				latestView, err := manager.Latest(ctx, 100)
				require.NoError(t, err)
				defer latestView.Deref()
				require.Equal(t, version+1, latestView.DataView().GetSchemaVersion(), "old callback must not regress DataView schema")
				require.Equal(t, version, initialView.DataView().GetSchemaVersion(), "published DataView references are immutable")
			})
		}
	}
}

func TestReplicatedCollectionCreationSchemaIdentityAfterRecovery(t *testing.T) {
	result := collectionCreationWithSchemaVersion(7)
	for _, mutable := range result.Message.BroadcastMessage().SplitIntoMutableMessage() {
		appendResult := result.Results[mutable.VChannel()]
		primary := mutable.WithTimeTick(appendResult.TimeTick).WithLastConfirmed(appendResult.LastConfirmedMessageID).
			IntoImmutableMessage(appendResult.MessageID)
		// Exercise the same message conversion as the receiving replication stream.
		replicated := message.MustNewReplicateMessage("primary", primary.IntoImmutableMessageProto())
		secondary := replicated.WithTimeTick(appendResult.TimeTick + 100).WithLastConfirmed(rmq.NewRmqID(0)).IntoImmutableMessage(rmq.NewRmqID(10))
		replayed := message.MilvusMessageToImmutableMessage(secondary.IntoImmutableMessageProto())
		callbackMsg := message.MustAsBroadcastCreateCollectionMessageV1(replayed.IntoBroadcastMutableMessage())
		coord := newCollectionModelWithMessage(callbackMsg.Header(), callbackMsg.MustBody(), result)
		require.Equal(t, int32(7), coord.SchemaVersion)
		if funcutil.IsControlChannel(replayed.VChannel()) {
			continue
		}
		snapshot := vchannel.NewVChannelMetaFromCreateCollectionMessage(message.MustAsImmutableCreateCollectionMessageV1(replayed))
		encoded, err := proto.Marshal(snapshot)
		require.NoError(t, err)
		recovered := &streamingpb.VChannelMeta{}
		require.NoError(t, proto.Unmarshal(encoded, recovered))
		_, snSchema := vchannel.NewVChannelViewFromMeta(recovered).GetSchema(replayed.TimeTick())
		require.NotNil(t, snSchema)
		require.Equal(t, int32(7), snSchema.GetVersion())
		require.Equal(t, snSchema.GetVersion(), coord.SchemaVersion)
		require.Equal(t, snSchema.GetVersion(), model.MarshalCollectionModel(coord).GetSchema().GetVersion())
	}
}
