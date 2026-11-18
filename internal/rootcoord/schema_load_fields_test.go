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
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	qmeta "github.com/milvus-io/milvus/internal/querycoordv2/meta"
	rootmocks "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func loadFieldsAlterResult(paths ...string) message.BroadcastResultAlterCollectionMessageV2 {
	target := &schemapb.CollectionSchema{Version: 2, Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}}}
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&message.AlterCollectionMessageHeader{CollectionId: 10, UpdateMask: &fieldmaskpb.FieldMask{Paths: paths}}).
		WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: target}}).
		WithBroadcast([]string{"v1"}).MustBuildBroadcast()
	return message.BroadcastResultAlterCollectionMessageV2{Message: message.MustAsSpecializedBroadcastMessage[*message.AlterCollectionMessageHeader, *message.AlterCollectionMessageBody](msg), Results: map[string]*message.AppendResult{"v1": {TimeTick: 100}}}
}

func TestSchemaLoadFieldsFailurePreventsSchemaPublication(t *testing.T) {
	mt := rootmocks.NewIMetaTable(t)
	published := &model.Collection{CollectionID: 10, SchemaVersion: 1, UpdateTimestamp: 10, Fields: []*model.Field{{FieldID: 100}}}
	mt.EXPECT().GetCollectionByIDWithMaxTs(mock.Anything, int64(10)).Return(published, nil)
	core := &Core{meta: mt}
	failed := merr.WrapErrServiceUnavailable("injected load metadata failure")
	calls := 0
	core.SetSchemaLoadFieldsUpdater(func(ctx context.Context, id int64, previous, target *schemapb.CollectionSchema) error {
		calls++
		require.EqualValues(t, 10, id)
		require.Len(t, previous.GetFields(), 1)
		require.Len(t, target.GetFields(), 2)
		return failed
	})
	callback := &DDLCallback{Core: core}
	result := loadFieldsAlterResult(message.FieldMaskCollectionSchema)
	require.ErrorIs(t, callback.alterCollectionV2AckCallback(t.Context(), result), failed)
	mt.AssertNotCalled(t, "AlterCollection", mock.Anything, mock.Anything)
	// With load metadata applied, a failed schema save retries the same old/new
	// schema pair. The updater is independently idempotent (covered in meta tests).
	core.SetSchemaLoadFieldsUpdater(func(context.Context, int64, *schemapb.CollectionSchema, *schemapb.CollectionSchema) error {
		calls++
		return nil
	})
	mt.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(failed).Twice()
	require.ErrorIs(t, callback.alterCollectionV2AckCallback(t.Context(), result), failed)
	require.ErrorIs(t, callback.alterCollectionV2AckCallback(t.Context(), result), failed)
	require.Equal(t, 3, calls)
	// After publication, the schema diff is empty. Reuse that snapshot instead
	// of inferring whether a load update happened from its publication time.
	published.UpdateTimestamp = 100
	published.SchemaVersion = 2
	published.Fields = append(published.Fields, &model.Field{FieldID: 101})
	core.SetSchemaLoadFieldsUpdater(func(_ context.Context, _ int64, previous, target *schemapb.CollectionSchema) error {
		calls++
		require.Equal(t, previous.GetVersion(), target.GetVersion())
		require.Len(t, previous.GetFields(), len(target.GetFields()))
		return nil
	})
	require.NoError(t, callback.updateSchemaLoadFields(t.Context(), result))
	require.Equal(t, 4, calls)
	require.NoError(t, callback.updateSchemaLoadFields(t.Context(), loadFieldsAlterResult(message.FieldMaskCollectionProperties)))
	require.Equal(t, 4, calls)
}

// Exercise the real AddCollectionField producer and registered ALTER callback,
// with the QueryCoord metadata updater installed as it is in Coordinator.
func TestAddCollectionFieldUpdatesPersistedLoadFields(t *testing.T) {
	endpoint := func() url.URL {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		address := listener.Addr().String()
		require.NoError(t, listener.Close())
		return url.URL{Scheme: "http", Host: address}
	}
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{endpoint()}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{endpoint()}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	server, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(server.Close)
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("embedded etcd did not become ready")
	}
	client := v3client.New(server.Server)
	t.Cleanup(func() { client.Close() })
	getClient := mockey.Mock(etcd.GetEtcdClient).Return(client, nil).Build()
	t.Cleanup(func() { getClient.UnPatch() })
	patch := mockey.Mock(kvfactory.GetEtcdAndPath).Return(client, "isolated-load-fields").Build()
	t.Cleanup(func() { patch.UnPatch() })
	core := initStreamingSystemAndCore(t)
	ctx := t.Context()
	db, name := "load_fields_db"+funcutil.RandomString(8), "load_fields_col"+funcutil.RandomString(8)
	createCollectionForTest(t, ctx, core, db, name)
	coll, err := core.meta.GetCollectionByName(ctx, db, name, ^uint64(0))
	require.NoError(t, err)
	catalog := catalogmocks.NewQueryCoordCatalog(t)
	mgr := qmeta.NewCollectionManager(catalog)
	require.NoError(t, mgr.PutCollectionWithoutSave(ctx, &qmeta.Collection{CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: coll.CollectionID, LoadFields: []int64{100}}, LoadPercentage: 100}))
	var persisted *querypb.CollectionLoadInfo
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Run(func(_ context.Context, info *querypb.CollectionLoadInfo, _ ...*querypb.PartitionLoadInfo) {
		persisted = proto.Clone(info).(*querypb.CollectionLoadInfo)
	}).Return(nil).Once()
	core.SetSchemaLoadFieldsUpdater(mgr.UpdateLoadFieldsForSchema)
	resp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{DbName: db, CollectionName: name, Schema: getFieldSchema("new_column")})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, []int64{100, 101}, persisted.GetLoadFields())
	require.Equal(t, []int64{100, 101}, mgr.GetLoadFields(ctx, coll.CollectionID))
	// Adding an opt-out column must not expand the preload set.
	skip := &schemapb.FieldSchema{Name: "cold_column", DataType: schemapb.DataType_Int64, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.FieldSkipLoadKey, Value: "true"}}}
	wire, err := proto.Marshal(skip)
	require.NoError(t, err)
	resp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{DbName: db, CollectionName: name, Schema: wire})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, []int64{100, 101}, mgr.GetLoadFields(ctx, coll.CollectionID))
}
