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
package rootcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	imocks "github.com/milvus-io/milvus/internal/mocks"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDDLCallbacksAlterDatabase(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	core.broker.(*mockBroker).ShowResourceGroupsFunc = func(ctx context.Context) ([]string, error) {
		return []string{"rg1", "rg2"}, nil
	}

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)

	// Cannot alter collection with empty properties and delete keys.
	resp, err := core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName: dbName,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// Cannot alter collection properties with delete keys at same time.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName:     dbName,
		Properties: []*commonpb.KeyValuePair{{Key: common.DatabaseReplicaNumber, Value: "1"}},
		DeleteKeys: []string{common.DatabaseReplicaNumber},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// hook related properties are not allowed to be altered.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName:     dbName,
		Properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEnabledKey, Value: "1"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// Alter a database that does not exist should return error.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName:     dbName,
		DeleteKeys: []string{common.DatabaseReplicaNumber},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrDatabaseNotFound)

	resp, err = core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName:     dbName,
		Properties: []*commonpb.KeyValuePair{{Key: common.DatabaseReplicaNumber, Value: "1"}},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDatabaseReplicaNumber(t, ctx, core, dbName, 1)

	// alter a property of a database.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName: dbName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.DatabaseReplicaNumber, Value: "2"},
			{Key: common.DatabaseResourceGroups, Value: "rg1,rg2"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDatabaseReplicaNumber(t, ctx, core, dbName, 2)
	assertDatabaseResourceGroups(t, ctx, core, dbName, []string{"rg1", "rg2"})

	// alter a property of a database should be idempotent.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName: dbName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.DatabaseReplicaNumber, Value: "2"},
			{Key: common.DatabaseResourceGroups, Value: "rg1,rg2"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDatabaseReplicaNumber(t, ctx, core, dbName, 2)
	assertDatabaseResourceGroups(t, ctx, core, dbName, []string{"rg1", "rg2"})

	// delete a property of a database.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName:     dbName,
		DeleteKeys: []string{common.DatabaseReplicaNumber, common.DatabaseResourceGroups},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDatabaseReplicaNumber(t, ctx, core, dbName, 0)
	assertDatabaseResourceGroups(t, ctx, core, dbName, []string{})

	// delete a property of a collection should be idempotent.
	resp, err = core.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{
		DbName:     dbName,
		DeleteKeys: []string{common.DatabaseReplicaNumber, common.DatabaseResourceGroups},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDatabaseReplicaNumber(t, ctx, core, dbName, 0)
	assertDatabaseResourceGroups(t, ctx, core, dbName, []string{})
}

func TestDDLCallbacksAlterDatabaseV1AckCallback_AlterDatabaseError(t *testing.T) {
	ctx := context.Background()
	controlChannel := funcutil.GetControlChannel("test")

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().AlterDatabase(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("meta error"))

	c := newTestCore(
		withMeta(meta),
		withValidProxyManager(),
		withBroker(&mockBroker{}),
	)
	cb := &DDLCallback{Core: c}

	raw := message.NewAlterDatabaseMessageBuilderV2().
		WithHeader(&messagespb.AlterDatabaseMessageHeader{
			DbName: "db",
			DbId:   1,
		}).
		WithBody(&messagespb.AlterDatabaseMessageBody{
			Properties: []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
		}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	msg := message.MustAsBroadcastAlterDatabaseMessageV2(raw)

	err := cb.alterDatabaseV1AckCallback(ctx, message.BroadcastResultAlterDatabaseMessageV2{
		Message: msg,
		Results: map[string]*message.AppendResult{
			controlChannel: {TimeTick: 1},
		},
	})
	require.Error(t, err)
}

func TestDDLCallbacksAlterDatabaseV1AckCallback_UpdateLoadConfigRPCError(t *testing.T) {
	ctx := context.Background()
	controlChannel := funcutil.GetControlChannel("test")

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().AlterDatabase(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mixc := imocks.NewMixCoord(t)
	mixc.On("UpdateLoadConfig", mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

	c := newTestCore(
		withMeta(meta),
		withMixCoord(mixc),
		withValidProxyManager(),
		withBroker(&mockBroker{}),
	)
	cb := &DDLCallback{Core: c}

	raw := message.NewAlterDatabaseMessageBuilderV2().
		WithHeader(&messagespb.AlterDatabaseMessageHeader{
			DbName: "db",
			DbId:   1,
		}).
		WithBody(&messagespb.AlterDatabaseMessageBody{
			Properties: []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
			AlterLoadConfig: &messagespb.AlterLoadConfigOfAlterDatabase{
				CollectionIds:  []int64{1},
				ReplicaNumber:  1,
				ResourceGroups: []string{"rg1"},
			},
		}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	msg := message.MustAsBroadcastAlterDatabaseMessageV2(raw)

	err := cb.alterDatabaseV1AckCallback(ctx, message.BroadcastResultAlterDatabaseMessageV2{
		Message: msg,
		Results: map[string]*message.AppendResult{
			controlChannel: {TimeTick: 1},
		},
	})
	require.Error(t, err)
}

func TestDDLCallbacksAlterDatabaseV1AckCallback_UpdateLoadConfigNonRGNotFoundError(t *testing.T) {
	ctx := context.Background()
	controlChannel := funcutil.GetControlChannel("test")

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().AlterDatabase(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mixc := imocks.NewMixCoord(t)
	mixc.On("UpdateLoadConfig", mock.Anything, mock.Anything).Return(merr.Status(errors.New("mock error")), nil)

	c := newTestCore(
		withMeta(meta),
		withMixCoord(mixc),
		withValidProxyManager(),
		withBroker(&mockBroker{}),
	)
	cb := &DDLCallback{Core: c}

	raw := message.NewAlterDatabaseMessageBuilderV2().
		WithHeader(&messagespb.AlterDatabaseMessageHeader{
			DbName: "db",
			DbId:   1,
		}).
		WithBody(&messagespb.AlterDatabaseMessageBody{
			Properties: []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
			AlterLoadConfig: &messagespb.AlterLoadConfigOfAlterDatabase{
				CollectionIds:  []int64{1},
				ReplicaNumber:  1,
				ResourceGroups: []string{"rg1"},
			},
		}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	msg := message.MustAsBroadcastAlterDatabaseMessageV2(raw)

	err := cb.alterDatabaseV1AckCallback(ctx, message.BroadcastResultAlterDatabaseMessageV2{
		Message: msg,
		Results: map[string]*message.AppendResult{
			controlChannel: {TimeTick: 1},
		},
	})
	require.Error(t, err)
	require.False(t, errors.Is(err, merr.ErrResourceGroupNotFound))
}

func TestDDLCallbacksAlterDatabaseV1AckCallback_StopRetryOnResourceGroupNotFound(t *testing.T) {
	ctx := context.Background()
	controlChannel := funcutil.GetControlChannel("test")

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().AlterDatabase(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mixc := imocks.NewMixCoord(t)
	mixc.On("UpdateLoadConfig", mock.Anything, mock.Anything).Return(
		merr.Status(merr.WrapErrResourceGroupNotFound("rg_not_exist")),
		nil,
	)

	c := newTestCore(
		withMeta(meta),
		withMixCoord(mixc),
		withValidProxyManager(),
		withBroker(&mockBroker{}),
	)
	cb := &DDLCallback{Core: c}

	raw := message.NewAlterDatabaseMessageBuilderV2().
		WithHeader(&messagespb.AlterDatabaseMessageHeader{
			DbName: "db",
			DbId:   1,
		}).
		WithBody(&messagespb.AlterDatabaseMessageBody{
			Properties: []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
			AlterLoadConfig: &messagespb.AlterLoadConfigOfAlterDatabase{
				CollectionIds:  []int64{1},
				ReplicaNumber:  1,
				ResourceGroups: []string{"rg_not_exist"},
			},
		}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	msg := message.MustAsBroadcastAlterDatabaseMessageV2(raw)

	err := cb.alterDatabaseV1AckCallback(ctx, message.BroadcastResultAlterDatabaseMessageV2{
		Message: msg,
		Results: map[string]*message.AppendResult{
			controlChannel: {TimeTick: 1},
		},
	})
	require.NoError(t, err)
}

func TestMergeProperties(t *testing.T) {
	props := MergeProperties(
		[]*commonpb.KeyValuePair{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
		[]*commonpb.KeyValuePair{
			{Key: "k2", Value: "v2b"},
			{Key: "k3", Value: "v3"},
		},
	)

	m := make(map[string]string, len(props))
	for _, kv := range props {
		m[kv.Key] = kv.Value
	}
	require.Equal(t, map[string]string{"k1": "v1", "k2": "v2b", "k3": "v3"}, m)
}

func TestCore_getAlterLoadConfigOfAlterDatabase(t *testing.T) {
	ctx := context.Background()

	t.Run("list collections error", func(t *testing.T) {
		core := newTestCore(withMeta(&mockMetaTable{
			ListCollectionsFunc: func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
				return nil, errors.New("meta error")
			},
		}))

		_, err := core.getAlterLoadConfigOfAlterDatabase(ctx, "db",
			[]*commonpb.KeyValuePair{
				{Key: common.DatabaseReplicaNumber, Value: "1"},
				{Key: common.DatabaseResourceGroups, Value: "rg1"},
			},
			[]*commonpb.KeyValuePair{
				{Key: common.DatabaseReplicaNumber, Value: "2"},
				{Key: common.DatabaseResourceGroups, Value: "rg1"},
			},
		)
		require.Error(t, err)
	})

	t.Run("empty collections", func(t *testing.T) {
		core := newTestCore(withMeta(&mockMetaTable{
			ListCollectionsFunc: func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
				return []*model.Collection{}, nil
			},
		}))

		cfg, err := core.getAlterLoadConfigOfAlterDatabase(ctx, "db",
			[]*commonpb.KeyValuePair{
				{Key: common.DatabaseReplicaNumber, Value: "1"},
				{Key: common.DatabaseResourceGroups, Value: "rg1"},
			},
			[]*commonpb.KeyValuePair{
				{Key: common.DatabaseReplicaNumber, Value: "2"},
				{Key: common.DatabaseResourceGroups, Value: "rg1"},
			},
		)
		require.NoError(t, err)
		require.Nil(t, cfg)
	})

	t.Run("build alter load config", func(t *testing.T) {
		core := newTestCore(withMeta(&mockMetaTable{
			ListCollectionsFunc: func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
				return []*model.Collection{{CollectionID: 10}}, nil
			},
		}))

		cfg, err := core.getAlterLoadConfigOfAlterDatabase(ctx, "db",
			[]*commonpb.KeyValuePair{
				{Key: common.DatabaseReplicaNumber, Value: "1"},
				{Key: common.DatabaseResourceGroups, Value: "rg1"},
			},
			[]*commonpb.KeyValuePair{
				{Key: common.DatabaseReplicaNumber, Value: "2"},
				{Key: common.DatabaseResourceGroups, Value: "rg1"},
			},
		)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, []int64{10}, cfg.CollectionIds)
		require.Equal(t, int32(2), cfg.ReplicaNumber)
		require.Equal(t, []string{"rg1"}, cfg.ResourceGroups)
	})
}

func assertDatabaseReplicaNumber(t *testing.T, ctx context.Context, core *Core, dbName string, replicaNumber int64) {
	db, err := core.meta.GetDatabaseByName(ctx, dbName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	replicaNum, err := common.DatabaseLevelReplicaNumber(db.Properties)
	if replicaNumber == 0 {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	require.Equal(t, replicaNumber, replicaNum)
}

func assertDatabaseResourceGroups(t *testing.T, ctx context.Context, core *Core, dbName string, resourceGroups []string) {
	db, err := core.meta.GetDatabaseByName(ctx, dbName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	rgs, err := common.DatabaseLevelResourceGroups(db.Properties)
	if len(resourceGroups) == 0 {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	require.ElementsMatch(t, resourceGroups, rgs)
}
