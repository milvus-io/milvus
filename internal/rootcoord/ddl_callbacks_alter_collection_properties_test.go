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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	imocks "github.com/milvus-io/milvus/internal/mocks"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDDLCallbacksAlterCollectionProperties(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	core.broker.(*mockBroker).ShowResourceGroupsFunc = func(ctx context.Context) ([]string, error) {
		return []string{"rg1", "rg2"}, nil
	}

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)

	// Cannot alter collection with empty properties and delete keys.
	resp, err := core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// Cannot alter collection properties with delete keys at same time.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.CollectionReplicaNumber, Value: "1"}},
		DeleteKeys:     []string{common.CollectionReplicaNumber},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// cipher related properties are not allowed to be altered.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EncryptionEnabledKey, Value: "1"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// cipher related properties are not allowed to be altered.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "1"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// cipher related properties are not allowed to be altered.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EncryptionRootKeyKey, Value: "1"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// Alter a database that does not exist should return error.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		DeleteKeys:     []string{common.CollectionReplicaNumber},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrDatabaseNotFound)

	// Alter a collection that does not exist should return error.
	resp, err = core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: util.DefaultDBName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         util.DefaultDBName,
		CollectionName: collectionName,
		DeleteKeys:     []string{common.CollectionReplicaNumber},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrCollectionNotFound)

	// atler a property of a collection.
	createCollectionAndAliasForTest(t, ctx, core, dbName, collectionName)
	assertReplicaNumber(t, ctx, core, dbName, collectionName, 1)
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionReplicaNumber, Value: "2"},
			{Key: common.CollectionResourceGroups, Value: "rg1,rg2"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertReplicaNumber(t, ctx, core, dbName, collectionName, 2)
	assertResourceGroups(t, ctx, core, dbName, collectionName, []string{"rg1", "rg2"})

	// delete a property of a collection.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		DeleteKeys:     []string{common.CollectionReplicaNumber, common.CollectionResourceGroups},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertReplicaNumber(t, ctx, core, dbName, collectionName, 0)
	assertResourceGroups(t, ctx, core, dbName, collectionName, []string{})

	// alter consistency level and description of a collection.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.ConsistencyLevel, Value: commonpb.ConsistencyLevel_Eventually.String()},
			{Key: common.CollectionDescription, Value: "description2"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertConsistencyLevel(t, ctx, core, dbName, collectionName, commonpb.ConsistencyLevel_Eventually)
	assertDescription(t, ctx, core, dbName, collectionName, "description2")

	// alter collection should be idempotent.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.ConsistencyLevel, Value: commonpb.ConsistencyLevel_Eventually.String()},
			{Key: common.CollectionDescription, Value: "description2"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertConsistencyLevel(t, ctx, core, dbName, collectionName, commonpb.ConsistencyLevel_Eventually)
	assertDescription(t, ctx, core, dbName, collectionName, "description2")
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 0) // schema version should not be changed with alter collection properties.

	// update dynamic schema property with other properties should return error.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EnableDynamicSchemaKey, Value: "true"}, {Key: common.CollectionReplicaNumber, Value: "1"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
}

func TestDDLCallbacksAlterCollectionV2AckCallback_UpdateLoadConfigRPCError(t *testing.T) {
	ctx := context.Background()

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(nil)

	mixc := imocks.NewMixCoord(t)
	mixc.On("UpdateLoadConfig", mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

	c := newTestCore(
		withMeta(meta),
		withMixCoord(mixc),
		withValidProxyManager(),
		withBroker(&mockBroker{}),
	)
	cb := &DDLCallback{Core: c}

	raw := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				AlterLoadConfig: &messagespb.AlterLoadConfigOfAlterCollection{
					ReplicaNumber:  1,
					ResourceGroups: []string{"rg1"},
				},
			},
		}).
		WithBroadcast([]string{funcutil.GetControlChannel("test")}).
		MustBuildBroadcast()
	msg := message.MustAsBroadcastAlterCollectionMessageV2(raw)

	err := cb.alterCollectionV2AckCallback(ctx, message.BroadcastResultAlterCollectionMessageV2{
		Message: msg,
		Results: map[string]*message.AppendResult{},
	})
	require.Error(t, err)
}

func TestDDLCallbacksAlterCollectionV2AckCallback_UpdateLoadConfigNonRGNotFoundError(t *testing.T) {
	ctx := context.Background()

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(nil)

	mixc := imocks.NewMixCoord(t)
	mixc.On("UpdateLoadConfig", mock.Anything, mock.Anything).Return(merr.Status(errors.New("mock error")), nil)

	c := newTestCore(
		withMeta(meta),
		withMixCoord(mixc),
		withValidProxyManager(),
		withBroker(&mockBroker{}),
	)
	cb := &DDLCallback{Core: c}

	raw := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				AlterLoadConfig: &messagespb.AlterLoadConfigOfAlterCollection{
					ReplicaNumber:  1,
					ResourceGroups: []string{"rg1"},
				},
			},
		}).
		WithBroadcast([]string{funcutil.GetControlChannel("test")}).
		MustBuildBroadcast()
	msg := message.MustAsBroadcastAlterCollectionMessageV2(raw)

	err := cb.alterCollectionV2AckCallback(ctx, message.BroadcastResultAlterCollectionMessageV2{
		Message: msg,
		Results: map[string]*message.AppendResult{},
	})
	require.Error(t, err)
	require.False(t, errors.Is(err, merr.ErrResourceGroupNotFound))
}

func TestDDLCallbacksAlterCollectionV2AckCallback_StopRetryOnResourceGroupNotFound(t *testing.T) {
	ctx := context.Background()

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(nil)

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

	raw := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				AlterLoadConfig: &messagespb.AlterLoadConfigOfAlterCollection{
					ReplicaNumber:  1,
					ResourceGroups: []string{"rg_not_exist"},
				},
			},
		}).
		WithBroadcast([]string{funcutil.GetControlChannel("test")}).
		MustBuildBroadcast()
	msg := message.MustAsBroadcastAlterCollectionMessageV2(raw)

	err := cb.alterCollectionV2AckCallback(ctx, message.BroadcastResultAlterCollectionMessageV2{
		Message: msg,
		Results: map[string]*message.AppendResult{},
	})
	require.NoError(t, err)
}

func TestCore_getAlterLoadConfigOfAlterCollection(t *testing.T) {
	core := &Core{}

	t.Run("no changes", func(t *testing.T) {
		cfg := core.getAlterLoadConfigOfAlterCollection(
			[]*commonpb.KeyValuePair{
				{Key: common.CollectionReplicaNumber, Value: "1"},
				{Key: common.CollectionResourceGroups, Value: "rg1"},
			},
			[]*commonpb.KeyValuePair{
				{Key: common.CollectionReplicaNumber, Value: "1"},
				{Key: common.CollectionResourceGroups, Value: "rg1"},
			},
		)
		require.Nil(t, cfg)
	})

	t.Run("replica changed", func(t *testing.T) {
		cfg := core.getAlterLoadConfigOfAlterCollection(
			[]*commonpb.KeyValuePair{
				{Key: common.CollectionReplicaNumber, Value: "1"},
				{Key: common.CollectionResourceGroups, Value: "rg1"},
			},
			[]*commonpb.KeyValuePair{
				{Key: common.CollectionReplicaNumber, Value: "2"},
				{Key: common.CollectionResourceGroups, Value: "rg1"},
			},
		)
		require.NotNil(t, cfg)
		require.Equal(t, int32(2), cfg.ReplicaNumber)
		require.Equal(t, []string{"rg1"}, cfg.ResourceGroups)
	})

	t.Run("rg changed", func(t *testing.T) {
		cfg := core.getAlterLoadConfigOfAlterCollection(
			[]*commonpb.KeyValuePair{
				{Key: common.CollectionReplicaNumber, Value: "1"},
				{Key: common.CollectionResourceGroups, Value: "rg1"},
			},
			[]*commonpb.KeyValuePair{
				{Key: common.CollectionReplicaNumber, Value: "1"},
				{Key: common.CollectionResourceGroups, Value: "rg2"},
			},
		)
		require.NotNil(t, cfg)
		require.Equal(t, int32(1), cfg.ReplicaNumber)
		require.Equal(t, []string{"rg2"}, cfg.ResourceGroups)
	})
}

func TestDDLCallbacksAlterCollectionV2AckCallback_BroadcastAlteredCollectionError(t *testing.T) {
	ctx := context.Background()

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(nil)

	mixc := imocks.NewMixCoord(t)
	mixc.On("UpdateLoadConfig", mock.Anything, mock.Anything).Return(merr.Success(), nil)

	c := newTestCore(
		withMeta(meta),
		withMixCoord(mixc),
		withValidProxyManager(),
		withBroker(&mockBroker{
			BroadcastAlteredCollectionFunc: func(ctx context.Context, collectionID int64) error {
				return errors.New("broadcast error")
			},
		}),
	)
	cb := &DDLCallback{Core: c}

	raw := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				AlterLoadConfig: &messagespb.AlterLoadConfigOfAlterCollection{
					ReplicaNumber:  1,
					ResourceGroups: []string{"rg1"},
				},
			},
		}).
		WithBroadcast([]string{funcutil.GetControlChannel("test")}).
		MustBuildBroadcast()
	msg := message.MustAsBroadcastAlterCollectionMessageV2(raw)

	err := cb.alterCollectionV2AckCallback(ctx, message.BroadcastResultAlterCollectionMessageV2{
		Message: msg,
		Results: map[string]*message.AppendResult{},
	})
	require.Error(t, err)
}

func TestDDLCallbacksAlterCollectionPropertiesForDynamicField(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)

	createCollectionAndAliasForTest(t, ctx, core, dbName, collectionName)

	// update dynamic schema property with other properties should return error.
	resp, err := core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EnableDynamicSchemaKey, Value: "true"}, {Key: common.CollectionReplicaNumber, Value: "1"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// update dynamic schema property with invalid value should return error.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EnableDynamicSchemaKey, Value: "123123"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	// update dynamic schema property with other properties should return error.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EnableDynamicSchemaKey, Value: "true"}},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDynamicSchema(t, ctx, core, dbName, collectionName, true)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1) // add dynamic field should increment schema version.

	// update dynamic schema property with other properties should be idempotent.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EnableDynamicSchemaKey, Value: "true"}},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDynamicSchema(t, ctx, core, dbName, collectionName, true)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)

	// disable dynamic schema property should succeed.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EnableDynamicSchemaKey, Value: "false"}},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDynamicSchema(t, ctx, core, dbName, collectionName, false)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2) // drop dynamic field should increment schema version.

	// disable dynamic schema property should be idempotent.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EnableDynamicSchemaKey, Value: "false"}},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDynamicSchema(t, ctx, core, dbName, collectionName, false)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2)

	// re-enable dynamic schema property should succeed with a new field ID.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EnableDynamicSchemaKey, Value: "true"}},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertDynamicSchema(t, ctx, core, dbName, collectionName, true)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 3)
	// The re-enabled $meta field should have a new FieldID (102, not 101).
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	dynamicField := coll.Fields[len(coll.Fields)-1]
	require.True(t, dynamicField.IsDynamic)
	require.Equal(t, int64(102), dynamicField.FieldID)
	assertMaxFieldIDProperty(t, ctx, core, dbName, collectionName, 102)
}

func TestDDLCallbacksAlterCollectionProperties_TTLFieldShouldBroadcastSchema(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()

	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollectionTTLField" + funcutil.RandomString(10)

	// Create collection with a ttl field.
	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: dbName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	testSchema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "description",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{Name: "field1", DataType: schemapb.DataType_Int64},
			{Name: "ttl", DataType: schemapb.DataType_Timestamptz, Nullable: true},
		},
	}
	schemaBytes, err := proto.Marshal(testSchema)
	require.NoError(t, err)
	resp, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Properties:       []*commonpb.KeyValuePair{{Key: common.CollectionReplicaNumber, Value: "1"}},
		Schema:           schemaBytes,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 0)

	// Alter properties to set ttl field should succeed and should NOT change schema version in meta.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.CollectionTTLFieldKey, Value: "ttl"}},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 0)
}

func TestDDLCallbacksAlterCollectionProperties_TTLFieldPreservesExternalSpec(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()

	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollectionTTLExtSpec" + funcutil.RandomString(10)

	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: dbName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	// Create as external collection with both source and spec atomically set.
	// Alter is no longer permitted to mutate source/spec; the TTL alter must
	// nevertheless preserve them when it triggers a schema snapshot rebuild.
	testSchema := &schemapb.CollectionSchema{
		Name:           collectionName,
		Description:    "description",
		AutoID:         false,
		ExternalSource: "s3://bucket/ttl-path",
		ExternalSpec:   `{"format":"parquet","extfs":{"anonymous":"true","region":"us-east-1","cloud_provider":"aws"}}`,
		Fields: []*schemapb.FieldSchema{
			{Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "id"},
			{Name: "ttl", DataType: schemapb.DataType_Timestamptz, ExternalField: "ttl"},
			{
				Name: "vec", DataType: schemapb.DataType_FloatVector, ExternalField: "vec",
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
			},
		},
	}
	schemaBytes, err := proto.Marshal(testSchema)
	require.NoError(t, err)
	resp, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Properties:       []*commonpb.KeyValuePair{{Key: common.CollectionReplicaNumber, Value: "1"}},
		Schema:           schemaBytes,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		ShardsNum:        1,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertExternalSource(t, ctx, core, dbName, collectionName, "s3://bucket/ttl-path")
	assertExternalSpec(t, ctx, core, dbName, collectionName, `{"format":"parquet","extfs":{"anonymous":"true","region":"us-east-1","cloud_provider":"aws"}}`)

	// Alter TTL field only — must preserve previously persisted external source/spec.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionTTLFieldKey, Value: "ttl"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertExternalSource(t, ctx, core, dbName, collectionName, "s3://bucket/ttl-path")
	assertExternalSpec(t, ctx, core, dbName, collectionName, `{"format":"parquet","extfs":{"anonymous":"true","region":"us-east-1","cloud_provider":"aws"}}`)

	// TTL with invalid field name still rejected.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionTTLFieldKey, Value: "nonexistent_field"},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
}

func assertExternalSource(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, expectedSource string) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, expectedSource, coll.ExternalSource)
}

func assertExternalSpec(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, expectedSpec string) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, expectedSpec, coll.ExternalSpec)
}

// TestDDLCallbacksAlterCollectionProperties_AcceptExternalSourceSpec verifies
// rootcoord accepts external_source/external_spec updates via AlterCollection.
// User-facing rejection lives in the proxy layer (see issue #49335); rootcoord
// trusts internal callers so the refresh-completion sync path
// (updateExternalSchemaViaWAL) can persist a new tuple after a successful
// override refresh.
func TestDDLCallbacksAlterCollectionProperties_AcceptExternalSourceSpec(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()

	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testAcceptExt" + funcutil.RandomString(10)

	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	testSchema := &schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{Name: "field1", DataType: schemapb.DataType_Int64},
		},
		ExternalSource: "s3://bucket/old/",
		ExternalSpec:   `{"format":"parquet"}`,
	}
	schemaBytes, err := proto.Marshal(testSchema)
	require.NoError(t, err)
	resp, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Schema:           schemaBytes,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertExternalSource(t, ctx, core, dbName, collectionName, "s3://bucket/old/")

	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName: dbName, CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionExternalSource, Value: "s3://bucket/new/"},
			{Key: common.CollectionExternalSpec, Value: `{"format":"parquet"}`},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertExternalSource(t, ctx, core, dbName, collectionName, "s3://bucket/new/")
	assertExternalSpec(t, ctx, core, dbName, collectionName, `{"format":"parquet"}`)
}

// Regression for #49335: refresh override path may carry source-only updates
// (or spec-only) since the tuple is preserved by the refresh manager. A
// partial alter must not blank the unspecified half by writing an empty
// string into the schema snapshot.
func TestDDLCallbacksAlterCollectionProperties_PartialExternalUpdatePreservesOther(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()

	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testPartialExt" + funcutil.RandomString(10)

	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	testSchema := &schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{Name: "field1", DataType: schemapb.DataType_Int64},
		},
		ExternalSource: "s3://bucket/old/",
		ExternalSpec:   `{"format":"parquet","extfs":{"region":"us-east-1"}}`,
	}
	schemaBytes, err := proto.Marshal(testSchema)
	require.NoError(t, err)
	resp, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Schema:           schemaBytes,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	// Source-only update; spec must be preserved verbatim.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName: dbName, CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionExternalSource, Value: "s3://bucket/new/"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertExternalSource(t, ctx, core, dbName, collectionName, "s3://bucket/new/")
	assertExternalSpec(t, ctx, core, dbName, collectionName, `{"format":"parquet","extfs":{"region":"us-east-1"}}`)
}

// Regression for #49335: alter that mixes external_source with a regular
// property (e.g. consistency level or replica number) must update both halves
// — the external tuple goes to the schema snapshot and the regular property
// to the Properties map. Neither side may swallow the other.
func TestDDLCallbacksAlterCollectionProperties_MixedExternalAndRegular(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()

	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testMixedExt" + funcutil.RandomString(10)

	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	testSchema := &schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{Name: "field1", DataType: schemapb.DataType_Int64},
		},
		ExternalSource: "s3://bucket/old/",
		ExternalSpec:   `{"format":"parquet"}`,
	}
	schemaBytes, err := proto.Marshal(testSchema)
	require.NoError(t, err)
	resp, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Properties:       []*commonpb.KeyValuePair{{Key: common.CollectionReplicaNumber, Value: "1"}},
		Schema:           schemaBytes,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName: dbName, CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionExternalSource, Value: "s3://bucket/new/"},
			{Key: common.CollectionReplicaNumber, Value: "2"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertExternalSource(t, ctx, core, dbName, collectionName, "s3://bucket/new/")
	assertReplicaNumber(t, ctx, core, dbName, collectionName, 2)
}

func createCollectionForTest(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string) {
	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: dbName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	testSchema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "description",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "field1",
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	schemaBytes, err := proto.Marshal(testSchema)
	require.NoError(t, err)
	resp, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Properties:       []*commonpb.KeyValuePair{{Key: common.CollectionReplicaNumber, Value: "1"}},
		Schema:           schemaBytes,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertReplicaNumber(t, ctx, core, dbName, collectionName, 1)
	assertConsistencyLevel(t, ctx, core, dbName, collectionName, commonpb.ConsistencyLevel_Bounded)
	assertDescription(t, ctx, core, dbName, collectionName, "description")
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 0)
}

func createCollectionAndAliasForTest(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string) {
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	// add an alias to the collection.
	aliasName := collectionName + "_alias"
	resp, err := core.CreateAlias(ctx, &milvuspb.CreateAliasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Alias:          aliasName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertReplicaNumber(t, ctx, core, dbName, aliasName, 1)
	assertConsistencyLevel(t, ctx, core, dbName, aliasName, commonpb.ConsistencyLevel_Bounded)
	assertDescription(t, ctx, core, dbName, aliasName, "description")
}

func assertReplicaNumber(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, replicaNumber int64) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	replicaNum, err := common.CollectionLevelReplicaNumber(coll.Properties)
	if replicaNumber == 0 {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	require.Equal(t, replicaNumber, replicaNum)
}

func assertResourceGroups(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, resourceGroups []string) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	rgs, err := common.CollectionLevelResourceGroups(coll.Properties)
	if len(resourceGroups) == 0 {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	require.ElementsMatch(t, resourceGroups, rgs)
}

func assertConsistencyLevel(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, consistencyLevel commonpb.ConsistencyLevel) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, consistencyLevel, coll.ConsistencyLevel)
}

func assertDescription(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, description string) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, description, coll.Description)
}

func assertSchemaVersion(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, schemaVersion int32) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, schemaVersion, coll.SchemaVersion)
}

func assertDynamicSchema(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, dynamicSchema bool) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, dynamicSchema, coll.EnableDynamicField)
	if !dynamicSchema {
		// Verify no dynamic field exists.
		for _, field := range coll.Fields {
			require.False(t, field.IsDynamic, "expected no dynamic field after disabling")
		}
		return
	}
	require.True(t, coll.Fields[len(coll.Fields)-1].IsDynamic)
	require.Equal(t, coll.Fields[len(coll.Fields)-1].DataType, schemapb.DataType_JSON)
}
