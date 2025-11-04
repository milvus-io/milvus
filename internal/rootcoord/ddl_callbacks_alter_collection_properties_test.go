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

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestDDLCallbacksAlterCollectionProperties(t *testing.T) {
	core := initStreamingSystemAndCore(t)

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

	// hook related properties are not allowed to be altered.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: hookutil.EncryptionEnabledKey, Value: "1"}},
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

	// disable dynamic schema property should return error.
	resp, err = core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EnableDynamicSchemaKey, Value: "false"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
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
		return
	}
	require.Len(t, coll.Fields, 4)
	require.True(t, coll.Fields[len(coll.Fields)-1].IsDynamic)
	require.Equal(t, coll.Fields[len(coll.Fields)-1].DataType, schemapb.DataType_JSON)
	require.Equal(t, coll.Fields[len(coll.Fields)-1].FieldID, int64(101))
}
