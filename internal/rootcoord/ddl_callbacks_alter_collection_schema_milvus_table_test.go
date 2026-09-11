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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBroadcastAlterCollectionSchemaAddMilvusTableFieldUsesSourceFieldID(t *testing.T) {
	mockAlterSchemaBroadcastAPI(t)
	mockReadMetadata := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).
		Return(&datapb.SnapshotMetadata{
			Collection: &datapb.CollectionDescription{
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
						{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}},
						{FieldID: 102, Name: "category", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}}},
						{FieldID: 103, Name: "score", DataType: schemapb.DataType_Int64, Nullable: true},
						{FieldID: 105, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
					},
				},
			},
		}, nil).Build()
	defer mockReadMetadata.UnPatch()

	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "milvusTableDB" + funcutil.RandomString(10)
	collectionName := "milvusTableAddField" + funcutil.RandomString(10)

	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	db, err := core.meta.GetDatabaseByName(ctx, dbName, typeutil.MaxTimestamp)
	require.NoError(t, err)

	coll := &model.Collection{
		CollectionID:   100,
		DBID:           db.ID,
		DBName:         dbName,
		Name:           collectionName,
		SchemaVersion:  3,
		State:          pb.CollectionState_CollectionCreated,
		ExternalSource: "s3://bucket/snapshots/100/metadata/200.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		Fields: []*model.Field{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "pk"},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text", TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}},
			{FieldID: 105, Name: "vec", DataType: schemapb.DataType_FloatVector, ExternalField: "vec", TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
			{FieldID: 106, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		Properties: []*commonpb.KeyValuePair{
			{Key: common.MaxFieldIDKey, Value: "106"},
			{Key: common.MilvusTableTargetOnlyFieldIDsKey, Value: "[106]"},
		},
	}
	require.NoError(t, core.meta.AddCollection(ctx, coll))

	req := &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{
						FieldSchema: &schemapb.FieldSchema{
							Name:          "category",
							DataType:      schemapb.DataType_VarChar,
							Nullable:      true,
							ExternalField: "category",
							TypeParams:    []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}},
						},
					}},
				},
			},
		},
	}

	broadcaster := &alterSchemaBroadcastAPIMock{}
	require.NoError(t, core.broadcastAlterCollectionSchemaAdd(ctx, broadcaster, coll, req))
	require.True(t, broadcaster.msg.IsUnreplicable())
	alterMsg := message.MustAsBroadcastAlterCollectionMessageV2(broadcaster.msg)
	schema := alterMsg.MustBody().GetUpdates().GetSchema()
	newField := typeutil.GetFieldByName(schema, "category")
	require.NotNil(t, newField)
	require.EqualValues(t, 102, newField.GetFieldID())
	require.EqualValues(t, 106, maxAssignedFieldIDFromSchema(schema))
}

func TestAlterMilvusTableFieldRejectsReservedTargetOnlyFieldID(t *testing.T) {
	sourceSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 106, Name: "category", DataType: schemapb.DataType_VarChar},
		},
	}
	mockReadMetadata := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).
		Return(&datapb.SnapshotMetadata{
			Collection: &datapb.CollectionDescription{Schema: sourceSchema},
		}, nil).Build()
	defer mockReadMetadata.UnPatch()

	coll := &model.Collection{
		CollectionID:   100,
		ExternalSource: "s3://bucket/snapshots/100/metadata/200.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		Fields: []*model.Field{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "pk"},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
		},
		Properties: []*commonpb.KeyValuePair{
			{Key: common.MaxFieldIDKey, Value: "106"},
			{Key: common.MilvusTableTargetOnlyFieldIDsKey, Value: "[106]"},
		},
	}
	plan := &schemautil.AlterSchemaAddPlan{
		Kind: schemautil.AlterSchemaAddField,
		Field: &schemapb.FieldSchema{
			Name:          "category",
			DataType:      schemapb.DataType_VarChar,
			Nullable:      true,
			ExternalField: "category",
		},
	}

	fieldID, resolvedSourceSchema, err := resolveAlterSchemaAddFieldID(coll, plan)
	require.NoError(t, err)
	require.EqualValues(t, 106, fieldID)
	targetSchema, _, err := buildAlterSchemaAddSchema(coll, plan, fieldID)
	require.NoError(t, err)
	err = typeutil.ValidateMilvusTableSchemaIdentity(targetSchema, resolvedSourceSchema, true)
	require.Error(t, err)
	require.ErrorContains(t, err, "permanently reserved")
}

func TestBuildAlterMilvusTableFunctionFieldReservesTargetOnlyFieldID(t *testing.T) {
	coll := &model.Collection{
		ExternalSource: "s3://bucket/snapshots/100/metadata/200.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		Fields: []*model.Field{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "pk"},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
			{FieldID: 106, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		Properties: []*commonpb.KeyValuePair{
			{Key: common.MaxFieldIDKey, Value: "106"},
			{Key: common.MilvusTableTargetOnlyFieldIDsKey, Value: "[106]"},
		},
	}
	plan := &schemautil.AlterSchemaAddPlan{
		Kind: schemautil.AlterSchemaAddFunctionField,
		Field: &schemapb.FieldSchema{
			Name:     "sparse",
			DataType: schemapb.DataType_SparseFloatVector,
		},
		Function: &schemapb.FunctionSchema{
			Name:             "bm25",
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"sparse"},
		},
	}

	schema, _, err := buildAlterSchemaAddSchema(coll, plan, 107)
	require.NoError(t, err)
	reserved, err := typeutil.GetMilvusTableTargetOnlyFieldIDs(schema)
	require.NoError(t, err)
	require.True(t, reserved.Contain(106, 107))

	badColl := coll.Clone()
	badColl.Properties = []*commonpb.KeyValuePair{{
		Key:   common.MilvusTableTargetOnlyFieldIDsKey,
		Value: "not-json",
	}}
	_, _, err = buildAlterSchemaAddSchema(badColl, plan, 107)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestDropMilvusTableFunctionSeedsLegacyTargetOnlyFieldIDs(t *testing.T) {
	mockAlterSchemaBroadcastAPI(t)
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "milvusTableDB" + funcutil.RandomString(10)
	collectionName := "milvusTableDropFunction" + funcutil.RandomString(10)

	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	db, err := core.meta.GetDatabaseByName(ctx, dbName, typeutil.MaxTimestamp)
	require.NoError(t, err)

	coll := &model.Collection{
		CollectionID:   100,
		DBID:           db.ID,
		DBName:         dbName,
		Name:           collectionName,
		SchemaVersion:  3,
		State:          pb.CollectionState_CollectionCreated,
		ExternalSource: "s3://bucket/snapshots/100/metadata/200.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		Fields: []*model.Field{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "pk"},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
			{
				FieldID:       105,
				Name:          "vec",
				DataType:      schemapb.DataType_FloatVector,
				ExternalField: "vec",
				TypeParams:    []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
			},
			{FieldID: 106, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 107, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*model.Function{{
			Name:             "bm25",
			Type:             schemapb.FunctionType_BM25,
			InputFieldIDs:    []int64{101},
			InputFieldNames:  []string{"text"},
			OutputFieldIDs:   []int64{107},
			OutputFieldNames: []string{"sparse"},
		}},
		// Simulate a collection created before the target-only reservation
		// property existed. The first drop must seed both the live virtual PK ID
		// and the function output ID that is about to become historical.
		Properties: []*commonpb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "107"}},
	}
	require.NoError(t, core.meta.AddCollection(ctx, coll))

	req := &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
				DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
					Identifier:               &milvuspb.AlterCollectionSchemaRequest_DropRequest_FunctionName{FunctionName: "bm25"},
					DropFunctionOutputFields: true,
				},
			},
		},
	}

	broadcaster := &alterSchemaBroadcastAPIMock{}
	require.NoError(t, core.broadcastAlterCollectionSchemaDrop(ctx, broadcaster, coll, req))
	require.NotNil(t, broadcaster.msg)
	require.True(t, broadcaster.msg.IsUnreplicable())
	alterMsg := message.MustAsBroadcastAlterCollectionMessageV2(broadcaster.msg)
	schema := alterMsg.MustBody().GetUpdates().GetSchema()
	require.Nil(t, typeutil.GetFieldByName(schema, "sparse"))
	require.Empty(t, schema.GetFunctions())
	reserved, err := typeutil.GetMilvusTableTargetOnlyFieldIDs(schema)
	require.NoError(t, err)
	require.True(t, reserved.Contain(106, 107))

	badColl := coll.Clone()
	badColl.Properties = []*commonpb.KeyValuePair{
		{Key: common.MaxFieldIDKey, Value: "107"},
		{Key: common.MilvusTableTargetOnlyFieldIDsKey, Value: "not-json"},
	}
	err = core.broadcastAlterCollectionSchemaDrop(ctx, &alterSchemaBroadcastAPIMock{}, badColl, req)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestResolveAlterSchemaAddMilvusTableFunctionFieldIDAvoidsSourceFields(t *testing.T) {
	sourceSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 105, Name: "optional_vec", DataType: schemapb.DataType_FloatVector},
		},
	}
	mockReadMetadata := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).
		Return(&datapb.SnapshotMetadata{
			Collection: &datapb.CollectionDescription{Schema: sourceSchema},
		}, nil).Build()
	defer mockReadMetadata.UnPatch()

	coll := &model.Collection{
		CollectionID:   100,
		ExternalSource: "s3://bucket/snapshots/100/metadata/200.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		Fields: []*model.Field{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "pk"},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
		},
		Properties: []*commonpb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "101"}},
	}
	plan := &schemautil.AlterSchemaAddPlan{
		Kind: schemautil.AlterSchemaAddFunctionField,
		Field: &schemapb.FieldSchema{
			Name:     "embedding",
			DataType: schemapb.DataType_FloatVector,
		},
	}

	fieldID, resolvedSourceSchema, err := resolveAlterSchemaAddFieldID(coll, plan)
	require.NoError(t, err)
	require.Same(t, sourceSchema, resolvedSourceSchema)
	require.EqualValues(t, 106, fieldID)
}
