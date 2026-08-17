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
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestValidateCollectionSchemaPayloadSize(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name:        "collection",
		Description: strings.Repeat("collection description", 256),
		Fields: []*schemapb.FieldSchema{
			{
				Name:        "text",
				Description: strings.Repeat("field description", 256),
				DataType:    schemapb.DataType_VarChar,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_StringData{StringData: strings.Repeat("default value", 256)},
				},
			},
		},
	}
	actual := proto.Size(schema)

	tests := []struct {
		name    string
		limit   int
		wantErr bool
	}{
		{name: "below limit", limit: actual + 1},
		{name: "equal to limit", limit: actual, wantErr: true},
		{name: "above limit", limit: actual - 1, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			old := paramtable.Get().ProxyCfg.MaxCollectionSchemaSize.SwapTempValue(strconv.Itoa(test.limit))
			defer paramtable.Get().ProxyCfg.MaxCollectionSchemaSize.SwapTempValue(old)

			err := validateCollectionSchemaPayloadSize(schema)
			if !test.wantErr {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, merr.ErrParameterTooLarge)
			require.Contains(t, err.Error(), strconv.Itoa(actual))
			require.Contains(t, err.Error(), strconv.Itoa(test.limit))
		})
	}
}

func TestValidateCollectionSchemaPayloadSizeCumulativeGrowth(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "collection"}
	for i := 0; i < 4; i++ {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			Name:        "field_" + strconv.Itoa(i),
			Description: strings.Repeat("d", 1024),
			DataType:    schemapb.DataType_VarChar,
		})
	}

	limit := proto.Size(schema) + 1
	old := paramtable.Get().ProxyCfg.MaxCollectionSchemaSize.SwapTempValue(strconv.Itoa(limit))
	defer paramtable.Get().ProxyCfg.MaxCollectionSchemaSize.SwapTempValue(old)
	require.NoError(t, validateCollectionSchemaPayloadSize(schema))

	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		Name:     "field_with_default",
		DataType: schemapb.DataType_VarChar,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{StringData: strings.Repeat("d", 2048)},
		},
	})
	require.ErrorIs(t, validateCollectionSchemaPayloadSize(schema), merr.ErrParameterTooLarge)
}

func TestDDLCallbacksSchemaPayloadRejectsAddCollectionFieldBeforeBroadcast(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	storedSchema := coll.ToCollectionSchemaPB()
	limit := proto.Size(storedSchema) + 1024
	old := paramtable.Get().ProxyCfg.MaxCollectionSchemaSize.SwapTempValue(strconv.Itoa(limit))
	t.Cleanup(func() { paramtable.Get().ProxyCfg.MaxCollectionSchemaSize.SwapTempValue(old) })

	field := &schemapb.FieldSchema{
		Name:        "large_default",
		Description: strings.Repeat("field description", 256),
		DataType:    schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "65535"},
		},
		Nullable: true,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{StringData: strings.Repeat("default value", 256)},
		},
	}
	mutatedSchema := proto.Clone(storedSchema).(*schemapb.CollectionSchema)
	mutatedField := proto.Clone(field).(*schemapb.FieldSchema)
	mutatedField.FieldID = maxAssignedFieldIDFromSchema(mutatedSchema) + 1
	mutatedSchema.Version = coll.SchemaVersion + 1
	mutatedSchema.Fields = append(mutatedSchema.Fields, mutatedField)
	mutatedSchema.Properties = updateMaxFieldIDProperty(coll.Properties, mutatedField.GetFieldID())
	require.Less(t, proto.Size(storedSchema), limit)
	require.GreaterOrEqual(t, proto.Size(mutatedSchema), limit)

	broadcasts := 0
	mockBroadcastAPI := mock_broadcaster.NewMockBroadcastAPI(t)
	mockBroadcastAPI.EXPECT().Close().Return().Maybe()
	mockBroadcastAPI.EXPECT().Broadcast(mock.Anything, mock.Anything).Run(func(context.Context, message.BroadcastMutableMessage) {
		broadcasts++
	}).Return(&types.BroadcastAppendResult{}, nil).Maybe()
	lockMocker := mockey.Mock((*Core).startBroadcastWithAliasOrCollectionLock).Return(mockBroadcastAPI, nil).Build()
	t.Cleanup(func() { lockMocker.UnPatch() })

	schemaBytes, err := proto.Marshal(field)
	require.NoError(t, err)
	resp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterTooLarge)
	require.Zero(t, broadcasts, "schema payload validation must reject before broadcast")
}

func TestDDLCallbacksSchemaEvolutionPayloadSizeMutationShapes(t *testing.T) {
	newCollection := func() *model.Collection {
		return &model.Collection{
			Name:          "collection",
			Description:   strings.Repeat("collection description", 256),
			SchemaVersion: 7,
			Fields: []*model.Field{
				{
					FieldID:      100,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:    101,
					Name:       "text",
					DataType:   schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}},
					Nullable:   true,
				},
			},
			Properties: []*commonpb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "101"}},
		}
	}

	tests := []struct {
		name   string
		mutate func(*schemapb.CollectionSchema)
	}{
		{
			name: "add struct field",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.StructArrayFields = append(schema.StructArrayFields, &schemapb.StructArrayFieldSchema{
					FieldID:  102,
					Name:     "profile",
					Nullable: true,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.MaxCapacityKey, Value: "16"},
					},
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     103,
							Name:        "profile[values]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int64,
							Nullable:    true,
							TypeParams: []*commonpb.KeyValuePair{
								{Key: common.MaxCapacityKey, Value: "16"},
							},
						},
					},
				})
				schema.Properties = updateMaxFieldIDProperty(schema.Properties, 103)
			},
		},
		{
			name: "alter schema add field",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
					FieldID:  102,
					Name:     "added",
					DataType: schemapb.DataType_Int64,
					Nullable: true,
				})
				schema.Properties = updateMaxFieldIDProperty(schema.Properties, 102)
			},
		},
		{
			name: "enable dynamic field",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.EnableDynamicField = true
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
					FieldID:   102,
					Name:      common.MetaFieldName,
					DataType:  schemapb.DataType_JSON,
					IsDynamic: true,
					Nullable:  true,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BytesData{BytesData: []byte("{}")},
					},
				})
				schema.Properties = updateMaxFieldIDProperty(schema.Properties, 102)
			},
		},
		{
			name: "alter field description",
			mutate: func(schema *schemapb.CollectionSchema) {
				for _, field := range schema.GetFields() {
					if field.GetFieldID() == 101 {
						field.Description = strings.Repeat("updated description", 64)
						return
					}
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldColl := newCollection()
			schema := proto.Clone(oldColl.ToCollectionSchemaPB()).(*schemapb.CollectionSchema)
			schema.Version = oldColl.SchemaVersion + 1
			test.mutate(schema)
			actual := proto.Size(schema)
			require.Less(t, proto.Size(oldColl.ToCollectionSchemaPB()), actual)

			old := paramtable.Get().ProxyCfg.MaxCollectionSchemaSize.SwapTempValue(strconv.Itoa(actual + 1))
			t.Cleanup(func() { paramtable.Get().ProxyCfg.MaxCollectionSchemaSize.SwapTempValue(old) })
			require.NoError(t, validateSchemaEvolution(oldColl, schema), "mutation must be semantically valid")

			paramtable.Get().ProxyCfg.MaxCollectionSchemaSize.SwapTempValue(strconv.Itoa(actual))
			require.ErrorIs(t, validateSchemaEvolution(oldColl, schema), merr.ErrParameterTooLarge)
		})
	}
}

func TestDDLCallbacksSchemaEvolutionRejectsUnsafeAddCollectionFieldBeforeSideEffects(t *testing.T) {
	tests := []struct {
		name            string
		field           *schemapb.FieldSchema
		expectsAnalyzer bool
	}{
		{
			name: "non-nullable field without default",
			field: &schemapb.FieldSchema{
				Name:     "required",
				DataType: schemapb.DataType_Int64,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core := initStreamingSystemAndCore(t)
			ctx := context.Background()
			dbName := "testDB" + funcutil.RandomString(10)
			collectionName := "testCollection" + funcutil.RandomString(10)
			createCollectionForTest(t, ctx, core, dbName, collectionName)

			analyzerCalls := 0
			if test.expectsAnalyzer {
				mixCoord := core.mixCoord.(*mocks.MixCoord)
				mixCoord.EXPECT().ValidateAnalyzer(mock.Anything, mock.Anything).Run(func(context.Context, *querypb.ValidateAnalyzerRequest) {
					analyzerCalls++
				}).Return(&querypb.ValidateAnalyzerResponse{Status: merr.Success()}, nil).Maybe()
			}
			broadcasts := 0
			mockBroadcastAPI := mock_broadcaster.NewMockBroadcastAPI(t)
			mockBroadcastAPI.EXPECT().Close().Return().Maybe()
			mockBroadcastAPI.EXPECT().Broadcast(mock.Anything, mock.Anything).Run(func(context.Context, message.BroadcastMutableMessage) {
				broadcasts++
			}).Return(&types.BroadcastAppendResult{}, nil).Maybe()
			lockMocker := mockey.Mock((*Core).startBroadcastWithAliasOrCollectionLock).Return(mockBroadcastAPI, nil).Build()
			t.Cleanup(func() { lockMocker.UnPatch() })

			schemaBytes, err := proto.Marshal(test.field)
			require.NoError(t, err)
			resp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
				DbName:         dbName,
				CollectionName: collectionName,
				Schema:         schemaBytes,
			})
			require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
			assertSchemaVersion(t, ctx, core, dbName, collectionName, 0)
			assertFieldNotExists(t, ctx, core, dbName, collectionName, test.field.GetName())

			meta := core.meta.(*MetaTable)
			require.Empty(t, meta.fileResourceRefCnt, "validation must reject before analyzer resource reservation")
			require.Zero(t, analyzerCalls, "validation must reject before ValidateAnalyzer")
			require.Zero(t, broadcasts, "validation must reject before broadcast")
		})
	}
}

func TestDDLCallbacksSchemaEvolutionRejectsInPlaceFieldMutation(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)

	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	schemaBytes, err := proto.Marshal(&schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}}},
		},
	})
	require.NoError(t, err)
	resp, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{DbName: dbName, CollectionName: collectionName, Schema: schemaBytes})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	// Resizing max_length (grow or shrink) is allowed; removing the bound
	// entirely is still rejected, since it would drop the write-time bound.
	resp, err = core.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      "text",
		DeleteKeys:     []string{common.MaxLengthKey},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 0)
	assertFieldProperties(t, ctx, core, dbName, collectionName, "text", common.MaxLengthKey, "128")
}

func TestDDLCallbacksSchemaEvolutionRejectsGraphBreakingAlterCollectionSchemaDrop(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	resp, err := core.AlterCollectionSchema(ctx, buildAlterSchemaAddFieldSchemaReq(dbName, collectionName, &schemapb.FieldSchema{
		Name:     "text_input",
		DataType: schemapb.DataType_VarChar,
		Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "128"},
			{Key: common.EnableAnalyzerKey, Value: "true"},
		},
	}, false))
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	resp, err = core.AlterCollectionSchema(ctx, buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_output", "bm25"))
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2)

	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
				DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
					Identifier: &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName{FieldName: "text_input"},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2)
	assertFieldExists(t, ctx, core, dbName, collectionName, "text_input", 101)
}

func TestDDLCallbacksSchemaEvolutionRejectsUnsafeDynamicEnable(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	meta := core.meta.(*MetaTable)
	coll, err := meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	meta.ddLock.Lock()
	meta.collID2Meta[coll.CollectionID].Fields = append(meta.collID2Meta[coll.CollectionID].Fields, &model.Field{
		FieldID:   101,
		Name:      common.MetaFieldName,
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
		Nullable:  true,
	})
	meta.ddLock.Unlock()

	resp, err := core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: common.EnableDynamicSchemaKey, Value: "true"}},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 0)
}

func TestDDLCallbacksPropertyOnlyAlterSkipsSchemaEvolutionValidation(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	meta := core.meta.(*MetaTable)
	coll, err := meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	meta.ddLock.Lock()
	meta.collID2Meta[coll.CollectionID].Fields[0].IsFunctionOutput = true
	meta.ddLock.Unlock()

	resp, err := core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties:     []*commonpb.KeyValuePair{{Key: "property_only", Value: "updated"}},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
}
