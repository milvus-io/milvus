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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

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

	// The function-add above leaves an add gate open until backfill reaches this version; drive that
	// release so the drop reaches schema validation instead of the gate's in-flight admission.
	core.dataViewGate.releaseCompletedAddGates(ctx)

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
