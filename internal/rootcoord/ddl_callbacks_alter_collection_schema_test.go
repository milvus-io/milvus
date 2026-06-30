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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	imocks "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/common"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// buildAlterSchemaReq constructs a valid AlterCollectionSchemaRequest with an add operation.
func buildAlterSchemaReq(dbName, collName, inputField, outputField, funcName string) *milvuspb.AlterCollectionSchemaRequest {
	outputFieldSchema := &schemapb.FieldSchema{
		Name:             outputField,
		DataType:         schemapb.DataType_SparseFloatVector,
		IsFunctionOutput: true,
	}
	functionSchema := &schemapb.FunctionSchema{
		Name:             funcName,
		Type:             schemapb.FunctionType_BM25,
		InputFieldNames:  []string{inputField},
		OutputFieldNames: []string{outputField},
	}
	return &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: outputFieldSchema},
					},
					FuncSchema: []*schemapb.FunctionSchema{functionSchema},
				},
			},
		},
	}
}

func buildAlterSchemaAddFieldReq(dbName, collName, fieldName string, doBackfill bool) *milvuspb.AlterCollectionSchemaRequest {
	return buildAlterSchemaAddFieldSchemaReq(dbName, collName, &schemapb.FieldSchema{
		Name:     fieldName,
		DataType: schemapb.DataType_VarChar,
		Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "128"},
		},
	}, doBackfill)
}

func buildAlterSchemaAddFieldSchemaReq(dbName, collName string, fieldSchema *schemapb.FieldSchema, doBackfill bool) *milvuspb.AlterCollectionSchemaRequest {
	return &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: fieldSchema},
					},
					DoPhysicalBackfill: doBackfill,
				},
			},
		},
	}
}

func buildAlterSchemaAddFunctionReq(dbName, collName string, functionSchema *schemapb.FunctionSchema) *milvuspb.AlterCollectionSchemaRequest {
	return &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{functionSchema},
				},
			},
		},
	}
}

type captureBroadcastAPI struct {
	msg message.BroadcastMutableMessage
	err error
}

func (b *captureBroadcastAPI) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	b.msg = msg
	if b.err != nil {
		return nil, b.err
	}
	return &types.BroadcastAppendResult{}, nil
}

func (b *captureBroadcastAPI) Close() {}

func TestDDLCallbacksBroadcastAlterCollectionSchema(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)

	createCollectionForTest(t, ctx, core, dbName, collectionName)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 0)

	// case 1: action == nil
	resp, err := core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action:         nil,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 2: add_request == nil (action present but no Op set)
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action:         &milvuspb.AlterCollectionSchemaRequest_Action{},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 3: add a plain field without funcSchema.
	resp, err = core.AlterCollectionSchema(ctx, buildAlterSchemaAddFieldReq(dbName, collectionName, "plain_text", false))
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Len(t, coll.Functions, 0)
	plainFieldFound := false
	for _, field := range coll.Fields {
		if field.Name == "plain_text" {
			plainFieldFound = true
			require.False(t, field.IsFunctionOutput)
		}
	}
	require.True(t, plainFieldFound)

	// case 3.1: DoPhysicalBackfill is ignored by alter schema.
	resp, err = core.AlterCollectionSchema(ctx, buildAlterSchemaAddFieldReq(dbName, collectionName, "plain_text_backfill", true))
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2)
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.False(t, coll.ToCollectionSchemaPB().GetDoPhysicalBackfill())

	status, err := core.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.TimezoneKey, Value: "Asia/Shanghai"},
		},
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// case 3.2: field-only TIMESTAMPTZ add rewrites string default value with collection timezone.
	defaultTimeString := "2024-01-02T03:04:05"
	resp, err = core.AlterCollectionSchema(ctx, buildAlterSchemaAddFieldSchemaReq(dbName, collectionName, &schemapb.FieldSchema{
		Name:     "created_at_tz",
		DataType: schemapb.DataType_Timestamptz,
		Nullable: true,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{StringData: defaultTimeString},
		},
	}, false))
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 3)
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	expectedTimestamptzDefault, err := timestamptz.ValidateAndReturnUnixMicroTz(defaultTimeString, "Asia/Shanghai")
	require.NoError(t, err)
	timestamptzDefaultFound := false
	for _, field := range coll.Fields {
		if field.Name == "created_at_tz" {
			timestamptzDefaultFound = true
			require.Equal(t, expectedTimestamptzDefault, field.DefaultValue.GetTimestamptzData())
			require.Empty(t, field.DefaultValue.GetStringData())
		}
	}
	require.True(t, timestamptzDefaultFound)

	// case 3.3: field-only TIMESTAMPTZ add rejects invalid string default value.
	resp, err = core.AlterCollectionSchema(ctx, buildAlterSchemaAddFieldSchemaReq(dbName, collectionName, &schemapb.FieldSchema{
		Name:     "invalid_created_at_tz",
		DataType: schemapb.DataType_Timestamptz,
		Nullable: true,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{StringData: "not-a-timestamp"},
		},
	}, false))
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 3.4: multiple function schemas remain unsupported.
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{Name: "fn_multi_1", Type: schemapb.FunctionType_BM25},
						{Name: "fn_multi_2", Type: schemapb.FunctionType_BM25},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{Name: "sparse1", DataType: schemapb.DataType_SparseFloatVector}},
					},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 3.5: nil function schema is rejected.
	resp, err = core.AlterCollectionSchema(ctx, buildAlterSchemaAddFunctionReq(dbName, collectionName, nil))
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)
	require.Contains(t, resp.GetAlterStatus().GetReason(), "function schema is nil")

	// case 3.6: multiple fieldInfos remain unsupported.
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{Name: "plain_text_multi_1", DataType: schemapb.DataType_Int64}},
						{FieldSchema: &schemapb.FieldSchema{Name: "plain_text_multi_2", DataType: schemapb.DataType_Int64}},
					},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 3.7: invalid field schema is rejected by RootCoord schema checks.
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{
							Name:         "invalid_primary",
							DataType:     schemapb.DataType_Int64,
							IsPrimaryKey: true,
							Nullable:     true,
						}},
					},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 4: both fieldInfos and funcSchema are empty.
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 4.1: BM25 function must add its output field in the same request.
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{Name: "fn1", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"field1"}, OutputFieldNames: []string{"sparse1"}},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)
	require.Contains(t, resp.GetAlterStatus().GetReason(), "add_function_field")

	// case 4.2: function-only output field must exist.
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{
							Name:             "minhash_missing_output",
							Type:             schemapb.FunctionType_MinHash,
							InputFieldNames:  []string{"field1"},
							OutputFieldNames: []string{"missing_minhash_output"},
							Params: []*commonpb.KeyValuePair{
								{Key: "num_hashes", Value: "128"},
								{Key: "shingle_size", Value: "3"},
								{Key: "hash_function", Value: "xxhash64"},
								{Key: "seed", Value: "42"},
							},
						},
					},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)
	require.Contains(t, resp.GetAlterStatus().GetReason(), "output field missing_minhash_output")

	// case 4.3: BM25 function with multiple output fields is rejected.
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{
							Name:             "bm25_multi_output",
							Type:             schemapb.FunctionType_BM25,
							InputFieldNames:  []string{"field1"},
							OutputFieldNames: []string{"sparse_multi_output", "sparse_multi_output_extra"},
						},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{
							Name:     "sparse_multi_output",
							DataType: schemapb.DataType_SparseFloatVector,
						}},
					},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)
	require.Contains(t, resp.GetAlterStatus().GetReason(), "exactly one output field")

	// case 5: BM25 arity invalid
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{
							Name:             "fn_bad_arity",
							Type:             schemapb.FunctionType_BM25,
							InputFieldNames:  []string{"field1", "field2", "field3"},
							OutputFieldNames: []string{"sparse_bad_arity"},
						},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{Name: "sparse_bad_arity", DataType: schemapb.DataType_SparseFloatVector}},
					},
				},
			},
		},
	})
	arityErr := merr.CheckRPCCall(resp.GetAlterStatus(), err)
	require.ErrorIs(t, arityErr, merr.ErrParameterInvalid)
	require.ErrorContains(t, arityErr, "exactly one input field and exactly one output field")

	// case 6: fieldSchema nil in fieldInfos
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{Name: "fn1", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"field1"}, OutputFieldNames: []string{"sparse_nil_field"}},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: nil},
					},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 7: output field already exists (field1 created by createCollectionForTest)
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{Name: "fn_dup_field", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"field1"}, OutputFieldNames: []string{"field1"}},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{Name: "field1", DataType: schemapb.DataType_SparseFloatVector}},
					},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 8: output field points to an existing field while FieldInfos adds a different field
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{Name: "fn_output_existing_field", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"field1"}, OutputFieldNames: []string{"field1"}},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{Name: "sparse_output_existing_bypass", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true}},
					},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// Add a VARCHAR input field so that the happy-path call below can succeed.
	varcharFieldSchema := &schemapb.FieldSchema{
		Name:     "text_input",
		DataType: schemapb.DataType_VarChar,
		Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "256"},
			{Key: common.EnableAnalyzerKey, Value: "true"},
		},
	}
	varcharBytes, err := proto.Marshal(varcharFieldSchema)
	require.NoError(t, err)
	addFieldResp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         varcharBytes,
	})
	require.NoError(t, merr.CheckRPCCall(addFieldResp, err))

	// happy path: add binary vector output field + MinHash function.
	minHashReq := buildAlterSchemaReq(dbName, collectionName, "text_input", "binary_minhash_output", "minhash_fn")
	minHashFieldSchema := minHashReq.GetAction().GetAddRequest().GetFieldInfos()[0].GetFieldSchema()
	minHashFieldSchema.DataType = schemapb.DataType_BinaryVector
	minHashFieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{Key: common.DimKey, Value: "4096"},
	}
	minHashFunction := minHashReq.GetAction().GetAddRequest().GetFuncSchema()[0]
	minHashFunction.Type = schemapb.FunctionType_MinHash
	minHashFunction.Params = []*commonpb.KeyValuePair{
		{Key: "num_hashes", Value: "128"},
		{Key: "shingle_size", Value: "3"},
		{Key: "hash_function", Value: "xxhash64"},
		{Key: "seed", Value: "42"},
	}
	resp, err = core.AlterCollectionSchema(ctx, minHashReq)
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 5)

	minHashBadArityReq := buildAlterSchemaReq(dbName, collectionName, "text_input", "minhash_bad_arity", "minhash_bad_arity_fn")
	minHashBadArityReq.GetAction().GetAddRequest().GetFuncSchema()[0].Type = schemapb.FunctionType_MinHash
	minHashBadArityReq.GetAction().GetAddRequest().GetFuncSchema()[0].InputFieldNames = []string{"text_input", "field1"}
	minHashBadArityReq.GetAction().GetAddRequest().GetFieldInfos()[0].GetFieldSchema().DataType = schemapb.DataType_BinaryVector
	minHashBadArityReq.GetAction().GetAddRequest().GetFieldInfos()[0].GetFieldSchema().TypeParams = []*commonpb.KeyValuePair{
		{Key: common.DimKey, Value: "4096"},
	}
	resp, err = core.AlterCollectionSchema(ctx, minHashBadArityReq)
	alterErr := merr.CheckRPCCall(resp.GetAlterStatus(), err)
	require.ErrorIs(t, alterErr, merr.ErrParameterInvalid)
	require.ErrorContains(t, alterErr, "MinHash function should have exactly one input field and exactly one output field")

	nullableOutputReq := buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_output_nullable", "bm25_nullable_output")
	nullableOutputReq.GetAction().GetAddRequest().GetFieldInfos()[0].GetFieldSchema().Nullable = true
	resp, err = core.AlterCollectionSchema(ctx, nullableOutputReq)
	alterErr = merr.CheckRPCCall(resp.GetAlterStatus(), err)
	require.ErrorIs(t, alterErr, merr.ErrParameterInvalid)
	require.ErrorContains(t, alterErr, "function output field cannot be nullable")

	existingOutputFieldSchema := &schemapb.FieldSchema{
		Name:     "existing_minhash_output",
		DataType: schemapb.DataType_BinaryVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "4096"},
		},
	}
	existingOutputBytes, err := proto.Marshal(existingOutputFieldSchema)
	require.NoError(t, err)
	addFieldResp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         existingOutputBytes,
	})
	require.NoError(t, merr.CheckRPCCall(addFieldResp, err))

	resp, err = core.AlterCollectionSchema(ctx, buildAlterSchemaAddFunctionReq(dbName, collectionName, &schemapb.FunctionSchema{
		Name:             "minhash_missing_output_late",
		Type:             schemapb.FunctionType_MinHash,
		InputFieldNames:  []string{"text_input"},
		OutputFieldNames: []string{"missing_minhash_output_late"},
		Params: []*commonpb.KeyValuePair{
			{Key: "num_hashes", Value: "128"},
			{Key: "shingle_size", Value: "3"},
			{Key: "hash_function", Value: "xxhash64"},
			{Key: "seed", Value: "42"},
		},
	}))
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)
	require.Contains(t, resp.GetAlterStatus().GetReason(), "output field missing_minhash_output_late")

	// happy path: add only a function and mark an existing output field.
	functionOnlyReq := buildAlterSchemaAddFunctionReq(dbName, collectionName, &schemapb.FunctionSchema{
		Name:             "minhash_existing_fn",
		Type:             schemapb.FunctionType_MinHash,
		InputFieldNames:  []string{"text_input"},
		OutputFieldNames: []string{"existing_minhash_output"},
		Params: []*commonpb.KeyValuePair{
			{Key: "num_hashes", Value: "128"},
			{Key: "shingle_size", Value: "3"},
			{Key: "hash_function", Value: "xxhash64"},
			{Key: "seed", Value: "42"},
		},
	})
	resp, err = core.AlterCollectionSchema(ctx, functionOnlyReq)
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 7)
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Len(t, coll.Functions, 2)
	existingOutputFound := false
	for _, field := range coll.Fields {
		if field.Name == "existing_minhash_output" {
			existingOutputFound = true
			require.True(t, field.IsFunctionOutput)
		}
	}
	require.True(t, existingOutputFound)

	// function-only rejects output fields already owned by another function.
	resp, err = core.AlterCollectionSchema(ctx, buildAlterSchemaAddFunctionReq(dbName, collectionName, &schemapb.FunctionSchema{
		Name:             "minhash_existing_fn2",
		Type:             schemapb.FunctionType_MinHash,
		InputFieldNames:  []string{"text_input"},
		OutputFieldNames: []string{"existing_minhash_output"},
		Params: []*commonpb.KeyValuePair{
			{Key: "num_hashes", Value: "128"},
			{Key: "shingle_size", Value: "3"},
			{Key: "hash_function", Value: "xxhash64"},
			{Key: "seed", Value: "42"},
		},
	}))
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// happy path: add sparse vector output field + BM25 function.
	firstAlterReq := buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_output", "bm25_fn")
	resp, err = core.AlterCollectionSchema(ctx, firstAlterReq)
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 8)

	// second happy path with DoPhysicalBackfill=true: the flag is ignored by alter schema.
	secondAlterReq := buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_output2", "bm25_fn2")
	secondAlterReq.GetAction().GetAddRequest().DoPhysicalBackfill = true
	resp, err = core.AlterCollectionSchema(ctx, secondAlterReq)
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 9)
	updated, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	schema := updated.ToCollectionSchemaPB()
	require.False(t, schema.GetDoPhysicalBackfill())
	require.EqualValues(t, 9, schema.GetVersion())

	// case 9: function already exists (same name "bm25_fn")
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{Name: "bm25_fn", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"text_input"}, OutputFieldNames: []string{"sparse_output2"}},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{Name: "sparse_output2", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true}},
					},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 10: input field not found
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{Name: "fn_missing_input", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"nonexistent_input"}, OutputFieldNames: []string{"sparse_output3"}},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{Name: "sparse_output3", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true}},
					},
				},
			},
		},
	})
	require.Error(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))

	// case 11: output field not found (function references "ghost_output" but FieldInfos has "sparse_output4")
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{Name: "fn_missing_output", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"text_input"}, OutputFieldNames: []string{"ghost_output"}},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{Name: "sparse_output4", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true}},
					},
				},
			},
		},
	})
	require.Error(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
}

func TestDDLCallbacksAlterCollectionSchemaValidatesFunctionOnlyFinalSchema(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)

	createCollectionForTest(t, ctx, core, dbName, collectionName)

	inputFieldBytes, err := proto.Marshal(&schemapb.FieldSchema{
		Name:     "text_input",
		DataType: schemapb.DataType_VarChar,
		Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "256"},
		},
	})
	require.NoError(t, err)
	addFieldResp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         inputFieldBytes,
	})
	require.NoError(t, merr.CheckRPCCall(addFieldResp, err))

	invalidOutputBytes, err := proto.Marshal(&schemapb.FieldSchema{
		Name:     "invalid_minhash_output",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	})
	require.NoError(t, err)
	addFieldResp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         invalidOutputBytes,
	})
	require.NoError(t, merr.CheckRPCCall(addFieldResp, err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2)

	resp, err := core.AlterCollectionSchema(ctx, buildAlterSchemaAddFunctionReq(dbName, collectionName, &schemapb.FunctionSchema{
		Name:             "minhash_invalid_output",
		Type:             schemapb.FunctionType_MinHash,
		InputFieldNames:  []string{"text_input"},
		OutputFieldNames: []string{"invalid_minhash_output"},
		Params: []*commonpb.KeyValuePair{
			{Key: "num_hashes", Value: "128"},
			{Key: "shingle_size", Value: "3"},
			{Key: "hash_function", Value: "xxhash64"},
			{Key: "seed", Value: "42"},
		},
	}))
	alterErr := merr.CheckRPCCall(resp.GetAlterStatus(), err)
	require.ErrorIs(t, alterErr, merr.ErrParameterInvalid)
	require.ErrorContains(t, alterErr, "MinHash function output field must be a BinaryVector field")
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2)
}

func TestDDLCallbacksAlterCollectionSchemaAddRejectsStructFieldNameConflicts(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	resp, err := core.AddCollectionStructField(ctx, &milvuspb.AddCollectionStructFieldRequest{
		DbName:                 dbName,
		CollectionName:         collectionName,
		StructArrayFieldSchema: newRootAddStructFieldSchema("profile"),
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)

	for _, fieldName := range []string{"profile", "profile[ints]", storedRootStructSubFieldName("profile", "profile[ints]")} {
		t.Run(fieldName, func(t *testing.T) {
			resp, err := core.AlterCollectionSchema(ctx, buildAlterSchemaAddFieldReq(dbName, collectionName, fieldName, false))
			require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)
			require.Contains(t, resp.GetAlterStatus().GetReason(), "field already exists")
			assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)
		})
	}
}

func TestBroadcastAlterCollectionSchemaAddExternalMinHash(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "externalDB" + funcutil.RandomString(10)
	collectionName := "externalMinHash" + funcutil.RandomString(10)

	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	db, err := core.meta.GetDatabaseByName(ctx, dbName, typeutil.MaxTimestamp)
	require.NoError(t, err)

	coll := &model.Collection{
		CollectionID:  100,
		DBID:          db.ID,
		DBName:        dbName,
		Name:          collectionName,
		SchemaVersion: 3,
		State:         pb.CollectionState_CollectionCreated,
		Fields: []*model.Field{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "id"},
			{
				FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, Nullable: true, ExternalField: "text",
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}},
			},
			{
				FieldID: 102, Name: "dense", DataType: schemapb.DataType_FloatVector, ExternalField: "dense",
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
			},
		},
		Properties: []*commonpb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "102"}},
	}
	require.NoError(t, core.meta.AddCollection(ctx, coll))

	buildReq := func() *milvuspb.AlterCollectionSchemaRequest {
		return &milvuspb.AlterCollectionSchemaRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Action: &milvuspb.AlterCollectionSchemaRequest_Action{
				Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
					AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
						FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
							{FieldSchema: &schemapb.FieldSchema{
								Name:       "mh",
								DataType:   schemapb.DataType_BinaryVector,
								TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "512"}},
							}},
						},
						FuncSchema: []*schemapb.FunctionSchema{{
							Name:             "minhash_func",
							Type:             schemapb.FunctionType_MinHash,
							InputFieldNames:  []string{"text"},
							OutputFieldNames: []string{"mh"},
						}},
					},
				},
			},
		}
	}

	broadcaster := &captureBroadcastAPI{}
	require.NoError(t, core.broadcastAlterCollectionSchemaAdd(ctx, broadcaster, coll, buildReq()))
	require.NotNil(t, broadcaster.msg)
	alterMsg := message.MustAsBroadcastAlterCollectionMessageV2(broadcaster.msg)
	body := alterMsg.MustBody()
	schema := body.GetUpdates().GetSchema()
	require.NotNil(t, schema)
	require.EqualValues(t, 4, schema.GetVersion())
	require.EqualValues(t, 103, maxAssignedFieldIDFromSchema(schema))

	newField := typeutil.GetFieldByName(schema, "mh")
	require.NotNil(t, newField)
	require.EqualValues(t, 103, newField.GetFieldID())
	require.True(t, newField.GetIsFunctionOutput())
	require.Empty(t, newField.GetExternalField())

	require.Len(t, schema.GetFunctions(), 1)
	require.NotZero(t, schema.GetFunctions()[0].GetId())
	require.Equal(t, []int64{101}, schema.GetFunctions()[0].GetInputFieldIds())
	require.Equal(t, []int64{103}, schema.GetFunctions()[0].GetOutputFieldIds())

	reqWithMapping := buildReq()
	reqWithMapping.GetAction().GetAddRequest().GetFieldInfos()[0].GetFieldSchema().ExternalField = "mh"
	err = core.broadcastAlterCollectionSchemaAdd(ctx, &captureBroadcastAPI{}, coll, reqWithMapping)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorContains(t, err, "must not have external_field")

	reqWithTextEmbedding := buildReq()
	textEmbeddingAdd := reqWithTextEmbedding.GetAction().GetAddRequest()
	textEmbeddingAdd.FieldInfos[0].FieldSchema = &schemapb.FieldSchema{
		Name:     "embedding",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	textEmbeddingAdd.FuncSchema = []*schemapb.FunctionSchema{
		{
			Name:             "embedding_func",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"embedding"},
			Params: []*commonpb.KeyValuePair{
				{Key: "provider", Value: "openai"},
				{Key: "model_name", Value: "text-embedding-ada-002"},
				{Key: "credential", Value: "mock"},
				{Key: "dim", Value: "128"},
			},
		},
	}
	broadcaster = &captureBroadcastAPI{}
	require.NoError(t, core.broadcastAlterCollectionSchemaAdd(ctx, broadcaster, coll, reqWithTextEmbedding))
	require.NotNil(t, broadcaster.msg)
	alterMsg = message.MustAsBroadcastAlterCollectionMessageV2(broadcaster.msg)
	schema = alterMsg.MustBody().GetUpdates().GetSchema()
	newField = typeutil.GetFieldByName(schema, "embedding")
	require.NotNil(t, newField)
	require.EqualValues(t, 103, newField.GetFieldID())
	require.True(t, newField.GetIsFunctionOutput())
	require.Empty(t, newField.GetExternalField())
	require.Len(t, schema.GetFunctions(), 1)
	require.Equal(t, []int64{101}, schema.GetFunctions()[0].GetInputFieldIds())
	require.Equal(t, []int64{103}, schema.GetFunctions()[0].GetOutputFieldIds())
}

func TestBroadcastAlterCollectionSchemaAddExternalField(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "externalDB" + funcutil.RandomString(10)
	collectionName := "externalAddField" + funcutil.RandomString(10)

	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	db, err := core.meta.GetDatabaseByName(ctx, dbName, typeutil.MaxTimestamp)
	require.NoError(t, err)

	coll := &model.Collection{
		CollectionID:  100,
		DBID:          db.ID,
		DBName:        dbName,
		Name:          collectionName,
		SchemaVersion: 3,
		State:         pb.CollectionState_CollectionCreated,
		Fields: []*model.Field{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "id"},
			{
				FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text",
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}},
			},
			{
				FieldID: 102, Name: "dense", DataType: schemapb.DataType_FloatVector, ExternalField: "dense",
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
			},
		},
		Properties: []*commonpb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "102"}},
	}
	require.NoError(t, core.meta.AddCollection(ctx, coll))

	baseField := func() *schemapb.FieldSchema {
		return &schemapb.FieldSchema{
			Name:          "category",
			DataType:      schemapb.DataType_VarChar,
			Nullable:      true,
			ExternalField: "category",
			TypeParams:    []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}},
		}
	}
	baseReq := func(field *schemapb.FieldSchema) *milvuspb.AlterCollectionSchemaRequest {
		return &milvuspb.AlterCollectionSchemaRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Action: &milvuspb.AlterCollectionSchemaRequest_Action{
				Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
					AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
						FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
							{FieldSchema: field},
						},
					},
				},
			},
		}
	}

	broadcaster := &captureBroadcastAPI{}
	require.NoError(t, core.broadcastAlterCollectionSchemaAdd(ctx, broadcaster, coll, baseReq(baseField())))
	require.NotNil(t, broadcaster.msg)
	alterMsg := message.MustAsBroadcastAlterCollectionMessageV2(broadcaster.msg)
	body := alterMsg.MustBody()
	schema := body.GetUpdates().GetSchema()
	require.NotNil(t, schema)
	require.EqualValues(t, 4, schema.GetVersion())
	require.EqualValues(t, 103, maxAssignedFieldIDFromSchema(schema))
	require.Empty(t, schema.GetFunctions())

	newField := typeutil.GetFieldByName(schema, "category")
	require.NotNil(t, newField)
	require.EqualValues(t, 103, newField.GetFieldID())
	require.False(t, newField.GetIsFunctionOutput())
	require.Equal(t, "category", newField.GetExternalField())
	require.True(t, newField.GetNullable())

	for _, tc := range []struct {
		name         string
		mutate       func(*milvuspb.AlterCollectionSchemaRequest, *schemapb.FieldSchema)
		broadcastErr error
		wantErr      string
	}{
		{
			name: "reject duplicate field name",
			mutate: func(req *milvuspb.AlterCollectionSchemaRequest, field *schemapb.FieldSchema) {
				field.Name = "text"
			},
			wantErr: "field already exists",
		},
		{
			name: "reject duplicate external mapping",
			mutate: func(req *milvuspb.AlterCollectionSchemaRequest, field *schemapb.FieldSchema) {
				field.ExternalField = "text"
			},
			wantErr: "mapped by multiple fields",
		},
		{
			name:         "reject broadcast failure",
			broadcastErr: errors.New("broadcast error"),
			wantErr:      "broadcast error",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := baseField()
			req := baseReq(field)
			if tc.mutate != nil {
				tc.mutate(req, field)
			}

			err := core.broadcastAlterCollectionSchemaAdd(ctx, &captureBroadcastAPI{err: tc.broadcastErr}, coll, req)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestDDLCallbacksAlterCollectionDropField(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)

	// database not found
	resp, err := core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
				DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
					Identifier: &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName{FieldName: "field1"},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrDatabaseNotFound)

	// create collection with field1
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	// add field2 and field3
	addResp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getFieldSchema("field2"),
	})
	require.NoError(t, merr.CheckRPCCall(addResp, err))
	assertFieldExists(t, ctx, core, dbName, collectionName, "field2", 101)

	addResp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getFieldSchema("field3"),
	})
	require.NoError(t, merr.CheckRPCCall(addResp, err))
	assertFieldExists(t, ctx, core, dbName, collectionName, "field3", 102)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2)

	// helper to build drop-field request
	dropFieldReq := func(fieldName string) *milvuspb.AlterCollectionSchemaRequest {
		return &milvuspb.AlterCollectionSchemaRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Action: &milvuspb.AlterCollectionSchemaRequest_Action{
				Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
					DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
						Identifier: &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName{FieldName: fieldName},
					},
				},
			},
		}
	}

	// field not found
	resp, err = core.AlterCollectionSchema(ctx, dropFieldReq("nonexistent"))
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// drop field2 successfully
	resp, err = core.AlterCollectionSchema(ctx, dropFieldReq("field2"))
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertFieldNotExists(t, ctx, core, dbName, collectionName, "field2")
	assertFieldExists(t, ctx, core, dbName, collectionName, "field1", 100)
	assertFieldExists(t, ctx, core, dbName, collectionName, "field3", 102)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 3)
	assertMaxFieldIDProperty(t, ctx, core, dbName, collectionName, 102)

	// add field4 after drop: fieldID should not reuse 101 (the dropped field2's ID)
	addResp, err = core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getFieldSchema("field4"),
	})
	require.NoError(t, merr.CheckRPCCall(addResp, err))
	assertFieldExists(t, ctx, core, dbName, collectionName, "field4", 103)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 4)
	assertMaxFieldIDProperty(t, ctx, core, dbName, collectionName, 103)

	// drop field3 successfully
	resp, err = core.AlterCollectionSchema(ctx, dropFieldReq("field3"))
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertFieldNotExists(t, ctx, core, dbName, collectionName, "field3")
	assertFieldExists(t, ctx, core, dbName, collectionName, "field1", 100)
	assertFieldExists(t, ctx, core, dbName, collectionName, "field4", 103)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 5)
	assertMaxFieldIDProperty(t, ctx, core, dbName, collectionName, 103)
}

func TestDDLCallbacksAlterCollectionDropFieldWaitsForSchemaDropReady(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)

	createCollectionForTest(t, ctx, core, dbName, collectionName)
	addResp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getFieldSchema("field2"),
	})
	require.NoError(t, merr.CheckRPCCall(addResp, err))
	assertFieldExists(t, ctx, core, dbName, collectionName, "field2", 101)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)

	barrierErr := errors.New("proxy version barrier")
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WaitUntilSchemaDropReady(mock.Anything).Return(barrierErr).Once()
	b.EXPECT().Close().Return().Maybe()
	balance.ResetBalancer()
	balance.Register(b)

	resp, err := core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
				DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
					Identifier: &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName{FieldName: "field2"},
				},
			},
		},
	})
	require.Error(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	require.Contains(t, resp.GetAlterStatus().GetDetail(), "failed to wait until schema drop ready")
	assertFieldExists(t, ctx, core, dbName, collectionName, "field2", 101)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)
}

func TestDDLCallbacksAlterCollectionSchemaAddSkipsSchemaDropReady(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)

	createCollectionForTest(t, ctx, core, dbName, collectionName)
	varcharFieldSchema := &schemapb.FieldSchema{
		Name:     "text_input",
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "256"},
			{Key: common.EnableAnalyzerKey, Value: "true"},
		},
	}
	varcharBytes, err := proto.Marshal(varcharFieldSchema)
	require.NoError(t, err)
	addFieldResp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         varcharBytes,
	})
	require.NoError(t, merr.CheckRPCCall(addFieldResp, err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 1)

	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().Close().Return().Maybe()
	balance.ResetBalancer()
	balance.Register(b)

	resp, err := core.AlterCollectionSchema(ctx, buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_output", "bm25_fn"))
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2)
}

func assertFieldNotExists(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, fieldName string) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	for _, field := range coll.Fields {
		if field.Name == fieldName {
			require.Fail(t, "field should not exist", "field %s still exists", fieldName)
		}
	}
}

func assertMaxFieldIDProperty(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, expectedMaxFieldID int64) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	for _, kv := range coll.Properties {
		if kv.Key == common.MaxFieldIDKey {
			require.Equal(t, expectedMaxFieldID, mustParseInt64(kv.Value))
			return
		}
	}
	require.Fail(t, "max_field_id property not found")
}

func mustParseInt64(s string) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func TestBuildSchemaForDetachFunction(t *testing.T) {
	t.Run("function not found", func(t *testing.T) {
		coll := &model.Collection{
			Functions: []*model.Function{
				{Name: "func1", OutputFieldIDs: []int64{103}},
			},
		}
		_, _, _, err := buildSchemaForDetachFunction(coll, "nonexistent")
		require.Error(t, err)
		require.Contains(t, err.Error(), "function not found")
	})

	t.Run("detach function keeps output fields", func(t *testing.T) {
		coll := &model.Collection{
			Name: "test_coll",
			Fields: []*model.Field{
				{FieldID: 100, Name: "pk"},
				{FieldID: 101, Name: "text"},
				{FieldID: 102, Name: "minhash_vec", IsFunctionOutput: true},
				{FieldID: 103, Name: "dense_vec"},
			},
			Functions: []*model.Function{
				{
					Name:             "minhash_func",
					Type:             schemapb.FunctionType_MinHash,
					InputFieldIDs:    []int64{101},
					InputFieldNames:  []string{"text"},
					OutputFieldIDs:   []int64{102},
					OutputFieldNames: []string{"minhash_vec"},
				},
				{
					Name:             "embed_func",
					Type:             schemapb.FunctionType_TextEmbedding,
					InputFieldIDs:    []int64{101},
					InputFieldNames:  []string{"text"},
					OutputFieldIDs:   []int64{103},
					OutputFieldNames: []string{"dense_vec"},
				},
			},
			Properties: []*commonpb.KeyValuePair{
				{Key: common.MaxFieldIDKey, Value: "103"},
			},
			SchemaVersion: 3,
		}

		schema, properties, droppedFieldIDs, err := buildSchemaForDetachFunction(coll, "minhash_func")
		require.NoError(t, err)
		require.Empty(t, droppedFieldIDs)
		require.Equal(t, coll.Properties, properties)
		require.Equal(t, int32(4), schema.Version)

		require.Len(t, schema.Fields, 4)
		var minhashField *schemapb.FieldSchema
		for _, field := range schema.Fields {
			if field.GetName() == "minhash_vec" {
				minhashField = field
				break
			}
		}
		require.NotNil(t, minhashField)
		require.False(t, minhashField.GetIsFunctionOutput())
		require.Len(t, schema.Functions, 1)
		require.Equal(t, "embed_func", schema.Functions[0].GetName())
	})

	t.Run("detach bm25 function fails", func(t *testing.T) {
		coll := &model.Collection{
			Functions: []*model.Function{
				{Name: "bm25_func", Type: schemapb.FunctionType_BM25, OutputFieldIDs: []int64{102}},
			},
		}
		_, _, _, err := buildSchemaForDetachFunction(coll, "bm25_func")
		require.Error(t, err)
		require.Contains(t, err.Error(), "BM25 function must be dropped with its output field")
	})
}

func TestBuildSchemaForDropFunctionField(t *testing.T) {
	t.Run("function not found", func(t *testing.T) {
		coll := &model.Collection{
			Functions: []*model.Function{
				{Name: "func1", OutputFieldIDs: []int64{103}},
			},
		}
		_, _, _, err := buildSchemaForDropFunctionField(coll, "nonexistent")
		require.Error(t, err)
		require.Contains(t, err.Error(), "function not found")
	})

	t.Run("unsupported function type", func(t *testing.T) {
		coll := &model.Collection{
			Functions: []*model.Function{
				{Name: "embed_func", Type: schemapb.FunctionType_TextEmbedding, OutputFieldIDs: []int64{103}},
			},
		}
		_, _, _, err := buildSchemaForDropFunctionField(coll, "embed_func")
		require.Error(t, err)
		require.Contains(t, err.Error(), "only BM25 and MinHash functions support dropping output fields")
	})

	t.Run("external text embedding drop removes function and output field", func(t *testing.T) {
		coll := &model.Collection{
			Name: "test_external_coll",
			Fields: []*model.Field{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, ExternalField: "pk"},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
				{FieldID: 102, Name: "dense_vec", DataType: schemapb.DataType_FloatVector},
				{FieldID: 103, Name: "seed_vec", DataType: schemapb.DataType_SparseFloatVector, ExternalField: "seed_vec"},
			},
			Functions: []*model.Function{
				{
					Name:             "embed_func",
					Type:             schemapb.FunctionType_TextEmbedding,
					InputFieldIDs:    []int64{101},
					InputFieldNames:  []string{"text"},
					OutputFieldIDs:   []int64{102},
					OutputFieldNames: []string{"dense_vec"},
				},
			},
			SchemaVersion: 7,
		}

		schema, _, droppedFieldIDs, err := buildSchemaForDropFunctionField(coll, "embed_func")
		require.NoError(t, err)
		require.Equal(t, []int64{102}, droppedFieldIDs)
		require.Equal(t, int32(8), schema.GetVersion())
		require.Nil(t, typeutil.GetFieldByName(schema, "dense_vec"))
		require.Empty(t, schema.GetFunctions())
		require.NotNil(t, typeutil.GetFieldByName(schema, "seed_vec"))
	})

	t.Run("drop function removes function and output fields", func(t *testing.T) {
		coll := &model.Collection{
			Name: "test_coll",
			Fields: []*model.Field{
				{FieldID: 100, Name: "pk"},
				{FieldID: 101, Name: "text"},
				{FieldID: 102, Name: "vec"},
				{FieldID: 103, Name: "embedding_vec"},
			},
			Functions: []*model.Function{
				{
					Name:             "embedding_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldIDs:    []int64{101},
					InputFieldNames:  []string{"text"},
					OutputFieldIDs:   []int64{103},
					OutputFieldNames: []string{"embedding_vec"},
				},
			},
			SchemaVersion: 5,
		}

		schema, properties, droppedFieldIDs, err := buildSchemaForDropFunctionField(coll, "embedding_func")
		require.NoError(t, err)
		require.Equal(t, []int64{103}, droppedFieldIDs)

		// output field removed
		require.Equal(t, 3, len(schema.Fields))
		for _, f := range schema.Fields {
			require.NotEqual(t, "embedding_vec", f.Name)
		}

		// function removed
		require.Equal(t, 0, len(schema.Functions))

		// max_field_id updated
		require.NotNil(t, properties)

		// schema version incremented
		require.Equal(t, int32(6), schema.Version)
	})

	t.Run("preserves input fields and other functions", func(t *testing.T) {
		coll := &model.Collection{
			Name: "test_coll",
			Fields: []*model.Field{
				{FieldID: 100, Name: "pk"},
				{FieldID: 101, Name: "text"},
				{FieldID: 102, Name: "sparse_vec"},
				{FieldID: 103, Name: "dense_vec"},
			},
			Functions: []*model.Function{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldIDs:    []int64{101},
					OutputFieldIDs:   []int64{102},
					OutputFieldNames: []string{"sparse_vec"},
				},
				{
					Name:             "embed_func",
					Type:             schemapb.FunctionType_TextEmbedding,
					InputFieldIDs:    []int64{101},
					OutputFieldIDs:   []int64{103},
					OutputFieldNames: []string{"dense_vec"},
				},
			},
			SchemaVersion: 3,
		}

		schema, _, droppedFieldIDs, err := buildSchemaForDropFunctionField(coll, "bm25_func")
		require.NoError(t, err)
		require.Equal(t, []int64{102}, droppedFieldIDs)

		// only sparse_vec removed, text and dense_vec preserved
		fieldNames := make([]string, 0, len(schema.Fields))
		for _, f := range schema.Fields {
			fieldNames = append(fieldNames, f.Name)
		}
		require.Contains(t, fieldNames, "pk")
		require.Contains(t, fieldNames, "text")
		require.Contains(t, fieldNames, "dense_vec")
		require.NotContains(t, fieldNames, "sparse_vec")

		// only bm25_func removed, embed_func preserved
		require.Equal(t, 1, len(schema.Functions))
		require.Equal(t, "embed_func", schema.Functions[0].Name)
	})
}

func TestBuildSchemaForDropField(t *testing.T) {
	baseColl := func() *model.Collection {
		return &model.Collection{
			Name: "test_coll",
			Fields: []*model.Field{
				{FieldID: 100, Name: "pk"},
				{FieldID: 101, Name: "vec"},
				{FieldID: 102, Name: "extra"},
			},
			SchemaVersion: 3,
		}
	}

	t.Run("drop by field name", func(t *testing.T) {
		schema, properties, droppedFieldIDs, err := buildSchemaForDropField(baseColl(), "extra", 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(schema.Fields))
		require.NotNil(t, properties)
		require.Equal(t, []int64{102}, droppedFieldIDs)
		require.Equal(t, int32(4), schema.Version)
	})

	t.Run("drop by field id", func(t *testing.T) {
		schema, _, droppedFieldIDs, err := buildSchemaForDropField(baseColl(), "", 101)
		require.NoError(t, err)
		require.Equal(t, []int64{101}, droppedFieldIDs)
		require.Equal(t, 2, len(schema.Fields))
		// remaining fields should be pk and extra
		fieldNames := make([]string, 0, len(schema.Fields))
		for _, f := range schema.Fields {
			fieldNames = append(fieldNames, f.Name)
		}
		require.Contains(t, fieldNames, "pk")
		require.Contains(t, fieldNames, "extra")
	})

	t.Run("field not found by name", func(t *testing.T) {
		_, _, _, err := buildSchemaForDropField(baseColl(), "nonexistent", 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "field not found: nonexistent")
	})

	t.Run("field not found by id", func(t *testing.T) {
		_, _, _, err := buildSchemaForDropField(baseColl(), "", 999)
		require.Error(t, err)
		require.Contains(t, err.Error(), "field not found with id: 999")
	})

	t.Run("max_field_id property updated", func(t *testing.T) {
		coll := baseColl()
		coll.Properties = []*commonpb.KeyValuePair{
			{Key: common.MaxFieldIDKey, Value: "102"},
		}
		_, properties, _, err := buildSchemaForDropField(coll, "extra", 0)
		require.NoError(t, err)
		var found bool
		for _, kv := range properties {
			if kv.Key == common.MaxFieldIDKey {
				require.Equal(t, "102", kv.Value)
				found = true
			}
		}
		require.True(t, found)
	})

	collWithStruct := func() *model.Collection {
		return &model.Collection{
			Name: "test_coll",
			Fields: []*model.Field{
				{FieldID: 100, Name: "pk"},
				{FieldID: 101, Name: "vec"},
			},
			StructArrayFields: []*model.StructArrayField{
				{
					FieldID: 102, Name: "paragraphs",
					Fields: []*model.Field{
						{FieldID: 103, Name: "para_text"},
						{FieldID: 104, Name: "para_embed"},
					},
				},
			},
			SchemaVersion: 3,
		}
	}

	t.Run("drop whole struct array field by name", func(t *testing.T) {
		schema, _, droppedFieldIDs, err := buildSchemaForDropField(collWithStruct(), "paragraphs", 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(schema.Fields))            // pk + vec unchanged
		require.Equal(t, 0, len(schema.StructArrayFields)) // struct removed
		require.Equal(t, []int64{102, 103, 104}, droppedFieldIDs)
		require.Equal(t, int32(4), schema.Version)
	})

	t.Run("drop whole struct array field by id", func(t *testing.T) {
		schema, _, droppedFieldIDs, err := buildSchemaForDropField(collWithStruct(), "", 102)
		require.NoError(t, err)
		require.Equal(t, 0, len(schema.StructArrayFields))
		require.Equal(t, []int64{102, 103, 104}, droppedFieldIDs)
	})
}

func TestCascadeDropFieldIndexesInline(t *testing.T) {
	buildResult := func(droppedFieldIDs []int64) message.BroadcastResultAlterCollectionMessageV2 {
		raw := message.NewAlterCollectionMessageBuilderV2().
			WithHeader(&messagespb.AlterCollectionMessageHeader{
				CollectionId:    1,
				DroppedFieldIds: droppedFieldIDs,
			}).
			WithBody(&messagespb.AlterCollectionMessageBody{
				Updates: &messagespb.AlterCollectionMessageUpdates{},
			}).
			WithBroadcast([]string{funcutil.GetControlChannel("test")}).
			MustBuildBroadcast()
		msg := message.MustAsBroadcastAlterCollectionMessageV2(raw)
		return message.BroadcastResultAlterCollectionMessageV2{
			Message: msg,
			Results: map[string]*message.AppendResult{},
		}
	}

	t.Run("no dropped fields short circuits", func(t *testing.T) {
		c := newTestCore()
		cb := &DDLCallback{Core: c}
		err := cb.cascadeDropFieldIndexesInline(context.Background(), buildResult(nil))
		require.NoError(t, err)
	})

	t.Run("DescribeIndex returns ErrIndexNotFound", func(t *testing.T) {
		mixc := imocks.NewMixCoord(t)
		mixc.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(
			&indexpb.DescribeIndexResponse{Status: merr.Status(merr.WrapErrIndexNotFound("idx"))}, nil,
		)
		c := newTestCore(withMixCoord(mixc))
		cb := &DDLCallback{Core: c}
		err := cb.cascadeDropFieldIndexesInline(context.Background(), buildResult([]int64{101}))
		require.NoError(t, err)
	})

	t.Run("DescribeIndex returns other error", func(t *testing.T) {
		mixc := imocks.NewMixCoord(t)
		mixc.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(
			nil, errors.New("rpc unavailable"),
		)
		c := newTestCore(withMixCoord(mixc))
		cb := &DDLCallback{Core: c}
		err := cb.cascadeDropFieldIndexesInline(context.Background(), buildResult([]int64{101}))
		require.Error(t, err)
		// Match the stable prefix of the DescribeIndex failure path. The
		// "for cascade drop" suffix is incidental and may be reworded without
		// changing behavior, so we don't assert on it.
		require.Contains(t, err.Error(), "failed to describe indexes")
		require.ErrorContains(t, err, "rpc unavailable")
	})

	t.Run("no matching indexes for dropped field", func(t *testing.T) {
		mixc := imocks.NewMixCoord(t)
		mixc.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(
			&indexpb.DescribeIndexResponse{
				Status: merr.Success(),
				IndexInfos: []*indexpb.IndexInfo{
					{FieldID: 200, IndexID: 1, IndexName: "idx_other"},
				},
			}, nil,
		)
		c := newTestCore(withMixCoord(mixc))
		cb := &DDLCallback{Core: c}
		err := cb.cascadeDropFieldIndexesInline(context.Background(), buildResult([]int64{101}))
		require.NoError(t, err)
	})
}

func TestAlterCollectionV2AckCallbackUsesHeaderDroppedFieldIDs(t *testing.T) {
	raw := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			CollectionId: 1,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{message.FieldMaskCollectionSchema},
			},
			CacheExpirations: &messagespb.CacheExpirations{},
			DroppedFieldIds:  []int64{101},
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Schema: &schemapb.CollectionSchema{
					Name:    "test",
					Version: 2,
				},
			},
		}).
		WithBroadcast([]string{funcutil.GetControlChannel("test")}).
		MustBuildBroadcast()

	meta := &mockMetaTable{}
	meta.AlterCollectionFunc = func(ctx context.Context, result message.BroadcastResultAlterCollectionMessageV2) error {
		return nil
	}

	mixc := imocks.NewMixCoord(t)
	mixc.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(
		&indexpb.DescribeIndexResponse{
			Status: merr.Success(),
			IndexInfos: []*indexpb.IndexInfo{
				{FieldID: 200, IndexID: 1, IndexName: "idx_other"},
			},
		}, nil,
	)

	broker := newValidMockBroker()
	c := newTestCore(withMeta(meta), withMixCoord(mixc), withBroker(broker))
	cb := &DDLCallback{Core: c}
	err := cb.alterCollectionV2AckCallback(context.Background(), message.BroadcastResultAlterCollectionMessageV2{
		Message: message.MustAsBroadcastAlterCollectionMessageV2(raw),
		Results: map[string]*message.AppendResult{
			funcutil.GetControlChannel("test"): {TimeTick: 100},
		},
	})
	require.NoError(t, err)
}
