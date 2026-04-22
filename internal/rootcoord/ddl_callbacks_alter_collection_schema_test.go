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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	imocks "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// buildAlterSchemaReq constructs a valid AlterCollectionSchemaRequest with an add operation.
func buildAlterSchemaReq(dbName, collName, inputField, outputField, funcName string, doBackfill bool) *milvuspb.AlterCollectionSchemaRequest {
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
					FuncSchema:         []*schemapb.FunctionSchema{functionSchema},
					DoPhysicalBackfill: doBackfill,
				},
			},
		},
	}
}

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

	// case 3: funcSchemas empty (len != 1)
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{Name: "sparse1", DataType: schemapb.DataType_SparseFloatVector}},
					},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 4: fieldInfos empty
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{Name: "fn1", Type: schemapb.FunctionType_BM25},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 4.1: physical backfill with non-BM25 function must be rejected.
	// Otherwise the backfill task would fail at datanode with "unsupported function type"
	// and permanently block subsequent schema-change DDLs via the consistency gate.
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{
							Name:             "minhash_fn",
							Type:             schemapb.FunctionType_MinHash,
							InputFieldNames:  []string{"text_input"},
							OutputFieldNames: []string{"sparse_minhash"},
						},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{
							Name:             "sparse_minhash",
							DataType:         schemapb.DataType_SparseFloatVector,
							IsFunctionOutput: true,
						}},
					},
					DoPhysicalBackfill: true,
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)
	require.Contains(t, resp.GetAlterStatus().GetReason(), "physical backfill is currently only supported for BM25 functions")

	// case 4.2: non-BM25 function with DoPhysicalBackfill=false should NOT be rejected by
	// the type check (it may still fail later for other reasons, but the type check must pass).
	// This path never invokes the backfill_compactor, so "unsupported type" cannot be triggered.
	// We don't assert success here because downstream validation may reject for unrelated
	// reasons in this minimal test setup — we only assert that the error, if any, is not the
	// physical-backfill type rejection.
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{
							Name:             "minhash_fn_nophys",
							Type:             schemapb.FunctionType_MinHash,
							InputFieldNames:  []string{"text_input"},
							OutputFieldNames: []string{"sparse_minhash2"},
						},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: &schemapb.FieldSchema{
							Name:             "sparse_minhash2",
							DataType:         schemapb.DataType_SparseFloatVector,
							IsFunctionOutput: true,
						}},
					},
					DoPhysicalBackfill: false,
				},
			},
		},
	})
	// Whatever happens, it must not be the BM25-only rejection.
	if err := merr.CheckRPCCall(resp.GetAlterStatus(), err); err != nil {
		require.NotContains(t, resp.GetAlterStatus().GetReason(),
			"physical backfill is currently only supported for BM25 functions",
			"non-BM25 with DoPhysicalBackfill=false must not be rejected by the BM25-only type check")
	}

	// case 5: fieldSchema nil in fieldInfos
	resp, err = core.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
				AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
					FuncSchema: []*schemapb.FunctionSchema{
						{Name: "fn1", Type: schemapb.FunctionType_BM25},
					},
					FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{
						{FieldSchema: nil},
					},
				},
			},
		},
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp.GetAlterStatus(), err), merr.ErrParameterInvalid)

	// case 6: output field already exists (field1 created by createCollectionForTest)
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

	// Add a VARCHAR input field so that the happy-path call below can succeed.
	varcharFieldSchema := &schemapb.FieldSchema{
		Name:     "text_input",
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "256"},
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

	// happy path: add sparse vector output field + BM25 function → schema version bumps (AddCollectionField already bumped to 1, so now 2)
	firstAlterReq := buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_output", "bm25_fn", false)
	resp, err = core.AlterCollectionSchema(ctx, firstAlterReq)
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 2)

	// happy path with DoPhysicalBackfill=true: flag propagates through broadcast, schema version bumps to 3
	secondAlterReq := buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_output2", "bm25_fn2", true)
	resp, err = core.AlterCollectionSchema(ctx, secondAlterReq)
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 3)

	// case 7: function already exists (same name "bm25_fn")
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

	// case 8: input field not found
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

	// case 9: output field not found (function references "ghost_output" but FieldInfos has "sparse_output4")
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

	// drop field3 successfully
	resp, err = core.AlterCollectionSchema(ctx, dropFieldReq("field3"))
	require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	assertFieldNotExists(t, ctx, core, dbName, collectionName, "field3")
	assertFieldExists(t, ctx, core, dbName, collectionName, "field1", 100)
	assertFieldExists(t, ctx, core, dbName, collectionName, "field4", 103)
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 5)
	assertMaxFieldIDProperty(t, ctx, core, dbName, collectionName, 103)
}

func assertFieldNotExists(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, fieldName string) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	for _, field := range coll.Fields {
		if field.Name == fieldName {
			require.Fail(t, "field should not exist", "field %s still exists", fieldName)
		}
	}
}

func assertMaxFieldIDProperty(t *testing.T, ctx context.Context, core *Core, dbName string, collectionName string, expectedMaxFieldID int64) {
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
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

func TestBuildSchemaForDropFunction(t *testing.T) {
	t.Run("function not found", func(t *testing.T) {
		coll := &model.Collection{
			Functions: []*model.Function{
				{Name: "func1", OutputFieldIDs: []int64{103}},
			},
		}
		_, _, _, err := buildSchemaForDropFunction(coll, "nonexistent")
		require.Error(t, err)
		require.Contains(t, err.Error(), "function not found")
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
					InputFieldIDs:    []int64{101},
					InputFieldNames:  []string{"text"},
					OutputFieldIDs:   []int64{103},
					OutputFieldNames: []string{"embedding_vec"},
				},
			},
			SchemaVersion: 5,
		}

		schema, properties, droppedFieldIds, err := buildSchemaForDropFunction(coll, "embedding_func")
		require.NoError(t, err)

		// output field removed
		require.Equal(t, 3, len(schema.Fields))
		for _, f := range schema.Fields {
			require.NotEqual(t, "embedding_vec", f.Name)
		}

		// function removed
		require.Equal(t, 0, len(schema.Functions))

		// droppedFieldIds contains output field
		require.Equal(t, []int64{103}, droppedFieldIds)

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
					InputFieldIDs:    []int64{101},
					OutputFieldIDs:   []int64{102},
					OutputFieldNames: []string{"sparse_vec"},
				},
				{
					Name:             "embed_func",
					InputFieldIDs:    []int64{101},
					OutputFieldIDs:   []int64{103},
					OutputFieldNames: []string{"dense_vec"},
				},
			},
			SchemaVersion: 3,
		}

		schema, _, droppedFieldIds, err := buildSchemaForDropFunction(coll, "bm25_func")
		require.NoError(t, err)

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

		require.Equal(t, []int64{102}, droppedFieldIds)
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
		schema, properties, droppedFieldIds, err := buildSchemaForDropField(baseColl(), "extra", 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(schema.Fields))
		require.Equal(t, []int64{102}, droppedFieldIds)
		require.NotNil(t, properties)
		require.Equal(t, int32(4), schema.Version)
	})

	t.Run("drop by field id", func(t *testing.T) {
		schema, _, droppedFieldIds, err := buildSchemaForDropField(baseColl(), "", 101)
		require.NoError(t, err)
		require.Equal(t, 2, len(schema.Fields))
		require.Equal(t, []int64{101}, droppedFieldIds)
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
		schema, _, droppedFieldIds, err := buildSchemaForDropField(collWithStruct(), "paragraphs", 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(schema.Fields))            // pk + vec unchanged
		require.Equal(t, 0, len(schema.StructArrayFields)) // struct removed
		require.ElementsMatch(t, []int64{102, 103, 104}, droppedFieldIds)
		require.Equal(t, int32(4), schema.Version)
	})

	t.Run("drop whole struct array field by id", func(t *testing.T) {
		schema, _, droppedFieldIds, err := buildSchemaForDropField(collWithStruct(), "", 102)
		require.NoError(t, err)
		require.Equal(t, 0, len(schema.StructArrayFields))
		require.ElementsMatch(t, []int64{102, 103, 104}, droppedFieldIds)
	})
}

func TestCascadeDropFieldIndexesInline(t *testing.T) {
	buildResult := func(droppedFieldIDs []int64) message.BroadcastResultAlterCollectionMessageV2 {
		raw := message.NewAlterCollectionMessageBuilderV2().
			WithHeader(&messagespb.AlterCollectionMessageHeader{
				CollectionId: 1,
			}).
			WithBody(&messagespb.AlterCollectionMessageBody{
				Updates: &messagespb.AlterCollectionMessageUpdates{
					DroppedFieldIds: droppedFieldIDs,
				},
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
