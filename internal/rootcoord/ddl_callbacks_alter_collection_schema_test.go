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
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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

// addVarcharInputField adds a VARCHAR "text_input" field to the given collection so
// that subsequent AlterCollectionSchema calls in consistency-gate tests can reference
// it as a BM25 function input. Must be called BEFORE installMixCoordWithStats because
// the AddCollectionField broadcast also uses the mixCoord.
func addVarcharInputField(t *testing.T, ctx context.Context, core *Core, dbName, collectionName string) {
	fieldBytes, err := proto.Marshal(&schemapb.FieldSchema{
		Name:     "text_input",
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "256"},
		},
	})
	require.NoError(t, err)
	resp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         fieldBytes,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
}

// installMixCoordWithStats replaces the core's mixCoord with a valid mock that returns
// the given consistency stats from GetCollectionStatistics. All other default
// expectations are preserved so the broadcast path still works.
func installMixCoordWithStats(core *Core, stats []*commonpb.KeyValuePair) {
	mixc := &mocks.MixCoord{}
	mixc.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(
		&milvuspb.ComponentStates{
			State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
			Status: merr.Success(),
		}, nil).Maybe()
	mixc.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
	mixc.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
	mixc.EXPECT().WatchChannels(mock.Anything, mock.Anything).Return(&datapb.WatchChannelsResponse{Status: merr.Success()}, nil).Maybe()
	mixc.EXPECT().Flush(mock.Anything, mock.Anything).Return(&datapb.FlushResponse{Status: merr.Success()}, nil).Maybe()
	mixc.EXPECT().BroadcastAlteredCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
	mixc.EXPECT().GetCollectionStatistics(mock.Anything, mock.Anything).Return(
		&datapb.GetCollectionStatisticsResponse{
			Status: merr.Success(),
			Stats:  stats,
		}, nil).Maybe()
	core.mixCoord = mixc
}

// TestDDLCallbacksBroadcastAlterCollectionSchemaConsistencyGate verifies the
// schema-version consistency re-check added at RootCoord. Cluster-wide mutual
// exclusion between schema-change DDLs is provided by the existing collection
// resource key lock in startBroadcastWithAliasOrCollectionLock — this gate only
// adds the "wait for previous backfill to complete" check on top of it.
func TestDDLCallbacksBroadcastAlterCollectionSchemaConsistencyGate(t *testing.T) {
	t.Run("rejects when consistent_count < total_count", func(t *testing.T) {
		core := initStreamingSystemAndCore(t)

		ctx := context.Background()
		dbName := "testDB" + funcutil.RandomString(10)
		collectionName := "testCollection" + funcutil.RandomString(10)
		createCollectionForTest(t, ctx, core, dbName, collectionName)

		installMixCoordWithStats(core, []*commonpb.KeyValuePair{
			{Key: common.SchemaVersionConsistentSegmentsKey, Value: "5"},
			{Key: common.SchemaVersionTotalSegmentsKey, Value: "10"},
		})

		resp, err := core.AlterCollectionSchema(ctx,
			buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_inc", "bm25_inc", false))
		require.Error(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
		require.Contains(t, resp.GetAlterStatus().GetReason(), "schema version consistency check failed")
		require.Contains(t, resp.GetAlterStatus().GetReason(), "retry after backfill completes")
	})

	t.Run("passes when consistent_count == total_count", func(t *testing.T) {
		core := initStreamingSystemAndCore(t)

		ctx := context.Background()
		dbName := "testDB" + funcutil.RandomString(10)
		collectionName := "testCollection" + funcutil.RandomString(10)
		createCollectionForTest(t, ctx, core, dbName, collectionName)
		addVarcharInputField(t, ctx, core, dbName, collectionName)

		installMixCoordWithStats(core, []*commonpb.KeyValuePair{
			{Key: common.SchemaVersionConsistentSegmentsKey, Value: "10"},
			{Key: common.SchemaVersionTotalSegmentsKey, Value: "10"},
		})

		resp, err := core.AlterCollectionSchema(ctx,
			buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_ok", "bm25_ok", false))
		require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	})

	t.Run("absent keys treated as trivially consistent", func(t *testing.T) {
		// Both keys absent → schema version has never been bumped, no backfill
		// can be in progress, so the gate must pass.
		core := initStreamingSystemAndCore(t)

		ctx := context.Background()
		dbName := "testDB" + funcutil.RandomString(10)
		collectionName := "testCollection" + funcutil.RandomString(10)
		createCollectionForTest(t, ctx, core, dbName, collectionName)
		addVarcharInputField(t, ctx, core, dbName, collectionName)

		installMixCoordWithStats(core, nil)

		resp, err := core.AlterCollectionSchema(ctx,
			buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_noop", "bm25_noop", false))
		require.NoError(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	})

	t.Run("invalid consistent_segments value is rejected", func(t *testing.T) {
		core := initStreamingSystemAndCore(t)

		ctx := context.Background()
		dbName := "testDB" + funcutil.RandomString(10)
		collectionName := "testCollection" + funcutil.RandomString(10)
		createCollectionForTest(t, ctx, core, dbName, collectionName)

		installMixCoordWithStats(core, []*commonpb.KeyValuePair{
			{Key: common.SchemaVersionConsistentSegmentsKey, Value: "not-a-number"},
			{Key: common.SchemaVersionTotalSegmentsKey, Value: "10"},
		})

		resp, err := core.AlterCollectionSchema(ctx,
			buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_bad", "bm25_bad", false))
		require.Error(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
		require.Contains(t, resp.GetAlterStatus().GetReason(), "invalid schema_version_consistent_segments value")
	})

	t.Run("only one of two stat keys present is treated as corruption", func(t *testing.T) {
		// Defensive branch: DataCoord is supposed to emit both keys atomically.
		// If only one is present, treat as a data-corruption signal and block the DDL.
		core := initStreamingSystemAndCore(t)

		ctx := context.Background()
		dbName := "testDB" + funcutil.RandomString(10)
		collectionName := "testCollection" + funcutil.RandomString(10)
		createCollectionForTest(t, ctx, core, dbName, collectionName)

		installMixCoordWithStats(core, []*commonpb.KeyValuePair{
			{Key: common.SchemaVersionConsistentSegmentsKey, Value: "5"},
			// SchemaVersionTotalSegmentsKey intentionally omitted
		})

		resp, err := core.AlterCollectionSchema(ctx,
			buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_half", "bm25_half", false))
		require.Error(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
		require.Contains(t, resp.GetAlterStatus().GetReason(), "incomplete schema version consistency stats")
	})

	t.Run("DataCoord RPC failure is surfaced", func(t *testing.T) {
		core := initStreamingSystemAndCore(t)

		ctx := context.Background()
		dbName := "testDB" + funcutil.RandomString(10)
		collectionName := "testCollection" + funcutil.RandomString(10)
		createCollectionForTest(t, ctx, core, dbName, collectionName)

		// Install a mock whose GetCollectionStatistics returns an error. Other
		// methods need default expectations so the broadcast path still works
		// up to the gate.
		mixc := &mocks.MixCoord{}
		mixc.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(
			&milvuspb.ComponentStates{
				State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
				Status: merr.Success(),
			}, nil).Maybe()
		mixc.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
		mixc.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
		mixc.EXPECT().WatchChannels(mock.Anything, mock.Anything).Return(&datapb.WatchChannelsResponse{Status: merr.Success()}, nil).Maybe()
		mixc.EXPECT().Flush(mock.Anything, mock.Anything).Return(&datapb.FlushResponse{Status: merr.Success()}, nil).Maybe()
		mixc.EXPECT().BroadcastAlteredCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
		mixc.EXPECT().GetCollectionStatistics(mock.Anything, mock.Anything).Return(
			nil, errors.New("mock datacoord error"),
		).Maybe()
		core.mixCoord = mixc

		resp, err := core.AlterCollectionSchema(ctx,
			buildAlterSchemaReq(dbName, collectionName, "text_input", "sparse_rpcerr", "bm25_rpcerr", false))
		require.Error(t, merr.CheckRPCCall(resp.GetAlterStatus(), err))
	})

	// NOTE: AddCollectionField does NOT yet run the consistency gate (it's deferred
	// until the backfill compaction series is merged — enabling the gate before
	// backfill can complete would block E2E tests). See the TODO in
	// broadcastAlterCollectionForAddField. A test for that path will be added
	// together with the call itself in the follow-up PR.
}

