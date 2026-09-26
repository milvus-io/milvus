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

package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/rls"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func newRLSOperationTestSchema(collectionName string) *schemaInfo {
	return mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "value",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  102,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "2"},
				},
			},
		},
	})
}

func installRLSOperationTestCache(t *testing.T, collectionID int64, schema *schemaInfo, rlsForce ...bool) Cache {
	force := len(rlsForce) > 0 && rlsForce[0]
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(collectionID, nil).Maybe()
	cache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
		CollID:     collectionID,
		Schema:     schema,
		RlsEnabled: true,
		RlsForce:   force,
	}, nil).Maybe()
	cache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schema, nil).Maybe()
	cache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{DBID: 1}, nil).Maybe()
	cache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&partitionInfo{
		Name:        paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		PartitionID: 10,
	}, nil).Maybe()
	cache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{
		paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(): 10,
	}, nil).Maybe()
	return cache
}

func refreshRLSOperationTestMetadata(t *testing.T, collectionID int64, policies []*rlsutil.RowPolicy) {
	ctx := context.Background()
	rls.InvalidatePolicies(collectionID, 0)
	t.Cleanup(func() {
		rls.InvalidatePolicies(collectionID, 0)
	})

	coord := mocks.NewMockMixCoordClient(t)
	coord.EXPECT().GetRLSMetadata(mock.Anything, mock.Anything).Return(&rootcoordpb.GetRLSMetadataResponse{
		Status:       merr.Success(),
		CollectionId: collectionID,
		Policies:     rlsPolicyInfos(collectionID, policies),
	}, nil).Once()
	require.NoError(t, rls.Init(ctx, coord))
}

func rlsPolicyInfos(collectionID int64, policies []*rlsutil.RowPolicy) []*rootcoordpb.RLSPolicyInfo {
	converted := make([]*rootcoordpb.RLSPolicyInfo, 0, len(policies))
	for _, policy := range policies {
		if policy == nil {
			continue
		}
		actions := make([]milvuspb.RowPolicyAction, len(policy.Actions))
		for i, action := range policy.Actions {
			actions[i] = milvuspb.RowPolicyAction(action)
		}
		converted = append(converted, &rootcoordpb.RLSPolicyInfo{
			CollectionId: collectionID,
			PolicyId:     int64(len(converted) + 1),
			PolicyName:   policy.PolicyName,
			PolicyType:   milvuspb.RowPolicyType(policy.PolicyType),
			Actions:      actions,
			UsingExpr:    policy.UsingExpr,
			CheckExpr:    policy.CheckExpr,
			Description:  policy.Description,
		})
	}
	return converted
}

func newRLSOperationTestAllocator(t *testing.T) *allocator.IDAllocator {
	ctx := context.Background()
	coord := mocks.NewMockRootCoordClient(t)
	coord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
		Status: merr.Success(),
		ID:     1000,
		Count:  100,
	}, nil).Maybe()
	idAllocator, err := allocator.NewIDAllocator(ctx, coord, paramtable.GetNodeID())
	require.NoError(t, err)
	idAllocator.Start()
	t.Cleanup(idAllocator.Close)
	return idAllocator
}

func newRLSOperationTestFieldsData() []*schemapb.FieldData {
	return []*schemapb.FieldData{
		{
			FieldName: "id",
			FieldId:   100,
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1}},
					},
				},
			},
		},
		{
			FieldName: "value",
			FieldId:   101,
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{10}},
					},
				},
			},
		},
		{
			FieldName: "vector",
			FieldId:   102,
			Type:      schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 2,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{Data: []float32{0.1, 0.2}},
					},
				},
			},
		},
	}
}

func newRLSOperationSearchParams() []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{Key: AnnsFieldKey, Value: "vector"},
		{Key: TopKKey, Value: "10"},
		{Key: common.MetricTypeKey, Value: "L2"},
		{Key: ParamsKey, Value: `{"nprobe": 10}`},
		{Key: RoundDecimalKey, Value: "-1"},
		{Key: IgnoreGrowingKey, Value: "false"},
	}
}

func TestRLSOperationsUseSchemaFromPinnedCollectionInfo(t *testing.T) {
	const (
		aliasName     = "rls_schema_alias"
		canonicalName = "rls_schema_collection"
	)
	ctx := context.Background()
	pinnedSchema := newRLSOperationTestSchema(canonicalName)
	foreignCollectionSchema := proto.Clone(pinnedSchema.CollectionSchema).(*schemapb.CollectionSchema)
	foreignCollectionSchema.Name = "foreign_collection"
	foreignCollectionSchema.Fields[1].Name = "foreign_value"
	foreignSchema := mustNewSchemaInfo(foreignCollectionSchema)

	newCache := func(t *testing.T, collectionID int64) (*MockCache, *int) {
		cache := NewMockCache(t)
		nameSchemaLoads := 0
		cache.EXPECT().GetCollectionID(mock.Anything, "default", aliasName).Return(collectionID, nil).Maybe()
		cache.EXPECT().GetCollectionInfo(mock.Anything, "default", aliasName, collectionID).Return(&collectionInfo{
			CollID:     collectionID,
			Schema:     pinnedSchema,
			RlsEnabled: true,
		}, nil).Maybe()
		cache.EXPECT().GetCollectionSchema(mock.Anything, "default", aliasName).
			Run(func(context.Context, string, string) { nameSchemaLoads++ }).
			Return(foreignSchema, nil).Maybe()
		cache.EXPECT().GetDatabaseInfo(mock.Anything, "default").Return(&databaseInfo{DBID: 1}, nil).Maybe()
		cache.EXPECT().GetPartitions(mock.Anything, "default", aliasName).Return(map[string]int64{
			paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(): 10,
		}, nil).Maybe()
		return cache, &nameSchemaLoads
	}

	t.Run("query", func(t *testing.T) {
		const collectionID = int64(993001)
		cache, nameSchemaLoads := newCache(t, collectionID)
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{{
			PolicyName: "query_policy",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
			UsingExpr:  "value == 10",
		}})
		task := NewQueryTask(ctx, &Proxy{metaCache: cache}, &milvuspb.QueryRequest{
			DbName:         "default",
			CollectionName: aliasName,
			Expr:           "id > 0",
			RlsPrincipal:   "alice",
		}, nil, &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}}, cache, false)
		require.NoError(t, task.OnEnqueue())
		require.NoError(t, task.PreExecute(ctx))
		require.Zero(t, *nameSchemaLoads)
	})

	t.Run("search", func(t *testing.T) {
		const collectionID = int64(993002)
		cache, nameSchemaLoads := newCache(t, collectionID)
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{{
			PolicyName: "search_policy",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionSearch},
			UsingExpr:  "value == 10",
		}})
		placeholderGroup, err := proto.Marshal(constructPlaceholderGroup(1, 2))
		require.NoError(t, err)
		task := NewSearchTask(ctx, &Proxy{metaCache: cache}, nil, &milvuspb.SearchRequest{
			DbName:         "default",
			CollectionName: aliasName,
			Nq:             1,
			Dsl:            "id > 0",
			DslType:        commonpb.DslType_BoolExprV1,
			SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{
				PlaceholderGroup: placeholderGroup,
			},
			SearchParams: newRLSOperationSearchParams(),
			RlsPrincipal: "alice",
		}, false, false, timerecord.NewTimeRecorder("rls-schema-search-test"))
		require.NoError(t, task.OnEnqueue())
		require.NoError(t, task.PreExecute(ctx))
		require.Zero(t, *nameSchemaLoads)
	})

	t.Run("insert", func(t *testing.T) {
		const collectionID = int64(993003)
		cache, nameSchemaLoads := newCache(t, collectionID)
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{{
			PolicyName: "insert_policy",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionInsert},
			CheckExpr:  "value == 10",
		}})
		task := &insertTask{
			baseTask:  baseTask{MetaCache: cache},
			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			insertMsg: &BaseInsertTask{InsertRequest: &msgpb.InsertRequest{
				Base:           &commonpb.MsgBase{},
				DbName:         "default",
				CollectionName: aliasName,
				PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
				FieldsData:     newRLSOperationTestFieldsData(),
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			}},
			idAllocator:  newRLSOperationTestAllocator(t),
			rlsPrincipal: "alice",
		}
		require.NoError(t, task.OnEnqueue())
		require.NoError(t, task.PreExecute(ctx))
		require.Same(t, pinnedSchema.CollectionSchema, task.schema)
		require.Zero(t, *nameSchemaLoads)
	})

	t.Run("delete", func(t *testing.T) {
		const collectionID = int64(993004)
		cache, nameSchemaLoads := newCache(t, collectionID)
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{{
			PolicyName: "delete_policy",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionDelete},
			UsingExpr:  "value == 10",
		}})
		chMgr := channelmgr.NewMockChannelsMgr(t)
		chMgr.EXPECT().GetVChannels(collectionID).Return([]string{"vchan1"}, nil)
		runner := &deleteRunner{
			metaCache: cache,
			chMgr:     chMgr,
			req: &milvuspb.DeleteRequest{
				DbName:         "default",
				CollectionName: aliasName,
				Expr:           "id == 1",
				RlsPrincipal:   "alice",
			},
		}
		require.NoError(t, runner.Init(ctx))
		require.Same(t, pinnedSchema, runner.schema)
		require.Zero(t, *nameSchemaLoads)
	})
}

func TestQueryTaskRLSEnforcement(t *testing.T) {
	const collectionID = int64(991001)
	const collectionName = "rls_query_collection"
	ctx := context.Background()
	schema := newRLSOperationTestSchema(collectionName)
	cache := installRLSOperationTestCache(t, collectionID, schema)
	node := &Proxy{metaCache: cache}

	newTask := func(t *testing.T, principalName string) *queryTask {
		task := NewQueryTask(ctx, node, &milvuspb.QueryRequest{
			CollectionName: collectionName,
			Expr:           "id > 0",
			RlsPrincipal:   principalName,
		}, nil, &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{},
		}, cache, false)
		require.NoError(t, task.OnEnqueue())
		return task
	}

	t.Run("missing principal", func(t *testing.T) {
		err := newTask(t, "").PreExecute(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	})

	t.Run("merge using predicate", func(t *testing.T) {
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{
			{
				PolicyName: "query_policy",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "id == 1",
			},
		})
		task := newTask(t, "alice")
		require.NoError(t, task.PreExecute(ctx))
		plan := &planpb.PlanNode{}
		require.NoError(t, proto.Unmarshal(task.GetSerializedExprPlan(), plan))
		predicate := plan.GetQuery().GetPredicates()
		require.NotNil(t, predicate)
		expected, err := planparserv2.ParseExpr(schema.SchemaHelper, "id > 0 and id == 1", nil)
		require.NoError(t, err)
		require.True(t, proto.Equal(expected, predicate), "unexpected merged predicate: %s", predicate.String())
	})

	t.Run("empty user expression still requires limit", func(t *testing.T) {
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{
			{
				PolicyName: "query_policy",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "id == 1",
			},
		})
		task := newTask(t, "alice")
		task.Request().Expr = ""
		err := task.PreExecute(ctx)
		require.ErrorContains(t, err, "empty expression should be used with limit")
	})
}

func TestSearchTaskRLSEnforcementRequiresPrincipal(t *testing.T) {
	const collectionID = int64(991002)
	const collectionName = "rls_search_collection"
	ctx := context.Background()
	cache := installRLSOperationTestCache(t, collectionID, newRLSOperationTestSchema(collectionName))
	node := &Proxy{metaCache: cache}
	task := NewSearchTask(ctx, node, nil, &milvuspb.SearchRequest{
		CollectionName: collectionName,
		Nq:             1,
		Dsl:            "id > 0",
		DslType:        commonpb.DslType_BoolExprV1,
		SearchParams:   newRLSOperationSearchParams(),
	}, false, false, timerecord.NewTimeRecorder("rls-search-test"))
	require.NoError(t, task.OnEnqueue())

	err := task.PreExecute(ctx)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
}

func TestDeleteRunnerRLSEnforcementRequiresPrincipal(t *testing.T) {
	const collectionID = int64(991003)
	const collectionName = "rls_delete_collection"
	ctx := context.Background()
	cache := installRLSOperationTestCache(t, collectionID, newRLSOperationTestSchema(collectionName))

	runner := &deleteRunner{
		metaCache: cache,
		req: &milvuspb.DeleteRequest{
			DbName:         "default",
			CollectionName: collectionName,
			Expr:           "id == 1",
		},
	}

	err := runner.Init(ctx)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
}

func TestInsertTaskRLSEnforcement(t *testing.T) {
	const collectionID = int64(991004)
	const collectionName = "rls_insert_collection"
	ctx := context.Background()
	schema := newRLSOperationTestSchema(collectionName)
	cache := installRLSOperationTestCache(t, collectionID, schema)

	newTask := func(t *testing.T, principalName string) *insertTask {
		task := &insertTask{
			baseTask:  baseTask{MetaCache: cache},
			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base:           &commonpb.MsgBase{},
					DbName:         "default",
					CollectionName: collectionName,
					PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
					FieldsData:     newRLSOperationTestFieldsData(),
					NumRows:        1,
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},
			idAllocator:  newRLSOperationTestAllocator(t),
			rlsPrincipal: principalName,
		}
		require.NoError(t, task.OnEnqueue())
		return task
	}

	t.Run("missing principal", func(t *testing.T) {
		called := false
		patch := mockey.Mock(genFunctionFields).To(func(context.Context, *msgstream.InsertMsg, *schemaInfo, bool) error {
			called = true
			return nil
		}).Build()
		defer patch.UnPatch()

		err := newTask(t, "").PreExecute(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		require.False(t, called)
	})

	t.Run("reject row violating check predicate", func(t *testing.T) {
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{
			{
				PolicyName: "insert_policy",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionInsert},
				CheckExpr:  "id == 2",
			},
		})
		called := false
		patch := mockey.Mock(genFunctionFields).To(func(context.Context, *msgstream.InsertMsg, *schemaInfo, bool) error {
			called = true
			rls.InvalidatePolicies(collectionID, 0)
			coord := mocks.NewMockMixCoordClient(t)
			coord.EXPECT().GetRLSMetadata(mock.Anything, mock.Anything).Return(&rootcoordpb.GetRLSMetadataResponse{
				Status:       merr.Success(),
				CollectionId: collectionID,
				Policies: rlsPolicyInfos(collectionID, []*rlsutil.RowPolicy{{
					PolicyName: "replacement",
					PolicyType: rlsutil.PolicyTypePermissive,
					Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionInsert},
					CheckExpr:  "id == 1",
				}}),
			}, nil).Maybe()
			require.NoError(t, rls.Init(ctx, coord))
			return nil
		}).Build()
		defer patch.UnPatch()

		err := newTask(t, "alice").PreExecute(ctx)
		require.Truef(t, called, "expected function generation before RLS row check, got %v", err)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	})

	t.Run("reject static false before function generation", func(t *testing.T) {
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{{
			PolicyName: "insert_policy",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionInsert},
			CheckExpr:  "false",
		}})
		called := false
		patch := mockey.Mock(genFunctionFields).To(func(context.Context, *msgstream.InsertMsg, *schemaInfo, bool) error {
			called = true
			return nil
		}).Build()
		defer patch.UnPatch()

		err := newTask(t, "alice").PreExecute(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		require.False(t, called)
	})
}

func TestUpsertTaskRLSEnforcementRequiresPrincipal(t *testing.T) {
	const collectionID = int64(991005)
	ctx := context.Background()
	task := createTestUpdateTask()
	task.baseTask.MetaCache = installRLSOperationTestCache(t, collectionID, task.schema)
	require.NoError(t, task.OnEnqueue())

	err := task.PreExecute(ctx)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
}

func TestUpsertUsingPolicyOnlyAppliesToExistingRows(t *testing.T) {
	newTask := func(collectionID int64) *upsertTask {
		task := createTestUpdateTask()
		task.collectionID = collectionID
		task.rlsEnabled = true
		task.req.RlsPrincipal = "alice"
		task.upsertMsg = &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{
				FieldsData: task.req.GetFieldsData(),
				NumRows:    uint64(task.req.GetNumRows()),
			}},
			DeleteMsg: &msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{
				NumRows: int64(task.req.GetNumRows()),
			}},
		}
		return task
	}

	mockey.PatchConvey("upsert with only new rows does not evaluate the using policy", t, func() {
		const collectionID = int64(991006)
		ctx := context.Background()
		task := newTask(collectionID)

		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{{
			PolicyName: "upsert_check",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionUpsert},
			UsingExpr:  "false",
			CheckExpr:  "true",
		}})
		mockey.Mock(retrieveByPKs).Return(&milvuspb.QueryResults{Status: merr.Success()}, segcore.StorageCost{}, nil).Build()

		missingRows, err := task.queryPreExecute(ctx)
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 2}, missingRows)
	})

	mockey.PatchConvey("always-true using policy preserves direct full upsert", t, func() {
		const collectionID = int64(991007)
		task := newTask(collectionID)
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{{
			PolicyName: "upsert_allow_all",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionUpsert},
			UsingExpr:  "true",
			CheckExpr:  "true",
		}})
		readCalls := 0
		mockey.Mock(retrieveByPKs).To(func(context.Context, *upsertTask, *schemapb.IDs, []string) (*milvuspb.QueryResults, segcore.StorageCost, error) {
			readCalls++
			return nil, segcore.StorageCost{}, nil
		}).Build()
		functionCalls := 0
		mockey.Mock(genFunctionFields).To(func(context.Context, *msgstream.InsertMsg, *schemaInfo, bool) error {
			functionCalls++
			return nil
		}).Build()

		require.NoError(t, task.prepareUpsert(context.Background()))
		require.Zero(t, readCalls)
		require.Equal(t, 1, functionCalls)
		require.Equal(t, []int64{1, 2, 3}, task.upsertMsg.DeleteMsg.GetPrimaryKeys().GetIntId().GetData())
		require.EqualValues(t, 3, task.upsertMsg.DeleteMsg.GetNumRows())
		require.Equal(t, task.req.GetFieldsData(), task.upsertMsg.InsertMsg.GetFieldsData())
	})

	mockey.PatchConvey("using denial precedes function generation", t, func() {
		const collectionID = int64(991009)
		ctx := context.Background()
		task := newTask(collectionID)
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{{
			PolicyName: "upsert_deny_existing",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionUpsert},
			UsingExpr:  "false",
			CheckExpr:  "true",
		}})
		mockey.Mock(retrieveByPKs).Return(&milvuspb.QueryResults{
			Status:     merr.Success(),
			FieldsData: []*schemapb.FieldData{proto.Clone(task.req.GetFieldsData()[0]).(*schemapb.FieldData)},
		}, segcore.StorageCost{}, nil).Build()
		functionCalls := 0
		mockey.Mock(genFunctionFields).To(func(context.Context, *msgstream.InsertMsg, *schemaInfo, bool) error {
			functionCalls++
			return nil
		}).Build()

		err := task.prepareUpsert(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		require.Zero(t, functionCalls)
	})

	mockey.PatchConvey("static check denial precedes read and function generation", t, func() {
		const collectionID = int64(991010)
		task := newTask(collectionID)
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{{
			PolicyName: "upsert_deny_write",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionUpsert},
			UsingExpr:  "true",
			CheckExpr:  "false",
		}})
		readCalls := 0
		mockey.Mock(retrieveByPKs).To(func(context.Context, *upsertTask, *schemapb.IDs, []string) (*milvuspb.QueryResults, segcore.StorageCost, error) {
			readCalls++
			return nil, segcore.StorageCost{}, nil
		}).Build()
		functionCalls := 0
		mockey.Mock(genFunctionFields).To(func(context.Context, *msgstream.InsertMsg, *schemaInfo, bool) error {
			functionCalls++
			return nil
		}).Build()

		err := task.prepareUpsert(context.Background())
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		require.Zero(t, readCalls)
		require.Zero(t, functionCalls)
	})
}

func TestUpsertPinsUsingAndCheckToOneSnapshot(t *testing.T) {
	mockey.PatchConvey("policy update between upsert read and write cannot mix predicates", t, func() {
		const collectionID = int64(991008)
		ctx := context.Background()
		schema := newRLSOperationTestSchema("rls_upsert_snapshot")
		fields := newRLSOperationTestFieldsData()
		task := &upsertTask{
			Condition:    NewTaskCondition(ctx),
			ctx:          ctx,
			collectionID: collectionID,
			rlsEnabled:   true,
			schema:       schema,
			idAllocator:  newRLSOperationTestAllocator(t),
			result:       &milvuspb.MutationResult{},
			req: &milvuspb.UpsertRequest{
				DbName:         "default",
				CollectionName: schema.CollectionSchema.GetName(),
				PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
				FieldsData:     fields,
				NumRows:        1,
				RlsPrincipal:   "alice",
			},
			upsertMsg: &msgstream.UpsertMsg{
				InsertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{
					CollectionName: schema.CollectionSchema.GetName(),
					PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
					FieldsData:     fields,
					NumRows:        1,
					Version:        msgpb.InsertDataVersion_ColumnBased,
				}},
				DeleteMsg: &msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{}},
			},
		}

		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{{
			PolicyName: "old",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionUpsert},
			UsingExpr:  "id == 1",
			CheckExpr:  "id == -1",
		}})
		mockey.Mock(retrieveByPKs).To(func(context.Context, *upsertTask, *schemapb.IDs, []string) (*milvuspb.QueryResults, segcore.StorageCost, error) {
			rls.InvalidatePolicies(collectionID, 0)
			coord := mocks.NewMockMixCoordClient(t)
			coord.EXPECT().GetRLSMetadata(mock.Anything, mock.Anything).Return(&rootcoordpb.GetRLSMetadataResponse{
				Status:       merr.Success(),
				CollectionId: collectionID,
				Policies: rlsPolicyInfos(collectionID, []*rlsutil.RowPolicy{{
					PolicyName: "new",
					PolicyType: rlsutil.PolicyTypePermissive,
					Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionUpsert},
					UsingExpr:  "id == 1",
					CheckExpr:  "id == 1",
				}}),
			}, nil).Maybe()
			require.NoError(t, rls.Init(ctx, coord))
			return &milvuspb.QueryResults{
				Status:     merr.Success(),
				FieldsData: []*schemapb.FieldData{proto.Clone(fields[0]).(*schemapb.FieldData)},
			}, segcore.StorageCost{}, nil
		}).Build()

		require.NoError(t, task.prepareUpsert(ctx))
		err := task.insertPreExecute(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	})
}

func TestRLSForceRejectsSkipAcrossOperations(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false")
	t.Cleanup(func() {
		Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	})

	t.Run("query", func(t *testing.T) {
		const collectionID = int64(992001)
		const canonicalName = "rls_force_query"
		const aliasName = "rls_force_query_alias"
		ctx := context.Background()
		schema := newRLSOperationTestSchema(canonicalName)
		cache := installRLSOperationTestCache(t, collectionID, schema, true)
		node := &Proxy{metaCache: cache}
		task := NewQueryTask(ctx, node, &milvuspb.QueryRequest{
			DbName:         "default",
			CollectionName: aliasName,
			Expr:           "id > 0",
			SkipRls:        true,
		}, nil, &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{},
		}, cache, false)
		require.NoError(t, task.OnEnqueue())
		err := task.PreExecute(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		require.Contains(t, err.Error(), "rls.force")
		require.Contains(t, err.Error(), canonicalName)
		require.NotContains(t, err.Error(), aliasName)
	})

	t.Run("search", func(t *testing.T) {
		const collectionID = int64(992002)
		const canonicalName = "rls_force_search"
		const aliasName = "rls_force_search_alias"
		ctx := context.Background()
		cache := installRLSOperationTestCache(t, collectionID, newRLSOperationTestSchema(canonicalName), true)
		node := &Proxy{metaCache: cache}
		task := NewSearchTask(ctx, node, nil, &milvuspb.SearchRequest{
			DbName:         "default",
			CollectionName: aliasName,
			Nq:             1,
			Dsl:            "id > 0",
			DslType:        commonpb.DslType_BoolExprV1,
			SearchParams:   newRLSOperationSearchParams(),
			SkipRls:        true,
		}, false, false, timerecord.NewTimeRecorder("rls-force-search-test"))
		require.NoError(t, task.OnEnqueue())
		err := task.PreExecute(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		require.Contains(t, err.Error(), "rls.force")
		require.Contains(t, err.Error(), canonicalName)
		require.NotContains(t, err.Error(), aliasName)
	})

	t.Run("delete", func(t *testing.T) {
		const collectionID = int64(992003)
		const canonicalName = "rls_force_delete"
		const aliasName = "rls_force_delete_alias"
		ctx := context.Background()
		cache := installRLSOperationTestCache(t, collectionID, newRLSOperationTestSchema(canonicalName), true)
		runner := &deleteRunner{metaCache: cache, req: &milvuspb.DeleteRequest{
			DbName:         "default",
			CollectionName: aliasName,
			Expr:           "id == 1",
			SkipRls:        true,
		}}
		err := runner.Init(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		require.Contains(t, err.Error(), "rls.force")
		require.Contains(t, err.Error(), canonicalName)
		require.NotContains(t, err.Error(), aliasName)
	})

	t.Run("insert", func(t *testing.T) {
		const collectionID = int64(992004)
		const canonicalName = "rls_force_insert"
		const aliasName = "rls_force_insert_alias"
		ctx := context.Background()
		cache := installRLSOperationTestCache(t, collectionID, newRLSOperationTestSchema(canonicalName), true)
		task := &insertTask{
			baseTask:  baseTask{MetaCache: cache},
			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			insertMsg: &BaseInsertTask{InsertRequest: &msgpb.InsertRequest{
				Base:           &commonpb.MsgBase{},
				DbName:         "default",
				CollectionName: aliasName,
				PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
				FieldsData:     newRLSOperationTestFieldsData(),
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			}},
			idAllocator: newRLSOperationTestAllocator(t),
			skipRLS:     true,
		}
		require.NoError(t, task.OnEnqueue())
		err := task.PreExecute(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		require.Contains(t, err.Error(), "rls.force")
		require.Contains(t, err.Error(), canonicalName)
		require.NotContains(t, err.Error(), aliasName)
	})

	t.Run("upsert", func(t *testing.T) {
		const collectionID = int64(992005)
		const aliasName = "rls_force_upsert_alias"
		ctx := context.Background()
		task := createTestUpdateTask()
		canonicalName := task.schema.GetName()
		task.req.CollectionName = aliasName
		task.req.SkipRls = true
		task.baseTask.MetaCache = installRLSOperationTestCache(t, collectionID, task.schema, true)
		require.NoError(t, task.OnEnqueue())
		err := task.PreExecute(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		require.Contains(t, err.Error(), "rls.force")
		require.Contains(t, err.Error(), canonicalName)
		require.NotContains(t, err.Error(), aliasName)
	})
}
