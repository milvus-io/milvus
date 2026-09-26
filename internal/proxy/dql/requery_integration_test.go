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

package dql

import (
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestSearchTask_Requery exercises the requeryOperator: it builds a requery
// QueryTask from the search results and runs it through the node's
// QueryRunner, then verifies the reconstructed fields. The host node is a mock
// taskmodel.TaskNode whose QueryRunner is patched by mockey.
func TestSearchTask_Requery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		dim          = 128
		rows         = 5
		collection   = "test-requery"
		collectionID = int64(123)

		pkField  = "pk"
		vecField = "vec"
	)

	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}

	collectionName := "col"
	cache := newTestCache()
	collSchema := constructCollectionSchema(pkField, vecField, dim, collection)
	schema := mustNewSchemaInfo(collSchema)
	mockTest(t, (*metacache.MetaCache).GetCollectionID, UniqueID(0), nil)
	mockTest(t, (*metacache.MetaCache).GetCollectionSchema, schema, nil)
	mockTest(t, (*metacache.MetaCache).GetPartitions, map[string]int64{"_default": UniqueID(1)}, nil)
	mockTest(t, (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{Schema: schema}, nil)

	node := &namespaceRequeryMockNode{}

	t.Run("Test normal", func(t *testing.T) {
		resultIDs := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		}

		outputFields := []string{pkField, vecField}
		qt := &SearchTask{
			baseTask: baseTask{MetaCache: cache},
			ctx:      ctx,
			SearchRequest: &internalpb.SearchRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Search,
					SourceID: paramtable.GetNodeID(),
				},
				CollectionID: collectionID,
			},
			request: &milvuspb.SearchRequest{
				CollectionName: collectionName,
				OutputFields:   outputFields,
			},
			result: &milvuspb.SearchResults{
				Results: &schemapb.SearchResultData{
					Ids: resultIDs,
				},
			},
			schema:                 schema,
			rlsCollectionName:      collection,
			tr:                     timerecord.NewTimeRecorder("search"),
			node:                   node,
			translatedOutputFields: outputFields,
			queryChannelsNode:      typeutil.NewConcurrentMap[string, int64](),
		}
		qt.queryChannelsNode.Insert("mock_qn", 1)

		// The requery query task is executed on the host node's QueryRunner.
		// Return a result carrying pk + vec fields.
		mockTestTo(t, (*namespaceRequeryMockNode).ExecuteQuery, func(_ *namespaceRequeryMockNode, _ context.Context, qt taskmodel.Task, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
			queryTask := qt.(*QueryTask)
			require.Equal(t, metrics.ReQueryLabel, queryTask.GetQueryLabel())
			require.True(t, queryTask.ReQuery())
			require.Equal(t, collection, queryTask.Request().GetCollectionName())
			pinnedID, err := funcutil.GetAttrByKeyFromRepeatedKV(CollectionID, queryTask.Request().GetQueryParams())
			require.NoError(t, err)
			require.Equal(t, strconv.FormatInt(collectionID, 10), pinnedID)
			return &milvuspb.QueryResults{
				Status: merr.Success(),
				FieldsData: []*schemapb.FieldData{
					{FieldName: pkField, FieldId: 100, Type: schemapb.DataType_Int64},
					{FieldName: vecField, FieldId: 101, Type: schemapb.DataType_FloatVector},
				},
			}, segcore.StorageCost{}, nil
		})

		op, err := newRequeryOperator(qt, nil)
		assert.NoError(t, err)
		queryResult, storageCost, err := op.(*requeryOperator).requery(ctx, nil, qt.result.Results.Ids, outputFields)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), storageCost.ScannedRemoteBytes)
		assert.Equal(t, int64(0), storageCost.ScannedTotalBytes)
		assert.Len(t, queryResult.FieldsData, 2)
		for _, field := range queryResult.FieldsData {
			assert.Contains(t, []string{pkField, vecField}, field.GetFieldName())
		}
	})

	t.Run("Test no primary key", func(t *testing.T) {
		collSchema := &schemapb.CollectionSchema{}
		schema := mustNewSchemaInfo(collSchema)

		qt := &SearchTask{
			baseTask: baseTask{MetaCache: cache},
			ctx:      ctx,
			SearchRequest: &internalpb.SearchRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Search,
					SourceID: paramtable.GetNodeID(),
				},
			},
			request: &milvuspb.SearchRequest{},
			schema:  schema,
			tr:      timerecord.NewTimeRecorder("search"),
			node:    node,
		}

		_, err := newRequeryOperator(qt, nil)
		assert.Error(t, err)
	})

	t.Run("Test requery failed", func(t *testing.T) {
		resultIDs := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		}
		outputFields := []string{pkField, vecField}
		qt := &SearchTask{
			baseTask: baseTask{MetaCache: cache},
			ctx:      ctx,
			SearchRequest: &internalpb.SearchRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Search,
					SourceID: paramtable.GetNodeID(),
				},
			},
			request: &milvuspb.SearchRequest{
				CollectionName: collectionName,
				OutputFields:   outputFields,
			},
			result: &milvuspb.SearchResults{
				Results: &schemapb.SearchResultData{
					Ids: resultIDs,
				},
			},
			schema:                 schema,
			tr:                     timerecord.NewTimeRecorder("search"),
			node:                   node,
			translatedOutputFields: outputFields,
			queryChannelsNode:      typeutil.NewConcurrentMap[string, int64](),
		}
		qt.queryChannelsNode.Insert("mock_qn", 1)

		mockTestTo(t, (*namespaceRequeryMockNode).ExecuteQuery, func(_ *namespaceRequeryMockNode, _ context.Context, _ taskmodel.Task, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
			return nil, segcore.StorageCost{}, errors.New("mock requery failure")
		})

		op, err := newRequeryOperator(qt, nil)
		assert.NoError(t, err)
		_, _, err = op.(*requeryOperator).requery(ctx, nil, &schemapb.IDs{}, []string{})
		assert.Error(t, err)
	})
}

// TestSearchTask_ErrExecute exercises the error propagation of SearchTask
// execution. The shard dispatch is stubbed at the lb level: Execute runs the
// workload, whose Exec invokes the query node client; each scenario returns a
// different failure and verifies the surfaced error.
func TestSearchTask_ErrExecute(t *testing.T) {
	ctx := context.TODO()
	collectionName := t.Name() + funcutil.GenRandomStr()

	cache := newTestCache()
	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testFloatVecField: schemapb.DataType_FloatVector,
	}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
	schemaInfo := mustNewSchemaInfo(schema)
	collectionID := int64(1000)
	mockTest(t, (*metacache.MetaCache).GetCollectionID, collectionID, nil)
	mockTest(t, (*metacache.MetaCache).GetCollectionSchema, schemaInfo, nil)
	mockTest(t, (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{CollID: collectionID, Schema: schemaInfo}, nil)
	mockTest(t, (*metacache.MetaCache).GetPartitions, map[string]int64{"_default": 1}, nil)

	qn := getQueryNodeClient()
	mgr := shardclient.NewMockShardClientManager(t)
	mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn, nil).Maybe()
	mgr.EXPECT().GetShardLeaderList(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]string{"mock_qn"}, nil).Maybe()
	mgr.EXPECT().GetShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]shardclient.NodeInfo{
		{NodeID: 1, Address: "mock_qn", Serviceable: true},
	}, nil).Maybe()
	mgr.EXPECT().InvalidateShardLeaderCache(mock.Anything).Return().Maybe()
	lb := shardclient.NewMockLBPolicy(t)

	task := &SearchTask{
		baseTask:  baseTask{MetaCache: cache},
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: paramtable.GetNodeID(),
			},
			CollectionID:   collectionID,
			OutputFieldsId: make([]int64, len(fieldName2Types)),
		},
		ctx: ctx,
		result: &milvuspb.SearchResults{
			Status: merr.Success(),
		},
		request: &milvuspb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: paramtable.GetNodeID(),
			},
			CollectionName: collectionName,
			Nq:             1,
			DslType:        commonpb.DslType_BoolExprV1,
		},
		lb:             lb,
		shardClientMgr: mgr,
	}
	for i := 0; i < len(fieldName2Types); i++ {
		task.OutputFieldsId[i] = int64(common.StartOfUserFieldID + i)
	}

	assert.NoError(t, task.OnEnqueue())
	task.ctx = ctx
	if enableMultipleVectorFields {
		err := task.PreExecute(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "multiple anns_fields exist, please specify a anns_field in search_params")
	} else {
		assert.NoError(t, task.PreExecute(ctx))
	}

	// stub shard dispatch: Execute runs the workload against the mock qn and
	// propagates the query node error back through lb.
	lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, "mock_qn")
		}).Maybe()
	lb.EXPECT().UpdateCostMetrics(mock.Anything, mock.Anything).Return().Maybe()

	qn.EXPECT().Search(mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Once()
	assert.Error(t, task.Execute(ctx))

	qn.EXPECT().Search(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
		Status: merr.Status(merr.ErrChannelNotAvailable),
	}, nil).Once()
	err := task.Execute(ctx)
	assert.ErrorIs(t, err, merr.ErrChannelNotAvailable)

	qn.EXPECT().Search(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
	}, nil).Once()
	assert.Error(t, task.Execute(ctx))
}
