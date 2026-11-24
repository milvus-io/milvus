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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func searchRequeryTestSchema(version int32, changing bool) *schemaInfo {
	schema := createTestSchema().CollectionSchema
	schema.Version = version
	fields := []*schemapb.FieldSchema{
		{FieldID: 104, Name: "clips[anchor]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
		{FieldID: 105, Name: "clips[emb]", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
	}
	if changing {
		fields = append(fields, &schemapb.FieldSchema{FieldID: 106, Name: "clips[changing]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, Nullable: true})
	}
	schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 103, Name: "clips", Fields: fields}}
	return mustNewSchemaInfo(schema)
}

func dynamicRequeryTestSchema(version int32, hasX bool) *schemaInfo {
	schema := createTestSchema().CollectionSchema
	schema.Version = version
	schema.EnableDynamicField = true
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID: 103, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true,
	})
	if hasX {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID: 104, Name: "x", DataType: schemapb.DataType_Int64, Nullable: true,
		})
	}
	return mustNewSchemaInfo(schema)
}

func TestQueryTaskRequerySchemaValidation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		searchVersion int32
		queryVersion  int32
		output        string
		requery       bool
		versionSet    bool
		recreated     bool
		dynamic       bool
		wantCode      int32
	}{
		{name: "drop_after_version_zero", searchVersion: 0, queryVersion: 1, output: "clips[changing]", requery: true, versionSet: true, wantCode: 2200},
		{name: "drop_after_nonzero_version", searchVersion: 7, queryVersion: 8, output: "clips[changing]", requery: true, versionSet: true, wantCode: 2200},
		{name: "same_version_invalid_field", searchVersion: 8, queryVersion: 8, output: "clips[changing]", requery: true, versionSet: true, wantCode: 1100},
		{name: "ordinary_query", searchVersion: 7, queryVersion: 8, output: "clips[changing]", versionSet: true, wantCode: 1100},
		{name: "requery_without_search_version", queryVersion: 8, output: "clips[changing]", requery: true, wantCode: 1100},
		{name: "changed_schema_valid_projection", searchVersion: 7, queryVersion: 8, output: "clips[anchor]", requery: true, versionSet: true, wantCode: 2200},
		{name: "same_schema_valid_projection", searchVersion: 8, queryVersion: 8, output: "clips[anchor]", requery: true, versionSet: true},
		{name: "dynamic_drop", searchVersion: 0, queryVersion: 1, output: "x", requery: true, versionSet: true, dynamic: true, wantCode: 2200},
		{name: "same_schema_dynamic_key", searchVersion: 1, queryVersion: 1, output: "x", requery: true, versionSet: true, dynamic: true},
		{name: "ordinary_dynamic_query", searchVersion: 0, queryVersion: 1, output: "x", versionSet: true, dynamic: true},
		{name: "collection_identity_changed", searchVersion: 7, queryVersion: 8, output: "clips[changing]", requery: true, versionSet: true, recreated: true, wantCode: 1100},
		{name: "dynamic_collection_identity_changed", searchVersion: 0, queryVersion: 1, output: "x", requery: true, versionSet: true, dynamic: true, recreated: true, wantCode: 1100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			schema := searchRequeryTestSchema(tc.queryVersion, false)
			if tc.dynamic {
				schema = dynamicRequeryTestSchema(tc.queryVersion, false)
			}
			collectionID := int64(1001)
			if tc.recreated {
				collectionID++
			}
			mockTest(t, (*metacache.MetaCache).GetCollectionID, collectionID, nil)
			mockTest(t, (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{CollID: collectionID, Schema: schema}, nil)
			mockTest(t, (*metacache.MetaCache).GetCollectionSchema, schema, nil)
			mockTest(t, GetPartitionIDs, []int64{1}, nil)
			ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
			pk, err := schema.GetPkField()
			require.NoError(t, err)
			task := &QueryTask{
				baseTask:        baseTask{MetaCache: newTestCache()},
				ctx:             ctx,
				RetrieveRequest: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{Timestamp: 200}},
				request: &milvuspb.QueryRequest{
					CollectionName: "test_collection",
					OutputFields:   []string{tc.output},
					QueryParams:    []*commonpb.KeyValuePair{{Key: CollectionID, Value: "1001"}},
				},
				plan:    planparserv2.CreateRequeryPlan(pk, ids),
				reQuery: tc.requery,
			}
			if tc.versionSet {
				task.requerySchemaVersion = &tc.searchVersion
			}
			err = task.PreExecute(ctx)
			require.Equal(t, tc.wantCode, merr.Code(err))
			if tc.wantCode == 0 {
				require.NoError(t, err)
				require.Same(t, schema, task.schema)
				if tc.dynamic {
					require.Equal(t, []string{"x"}, task.plan.GetDynamicFields())
					require.Contains(t, task.plan.GetOutputFieldIds(), int64(103))
				}
				return
			}
			require.Error(t, err)
			status := merr.Status(err)
			require.Equal(t, tc.wantCode, status.GetCode())
			if tc.wantCode == 2200 {
				require.ErrorIs(t, err, merr.ErrInconsistentRequery)
				require.Contains(t, err.Error(), "collection=1001")
				require.Contains(t, err.Error(), "search schema version="+strconv.Itoa(int(tc.searchVersion)))
				require.Contains(t, err.Error(), "query schema version="+strconv.Itoa(int(tc.queryVersion)))
				require.Equal(t, merr.SystemError, merr.GetErrorType(err))
				require.True(t, status.GetRetriable())
				require.NotEqual(t, "true", status.GetExtraInfo()[merr.InputErrorFlagKey])
				require.ErrorIs(t, merr.Error(status), merr.ErrInconsistentRequery)
			} else {
				require.NotErrorIs(t, err, merr.ErrInconsistentRequery)
				require.False(t, status.GetRetriable())
			}
		})
	}
}

// Exercise real Search preprocessing and requery preprocessing on either side of
// a field drop. Only metadata RPCs and the QueryRunner dispatch are stubbed.
func TestSearchTaskRequerySchemaChangeRebuildsProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const collectionID int64 = 1001
	before := searchRequeryTestSchema(0, true)
	after := searchRequeryTestSchema(1, false)
	current := before
	mockTest(t, (*metacache.MetaCache).GetCollectionID, collectionID, nil)
	mockTestTo(t, (*metacache.MetaCache).GetCollectionInfo, func(*metacache.MetaCache, context.Context, string, string, int64) (*collectionInfo, error) {
		return &collectionInfo{CollID: collectionID, Schema: current}, nil
	})
	mockTestTo(t, (*metacache.MetaCache).GetCollectionSchema, func(*metacache.MetaCache, context.Context, string, string) (*schemaInfo, error) {
		return current, nil
	})
	mockTestTo(t, (*namespaceRequeryMockNode).ExecuteQuery, func(_ *namespaceRequeryMockNode, _ context.Context, task taskmodel.Task, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
		qt := task.(*QueryTask)
		err := qt.PreExecute(ctx)
		require.Same(t, after, qt.schema)
		return &milvuspb.QueryResults{Status: merr.Status(err)}, segcore.StorageCost{}, nil
	})
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, 128*4)},
	}}})
	require.NoError(t, err)
	request := &milvuspb.SearchRequest{
		CollectionName: "test_collection",
		Dsl:            "id >= 0",
		OutputFields:   []string{"clips"},
		SearchInput:    &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder},
		SearchParams: []*commonpb.KeyValuePair{
			{Key: AnnsFieldKey, Value: "vector"},
			{Key: TopKKey, Value: "1"},
			{Key: MetricTypeKey, Value: "L2"},
		},
	}
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
	attempts := 0
	err = retry.Handle(ctx, func() (bool, error) {
		attempts++
		search := NewSearchTask(ctx, &namespaceRequeryMockNode{}, nil, request, true, false, timerecord.NewTimeRecorder("search"))
		search.SetTs(uint64(200 + attempts))
		require.NoError(t, search.PreExecute(ctx))
		require.True(t, search.needRequery)
		require.Equal(t, []string{"clips"}, request.GetOutputFields())
		op, err := newRequeryOperator(search, nil)
		require.NoError(t, err)
		if attempts == 1 {
			require.Contains(t, search.translatedOutputFields, "clips[changing]")
			current = after // Drop completes between Search and requery.
		} else {
			require.NotContains(t, search.translatedOutputFields, "clips[changing]")
			require.Contains(t, search.translatedOutputFields, "clips[emb]")
		}
		requery := op.(*requeryOperator)
		_, _, err = requery.requery(ctx, nil, ids, requery.outputFieldNames)
		// Match Proxy.Search's predicate after the internal Query status round trip.
		return errors.Is(err, merr.ErrInconsistentRequery), err
	}, retry.Attempts(2), retry.Sleep(time.Millisecond))
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
}

// Check the serialized retrieve plan, not just the retry code: a stale wildcard
// expansion can resolve a dropped column as a dynamic key and trim unrelated
// $meta keys without any validation error. Metadata and query dispatch are stubbed;
// Search/HybridSearch preprocessing, projection rebuilding and serialization run.
func TestSearchTaskRequeryDynamicSchemaChangeRebuildsProjection(t *testing.T) {
	for _, change := range []string{"drop", "add"} {
		for _, api := range []string{"search", "hybrid_search"} {
			t.Run(change+"/"+api, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				const collectionID int64 = 1001
				before := dynamicRequeryTestSchema(0, change == "drop")
				after := dynamicRequeryTestSchema(1, change == "add")
				current := before
				mockTest(t, (*metacache.MetaCache).GetCollectionID, collectionID, nil)
				mockTestTo(t, (*metacache.MetaCache).GetCollectionInfo, func(*metacache.MetaCache, context.Context, string, string, int64) (*collectionInfo, error) {
					return &collectionInfo{CollID: collectionID, Schema: current}, nil
				})
				mockTestTo(t, (*metacache.MetaCache).GetCollectionSchema, func(*metacache.MetaCache, context.Context, string, string) (*schemaInfo, error) {
					return current, nil
				})
				attempts, successfulQueries := 0, 0
				mockTestTo(t, (*namespaceRequeryMockNode).ExecuteQuery, func(_ *namespaceRequeryMockNode, _ context.Context, task taskmodel.Task, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
					qt := task.(*QueryTask)
					err := qt.PreExecute(ctx)
					if attempts == 1 {
						require.ErrorIs(t, err, merr.ErrInconsistentRequery)
						require.Empty(t, qt.SerializedExprPlan)
					} else {
						require.NoError(t, err)
						plan := &planpb.PlanNode{}
						require.NoError(t, proto.Unmarshal(qt.SerializedExprPlan, plan))
						// Empty DynamicFields means retrieve the entire $meta JSON,
						// preserving unrelated keys such as {"tag":"keep"}.
						require.Empty(t, plan.GetDynamicFields())
						wantIDs := []int64{100, 101, 102, 103, common.TimeStampField}
						if change == "add" {
							wantIDs = append(wantIDs, 104)
						}
						require.ElementsMatch(t, wantIDs, plan.GetOutputFieldIds())
						successfulQueries++
					}
					return &milvuspb.QueryResults{Status: merr.Status(err)}, segcore.StorageCost{}, nil
				})

				placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
					Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, 128*4)},
				}}})
				require.NoError(t, err)
				request := &milvuspb.SearchRequest{
					CollectionName: "test_collection",
					Dsl:            "id >= 0",
					OutputFields:   []string{"*"},
					SearchInput:    &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder},
					SearchParams: []*commonpb.KeyValuePair{
						{Key: AnnsFieldKey, Value: "vector"},
						{Key: TopKKey, Value: "1"},
						{Key: MetricTypeKey, Value: "L2"},
					},
				}
				hybridRequest := &milvuspb.HybridSearchRequest{
					CollectionName: request.GetCollectionName(),
					OutputFields:   []string{"*"},
					Requests:       []*milvuspb.SearchRequest{request},
					RankParams:     []*commonpb.KeyValuePair{{Key: LimitKey, Value: "1"}},
				}
				ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
				err = retry.Handle(ctx, func() (bool, error) {
					attempts++
					searchRequest := request
					if api == "hybrid_search" {
						searchRequest = ConvertHybridSearchToSearch(hybridRequest)
					}
					search := NewSearchTask(ctx, &namespaceRequeryMockNode{}, nil, searchRequest, true, false, timerecord.NewTimeRecorder("search"))
					search.SetTs(uint64(200 + attempts))
					require.NoError(t, search.PreExecute(ctx))
					require.True(t, search.needRequery)
					require.Equal(t, []string{"*"}, request.GetOutputFields())
					require.Equal(t, []string{"*"}, hybridRequest.GetOutputFields())
					op, err := newRequeryOperator(search, nil)
					require.NoError(t, err)
					requery := op.(*requeryOperator)
					if attempts == 1 {
						current = after // DDL lands between Search and requery.
						// Demonstrate why checking only translation errors misses
						// both changes, and why Drop narrows the dynamic projection.
						_, _, dynamicFields, _, _, err := translateOutputFields(requery.outputFieldNames, after, false)
						require.NoError(t, err)
						if change == "drop" {
							require.Equal(t, []string{"x"}, dynamicFields)
						}
					}
					_, _, err = requery.requery(ctx, nil, ids, requery.outputFieldNames)
					// Match the Search/HybridSearch predicate after status conversion.
					err = merr.Error(merr.Status(err))
					return errors.Is(err, merr.ErrInconsistentRequery), err
				}, retry.Attempts(2), retry.Sleep(time.Millisecond))
				require.NoError(t, err)
				require.Equal(t, 2, attempts)
				require.Equal(t, 1, successfulQueries)
			})
		}
	}
}
