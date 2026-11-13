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
package dql

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	queryreducestream "github.com/milvus-io/milvus/internal/util/queryutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type fakeLegacyViewQueryClient struct {
	legacy queryclient.LegacyClient
}

func TestQuerySupportsStreamingReduce(t *testing.T) {
	order := []*planpb.OrderByField{{FieldId: 101, Ascending: true}}
	count := []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 101}}
	for _, tc := range []struct {
		name string
		req  *internalpb.RetrieveRequest
		want bool
	}{
		{name: "nil"},
		{name: "legacy count", req: &internalpb.RetrieveRequest{IsCount: true}},
		{name: "missing limit", req: &internalpb.RetrieveRequest{}},
		{name: "invalid limit", req: &internalpb.RetrieveRequest{Limit: -2}},
		{name: "unlimited iterator", req: &internalpb.RetrieveRequest{IsIterator: true, Limit: typeutil.Unlimited}, want: true},
		{name: "unlimited", req: &internalpb.RetrieveRequest{Limit: typeutil.Unlimited}, want: true},
		{name: "limited", req: &internalpb.RetrieveRequest{Limit: 1}, want: true},
		{name: "ordered", req: &internalpb.RetrieveRequest{Limit: 1, OrderByFields: order}, want: true},
		{name: "unlimited ordered", req: &internalpb.RetrieveRequest{Limit: typeutil.Unlimited, OrderByFields: order}},
		{name: "grouped ordered missing limit", req: &internalpb.RetrieveRequest{GroupByFieldIds: []int64{101}, OrderByFields: order}},
		{name: "grouped ordered", req: &internalpb.RetrieveRequest{Limit: 1, GroupByFieldIds: []int64{101}, OrderByFields: order}, want: true},
		{name: "grouped aggregate ordered", req: &internalpb.RetrieveRequest{Limit: 1, GroupByFieldIds: []int64{101}, Aggregates: count, OrderByFields: order}, want: true},
		{name: "grouped ordered unlimited", req: &internalpb.RetrieveRequest{Limit: typeutil.Unlimited, GroupByFieldIds: []int64{101}, OrderByFields: order}},
		{name: "global aggregate ordered", req: &internalpb.RetrieveRequest{Limit: 1, Aggregates: count, OrderByFields: order}},
		{name: "grouped aggregate", req: &internalpb.RetrieveRequest{GroupByFieldIds: []int64{101}, Aggregates: count}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, (&QueryTask{RetrieveRequest: tc.req}).supportsStreamingReduce())
		})
	}
	for _, op := range []planpb.AggregateOp{planpb.AggregateOp_count, planpb.AggregateOp_sum, planpb.AggregateOp_min, planpb.AggregateOp_max} {
		task := &QueryTask{RetrieveRequest: &internalpb.RetrieveRequest{Aggregates: []*planpb.Aggregate{{Op: op, FieldId: 101}}}}
		require.True(t, task.supportsStreamingReduce())
	}
}

func (*fakeLegacyViewQueryClient) Close() {}

func (c *fakeLegacyViewQueryClient) Legacy() queryclient.LegacyClient {
	return c.legacy
}

type fakeLegacyQueryClient struct {
	queryclient.LegacyClient
	searchCalled      int
	queryCalled       int
	maxRetries        int
	searchResult      *queryclient.LegacySearchResult
	lastSearchRequest *queryclient.LegacySearchRequest
	queryResult       *queryclient.LegacyQueryResult
	lastQuery         *queryclient.LegacyQueryRequest
	lastQueryRequest  *queryclient.LegacyQueryRequest
	err               error
	search            func(int) (*queryclient.LegacySearchResult, error)
	query             func(int) (*queryclient.LegacyQueryResult, error)
}

func (c *fakeLegacyQueryClient) Search(_ context.Context, request *queryclient.LegacySearchRequest) (*queryclient.LegacySearchResult, error) {
	c.searchCalled++
	c.lastSearchRequest = request
	if c.search != nil {
		return c.search(c.searchCalled)
	}
	return c.searchResult, c.err
}

func (c *fakeLegacyQueryClient) Query(_ context.Context, request *queryclient.LegacyQueryRequest) (*queryclient.LegacyQueryResult, error) {
	c.queryCalled++
	c.lastQuery = request
	c.lastQueryRequest = request
	if c.query != nil {
		return c.query(c.queryCalled)
	}
	return c.queryResult, c.err
}

func (c *fakeLegacyQueryClient) MaxRetries() int {
	return max(1, c.maxRetries)
}

type fakeLegacySearchRecv struct {
	chunk *internalpb.SearchResults
	err   error
}

type fakeLegacySearchStream struct {
	recv       []fakeLegacySearchRecv
	next       int
	closeCalls int
	closeErr   error
}

func (s *fakeLegacySearchStream) Recv() (*internalpb.SearchResults, error) {
	if s.next >= len(s.recv) {
		return nil, io.EOF
	}
	result := s.recv[s.next]
	s.next++
	return result.chunk, result.err
}

func (s *fakeLegacySearchStream) Close() error {
	s.closeCalls++
	return s.closeErr
}

func (*fakeLegacySearchStream) Interrupt() (*internalpb.SearchResults, error) {
	return nil, nil
}

type fakeLegacyQueryRecv struct {
	chunk *internalpb.RetrieveResults
	err   error
}

type fakeLegacyQueryStream struct {
	recv       []fakeLegacyQueryRecv
	next       int
	closeCalls int
	closeErr   error
}

func (s *fakeLegacyQueryStream) Recv() (*internalpb.RetrieveResults, error) {
	if s.next >= len(s.recv) {
		return nil, io.EOF
	}
	result := s.recv[s.next]
	s.next++
	return result.chunk, result.err
}

func (s *fakeLegacyQueryStream) Close() error {
	s.closeCalls++
	return s.closeErr
}

func (*fakeLegacyQueryStream) Interrupt() (*internalpb.RetrieveResults, error) {
	return nil, nil
}

var _ searchreducestream.ReduceStream = (*fakeLegacySearchStream)(nil)
var _ queryreducestream.ReduceStream = (*fakeLegacyQueryStream)(nil)

func newQueryViewSearchTask(t *testing.T, legacy queryclient.LegacyClient, topK int64) *SearchTask {
	t.Helper()
	params := paramtable.Get()
	key, old := params.ProxyCfg.EnableReduceStream.Key, params.ProxyCfg.EnableReduceStream.GetValue()
	require.NoError(t, params.Save(key, "true"))
	t.Cleanup(func() { require.NoError(t, params.Save(key, old)) })
	return &SearchTask{
		schema: &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{}},
		SearchRequest: &internalpb.SearchRequest{
			Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgID(2)),
			CollectionID: 1,
			Nq:           1,
			Topk:         topK,
			MetricType:   "IP",
			IsIterator:   true,
		},
		request:         &milvuspb.SearchRequest{DbName: "default"},
		resultBuf:       typeutil.NewConcurrentSet[*internalpb.SearchResults](),
		viewQueryClient: &fakeLegacyViewQueryClient{legacy: legacy},
	}
}

func newQueryViewQueryTask(t *testing.T, legacy queryclient.LegacyClient, limit int64) *QueryTask {
	t.Helper()
	params := paramtable.Get()
	key, old := params.ProxyCfg.EnableReduceStream.Key, params.ProxyCfg.EnableReduceStream.GetValue()
	require.NoError(t, params.Save(key, "true"))
	t.Cleanup(func() { require.NoError(t, params.Save(key, old)) })
	return &QueryTask{
		schema: &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{}},
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgID(1)),
			CollectionID: 1,
			Limit:        limit,
		},
		request:         &milvuspb.QueryRequest{DbName: "default"},
		viewQueryClient: &fakeLegacyViewQueryClient{legacy: legacy},
	}
}

func newFinalSearchChunk(ids []int64, scores []float32, channel string) *internalpb.SearchResults {
	return &internalpb.SearchResults{
		Status:       merr.Success(),
		MetricType:   "IP",
		ChannelsMvcc: map[string]uint64{channel: 1},
		ResultData: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       int64(len(ids)),
			Topks:      []int64{int64(len(ids))},
			Scores:     scores,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}},
			},
		},
	}
}

func newFinalQueryChunk(ids []int64, mvcc uint64) *internalpb.RetrieveResults {
	values := append([]int64(nil), ids...)
	return &internalpb.RetrieveResults{
		Status:        merr.Success(),
		MvccTimestamp: mvcc,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}},
		},
		FieldsData: []*schemapb.FieldData{{
			Type:      schemapb.DataType_Int64,
			FieldName: "value",
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
			}},
		}},
	}
}

func TestQueryScanStreamComputedResultsMatchBatch(t *testing.T) {
	for _, chunkBytes := range []int{1, 256, 1 << 20} {
		for _, empty := range []bool{false, true} {
			for _, page := range [][2]int64{{2, 0}, {2, 1}, {2, 100}, {typeutil.Unlimited, 0}} {
				t.Run(fmt.Sprintf("bytes=%d/empty=%t/page=%v", chunkBytes, empty, page), func(t *testing.T) {
					limit, offset := page[0], page[1]
					internalLimit := limit
					if limit > 0 {
						internalLimit += offset
					}
					legacy := &fakeLegacyQueryClient{}
					task := newQueryViewQueryTask(t, legacy, internalLimit)
					task.schema = mustNewSchemaInfo(testSchema())
					task.request.FunctionChains = []*schemapb.FunctionChain{{Stage: schemapb.FunctionChainStage_FunctionChainStageQueryScan}}
					var shards []queryreducestream.ReduceStream
					var batches []*internalpb.RetrieveResults
					for shard := int64(0); shard < 2; shard++ {
						var children []queryreducestream.ReduceStream
						var inputs []*internalpb.RetrieveResults
						for child := int64(0); child < 2; child++ {
							ids := []int64{1 + shard + 2*child, 5 + shard}
							if empty {
								ids = nil
							}
							input := computedQueryInput(ids)
							input.Status = merr.Success()
							var timestamps []int64
							var details [][]byte
							for _, id := range ids {
								timestamps = append(timestamps, 10+child)
								details = append(details, []byte(fmt.Sprintf(`[{"content":"pk=%d version=%d","starts":[0],"ends":[2]}]`, id, child)))
							}
							input.FieldsData = append(input.FieldsData, makeTestInt64Field(1, "", timestamps))
							input.ComputedResults[0].FieldId = 300 + child // Computed identities are names, not IDs.
							jsonField := &schemapb.FieldData{FieldName: "scan.matches", Type: schemapb.DataType_JSON,
								Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: details}}}},
							}
							input.ComputedResults = append(input.ComputedResults, jsonField)
							if child == 1 {
								if !empty {
									typeutil.SetFieldDataValidData(input.ComputedResults[0], []bool{true, false})
								}
								input.ComputedResults[0], input.ComputedResults[1] = input.ComputedResults[1], input.ComputedResults[0]
							}
							inputs = append(inputs, proto.Clone(input).(*internalpb.RetrieveResults))
							chunks, err := queryreducestream.SplitRetrieveResult(input, chunkBytes)
							require.NoError(t, err)
							worker := &fakeLegacyQueryStream{}
							for _, chunk := range chunks {
								worker.recv = append(worker.recv, fakeLegacyQueryRecv{chunk: chunk})
							}
							children = append(children, worker)
						}
						batch, err := queryutil.NewReduceByPKWithTimestampOperator(0, 1<<20, internalLimit, testSchema()).Run(t.Context(), nil, inputs)
						require.NoError(t, err)
						batches = append(batches, batch[0].(*internalpb.RetrieveResults))
						stream, err := queryreducestream.NewShardReduceStream(task.RetrieveRequest, testSchema(), children, chunkBytes)
						require.NoError(t, err)
						shards = append(shards, stream)
					}
					stream, err := queryreducestream.NewReduceStream(task.RetrieveRequest, testSchema(), shards, chunkBytes)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, stream.Close()) })
					legacy.queryResult = &queryclient.LegacyQueryResult{Stream: stream}
					require.NoError(t, task.Execute(t.Context()))
					require.True(t, legacy.lastQueryRequest.AllowStreaming)
					pipeline, err := NewQueryPipeline(testSchema(), limit, offset, 0, nil, nil, nil, nil, []int64{100})
					require.NoError(t, err)
					want, err := pipeline.Execute(t.Context(), batches)
					require.NoError(t, err)
					got, err := pipeline.Execute(t.Context(), task.resultBuf.Collect())
					require.NoError(t, err)
					// Header order/producer-local IDs may differ; names, types and row values must agree.
					require.Len(t, got.FieldsData, len(want.FieldsData))
					for _, expected := range want.FieldsData {
						var actual *schemapb.FieldData
						for _, field := range got.FieldsData {
							if field.FieldName == expected.FieldName {
								actual = field
							}
						}
						require.NotNil(t, actual)
						require.Equal(t, expected.Type, actual.Type)
						// An omitted validity mask and an explicit all-valid mask are equivalent.
						for _, field := range []*schemapb.FieldData{expected, actual} {
							if len(typeutil.GetFieldDataValidData(field)) == 0 {
								rows := len(field.GetScalars().GetLongData().GetData())
								if field.Type == schemapb.DataType_JSON {
									rows = len(field.GetScalars().GetJsonData().GetData())
								}
								valid := make([]bool, rows)
								for i := range valid {
									valid[i] = true
								}
								typeutil.SetFieldDataValidData(field, valid)
							}
						}
						require.True(t, proto.Equal(expected.GetScalars(), actual.GetScalars()), "batch=%v streaming=%v", expected, actual)
					}
				})
			}
		}
	}
}

func TestConsumeQueryStreamComputedValidationAndQuota(t *testing.T) {
	paramtable.Init()
	for _, missingFirst := range []bool{false, true} {
		first, second := computedQueryInput([]int64{1}), computedQueryInput([]int64{2})
		first.Status, second.Status = merr.Success(), merr.Success()
		if missingFirst {
			first.ComputedResults = nil
		} else {
			second.ComputedResults = nil
		}
		task := &QueryTask{RetrieveRequest: &internalpb.RetrieveRequest{Limit: 2}}
		_, err := task.consumeQueryResultStream(&fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: first}, {chunk: second}}})
		require.ErrorContains(t, err, "computed")
	}
	input := computedQueryInput([]int64{1})
	input.Status = merr.Success()
	input.ComputedResults[0].Type = schemapb.DataType_JSON
	input.ComputedResults[0].GetScalars().Data = &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`"` + strings.Repeat("x", 4096) + `"`)}}}
	maxSize := &paramtable.Get().QuotaConfig.MaxOutputSize
	previous := maxSize.GetValue()
	t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(maxSize.Key, previous)) })
	require.NoError(t, paramtable.Get().Save(maxSize.Key, "1024"))
	task := &QueryTask{RetrieveRequest: &internalpb.RetrieveRequest{Limit: 1}}
	_, err := task.consumeQueryResultStream(&fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: input}}})
	require.ErrorContains(t, err, "maxOutputSize")
}

func streamViewError(code viewpb.ViewCode, format string, args ...interface{}) error {
	return viewerror.NewGRPCStatusFromViewError(viewerror.New(code, format, args...)).Err()
}

func TestQueryTaskExecuteUsesQueryViewLegacyClient(t *testing.T) {
	legacy := &fakeLegacyQueryClient{
		queryResult: &queryclient.LegacyQueryResult{Results: []*internalpb.RetrieveResults{{
			Base:   commonpbutil.NewMsgBase(commonpbutil.WithSourceID(101)),
			Status: merr.Success(),
		}}},
	}
	task := newQueryViewQueryTask(t, legacy, 0)

	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, 1, legacy.queryCalled)
	require.Equal(t, 0, legacy.searchCalled)

	var sourceIDs []int64
	task.resultBuf.Range(func(result *internalpb.RetrieveResults) bool {
		sourceIDs = append(sourceIDs, result.GetBase().GetSourceID())
		return true
	})
	require.ElementsMatch(t, []int64{101}, sourceIDs)
}

func TestSearchTaskExecuteUsesQueryViewLegacyClient(t *testing.T) {
	legacy := &fakeLegacyQueryClient{
		searchResult: &queryclient.LegacySearchResult{Results: []*internalpb.SearchResults{{
			Base:   commonpbutil.NewMsgBase(commonpbutil.WithSourceID(202)),
			Status: merr.Success(),
		}}},
	}
	task := newQueryViewSearchTask(t, legacy, 1)

	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, 1, legacy.searchCalled)
	require.Equal(t, 0, legacy.queryCalled)
	require.Nil(t, task.reducedResult)

	var sourceIDs []int64
	task.resultBuf.Range(func(result *internalpb.SearchResults) bool {
		sourceIDs = append(sourceIDs, result.GetBase().GetSourceID())
		return true
	})
	require.ElementsMatch(t, []int64{202}, sourceIDs)
	require.Zero(t, task.queryChannelsNode.Len())
}

func TestQueryViewRecordsFinalReadFrontiersIncludingEmptyShards(t *testing.T) {
	mockey.PatchConvey("final successful plans supply CAS frontiers", t, func() {
		legacy := &fakeLegacyQueryClient{}
		mockey.Mock((*fakeLegacyQueryClient).Query).Return(&queryclient.LegacyQueryResult{Plans: []queryclient.ShardPlan{
			{ShardID: qviews.ShardID{VChannel: "insert"}, Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 200, TransformingTimetick: 100}},
			{ShardID: qviews.ShardID{VChannel: "transform"}, Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 300}},
		}}, nil).Build()
		task := &QueryTask{
			schema:          &schemaInfo{},
			RetrieveRequest: &internalpb.RetrieveRequest{Base: commonpbutil.NewMsgBase()}, request: &milvuspb.QueryRequest{},
			viewQueryClient: &fakeLegacyViewQueryClient{legacy: legacy}, actualChannelsMvcc: typeutil.NewConcurrentMap[string, uint64](),
		}
		require.NoError(t, task.Execute(t.Context()))
		ts, ok := task.actualChannelsMvcc.Get("insert")
		require.True(t, ok)
		require.Equal(t, uint64(200), ts)
		ts, ok = task.actualChannelsMvcc.Get("transform")
		require.True(t, ok)
		require.Equal(t, uint64(300), ts)
	})
}

func TestSearchTaskExecuteConsumesReduceStream(t *testing.T) {
	stream := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{
		chunk: newFinalSearchChunk([]int64{10}, []float32{0.9}, "replacement"),
	}}}
	legacy := &fakeLegacyQueryClient{
		searchResult: &queryclient.LegacySearchResult{Stream: stream},
	}
	task := newQueryViewSearchTask(t, legacy, 1)

	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, 1, stream.closeCalls)
	require.Empty(t, task.resultBuf.Collect())
	require.Equal(t, []int64{10}, task.reducedResult.GetResults().GetIds().GetIntId().GetData())
	require.Equal(t, map[string]uint64{"replacement": 1}, task.queryChannelsTs)
}

func TestSearchTaskRetriesCompleteStreamAttempt(t *testing.T) {
	for _, code := range []viewpb.ViewCode{
		viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED,
		viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND,
		viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN,
		viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY,
	} {
		t.Run(code.String(), func(t *testing.T) {
			failed := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{
				{chunk: newFinalSearchChunk([]int64{1}, []float32{1}, "failed")},
				{err: streamViewError(code, "retry attempt")},
			}}
			replacement := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{
				chunk: newFinalSearchChunk([]int64{2, 3}, []float32{0.9, 0.8}, "replacement"),
			}}}
			legacy := &fakeLegacyQueryClient{maxRetries: 2}
			legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
				if attempt == 1 {
					return &queryclient.LegacySearchResult{Stream: failed}, nil
				}
				return &queryclient.LegacySearchResult{Stream: replacement}, nil
			}
			task := newQueryViewSearchTask(t, legacy, 2)

			require.NoError(t, task.Execute(context.Background()))
			require.Equal(t, 2, legacy.searchCalled)
			require.Equal(t, 1, failed.closeCalls)
			require.Equal(t, 1, replacement.closeCalls)
			require.Equal(t, []int64{2, 3}, task.reducedResult.GetResults().GetIds().GetIntId().GetData())
			require.Equal(t, map[string]uint64{"replacement": 1}, task.queryChannelsTs)
		})
	}
}

func TestSearchTaskRetriesUnavailableStream(t *testing.T) {
	for _, afterChunk := range []bool{false, true} {
		t.Run(fmt.Sprintf("after_chunk_%t", afterChunk), func(t *testing.T) {
			failed := &fakeLegacySearchStream{}
			if afterChunk {
				failed.recv = append(failed.recv, fakeLegacySearchRecv{chunk: newFinalSearchChunk([]int64{1}, []float32{1}, "failed")})
			}
			failed.recv = append(failed.recv, fakeLegacySearchRecv{err: status.Error(codes.Unavailable, "worker unavailable")})
			replacement := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{chunk: newFinalSearchChunk([]int64{2, 3}, []float32{0.9, 0.8}, "replacement")}}}
			legacy := &fakeLegacyQueryClient{maxRetries: 2}
			legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
				if attempt == 1 {
					return &queryclient.LegacySearchResult{Stream: failed}, nil
				}
				return &queryclient.LegacySearchResult{Stream: replacement}, nil
			}
			task := newQueryViewSearchTask(t, legacy, 2)
			require.NoError(t, task.Execute(t.Context()))
			require.Equal(t, 2, legacy.searchCalled)
			require.Len(t, legacy.lastSearchRequest.RetryErrors, 1)
			require.Equal(t, codes.Unavailable, status.Code(legacy.lastSearchRequest.RetryErrors[0]))
			require.Equal(t, 1, failed.closeCalls)
			require.Equal(t, 1, replacement.closeCalls)
			require.Equal(t, []int64{2, 3}, task.reducedResult.GetResults().GetIds().GetIntId().GetData())
			require.Equal(t, map[string]uint64{"replacement": 1}, task.queryChannelsTs)
		})
	}
}

func TestSearchTaskRetriesStreamCreation(t *testing.T) {
	replacement := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{
		chunk: newFinalSearchChunk([]int64{2}, []float32{0.9}, "replacement"),
	}}}
	legacy := &fakeLegacyQueryClient{maxRetries: 2}
	legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
		if attempt == 1 {
			return nil, streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, "re-plan")
		}
		return &queryclient.LegacySearchResult{Stream: replacement}, nil
	}
	task := newQueryViewSearchTask(t, legacy, 1)

	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, 2, legacy.searchCalled)
	require.Equal(t, []int64{2}, task.reducedResult.GetResults().GetIds().GetIntId().GetData())
}

func TestSearchTaskRetriesRecvErrorBeforeFirstChunk(t *testing.T) {
	failed := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{
		err: streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED, "retry before first Chunk"),
	}}}
	replacement := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{
		chunk: newFinalSearchChunk([]int64{2}, []float32{0.9}, "replacement"),
	}}}
	legacy := &fakeLegacyQueryClient{maxRetries: 2}
	legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
		if attempt == 1 {
			return &queryclient.LegacySearchResult{Stream: failed}, nil
		}
		return &queryclient.LegacySearchResult{Stream: replacement}, nil
	}
	task := newQueryViewSearchTask(t, legacy, 1)

	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, 2, legacy.searchCalled)
	require.Equal(t, 1, failed.closeCalls)
	require.Equal(t, 1, replacement.closeCalls)
}

func TestSearchTaskDoesNotRetryNonRetryableStreamError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		chunk *internalpb.SearchResults
	}{
		{name: "unknown before Chunk", err: streamViewError(viewpb.ViewCode_VIEW_CODE_UNKNOWN, "unknown")},
		{name: "unknown after Chunk", err: streamViewError(viewpb.ViewCode_VIEW_CODE_UNKNOWN, "unknown"), chunk: newFinalSearchChunk([]int64{1}, []float32{1}, "failed")},
		{name: "canceled", err: context.Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recv := make([]fakeLegacySearchRecv, 0, 2)
			if tc.chunk != nil {
				recv = append(recv, fakeLegacySearchRecv{chunk: tc.chunk})
			}
			recv = append(recv, fakeLegacySearchRecv{err: tc.err})
			stream := &fakeLegacySearchStream{recv: recv}
			legacy := &fakeLegacyQueryClient{
				maxRetries:   3,
				searchResult: &queryclient.LegacySearchResult{Stream: stream},
			}
			task := newQueryViewSearchTask(t, legacy, 2)

			require.Error(t, task.Execute(context.Background()))
			require.Equal(t, 1, legacy.searchCalled)
			require.Equal(t, 1, stream.closeCalls)
			require.Nil(t, task.reducedResult)
		})
	}
}

func TestSearchTaskRetryExhaustionReturnsLastFailure(t *testing.T) {
	mixCoord := mockey.Mock((*namespaceRequeryMockNode).MixCoord).Return(&mocks.MockMixCoordClient{}).Build()
	defer mixCoord.UnPatch()
	lookup := mockey.Mock((*mocks.MockMixCoordClient).GetQueryViewLoadInfo).Return(
		&querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil).Build()
	defer lookup.UnPatch()
	streams := make([]*fakeLegacySearchStream, 3)
	legacy := &fakeLegacyQueryClient{maxRetries: len(streams)}
	legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
		stream := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{
			err: streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED, "attempt %d", attempt),
		}}}
		streams[attempt-1] = stream
		return &queryclient.LegacySearchResult{Stream: stream}, nil
	}
	task := newQueryViewSearchTask(t, legacy, 1)
	task.node = &namespaceRequeryMockNode{}

	err := task.Execute(context.Background())
	require.ErrorIs(t, err, merr.ErrQueryViewNotReady)
	require.Contains(t, err.Error(), "attempt 3")
	require.EqualValues(t, 1, lookup.Times())
	require.Equal(t, 3, legacy.searchCalled)
	for _, stream := range streams {
		require.Equal(t, 1, stream.closeCalls)
	}
}

func TestQueryViewUnavailableRetryBudget(t *testing.T) {
	for _, query := range []bool{false, true} {
		for _, canceled := range []bool{false, true} {
			t.Run(fmt.Sprintf("query_%t/canceled_%t", query, canceled), func(t *testing.T) {
				mixCoord := mockey.Mock((*namespaceRequeryMockNode).MixCoord).Return(&mocks.MockMixCoordClient{}).Build()
				defer mixCoord.UnPatch()
				lookup := mockey.Mock((*mocks.MockMixCoordClient).GetQueryViewLoadInfo).Return(
					&querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil).Build()
				defer lookup.UnPatch()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				failure := status.Error(codes.Unavailable, "worker disconnected")
				legacy := &fakeLegacyQueryClient{maxRetries: 2}
				var queryStreams []*fakeLegacyQueryStream
				var searchStreams []*fakeLegacySearchStream
				legacy.query = func(int) (*queryclient.LegacyQueryResult, error) {
					if canceled {
						cancel()
					}
					stream := &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{err: failure}}}
					queryStreams = append(queryStreams, stream)
					return &queryclient.LegacyQueryResult{Stream: stream}, nil
				}
				legacy.search = func(int) (*queryclient.LegacySearchResult, error) {
					if canceled {
						cancel()
					}
					stream := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{err: failure}}}
					searchStreams = append(searchStreams, stream)
					return &queryclient.LegacySearchResult{Stream: stream}, nil
				}
				var err error
				if query {
					task := newQueryViewQueryTask(t, legacy, 2)
					task.node = &namespaceRequeryMockNode{}
					err = task.Execute(ctx)
					require.Empty(t, task.resultBuf.Collect())
				} else {
					task := newQueryViewSearchTask(t, legacy, 2)
					task.node = &namespaceRequeryMockNode{}
					err = task.Execute(ctx)
					require.Nil(t, task.reducedResult)
				}
				wantAttempts := 2
				if canceled {
					wantAttempts = 1
					require.ErrorIs(t, err, context.Canceled)
					require.Zero(t, lookup.Times())
				} else {
					require.ErrorIs(t, err, merr.ErrServiceUnavailable)
					require.Equal(t, merr.Status(merr.ErrServiceUnavailable).GetCode(), merr.Status(err).GetCode())
					require.EqualValues(t, 1, lookup.Times())
				}
				require.Equal(t, wantAttempts, legacy.queryCalled+legacy.searchCalled)
				for _, stream := range queryStreams {
					require.Equal(t, 1, stream.closeCalls)
				}
				for _, stream := range searchStreams {
					require.Equal(t, 1, stream.closeCalls)
				}
			})
		}
	}
}

func TestSearchTaskDoesNotAddRequestRetryToBatchPath(t *testing.T) {
	legacy := &fakeLegacyQueryClient{
		maxRetries: 3,
		err:        merr.WrapErrServiceInternalMsg("batch failed after its own retries"),
	}
	task := newQueryViewSearchTask(t, legacy, 1)
	require.NoError(t, paramtable.Get().Save(paramtable.Get().ProxyCfg.EnableReduceStream.Key, "false"))

	require.Error(t, task.Execute(context.Background()))
	require.Equal(t, 1, legacy.searchCalled)
}

func TestQueryTaskStopsAtRequestedWindow(t *testing.T) {
	for _, limit := range []int64{3, typeutil.Unlimited} {
		offset := int64(0)
		end := fakeLegacyQueryRecv{err: io.EOF}
		if limit > 0 {
			offset = 2
			end.err = io.ErrUnexpectedEOF // An extra Recv after the fifth row must fail the test.
		}
		first := newFinalQueryChunk([]int64{1, 2}, 10)
		last := newFinalQueryChunk([]int64{3, 4, 5, 6}, 20)
		stream := &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: first}, {chunk: last}, end}}
		task := newQueryViewQueryTask(t, &fakeLegacyQueryClient{
			queryResult: &queryclient.LegacyQueryResult{Stream: stream},
		}, offset+limit)
		require.NoError(t, task.Execute(t.Context()))
		require.Equal(t, 1, stream.closeCalls)
		output := task.resultBuf.Collect()
		require.Len(t, output, 1)
		require.Equal(t, uint64(20), output[0].GetMvccTimestamp())
		if limit > 0 {
			require.Equal(t, []int64{1, 2, 3, 4, 5}, output[0].GetIds().GetIntId().GetData())
			require.Equal(t, 2, stream.next)
		} else {
			require.Equal(t, 3, stream.next)
		}
		op := queryutil.NewSortAndCheckPKPageOperator(0, nil, limit, offset)
		got, err := op.Run(t.Context(), nil, output)
		require.NoError(t, err)
		batch, err := op.Run(t.Context(), nil, []*internalpb.RetrieveResults{first, last})
		require.NoError(t, err)
		wantPage, gotPage := batch[0].(*internalpb.RetrieveResults), got[0].(*internalpb.RetrieveResults)
		require.Equal(t, wantPage.GetIds().GetIntId().GetData(), gotPage.GetIds().GetIntId().GetData())
		require.Len(t, gotPage.GetFieldsData(), 1)
		require.True(t, proto.Equal(wantPage.GetFieldsData()[0], gotPage.GetFieldsData()[0]))
	}
}

func TestQueryTaskOrderByDrainsBeyondPage(t *testing.T) {
	for _, failTail := range []bool{false, true} {
		stream := &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{
			{chunk: newFinalQueryChunk([]int64{1, 2}, 10)},
			{chunk: newFinalQueryChunk([]int64{3, 4}, 20)},
			{err: io.EOF},
		}}
		if failTail {
			stream.recv[2].err = merr.WrapErrDataIntegrityMsg("duplicate PK 1")
		}
		legacy := &fakeLegacyQueryClient{
			queryResult: &queryclient.LegacyQueryResult{Stream: stream},
		}
		task := newQueryViewQueryTask(t, legacy, 2)
		task.OrderByFields = []*planpb.OrderByField{{Ascending: true}}
		err := task.Execute(t.Context())
		require.True(t, legacy.lastQueryRequest.AllowStreaming)
		if failTail {
			require.ErrorContains(t, err, "duplicate PK 1")
			require.Empty(t, task.resultBuf.Collect())
		} else {
			require.NoError(t, err)
			output := task.resultBuf.Collect()
			require.Len(t, output, 1)
			require.Equal(t, []int64{1, 2}, output[0].GetIds().GetIntId().GetData())
			require.EqualValues(t, 20, output[0].GetMvccTimestamp())
			page, err := queryutil.NewSliceOperator(1, 1).Run(t.Context(), nil, output[0])
			require.NoError(t, err)
			require.Equal(t, []int64{2}, page[0].(*internalpb.RetrieveResults).GetIds().GetIntId().GetData())
		}
		require.Equal(t, 3, stream.next)
		require.Equal(t, 1, stream.closeCalls)
	}
}

func TestQueryTaskUnlimitedStreamEnforcesMaxOutputSize(t *testing.T) {
	params := paramtable.Get()
	key := params.QuotaConfig.MaxOutputSize.Key
	old := params.QuotaConfig.MaxOutputSize.GetValue()
	t.Cleanup(func() { params.Save(key, old) })
	params.Save(key, "1")
	stream := &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: newFinalQueryChunk([]int64{1}, 10)}}}
	task := newQueryViewQueryTask(t, &fakeLegacyQueryClient{queryResult: &queryclient.LegacyQueryResult{Stream: stream}}, typeutil.Unlimited)
	require.ErrorContains(t, task.Execute(t.Context()), "maxOutputSize")
	require.Equal(t, 1, stream.closeCalls)
	require.Empty(t, task.resultBuf.Collect())
}

func TestQueryTaskGlobalAggregationBatchParity(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "val", DataType: schemapb.DataType_Int64, Nullable: true},
	}}
	for _, outputs := range [][]string{{"count(*)"}, {"count(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}} {
		for _, shape := range []string{"values", "zero", "null", "empty", "no_children"} {
			for _, chunkBytes := range []int{1, 1 << 20} {
				t.Run(fmt.Sprintf("%v/%s/bytes=%d", outputs, shape, chunkBytes), func(t *testing.T) {
					legacy := &fakeLegacyQueryClient{}
					task := newQueryViewQueryTask(t, legacy, typeutil.Unlimited)
					task.schema = mustNewSchemaInfo(schema)
					task.queryParams = &queryParams{limit: typeutil.Unlimited}
					task.request.Expr, task.request.OutputFields = "pk >= 0", outputs
					require.NoError(t, task.createPlan(t.Context()))
					require.NotEmpty(t, task.GetAggregates()) // COUNT(*) uses the same aggregate plan.
					require.False(t, task.GetIsCount())
					var batches []*internalpb.RetrieveResults
					var shards []queryreducestream.ReduceStream
					var workers []*fakeLegacyQueryStream
					for shard := 0; shard < 2; shard++ {
						var children []queryreducestream.ReduceStream
						if shape != "no_children" {
							for child := 0; child < 2; child++ {
								n := int64(1 + shard*2 + child)
								batch := &internalpb.RetrieveResults{Status: merr.Success(), AllRetrieveCount: n}
								if shape != "empty" {
									for _, operator := range task.GetAggregates() {
										value, valid := 30*n, shape != "null"
										if shape == "zero" || shape == "null" {
											value = 0
										}
										if operator.GetOp() == planpb.AggregateOp_count {
											value, valid = n+1, true
											if shape == "null" && outputs[0] != "count(*)" {
												value = 0
											}
										}
										field := makeTestInt64Field(101, "", []int64{value})
										typeutil.SetFieldDataValidData(field, []bool{valid})
										batch.FieldsData = append(batch.FieldsData, field)
									}
								}
								batches = append(batches, proto.Clone(batch).(*internalpb.RetrieveResults))
								chunks, err := queryreducestream.SplitRetrieveResult(batch, chunkBytes)
								require.NoError(t, err)
								worker := &fakeLegacyQueryStream{}
								for _, chunk := range chunks {
									worker.recv = append(worker.recv, fakeLegacyQueryRecv{chunk: chunk})
								}
								workers = append(workers, worker)
								children = append(children, worker)
							}
						}
						stream, err := queryreducestream.NewShardReduceStream(task.RetrieveRequest, schema, children, chunkBytes)
						require.NoError(t, err)
						require.IsType(t, &queryreducestream.UnOrderedReduceStream{}, stream)
						shards = append(shards, stream)
					}
					stream, err := queryreducestream.NewReduceStream(task.RetrieveRequest, schema, shards, chunkBytes)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, stream.Close()) })
					legacy.queryResult = &queryclient.LegacyQueryResult{Stream: stream}
					require.NoError(t, task.Execute(t.Context()))
					require.True(t, legacy.lastQueryRequest.AllowStreaming)
					output := task.resultBuf.Collect()
					require.Len(t, output, 1)
					require.Equal(t, 1, queryutil.GetRowCount(output[0]))
					wantCount := int64(10)
					if shape == "no_children" {
						wantCount = 0
					}
					require.Equal(t, wantCount, output[0].GetAllRetrieveCount())
					for _, worker := range workers {
						require.Equal(t, len(worker.recv), worker.next)
						require.Equal(t, 1, worker.closeCalls)
					}
					pipeline, err := NewQueryPipeline(schema, typeutil.Unlimited, 0, 0, nil, nil, task.GetAggregates(), task.aggregationFieldMap, nil)
					require.NoError(t, err)
					want, err := pipeline.Execute(t.Context(), batches)
					require.NoError(t, err)
					got, err := pipeline.Execute(t.Context(), output)
					require.NoError(t, err)
					require.True(t, proto.Equal(want, got), "batch=%v streaming=%v", want, got)
					for _, field := range got.GetFieldsData() {
						isCount := strings.HasPrefix(field.GetFieldName(), "count(")
						isNull := !isCount && (shape == "empty" || shape == "no_children" || shape == "null")
						if isNull {
							require.Equal(t, []bool{false}, typeutil.GetFieldDataValidData(field))
							continue
						}
						if field.GetFieldName() == "avg(val)" {
							expected := float64(0)
							if shape == "values" {
								expected = 300.0 / 14
							}
							require.InDelta(t, expected, field.GetScalars().GetDoubleData().GetData()[0], 1e-12)
						} else {
							expected := int64(0)
							if isCount && (shape == "values" || shape == "zero" || (shape == "null" && field.GetFieldName() == "count(*)")) {
								expected = 14
							} else if shape == "values" {
								expected = map[string]int64{"sum(val)": 300, "min(val)": 30, "max(val)": 120}[field.GetFieldName()]
							}
							require.Equal(t, []int64{expected}, field.GetScalars().GetLongData().GetData())
						}
					}
				})
			}
		}
	}
}

func TestQueryTaskGroupedOrderByBatchParity(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 201, Name: "region", DataType: schemapb.DataType_Int64, Nullable: true},
		{FieldID: 202, Name: "city", DataType: schemapb.DataType_Int64},
		{FieldID: 203, Name: "value", DataType: schemapb.DataType_Int64, Nullable: true},
	}}
	for _, operation := range []string{"distinct", "count", "sum", "avg", "min", "max"} {
		for _, ascending := range []bool{false, true} {
			for _, chunkBytes := range []int{1, 1 << 20} {
				for _, offset := range []int64{0, 1, 10} {
					t.Run(fmt.Sprintf("%s/asc=%v/bytes=%d/offset=%d", operation, ascending, chunkBytes, offset), func(t *testing.T) {
						var operators []agg.AggregateBase
						names := []string{"city", "region"} // Output order differs from the raw group layout.
						if operation != "distinct" {
							name := operation + "(value)"
							var err error
							operators, err = agg.NewAggregate(operation, 203, name, schemapb.DataType_Int64)
							require.NoError(t, err)
							names = append(names, name)
						}
						outputMap, err := agg.NewAggregationFieldMap(names, []string{"region", "city"}, operators)
						require.NoError(t, err)
						direction := "desc"
						if ascending {
							direction = "asc"
						}
						orderFields, err := translateOrderByFields([]string{"region:" + direction}, []int64{201, 202}, schema)
						require.NoError(t, err)
						request := &internalpb.RetrieveRequest{
							Limit: 2 + offset, GroupByFieldIds: []int64{201, 202},
							Aggregates: agg.AggregatesToPB(operators), OrderByFields: orderFields,
						}
						var batches []*internalpb.RetrieveResults
						var shards []queryreducestream.ReduceStream
						var workers []*fakeLegacyQueryStream
						for shard := 0; shard < 2; shard++ {
							var children []queryreducestream.ReduceStream
							for child := 0; child < 2; child++ {
								n := int64(1 + shard*2 + child)
								region := makeTestInt64Field(201, "region", []int64{1, 1, 2, 0})
								typeutil.SetFieldDataValidData(region, []bool{true, true, true, false})
								fields := []*schemapb.FieldData{region, makeTestInt64Field(202, "city", []int64{2, 1, 1, 1})}
								for _, operator := range operators {
									values := makeTestInt64Field(203, "value", []int64{10 * n, 20 * n, 0, 0})
									typeutil.SetFieldDataValidData(values, []bool{true, true, true, false})
									if operator.ToPB().GetOp() == planpb.AggregateOp_count {
										values = makeTestInt64Field(203, "count", []int64{n + 1, n + 2, 1, 0})
									}
									fields = append(fields, values)
								}
								batch := &internalpb.RetrieveResults{Status: merr.Success(), FieldsData: fields, AllRetrieveCount: n}
								batches = append(batches, proto.Clone(batch).(*internalpb.RetrieveResults))
								chunks, err := queryreducestream.SplitRetrieveResult(batch, chunkBytes)
								require.NoError(t, err)
								worker := &fakeLegacyQueryStream{}
								for _, chunk := range chunks {
									worker.recv = append(worker.recv, fakeLegacyQueryRecv{chunk: chunk})
								}
								workers = append(workers, worker)
								children = append(children, worker)
							}
							stream, err := queryreducestream.NewShardReduceStream(request, schema, children, chunkBytes)
							require.NoError(t, err)
							shards = append(shards, stream)
						}
						stream, err := queryreducestream.NewReduceStream(request, schema, shards, chunkBytes)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, stream.Close()) })
						legacy := &fakeLegacyQueryClient{queryResult: &queryclient.LegacyQueryResult{Stream: stream}}
						task := newQueryViewQueryTask(t, legacy, request.Limit)
						task.GroupByFieldIds, task.Aggregates, task.OrderByFields = request.GroupByFieldIds, request.Aggregates, request.OrderByFields
						task.schema.CollectionSchema = schema
						require.NoError(t, task.Execute(t.Context()))
						require.True(t, legacy.lastQueryRequest.AllowStreaming)
						output := task.resultBuf.Collect()
						require.Len(t, output, 1)
						require.Equal(t, 4, queryutil.GetRowCount(output[0])) // Never truncate before ordering.
						require.EqualValues(t, 10, output[0].GetAllRetrieveCount())
						require.Equal(t, request.Limit, task.GetLimit())
						for _, worker := range workers {
							require.Equal(t, len(worker.recv), worker.next)
							require.Equal(t, 1, worker.closeCalls)
						}
						ordering, err := orderby.ConvertFromPlanOrderByFields(orderFields, schema)
						require.NoError(t, err)
						pipeline, err := NewQueryPipeline(schema, 2, offset, 0, ordering, request.GroupByFieldIds, request.Aggregates, outputMap, nil)
						require.NoError(t, err)
						want, err := pipeline.Execute(t.Context(), batches)
						require.NoError(t, err)
						got, err := pipeline.Execute(t.Context(), output)
						require.NoError(t, err)
						require.True(t, proto.Equal(want, got), "batch=%v streaming=%v", want, got)
					})
				}
			}
		}
	}
}

func TestQueryTaskGroupedOrderByDrainErrors(t *testing.T) {
	for _, tail := range []error{io.EOF, io.ErrUnexpectedEOF, context.Canceled} {
		stream := &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{
			{chunk: newFinalQueryChunk([]int64{3, 2}, 10)},
			{chunk: newFinalQueryChunk([]int64{1}, 20)},
			{err: tail},
		}}
		for _, item := range stream.recv[:2] {
			item.chunk.Ids = nil
			item.chunk.FieldsData[0].FieldId = 100
		}
		legacy := &fakeLegacyQueryClient{queryResult: &queryclient.LegacyQueryResult{Stream: stream}}
		task := newQueryViewQueryTask(t, legacy, 1)
		task.GroupByFieldIds = []int64{100}
		task.OrderByFields = []*planpb.OrderByField{{FieldId: 100, Ascending: true}}
		err := task.Execute(t.Context())
		if tail == io.EOF {
			require.NoError(t, err)
			require.Equal(t, []int64{3, 2, 1}, task.resultBuf.Collect()[0].GetFieldsData()[0].GetScalars().GetLongData().GetData())
		} else {
			require.ErrorIs(t, err, tail)
			require.Empty(t, task.resultBuf.Collect())
		}
		require.Equal(t, 3, stream.next)
		require.Equal(t, 1, stream.closeCalls)
		require.Equal(t, 1, legacy.queryCalled)
	}
}

func TestQueryTaskGroupedOrderByEmptyStream(t *testing.T) {
	task := newQueryViewQueryTask(t, nil, 1)
	task.GroupByFieldIds = []int64{101}
	task.OrderByFields = []*planpb.OrderByField{{FieldId: 101, Ascending: true}}
	stream, err := queryreducestream.NewReduceStream(task.RetrieveRequest, testSchema(), nil, 1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	output, err := task.consumeQueryResultStream(stream)
	require.NoError(t, err)
	require.Zero(t, queryutil.GetRowCount(output))
	outputMap, err := agg.NewAggregationFieldMap([]string{"val"}, []string{"val"}, nil)
	require.NoError(t, err)
	ordering, err := orderby.ConvertFromPlanOrderByFields(task.OrderByFields, testSchema())
	require.NoError(t, err)
	pipeline, err := NewQueryPipeline(testSchema(), 1, 0, 0, ordering, task.GroupByFieldIds, nil, outputMap, nil)
	require.NoError(t, err)
	result, err := pipeline.Execute(t.Context(), []*internalpb.RetrieveResults{output})
	require.NoError(t, err)
	require.Len(t, result.GetFieldsData(), 1)
	require.Empty(t, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
}

func TestQueryTaskConsumesElementChunks(t *testing.T) {
	first := newFinalQueryChunk([]int64{1}, 10)
	first.ElementLevel = true
	first.ElementIndices = []*internalpb.ElementIndices{{Indices: []int32{0, 2, 4}}}
	second := newFinalQueryChunk([]int64{2}, 10)
	second.ElementLevel = true
	second.ElementIndices = []*internalpb.ElementIndices{{Indices: []int32{1, 3}}}
	task := newQueryViewQueryTask(t, nil, 4)
	output, err := task.consumeQueryResultStream(&fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{
		{chunk: &internalpb.RetrieveResults{Status: merr.Success(), ElementLevel: true}},
		{chunk: first}, {chunk: second}, {err: io.ErrUnexpectedEOF},
	}})
	require.NoError(t, err)
	require.True(t, output.GetElementLevel())
	require.Equal(t, []int64{1, 2}, output.GetIds().GetIntId().GetData())
	require.Equal(t, []*internalpb.ElementIndices{first.ElementIndices[0], second.ElementIndices[0]}, output.ElementIndices)
	for _, page := range [][2]int64{{4, 0}, {4, 4}, {3, 1}, {1, 3}, {1, 6}} {
		op := queryutil.NewSortAndCheckPKPageOperator(0, nil, page[0], page[1])
		got, err := op.Run(context.Background(), nil, []*internalpb.RetrieveResults{proto.Clone(output).(*internalpb.RetrieveResults)})
		require.NoError(t, err)
		batch, err := op.Run(context.Background(), nil, []*internalpb.RetrieveResults{first, second})
		require.NoError(t, err)
		require.True(t, proto.Equal(batch[0].(*internalpb.RetrieveResults), got[0].(*internalpb.RetrieveResults)),
			"page=%v batch=%v streaming=%v", page, batch[0], got[0])
	}
	for _, inconsistent := range []bool{false, true} {
		invalid := proto.Clone(second).(*internalpb.RetrieveResults)
		if inconsistent {
			invalid.ElementLevel = false
		} else {
			invalid.ElementIndices = nil
		}
		_, err := task.consumeQueryResultStream(&fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: first}, {chunk: invalid}}})
		require.ErrorContains(t, err, "element")
	}
}

func TestQueryTaskRetriesCompleteStreamAttempt(t *testing.T) {
	for _, code := range []viewpb.ViewCode{
		viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED,
		viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND,
		viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN,
		viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY,
	} {
		t.Run(code.String(), func(t *testing.T) {
			failed := &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{
				{chunk: newFinalQueryChunk([]int64{1}, 1)},
				{err: streamViewError(code, "retry Query")},
			}}
			replacement := &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{
				{chunk: newFinalQueryChunk([]int64{2, 3}, 2)},
				{err: io.EOF},
			}}
			legacy := &fakeLegacyQueryClient{maxRetries: 2}
			legacy.query = func(attempt int) (*queryclient.LegacyQueryResult, error) {
				if attempt == 1 {
					return &queryclient.LegacyQueryResult{Stream: failed}, nil
				}
				return &queryclient.LegacyQueryResult{Stream: replacement}, nil
			}
			task := newQueryViewQueryTask(t, legacy, 2)

			require.NoError(t, task.Execute(context.Background()))
			require.Equal(t, 2, legacy.queryCalled)
			require.Equal(t, 1, failed.closeCalls)
			require.Equal(t, 1, replacement.closeCalls)
			require.Len(t, task.resultBuf.Collect(), 1)
			result := task.resultBuf.Collect()[0]
			require.Equal(t, []int64{2, 3}, result.GetIds().GetIntId().GetData())
			require.Equal(t, uint64(2), result.GetMvccTimestamp())
		})
	}
}

func TestQueryTaskRetriesUnavailableStream(t *testing.T) {
	for _, afterChunk := range []bool{false, true} {
		t.Run(fmt.Sprintf("after_chunk_%t", afterChunk), func(t *testing.T) {
			failed := &fakeLegacyQueryStream{}
			if afterChunk {
				failed.recv = append(failed.recv, fakeLegacyQueryRecv{chunk: newFinalQueryChunk([]int64{1}, 1)})
			}
			failed.recv = append(failed.recv, fakeLegacyQueryRecv{err: status.Error(codes.Unavailable, "worker unavailable")})
			replacement := &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: newFinalQueryChunk([]int64{2, 3}, 2)}, {err: io.EOF}}}
			legacy := &fakeLegacyQueryClient{maxRetries: 2}
			legacy.query = func(attempt int) (*queryclient.LegacyQueryResult, error) {
				if attempt == 1 {
					return &queryclient.LegacyQueryResult{Stream: failed}, nil
				}
				return &queryclient.LegacyQueryResult{Stream: replacement}, nil
			}
			task := newQueryViewQueryTask(t, legacy, 2)
			require.NoError(t, task.Execute(t.Context()))
			require.Equal(t, 2, legacy.queryCalled)
			require.Len(t, legacy.lastQueryRequest.RetryErrors, 1)
			require.Equal(t, codes.Unavailable, status.Code(legacy.lastQueryRequest.RetryErrors[0]))
			require.Equal(t, 1, failed.closeCalls)
			require.Equal(t, 1, replacement.closeCalls)
			require.Len(t, task.resultBuf.Collect(), 1)
			result := task.resultBuf.Collect()[0]
			require.Equal(t, []int64{2, 3}, result.GetIds().GetIntId().GetData())
			require.Equal(t, uint64(2), result.GetMvccTimestamp())
		})
	}
}

func TestQueryTaskDoesNotRetryNonRetryableStreamError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		chunk *internalpb.RetrieveResults
	}{
		{name: "unknown before Chunk", err: streamViewError(viewpb.ViewCode_VIEW_CODE_UNKNOWN, "unknown")},
		{name: "unknown after Chunk", err: streamViewError(viewpb.ViewCode_VIEW_CODE_UNKNOWN, "unknown"), chunk: newFinalQueryChunk([]int64{1}, 1)},
		{name: "canceled", err: context.Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recv := make([]fakeLegacyQueryRecv, 0, 2)
			if tc.chunk != nil {
				recv = append(recv, fakeLegacyQueryRecv{chunk: tc.chunk})
			}
			recv = append(recv, fakeLegacyQueryRecv{err: tc.err})
			stream := &fakeLegacyQueryStream{recv: recv}
			legacy := &fakeLegacyQueryClient{
				maxRetries:  3,
				queryResult: &queryclient.LegacyQueryResult{Stream: stream},
			}
			task := newQueryViewQueryTask(t, legacy, 2)

			require.Error(t, task.Execute(context.Background()))
			require.Equal(t, 1, legacy.queryCalled)
			require.Equal(t, 1, stream.closeCalls)
			require.Empty(t, task.resultBuf.Collect())
		})
	}
}
