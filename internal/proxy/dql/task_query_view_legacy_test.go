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
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type fakeLegacyViewQueryClient struct {
	legacy queryclient.LegacyClient
}

func (c *fakeLegacyViewQueryClient) Close() {}

func (c *fakeLegacyViewQueryClient) Legacy() queryclient.LegacyClient {
	return c.legacy
}

type fakeLegacyQueryClient struct {
	queryclient.LegacyClient
	searchCalled        int
	queryCalled         int
	maxRetries          int
	reduceStreamEnabled bool
	searchResult        *queryclient.LegacySearchResult
	queryResult         *queryclient.LegacyQueryResult
	err                 error
	search              func(int) (*queryclient.LegacySearchResult, error)
	query               func(int) (*queryclient.LegacyQueryResult, error)
}

func (c *fakeLegacyQueryClient) Search(context.Context, *queryclient.LegacySearchRequest) (*queryclient.LegacySearchResult, error) {
	c.searchCalled++
	if c.search != nil {
		return c.search(c.searchCalled)
	}
	return c.searchResult, c.err
}

func (c *fakeLegacyQueryClient) Query(context.Context, *queryclient.LegacyQueryRequest) (*queryclient.LegacyQueryResult, error) {
	c.queryCalled++
	if c.query != nil {
		return c.query(c.queryCalled)
	}
	return c.queryResult, c.err
}

func (c *fakeLegacyQueryClient) MaxRetries() int {
	return max(1, c.maxRetries)
}

func (c *fakeLegacyQueryClient) ReduceStreamEnabled() bool {
	return c.reduceStreamEnabled
}

type fakeLegacySearchRecv struct {
	chunk *internalpb.SearchResults
	err   error
}

type fakeLegacySearchStream struct {
	recv       []fakeLegacySearchRecv
	next       int
	closeCalls int
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
	return nil
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
	return nil
}

func (*fakeLegacyQueryStream) Interrupt() (*internalpb.RetrieveResults, error) {
	return nil, nil
}

var _ searchutil.ReduceStream = (*fakeLegacySearchStream)(nil)
var _ queryutil.ReduceStream = (*fakeLegacyQueryStream)(nil)

func newQueryViewSearchTask(legacy queryclient.LegacyClient, topK int64) *SearchTask {
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

func newQueryViewQueryTask(legacy queryclient.LegacyClient, limit int64) *QueryTask {
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
	task := newQueryViewQueryTask(legacy, 0)

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
	task := newQueryViewSearchTask(legacy, 1)

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
		reduceStreamEnabled: true,
		searchResult:        &queryclient.LegacySearchResult{Stream: stream},
	}
	task := newQueryViewSearchTask(legacy, 1)

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
			legacy := &fakeLegacyQueryClient{maxRetries: 2, reduceStreamEnabled: true}
			legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
				if attempt == 1 {
					return &queryclient.LegacySearchResult{Stream: failed}, nil
				}
				return &queryclient.LegacySearchResult{Stream: replacement}, nil
			}
			task := newQueryViewSearchTask(legacy, 2)

			require.NoError(t, task.Execute(context.Background()))
			require.Equal(t, 2, legacy.searchCalled)
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
	legacy := &fakeLegacyQueryClient{maxRetries: 2, reduceStreamEnabled: true}
	legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
		if attempt == 1 {
			return nil, streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, "re-plan")
		}
		return &queryclient.LegacySearchResult{Stream: replacement}, nil
	}
	task := newQueryViewSearchTask(legacy, 1)

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
	legacy := &fakeLegacyQueryClient{maxRetries: 2, reduceStreamEnabled: true}
	legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
		if attempt == 1 {
			return &queryclient.LegacySearchResult{Stream: failed}, nil
		}
		return &queryclient.LegacySearchResult{Stream: replacement}, nil
	}
	task := newQueryViewSearchTask(legacy, 1)

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
		{name: "raw unavailable", err: status.Error(codes.Unavailable, "transport unavailable")},
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
				maxRetries:          3,
				reduceStreamEnabled: true,
				searchResult:        &queryclient.LegacySearchResult{Stream: stream},
			}
			task := newQueryViewSearchTask(legacy, 2)

			require.Error(t, task.Execute(context.Background()))
			require.Equal(t, 1, legacy.searchCalled)
			require.Equal(t, 1, stream.closeCalls)
			require.Nil(t, task.reducedResult)
		})
	}
}

func TestSearchTaskRetryExhaustionReturnsLastFailure(t *testing.T) {
	streams := make([]*fakeLegacySearchStream, 3)
	legacy := &fakeLegacyQueryClient{maxRetries: len(streams), reduceStreamEnabled: true}
	legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
		stream := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{
			err: streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED, "attempt %d", attempt),
		}}}
		streams[attempt-1] = stream
		return &queryclient.LegacySearchResult{Stream: stream}, nil
	}
	task := newQueryViewSearchTask(legacy, 1)

	err := task.Execute(context.Background())
	require.Equal(t, "attempt 3", viewerror.AsViewError(err).Cause)
	require.Equal(t, 3, legacy.searchCalled)
	for _, stream := range streams {
		require.Equal(t, 1, stream.closeCalls)
	}
}

func TestSearchTaskDoesNotAddRequestRetryToBatchPath(t *testing.T) {
	legacy := &fakeLegacyQueryClient{
		maxRetries: 3,
		err:        merr.WrapErrServiceInternalMsg("batch failed after its own retries"),
	}
	task := newQueryViewSearchTask(legacy, 1)

	require.Error(t, task.Execute(context.Background()))
	require.Equal(t, 1, legacy.searchCalled)
}

func TestQueryTaskConsumesElementChunks(t *testing.T) {
	first := newFinalQueryChunk([]int64{1}, 10)
	first.ElementLevel = true
	first.ElementIndices = []*internalpb.ElementIndices{{Indices: []int32{0, 2, 4}}}
	second := newFinalQueryChunk([]int64{2}, 10)
	second.ElementLevel = true
	second.ElementIndices = []*internalpb.ElementIndices{{Indices: []int32{1, 3}}}
	task := newQueryViewQueryTask(nil, 4)
	output, err := task.consumeQueryResultStream(&fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{
		{chunk: &internalpb.RetrieveResults{Status: merr.Success(), ElementLevel: true}},
		{chunk: first}, {chunk: second},
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
			legacy := &fakeLegacyQueryClient{maxRetries: 2, reduceStreamEnabled: true}
			legacy.query = func(attempt int) (*queryclient.LegacyQueryResult, error) {
				if attempt == 1 {
					return &queryclient.LegacyQueryResult{Stream: failed}, nil
				}
				return &queryclient.LegacyQueryResult{Stream: replacement}, nil
			}
			task := newQueryViewQueryTask(legacy, 2)

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

func TestQueryTaskDoesNotRetryNonRetryableStreamError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		chunk *internalpb.RetrieveResults
	}{
		{name: "unknown before Chunk", err: streamViewError(viewpb.ViewCode_VIEW_CODE_UNKNOWN, "unknown")},
		{name: "unknown after Chunk", err: streamViewError(viewpb.ViewCode_VIEW_CODE_UNKNOWN, "unknown"), chunk: newFinalQueryChunk([]int64{1}, 1)},
		{name: "raw unavailable", err: status.Error(codes.Unavailable, "transport unavailable")},
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
				maxRetries:          3,
				reduceStreamEnabled: true,
				queryResult:         &queryclient.LegacyQueryResult{Stream: stream},
			}
			task := newQueryViewQueryTask(legacy, 2)

			require.Error(t, task.Execute(context.Background()))
			require.Equal(t, 1, legacy.queryCalled)
			require.Equal(t, 1, stream.closeCalls)
			require.Empty(t, task.resultBuf.Collect())
		})
	}
}
