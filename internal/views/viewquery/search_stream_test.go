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

package viewquery

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSearchOnViewStreamSendsChunksAndEOF(t *testing.T) {
	tasks := &streamTestSearchTasks{tasks: []SearchSegmentTask{struct{}{}}}
	provider := &streamTestProvider{searchTasks: tasks}
	scheduler := &streamTestScheduler{result: streamTestResult()}
	server := NewServer(provider, scheduler)

	client, cleanup := startSearchStreamTestServer(t, server)
	defer cleanup()
	request := streamTestRequest()
	request.StreamChunkSize = 2
	stream, err := searchutil.NewGRPCReduceStream(context.Background(), client, request)
	require.NoError(t, err)

	first := recvSearchStreamChunk(t, stream)
	assertStreamChunk(t, first, []int64{1, 2}, []int64{2, 0})
	require.Equal(t, int64(100), first.GetCostAggregation().GetTotalRelatedDataSize())
	require.Equal(t, map[string]uint64{"channel-a": 1000}, first.GetChannelsMvcc())
	require.Equal(t, int64(30), first.GetScannedRemoteBytes())
	require.Equal(t, int64(40), first.GetScannedTotalBytes())
	require.Equal(t, int64(50), first.GetResultData().GetAllSearchCount())

	second := recvSearchStreamChunk(t, stream)
	assertStreamChunk(t, second, []int64{3, 10}, []int64{1, 1})
	require.Nil(t, second.GetCostAggregation())
	require.Empty(t, second.GetChannelsMvcc())
	require.Zero(t, second.GetScannedRemoteBytes())
	require.Zero(t, second.GetScannedTotalBytes())
	require.Zero(t, second.GetResultData().GetAllSearchCount())
	assertStreamChunk(t, recvSearchStreamChunk(t, stream), []int64{11}, []int64{0, 1})

	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, stream.Close())
	require.Equal(t, 1, tasks.releaseCount)
	require.Equal(t, int64(10), provider.request.GetCollectionID())
}

func TestSearchOnViewStreamSendsEmptyChunkWithMetadata(t *testing.T) {
	result := &internalpb.SearchResults{
		Status:             merr.Success(),
		MetricType:         "IP",
		NumQueries:         2,
		TopK:               3,
		CostAggregation:    &internalpb.CostAggregation{TotalRelatedDataSize: 100},
		ChannelsMvcc:       map[string]uint64{"channel-a": 1000},
		ScannedRemoteBytes: 30,
		ScannedTotalBytes:  40,
		ResultData: &schemapb.SearchResultData{
			NumQueries:     2,
			TopK:           3,
			Topks:          []int64{0, 0},
			Ids:            &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}},
			AllSearchCount: 50,
		},
	}
	server := NewServer(
		&streamTestProvider{searchTasks: &streamTestSearchTasks{tasks: []SearchSegmentTask{struct{}{}}}},
		&streamTestScheduler{result: result},
	)
	client, cleanup := startSearchStreamTestServer(t, server)
	defer cleanup()
	stream, err := searchutil.NewGRPCReduceStream(context.Background(), client, streamTestRequest())
	require.NoError(t, err)

	chunk := recvSearchStreamChunk(t, stream)
	require.Empty(t, chunk.GetResultData().GetIds().GetIntId().GetData())
	require.Equal(t, []int64{0, 0}, chunk.GetResultData().GetTopks())
	require.Equal(t, int64(100), chunk.GetCostAggregation().GetTotalRelatedDataSize())
	require.Equal(t, map[string]uint64{"channel-a": 1000}, chunk.GetChannelsMvcc())
	require.Equal(t, int64(30), chunk.GetScannedRemoteBytes())
	require.Equal(t, int64(40), chunk.GetScannedTotalBytes())
	require.Equal(t, int64(50), chunk.GetResultData().GetAllSearchCount())

	chunk, err = stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, stream.Close())
}

func TestSearchOnViewStreamReturnsServerError(t *testing.T) {
	server := NewServer(&streamTestProvider{
		searchErr: viewerror.NewViewNotFound("missing view"),
	}, &streamTestScheduler{})
	client, cleanup := startSearchStreamTestServer(t, server)
	defer cleanup()
	stream, err := searchutil.NewGRPCReduceStream(context.Background(), client, streamTestRequest())
	require.NoError(t, err)

	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestSearchOnViewStreamAcceptsPlainANNRequest(t *testing.T) {
	server := NewServer(
		&streamTestProvider{searchTasks: &streamTestSearchTasks{tasks: []SearchSegmentTask{struct{}{}}}},
		&streamTestScheduler{result: streamTestResult()},
	)
	client, cleanup := startSearchStreamTestServer(t, server)
	defer cleanup()

	request := streamTestRequest()
	request.LegacyReq.IsIterator = false
	stream, err := searchutil.NewGRPCReduceStream(context.Background(), client, request)
	require.NoError(t, err)

	chunk := recvSearchStreamChunk(t, stream)
	assertStreamChunk(t, chunk, []int64{1, 2, 3, 10, 11}, []int64{3, 2})
	chunk, err = stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, stream.Close())
}

func TestSearchOnViewStreamRejectsUnsupportedSearch(t *testing.T) {
	server := NewServer(&streamTestProvider{}, &streamTestScheduler{})
	client, cleanup := startSearchStreamTestServer(t, server)
	defer cleanup()

	request := streamTestRequest()
	request.LegacyReq.GroupByFieldId = 100
	stream, err := searchutil.NewGRPCReduceStream(context.Background(), client, request)
	require.NoError(t, err)

	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSearchOnViewStreamCloseCancelsServer(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	server := NewServer(
		&streamTestProvider{searchTasks: &streamTestSearchTasks{tasks: []SearchSegmentTask{struct{}{}}}},
		&streamBlockingScheduler{started: started, stopped: stopped},
	)
	client, cleanup := startSearchStreamTestServer(t, server)
	defer cleanup()
	stream, err := searchutil.NewGRPCReduceStream(context.Background(), client, streamTestRequest())
	require.NoError(t, err)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("SearchOnViewStream server did not start")
	}
	require.NoError(t, stream.Close())

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("SearchOnViewStream server was not canceled")
	}
}

func startSearchStreamTestServer(t *testing.T, service viewpb.ViewQueryServiceServer) (viewpb.ViewQueryServiceClient, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	viewpb.RegisterViewQueryServiceServer(grpcServer, service)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	connection, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	cancel()
	require.NoError(t, err)

	return viewpb.NewViewQueryServiceClient(connection), func() {
		_ = connection.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}
}

func recvSearchStreamChunk(t *testing.T, stream searchutil.ReduceStream) *internalpb.SearchResults {
	t.Helper()
	chunk, err := stream.Recv()
	require.NoError(t, err)
	return chunk
}

func assertStreamChunk(t *testing.T, chunk *internalpb.SearchResults, ids, topks []int64) {
	t.Helper()
	require.True(t, merr.Ok(chunk.GetStatus()))
	require.Equal(t, ids, chunk.GetResultData().GetIds().GetIntId().GetData())
	require.Equal(t, topks, chunk.GetResultData().GetTopks())
}

func streamTestRequest() *viewpb.SearchOnViewRequest {
	return &viewpb.SearchOnViewRequest{
		LegacyReq: &internalpb.SearchRequest{
			CollectionID: 10,
			Nq:           2,
			Topk:         3,
			MetricType:   "IP",
			IsIterator:   true,
		},
		ShardId: &viewpb.ShardID{ReplicaId: 1, Vchannel: "by-dev-rootcoord-dml_0_100v0"},
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 2},
			QueryVersion: 3,
		},
		Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 10, TransformingTimetick: 9},
	}
}

func streamTestResult() *internalpb.SearchResults {
	return &internalpb.SearchResults{
		Status:             merr.Success(),
		MetricType:         "IP",
		NumQueries:         2,
		TopK:               3,
		CostAggregation:    &internalpb.CostAggregation{TotalRelatedDataSize: 100},
		ChannelsMvcc:       map[string]uint64{"channel-a": 1000},
		ScannedRemoteBytes: 30,
		ScannedTotalBytes:  40,
		ResultData: &schemapb.SearchResultData{
			NumQueries:     2,
			TopK:           3,
			Topks:          []int64{3, 2},
			AllSearchCount: 50,
			Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 10, 11}},
			}},
			Scores: []float32{0.9, 0.8, 0.7, 0.95, 0.85},
		},
	}
}

type streamTestProvider struct {
	searchTasks SearchSegmentTasks
	searchErr   error
	request     *internalpb.SearchRequest
}

func (p *streamTestProvider) AcquireSearchSegmentTasks(
	_ context.Context,
	_ qviews.ShardID,
	_ qviews.QueryViewVersion,
	_ *viewpb.QueryPlanMVCC,
	request *internalpb.SearchRequest,
) (SearchSegmentTasks, error) {
	p.request = request
	if p.searchErr != nil {
		return nil, p.searchErr
	}
	return p.searchTasks, nil
}

func (p *streamTestProvider) AcquireQuerySegmentTasks(
	context.Context,
	qviews.ShardID,
	qviews.QueryViewVersion,
	*viewpb.QueryPlanMVCC,
	*internalpb.RetrieveRequest,
) (QuerySegmentTasks, error) {
	return nil, nil
}

type streamTestSearchTasks struct {
	tasks        []SearchSegmentTask
	releaseCount int
}

func (t *streamTestSearchTasks) Tasks() []SearchSegmentTask {
	return t.tasks
}

func (t *streamTestSearchTasks) Release() {
	t.releaseCount++
}

type streamTestScheduler struct {
	result *internalpb.SearchResults
}

func (s *streamTestScheduler) Search(context.Context, SearchSegmentTasks) (*internalpb.SearchResults, error) {
	return s.result, nil
}

func (s *streamTestScheduler) Query(context.Context, QuerySegmentTasks) (*internalpb.RetrieveResults, error) {
	return nil, nil
}

type streamBlockingScheduler struct {
	started chan struct{}
	stopped chan struct{}
}

func (s *streamBlockingScheduler) Search(ctx context.Context, _ SearchSegmentTasks) (*internalpb.SearchResults, error) {
	close(s.started)
	<-ctx.Done()
	close(s.stopped)
	return nil, ctx.Err()
}

func (s *streamBlockingScheduler) Query(context.Context, QuerySegmentTasks) (*internalpb.RetrieveResults, error) {
	return nil, nil
}
