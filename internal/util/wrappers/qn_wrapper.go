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

package wrappers

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type qnServerWrapper struct {
	types.QueryNode
}

func (qn *qnServerWrapper) Close() error {
	return nil
}

func (qn *qnServerWrapper) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return qn.QueryNode.GetComponentStates(ctx, in)
}

func (qn *qnServerWrapper) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return qn.QueryNode.GetTimeTickChannel(ctx, in)
}

func (qn *qnServerWrapper) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return qn.QueryNode.GetStatisticsChannel(ctx, in)
}

func (qn *qnServerWrapper) WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return qn.QueryNode.WatchDmChannels(ctx, in)
}

func (qn *qnServerWrapper) UnsubDmChannel(ctx context.Context, in *querypb.UnsubDmChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return qn.QueryNode.UnsubDmChannel(ctx, in)
}

func (qn *qnServerWrapper) LoadSegments(ctx context.Context, in *querypb.LoadSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return qn.QueryNode.LoadSegments(ctx, in)
}

func (qn *qnServerWrapper) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return qn.QueryNode.ReleaseCollection(ctx, in)
}

func (qn *qnServerWrapper) LoadPartitions(ctx context.Context, in *querypb.LoadPartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return qn.QueryNode.LoadPartitions(ctx, in)
}

func (qn *qnServerWrapper) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return qn.QueryNode.ReleasePartitions(ctx, in)
}

func (qn *qnServerWrapper) ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return qn.QueryNode.ReleaseSegments(ctx, in)
}

func (qn *qnServerWrapper) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	return qn.QueryNode.GetSegmentInfo(ctx, in)
}

func (qn *qnServerWrapper) SyncReplicaSegments(ctx context.Context, in *querypb.SyncReplicaSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return qn.QueryNode.SyncReplicaSegments(ctx, in)
}

func (qn *qnServerWrapper) GetStatistics(ctx context.Context, in *querypb.GetStatisticsRequest, opts ...grpc.CallOption) (*internalpb.GetStatisticsResponse, error) {
	return qn.QueryNode.GetStatistics(ctx, in)
}

func (qn *qnServerWrapper) Search(ctx context.Context, in *querypb.SearchRequest, opts ...grpc.CallOption) (*internalpb.SearchResults, error) {
	return qn.QueryNode.Search(ctx, in)
}

func (qn *qnServerWrapper) SearchSegments(ctx context.Context, in *querypb.SearchRequest, opts ...grpc.CallOption) (*internalpb.SearchResults, error) {
	return qn.QueryNode.SearchSegments(ctx, in)
}

func (qn *qnServerWrapper) Query(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	return qn.QueryNode.Query(ctx, in)
}

func (qn *qnServerWrapper) QueryStream(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.QueryNode_QueryStreamClient, error) {
	streamer := streamrpc.NewInMemoryStreamer[*internalpb.RetrieveResults](ctx, 16)

	go func() {
		qn.QueryNode.QueryStream(in, streamer)
		streamer.Close()
	}()

	return streamer, nil
}

func (qn *qnServerWrapper) QuerySegments(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	return qn.QueryNode.QuerySegments(ctx, in)
}

func (qn *qnServerWrapper) QuerySegmentsOffset(ctx context.Context, in *querypb.QueryOffsetsRequest, opts ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	return qn.QueryNode.QuerySegmentsOffset(ctx, in)
}

func (qn *qnServerWrapper) QueryStreamSegments(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.QueryNode_QueryStreamSegmentsClient, error) {
	streamer := streamrpc.NewInMemoryStreamer[*internalpb.RetrieveResults](ctx, 16)

	go func() {
		qn.QueryNode.QueryStreamSegments(in, streamer)
		streamer.Close()
	}()

	return streamer, nil
}

func (qn *qnServerWrapper) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return qn.QueryNode.ShowConfigurations(ctx, in)
}

// https://wiki.lfaidata.foundation/display/MIL/MEP+8+--+Add+metrics+for+proxy
func (qn *qnServerWrapper) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return qn.QueryNode.GetMetrics(ctx, in)
}

func (qn *qnServerWrapper) GetDataDistribution(ctx context.Context, in *querypb.GetDataDistributionRequest, opts ...grpc.CallOption) (*querypb.GetDataDistributionResponse, error) {
	return qn.QueryNode.GetDataDistribution(ctx, in)
}

func (qn *qnServerWrapper) SyncDistribution(ctx context.Context, in *querypb.SyncDistributionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return qn.QueryNode.SyncDistribution(ctx, in)
}

func (qn *qnServerWrapper) Delete(ctx context.Context, in *querypb.DeleteRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return qn.QueryNode.Delete(ctx, in)
}

func (qn *qnServerWrapper) DeleteBatch(ctx context.Context, in *querypb.DeleteBatchRequest, opts ...grpc.CallOption) (*querypb.DeleteBatchResponse, error) {
	return qn.QueryNode.DeleteBatch(ctx, in)
}

func WrapQueryNodeServerAsClient(qn types.QueryNode) types.QueryNodeClient {
	return &qnServerWrapper{
		QueryNode: qn,
	}
}
