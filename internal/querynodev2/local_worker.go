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

package querynodev2

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/resource"
)

var _ cluster.Worker = &LocalWorker{}

type LocalWorker struct {
	node *QueryNode
}

func NewLocalWorker(node *QueryNode) *LocalWorker {
	return &LocalWorker{
		node: node,
	}
}

func (w *LocalWorker) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) error {
	status, err := w.node.LoadSegments(ctx, req)
	return merr.CheckRPCCall(status, err)
}

func (w *LocalWorker) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) error {
	status, err := w.node.ReleaseSegments(ctx, req)
	return merr.CheckRPCCall(status, err)
}

func (w *LocalWorker) Delete(ctx context.Context, req *querypb.DeleteRequest) error {
	status, err := w.node.Delete(ctx, req)
	return merr.CheckRPCCall(status, err)
}

func (w *LocalWorker) DeleteBatch(ctx context.Context, req *querypb.DeleteBatchRequest) (*querypb.DeleteBatchResponse, error) {
	return w.node.DeleteBatch(ctx, req)
}

func (w *LocalWorker) SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	resp, err := w.node.SearchSegments(ctx, req)
	if err != nil {
		return nil, err
	}
	return consumeLocalSearchResults(resp)
}

func consumeLocalSearchResults(resp *internalpb.SearchResults) (*internalpb.SearchResults, error) {
	defer resource.MsgPins.Release(resp)
	// The gRPC codec (releaseCodec) never runs for in-process calls, so any C
	// memory pinned in MsgPins will never be triggered by Marshal. We must
	// consume top-level and grouped branch blobs here, then release the final
	// response after every C-backed slice has been materialized into Go memory.
	if blob := resp.GetSlicedBlob(); len(blob) > 0 {
		var resultData schemapb.SearchResultData
		if unmarshalErr := proto.Unmarshal(blob, &resultData); unmarshalErr != nil {
			return nil, merr.WrapErrServiceInternalErr(unmarshalErr, "unmarshal SearchResultData from SlicedBlob")
		}
		resp.ResultData = &resultData
		resp.SlicedBlob = nil
	}
	for i, subResult := range resp.GetSubResults() {
		if blob := subResult.GetSlicedBlob(); len(blob) > 0 {
			var resultData schemapb.SearchResultData
			if unmarshalErr := proto.Unmarshal(blob, &resultData); unmarshalErr != nil {
				return nil, merr.WrapErrServiceInternalErr(
					unmarshalErr,
					"unmarshal grouped SearchResultData at branch %d from SlicedBlob",
					i,
				)
			}
			subResult.ResultData = &resultData
			subResult.SlicedBlob = nil
		}
	}
	return resp, nil
}

func (w *LocalWorker) QueryStreamSegments(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) error {
	return w.node.queryStreamSegments(ctx, req, srv)
}

func (w *LocalWorker) QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return w.node.QuerySegments(ctx, req)
}

func (w *LocalWorker) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	return w.node.GetStatistics(ctx, req)
}

func (w *LocalWorker) UpdateSchema(ctx context.Context, req *querypb.UpdateSchemaRequest) (*commonpb.Status, error) {
	return w.node.UpdateSchema(ctx, req)
}

func (w *LocalWorker) IsHealthy() bool {
	return true
}

func (w *LocalWorker) DropIndex(ctx context.Context, req *querypb.DropIndexRequest) error {
	status, err := w.node.DropIndex(ctx, req)
	return merr.CheckRPCCall(status, err)
}

func (w *LocalWorker) Stop() {
}
