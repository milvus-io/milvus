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

	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
	return w.node.SearchSegments(ctx, req)
}

func (w *LocalWorker) QueryStreamSegments(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) error {
	return w.node.queryStreamSegments(ctx, req, srv)
}

func (w *LocalWorker) QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return w.node.QuerySegments(ctx, req)
}

func (w *LocalWorker) QuerySegmentsOffset(ctx context.Context, req *querypb.QueryOffsetsRequest) (*internalpb.RetrieveResults, error) {
	return w.node.QuerySegmentsOffset(ctx, req)
}

func (w *LocalWorker) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	return w.node.GetStatistics(ctx, req)
}

func (w *LocalWorker) IsHealthy() bool {
	return true
}

func (w *LocalWorker) Stop() {
}
