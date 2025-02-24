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

// delegator package contains the logic of shard delegator.
package cluster

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// Worker is the interface definition for querynode worker role.
type Worker interface {
	LoadSegments(context.Context, *querypb.LoadSegmentsRequest) error
	ReleaseSegments(context.Context, *querypb.ReleaseSegmentsRequest) error
	Delete(ctx context.Context, req *querypb.DeleteRequest) error
	DeleteBatch(ctx context.Context, req *querypb.DeleteBatchRequest) (*querypb.DeleteBatchResponse, error)
	SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error)
	QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error)
	QueryStreamSegments(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) error
	GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error)

	IsHealthy() bool
	Stop()
}

// remoteWorker wraps grpc QueryNode client as Worker.
type remoteWorker struct {
	client   types.QueryNodeClient
	clients  []types.QueryNodeClient
	poolSize int
	idx      atomic.Int64
	pooling  bool
}

// NewRemoteWorker creates a grpcWorker.
func NewRemoteWorker(client types.QueryNodeClient) Worker {
	return &remoteWorker{
		client:  client,
		pooling: false,
	}
}

func NewPoolingRemoteWorker(fn func() (types.QueryNodeClient, error)) (Worker, error) {
	num := paramtable.Get().QueryNodeCfg.WorkerPoolingSize.GetAsInt()
	if num <= 0 {
		num = 1
	}
	clients := make([]types.QueryNodeClient, 0, num)
	for i := 0; i < num; i++ {
		c, err := fn()
		if err != nil {
			return nil, err
		}
		clients = append(clients, c)
	}
	return &remoteWorker{
		pooling:  true,
		clients:  clients,
		poolSize: num,
	}, nil
}

func (w *remoteWorker) getClient() types.QueryNodeClient {
	if w.pooling {
		idx := w.idx.Inc()
		return w.clients[int(idx)%w.poolSize]
	}
	return w.client
}

// LoadSegments implements Worker.
func (w *remoteWorker) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) error {
	log := log.Ctx(ctx).With(
		zap.Int64("workerID", req.GetDstNodeID()),
	)
	client := w.getClient()
	status, err := client.LoadSegments(ctx, req)
	if err = merr.CheckRPCCall(status, err); err != nil {
		log.Warn("failed to call LoadSegments via grpc worker",
			zap.Error(err),
		)
		return err
	}
	return nil
}

func (w *remoteWorker) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) error {
	log := log.Ctx(ctx).With(
		zap.Int64("workerID", req.GetNodeID()),
	)
	client := w.getClient()
	status, err := client.ReleaseSegments(ctx, req)
	if err = merr.CheckRPCCall(status, err); err != nil {
		log.Warn("failed to call ReleaseSegments via grpc worker",
			zap.Error(err),
		)
		return err
	}
	return nil
}

func (w *remoteWorker) Delete(ctx context.Context, req *querypb.DeleteRequest) error {
	log := log.Ctx(ctx).With(
		zap.Int64("workerID", req.GetBase().GetTargetID()),
	)
	client := w.getClient()
	status, err := client.Delete(ctx, req)
	if err := merr.CheckRPCCall(status, err); err != nil {
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			log.Warn("invoke legacy querynode Delete method, ignore error", zap.Error(err))
			return nil
		}
		log.Warn("failed to call Delete, worker return error", zap.Error(err))
		return err
	}
	return nil
}

func (w *remoteWorker) DeleteBatch(ctx context.Context, req *querypb.DeleteBatchRequest) (*querypb.DeleteBatchResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("workerID", req.GetBase().GetTargetID()),
	)
	client := w.getClient()
	resp, err := client.DeleteBatch(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			log.Warn("invoke legacy querynode DeleteBatch method, fallback to ")
			return w.splitDeleteBatch(ctx, req)
		}
		return nil, err
	}
	return resp, nil
}

func (w *remoteWorker) splitDeleteBatch(ctx context.Context, req *querypb.DeleteBatchRequest) (*querypb.DeleteBatchResponse, error) {
	sReq := &querypb.DeleteRequest{
		CollectionId: req.GetCollectionId(),
		PartitionId:  req.GetPartitionId(),
		VchannelName: req.GetVchannelName(),
		PrimaryKeys:  req.GetPrimaryKeys(),
		Timestamps:   req.GetTimestamps(),
		Scope:        req.GetScope(),
	}
	// do fallback without parallel, to protect the mem limit
	var missingIDs []int64
	var failedIDs []int64
	for _, segmentID := range req.GetSegmentIds() {
		sReq.SegmentId = segmentID
		err := w.Delete(ctx, sReq)
		switch {
		case errors.Is(err, merr.ErrSegmentNotFound):
			missingIDs = append(missingIDs, segmentID)
		case err != nil:
			failedIDs = append(failedIDs, segmentID)
		default:
		}
	}
	return &querypb.DeleteBatchResponse{
		Status:     merr.Success(),
		FailedIds:  failedIDs,
		MissingIds: missingIDs,
	}, nil
}

func (w *remoteWorker) SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	client := w.getClient()
	ret, err := client.SearchSegments(ctx, req)
	if err != nil && errors.Is(err, merr.ErrServiceUnimplemented) {
		// for compatible with rolling upgrade from version before v2.2.9
		return client.Search(ctx, req)
	}

	return ret, err
}

func (w *remoteWorker) QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	client := w.getClient()
	ret, err := client.QuerySegments(ctx, req)
	if err != nil && errors.Is(err, merr.ErrServiceUnimplemented) {
		// for compatible with rolling upgrade from version before v2.2.9
		return client.Query(ctx, req)
	}

	return ret, err
}

func (w *remoteWorker) QueryStreamSegments(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) error {
	c := w.getClient()
	client, err := c.QueryStreamSegments(ctx, req)
	if err != nil {
		return err
	}

	for {
		result, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		err = merr.Error(result.GetStatus())
		if err != nil {
			return err
		}

		err = srv.Send(result)
		if err != nil {
			log.Warn("send stream pks from remote woker failed",
				zap.Int64("collectionID", req.Req.GetCollectionID()),
				zap.Int64s("segmentIDs", req.GetSegmentIDs()),
			)
			return err
		}
	}
}

func (w *remoteWorker) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	client := w.getClient()
	return client.GetStatistics(ctx, req)
}

func (w *remoteWorker) IsHealthy() bool {
	return true
}

func (w *remoteWorker) Stop() {
	if w.pooling {
		for _, client := range w.clients {
			client.Close()
		}
		return
	}
	if err := w.client.Close(); err != nil {
		log.Warn("failed to call Close via grpc worker", zap.Error(err))
	}
}
