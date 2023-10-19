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
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// Worker is the interface definition for querynode worker role.
type Worker interface {
	LoadSegments(context.Context, *querypb.LoadSegmentsRequest) error
	ReleaseSegments(context.Context, *querypb.ReleaseSegmentsRequest) error
	Delete(ctx context.Context, req *querypb.DeleteRequest) error
	SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error)
	QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error)
	QueryStreamSegments(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) error
	GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error)

	IsHealthy() bool
	Stop()
}

// remoteWorker wraps grpc QueryNode client as Worker.
type remoteWorker struct {
	client types.QueryNodeClient
}

// NewRemoteWorker creates a grpcWorker.
func NewRemoteWorker(client types.QueryNodeClient) Worker {
	return &remoteWorker{
		client: client,
	}
}

// LoadSegments implements Worker.
func (w *remoteWorker) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) error {
	log := log.Ctx(ctx).With(
		zap.Int64("workerID", req.GetDstNodeID()),
	)
	status, err := w.client.LoadSegments(ctx, req)
	if err != nil {
		log.Warn("failed to call LoadSegments via grpc worker",
			zap.Error(err),
		)
		return err
	} else if status.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("failed to call LoadSegments, worker return error",
			zap.String("errorCode", status.GetErrorCode().String()),
			zap.String("reason", status.GetReason()),
		)
		return fmt.Errorf(status.Reason)
	}
	return nil
}

func (w *remoteWorker) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) error {
	log := log.Ctx(ctx).With(
		zap.Int64("workerID", req.GetNodeID()),
	)
	status, err := w.client.ReleaseSegments(ctx, req)
	if err != nil {
		log.Warn("failed to call ReleaseSegments via grpc worker",
			zap.Error(err),
		)
		return err
	} else if status.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("failed to call ReleaseSegments, worker return error",
			zap.String("errorCode", status.GetErrorCode().String()),
			zap.String("reason", status.GetReason()),
		)
		return fmt.Errorf(status.Reason)
	}
	return nil
}

func (w *remoteWorker) Delete(ctx context.Context, req *querypb.DeleteRequest) error {
	log := log.Ctx(ctx).With(
		zap.Int64("workerID", req.GetBase().GetTargetID()),
	)
	status, err := w.client.Delete(ctx, req)
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

func (w *remoteWorker) SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	ret, err := w.client.SearchSegments(ctx, req)
	if err != nil && errors.Is(err, merr.ErrServiceUnimplemented) {
		// for compatible with rolling upgrade from version before v2.2.9
		return w.client.Search(ctx, req)
	}

	return ret, err
}

func (w *remoteWorker) QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	ret, err := w.client.QuerySegments(ctx, req)
	if err != nil && errors.Is(err, merr.ErrServiceUnimplemented) {
		// for compatible with rolling upgrade from version before v2.2.9
		return w.client.Query(ctx, req)
	}

	return ret, err
}

func (w *remoteWorker) QueryStreamSegments(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) error {
	client, err := w.client.QueryStreamSegments(ctx, req)
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
	return w.client.GetStatistics(ctx, req)
}

func (w *remoteWorker) IsHealthy() bool {
	return true
}

func (w *remoteWorker) Stop() {
	if err := w.client.Close(); err != nil {
		log.Warn("failed to call Close via grpc worker", zap.Error(err))
	}
}
