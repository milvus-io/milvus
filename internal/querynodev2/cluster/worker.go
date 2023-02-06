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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"go.uber.org/zap"
)

// Worker is the interface definition for querynode worker role.
type Worker interface {
	LoadSegments(context.Context, *querypb.LoadSegmentsRequest) error
	ReleaseSegments(context.Context, *querypb.ReleaseSegmentsRequest) error
	Delete(ctx context.Context, req *querypb.DeleteRequest) error
	Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error)
	Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error)
	GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error)

	IsHealthy() bool
	Stop()
}

// remoteWorker wraps grpc QueryNode client as Worker.
type remoteWorker struct {
	client types.QueryNode
}

// NewRemoteWorker creates a grpcWorker.
func NewRemoteWorker(client types.QueryNode) Worker {
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
	} else if status.ErrorCode != commonpb.ErrorCode_Success {
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
	} else if status.ErrorCode != commonpb.ErrorCode_Success {
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
	if err != nil {
		log.Warn("failed to call Delete via grpc worker",
			zap.Error(err),
		)
		return err
	} else if status.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("failed to call Delete, worker return error",
			zap.String("errorCode", status.GetErrorCode().String()),
			zap.String("reason", status.GetReason()),
		)
		return fmt.Errorf(status.GetReason())
	}
	return nil
}

func (w *remoteWorker) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	return w.client.Search(ctx, req)
}

func (w *remoteWorker) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return w.client.Query(ctx, req)
}

func (w *remoteWorker) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	return w.client.GetStatistics(ctx, req)
}

func (w *remoteWorker) IsHealthy() bool {
	return true
}

func (w *remoteWorker) Stop() {
	w.client.Stop()
}
