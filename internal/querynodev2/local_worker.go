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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/samber/lo"
	"go.uber.org/zap"
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
	log := log.Ctx(ctx)
	log.Info("start to load segments...")
	loaded, err := w.node.loader.Load(ctx,
		req.GetCollectionID(),
		segments.SegmentTypeSealed,
		req.GetVersion(),
		req.GetInfos()...,
	)
	if err != nil {
		return err
	}

	w.node.manager.Collection.Ref(req.GetCollectionID(), uint32(len(loaded)))

	log.Info("load segments done...",
		zap.Int64s("segments", lo.Map(loaded, func(s segments.Segment, _ int) int64 { return s.ID() })))
	return err
}

func (w *LocalWorker) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) error {
	log := log.Ctx(ctx)
	log.Info("start to release segments")
	for _, id := range req.GetSegmentIDs() {
		w.node.manager.Segment.Remove(id, req.GetScope())
	}
	return nil
}

func (w *LocalWorker) Delete(ctx context.Context, req *querypb.DeleteRequest) error {
	log := log.Ctx(ctx)
	log.Info("start to process segment delete")
	status, err := w.node.Delete(ctx, req)
	if err != nil {
		return err
	}
	if status.GetErrorCode() != commonpb.ErrorCode_Success {
		return fmt.Errorf(status.GetReason())
	}
	return nil
}

func (w *LocalWorker) SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	return w.node.SearchSegments(ctx, req)
}

func (w *LocalWorker) QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return w.node.querySegments(ctx, req)
}

func (w *LocalWorker) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	return w.node.GetStatistics(ctx, req)
}

func (w *LocalWorker) IsHealthy() bool {
	return true
}

func (w *LocalWorker) Stop() {
}
