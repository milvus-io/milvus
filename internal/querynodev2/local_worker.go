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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
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

	log.Info("save loaded segments...",
		zap.Int64s("segments", lo.Map(loaded, func(s segments.Segment, _ int) int64 { return s.ID() })))
	w.node.manager.Segment.Put(segments.SegmentTypeSealed, loaded...)
	return nil
}

func (w *LocalWorker) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) error {
	log.Info("start to release segments")
	for _, id := range req.GetSegmentIDs() {
		w.node.manager.Segment.Remove(id, req.GetScope())
	}
	return nil
}

func (w *LocalWorker) Delete(ctx context.Context, req *querypb.DeleteRequest) error {
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

func (w *LocalWorker) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	return w.node.Search(ctx, req)
}

func (w *LocalWorker) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return w.node.Query(ctx, req)
}

func (w *LocalWorker) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	return w.node.GetStatistics(ctx, req)
}

func (w *LocalWorker) IsHealthy() bool {
	return true
}

func (w *LocalWorker) Stop() {
}
