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

package segments

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/metricsutil"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// retrieveOnSegments performs retrieve on listed segments
// all segment ids are validated before calling this function
func retrieveOnSegments(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, plan *RetrievePlan, req *querypb.QueryRequest) ([]*segcorepb.RetrieveResults, []Segment, error) {
	type segmentResult struct {
		result  *segcorepb.RetrieveResults
		segment Segment
	}
	var (
		futures  = make([]*conc.Future[any], 0, len(segments))
		resultCh = make(chan segmentResult, len(segments))
	)

	plan.ignoreNonPk = len(segments) > 1 && req.GetReq().GetLimit() != typeutil.Unlimited && plan.ShouldIgnoreNonPk()

	label := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		label = metrics.GrowingSegmentLabel
	}

	retriever := func(ctx context.Context, s Segment) error {
		tr := timerecord.NewTimeRecorder("retrieveOnSegments")
		result, err := s.Retrieve(ctx, plan)
		if err != nil {
			return err
		}
		resultCh <- segmentResult{
			result,
			s,
		}
		metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			metrics.QueryLabel, label).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return nil
	}

	for _, segment := range segments {
		seg := segment
		future := GetSQPool().Submit(func() (any, error) {
			if ctx.Err() != nil {
				return ctx.Err(), ctx.Err()
			}
			// record search time and cache miss
			var err error
			accessRecord := metricsutil.NewQuerySegmentAccessRecord(getSegmentMetricLabel(seg))
			defer func() {
				accessRecord.Finish(err)
			}()

			if seg.IsLazyLoad() {
				var timeout time.Duration
				timeout, err = lazyloadWaitTimeout(ctx)
				if err != nil {
					return err, err
				}
				var missing bool
				missing, err = mgr.DiskCache.DoWait(ctx, seg.ID(), timeout, retriever)
				if missing {
					accessRecord.CacheMissing()
				}
				return err, err
			}
			err = retriever(ctx, seg)
			return err, err
		})
		futures = append(futures, future)
	}
	err := conc.AwaitAll(futures...)
	close(resultCh)

	if err != nil {
		return nil, nil, err
	}

	var retrieveSegments []Segment
	var retrieveResults []*segcorepb.RetrieveResults
	for result := range resultCh {
		retrieveSegments = append(retrieveSegments, result.segment)
		retrieveResults = append(retrieveResults, result.result)
	}

	return retrieveResults, retrieveSegments, nil
}

func retrieveOnSegmentsWithStream(ctx context.Context, segments []Segment, segType SegmentType, plan *RetrievePlan, svr streamrpc.QueryStreamServer) error {
	futures := make([]*conc.Future[any], 0, len(segments))

	label := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		label = metrics.GrowingSegmentLabel
	}

	for _, segment := range segments {
		segment := segment
		future := GetSQPool().Submit(func() (any, error) {
			tr := timerecord.NewTimeRecorder("retrieveOnSegmentsWithStream")
			result, err := segment.Retrieve(ctx, plan)
			if err != nil {
				return err, err
			}

			if len(result.GetOffset()) != 0 {
				if err = svr.Send(&internalpb.RetrieveResults{
					Status:     merr.Success(),
					Ids:        result.GetIds(),
					FieldsData: result.GetFieldsData(),
					CostAggregation: &internalpb.CostAggregation{
						TotalRelatedDataSize: segment.MemSize(),
					},
					AllRetrieveCount: result.GetAllRetrieveCount(),
				}); err != nil {
					return err, err
				}
			}

			metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
				metrics.QueryLabel, label).Observe(float64(tr.ElapseSpan().Milliseconds()))
			return nil, nil
		})
		futures = append(futures, future)
	}
	return conc.AwaitAll(futures...)
}

// retrieve will retrieve all the validate target segments
func Retrieve(ctx context.Context, manager *Manager, plan *RetrievePlan, req *querypb.QueryRequest) ([]*segcorepb.RetrieveResults, []Segment, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	var err error
	var SegType commonpb.SegmentState
	var retrieveSegments []Segment

	segIDs := req.GetSegmentIDs()
	collID := req.Req.GetCollectionID()
	log.Debug("retrieve on segments", zap.Int64s("segmentIDs", segIDs), zap.Int64("collectionID", collID))

	if req.GetScope() == querypb.DataScope_Historical {
		SegType = SegmentTypeSealed
		retrieveSegments, err = validateOnHistorical(ctx, manager, collID, nil, segIDs)
	} else {
		SegType = SegmentTypeGrowing
		retrieveSegments, err = validateOnStream(ctx, manager, collID, nil, segIDs)
	}

	if err != nil {
		return nil, nil, err
	}

	return retrieveOnSegments(ctx, manager, retrieveSegments, SegType, plan, req)
}

// retrieveStreaming will retrieve all the validate target segments  and  return by stream
func RetrieveStream(ctx context.Context, manager *Manager, plan *RetrievePlan, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) ([]Segment, error) {
	var err error
	var SegType commonpb.SegmentState
	var retrieveSegments []Segment

	segIDs := req.GetSegmentIDs()
	collID := req.Req.GetCollectionID()

	if req.GetScope() == querypb.DataScope_Historical {
		SegType = SegmentTypeSealed
		retrieveSegments, err = validateOnHistorical(ctx, manager, collID, nil, segIDs)
	} else {
		SegType = SegmentTypeGrowing
		retrieveSegments, err = validateOnStream(ctx, manager, collID, nil, segIDs)
	}

	if err != nil {
		return retrieveSegments, err
	}

	err = retrieveOnSegmentsWithStream(ctx, retrieveSegments, SegType, plan, srv)
	return retrieveSegments, err
}
