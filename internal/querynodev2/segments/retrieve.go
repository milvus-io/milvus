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
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type RetrieveSegmentResult struct {
	Result  *segcorepb.RetrieveResults
	Segment Segment
}

// retrieveOnSegments performs retrieve on listed segments
// all segment ids are validated before calling this function
func retrieveOnSegments(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, plan *RetrievePlan, req *querypb.QueryRequest) ([]RetrieveSegmentResult, error) {
	resultCh := make(chan RetrieveSegmentResult, len(segments))

	plan.SetIgnoreNonPk(len(segments) > 1 && req.GetReq().GetLimit() != typeutil.Unlimited && plan.ShouldIgnoreNonPk())

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

		log := log.Ctx(ctx)
		if log.Core().Enabled(zap.DebugLevel) && req.GetReq().GetIsCount() {
			allRetrieveCount := result.AllRetrieveCount
			countRet := result.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0]
			if allRetrieveCount != countRet {
				log.Debug("count segment done with delete",
					zap.Uint64("mvcc", req.GetReq().GetMvccTimestamp()),
					zap.String("channel", s.LoadInfo().GetInsertChannel()),
					zap.Int64("segmentID", s.ID()),
					zap.Int64("allRetrieveCount", allRetrieveCount),
					zap.Int64("countRet", countRet))
			} else {
				log.Debug("count segment done",
					zap.Uint64("mvcc", req.GetReq().GetMvccTimestamp()),
					zap.String("channel", s.LoadInfo().GetInsertChannel()),
					zap.Int64("segmentID", s.ID()),
					zap.Int64("allRetrieveCount", allRetrieveCount),
					zap.Int64("countRet", countRet))
			}
		}
		resultCh <- RetrieveSegmentResult{
			result,
			s,
		}
		metrics.QueryNodeSQSegmentLatency.WithLabelValues(paramtable.GetStringNodeID(),
			metrics.QueryLabel, label).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return nil
	}

	err := doOnSegments(ctx, mgr, segments, retriever)
	close(resultCh)
	if err != nil {
		return nil, err
	}

	results := make([]RetrieveSegmentResult, 0, len(segments))
	for r := range resultCh {
		results = append(results, r)
	}
	return results, nil
}

func retrieveOnSegmentsWithStream(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, plan *RetrievePlan, svr streamrpc.QueryStreamServer) error {
	var (
		errs = make([]error, len(segments))
		wg   sync.WaitGroup
	)

	label := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		label = metrics.GrowingSegmentLabel
	}

	for i, segment := range segments {
		wg.Add(1)
		go func(segment Segment, i int) {
			defer wg.Done()
			tr := timerecord.NewTimeRecorder("retrieveOnSegmentsWithStream")
			var result *segcorepb.RetrieveResults
			err := doOnSegment(ctx, mgr, segment, func(ctx context.Context, segment Segment) error {
				var err error
				result, err = segment.Retrieve(ctx, plan)
				return err
			})
			if err != nil {
				errs[i] = err
				return
			}

			if len(result.GetOffset()) != 0 {
				if err = svr.Send(&internalpb.RetrieveResults{
					Status:     merr.Success(),
					Ids:        result.GetIds(),
					FieldsData: result.GetFieldsData(),
					CostAggregation: &internalpb.CostAggregation{
						TotalRelatedDataSize: GetSegmentRelatedDataSize(segment),
					},
					SealedSegmentIDsRetrieved: []int64{segment.ID()},
					AllRetrieveCount:          result.GetAllRetrieveCount(),
					ScannedRemoteBytes:        result.GetScannedRemoteBytes(),
					ScannedTotalBytes:         result.GetScannedTotalBytes(),
				}); err != nil {
					errs[i] = err
				}
			}

			errs[i] = nil
			metrics.QueryNodeSQSegmentLatency.WithLabelValues(paramtable.GetStringNodeID(),
				metrics.QueryLabel, label).Observe(float64(tr.ElapseSpan().Milliseconds()))
		}(segment, i)
	}
	wg.Wait()
	return merr.Combine(errs...)
}

// retrieve will retrieve all the validate target segments
func Retrieve(ctx context.Context, manager *Manager, plan *RetrievePlan, req *querypb.QueryRequest, queryPlan *planpb.PlanNode) ([]RetrieveSegmentResult, []Segment, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	var err error
	var SegType commonpb.SegmentState
	var retrieveSegments []Segment

	segIDs := req.GetSegmentIDs()
	collID := req.Req.GetCollectionID()
	log := log.Ctx(ctx)
	log.Debug("retrieve on segments", zap.Int64s("segmentIDs", segIDs), zap.Int64("collectionID", collID))

	if req.GetScope() == querypb.DataScope_Historical {
		SegType = SegmentTypeSealed
		retrieveSegments, err = validateOnHistorical(ctx, manager, collID, req.GetReq().GetPartitionIDs(), segIDs, queryPlan)
	} else {
		SegType = SegmentTypeGrowing
		retrieveSegments, err = validateOnStream(ctx, manager, collID, req.GetReq().GetPartitionIDs(), segIDs, queryPlan)
	}

	if err != nil {
		return nil, retrieveSegments, err
	}

	result, err := retrieveOnSegments(ctx, manager, retrieveSegments, SegType, plan, req)
	return result, retrieveSegments, err
}

// retrieveStreaming will retrieve all the validate target segments  and  return by stream
func RetrieveStream(ctx context.Context, manager *Manager, plan *RetrievePlan, req *querypb.QueryRequest, queryPlan *planpb.PlanNode, srv streamrpc.QueryStreamServer) ([]Segment, error) {
	var err error
	var SegType commonpb.SegmentState
	var retrieveSegments []Segment

	segIDs := req.GetSegmentIDs()
	collID := req.Req.GetCollectionID()
	log.Ctx(ctx).Debug("retrieve stream on segments", zap.Int64s("segmentIDs", segIDs), zap.Int64("collectionID", collID))

	if req.GetScope() == querypb.DataScope_Historical {
		SegType = SegmentTypeSealed
		retrieveSegments, err = validateOnHistorical(ctx, manager, collID, req.GetReq().GetPartitionIDs(), segIDs, queryPlan)
	} else {
		SegType = SegmentTypeGrowing
		retrieveSegments, err = validateOnStream(ctx, manager, collID, req.GetReq().GetPartitionIDs(), segIDs, queryPlan)
	}

	if err != nil {
		return retrieveSegments, err
	}

	err = retrieveOnSegmentsWithStream(ctx, manager, retrieveSegments, SegType, plan, srv)
	return retrieveSegments, err
}
