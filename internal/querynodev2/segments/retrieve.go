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
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/metricsutil"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

// retrieveOnSegments performs retrieve on listed segments
// all segment ids are validated before calling this function
func retrieveOnSegments(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, plan *RetrievePlan) ([]*segcorepb.RetrieveResults, error) {
	label := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		label = metrics.GrowingSegmentLabel
	}

	resultCh := make(chan *segcorepb.RetrieveResults, len(segments))
	retriever := func(ctx context.Context, s Segment) error {
		tr := timerecord.NewTimeRecorder("retrieveOnSegments")
		result, err := s.Retrieve(ctx, plan)
		if err != nil {
			return err
		}
		resultCh <- result
		metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			metrics.QueryLabel, label).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return nil
	}

	errGroup, ctx := errgroup.WithContext(ctx)
	for _, segment := range segments {
		seg := segment
		errGroup.Go(func() error {
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
					return err
				}
				var missing bool
				missing, err = mgr.DiskCache.DoWait(ctx, seg.ID(), timeout, retriever)
				if missing {
					accessRecord.CacheMissing()
				}
				return err
			}
			return retriever(ctx, seg)
		})
	}
	err := errGroup.Wait()
	close(resultCh)

	if err != nil {
		return nil, err
	}

	var retrieveResults []*segcorepb.RetrieveResults
	for result := range resultCh {
		retrieveResults = append(retrieveResults, result)
	}

	return retrieveResults, nil
}

func retrieveOnSegmentsWithStream(ctx context.Context, segments []Segment, segType SegmentType, plan *RetrievePlan, svr streamrpc.QueryStreamServer) error {
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
			result, err := segment.Retrieve(ctx, plan)
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
						TotalRelatedDataSize: segment.MemSize(),
					},
					AllRetrieveCount: result.GetAllRetrieveCount(),
				}); err != nil {
					errs[i] = err
				}
			}

			errs[i] = nil
			metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
				metrics.QueryLabel, label).Observe(float64(tr.ElapseSpan().Milliseconds()))
		}(segment, i)
	}
	wg.Wait()
	return merr.Combine(errs...)
}

// retrieve will retrieve all the validate target segments
func Retrieve(ctx context.Context, manager *Manager, plan *RetrievePlan, req *querypb.QueryRequest) ([]*segcorepb.RetrieveResults, []Segment, error) {
	var err error
	var SegType commonpb.SegmentState
	var retrieveResults []*segcorepb.RetrieveResults
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
		return retrieveResults, retrieveSegments, err
	}

	retrieveResults, err = retrieveOnSegments(ctx, manager, retrieveSegments, SegType, plan)
	return retrieveResults, retrieveSegments, err
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
