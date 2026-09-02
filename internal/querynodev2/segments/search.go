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

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/metricsutil"
	segcoreutil "github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type searchResultsCleanup func([]*SearchResult)

// searchOnSegments performs search on listed segments
// all segment ids are validated before calling this function
func searchSegments(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, searchReq *SearchRequest) ([]*SearchResult, error) {
	return searchSegmentsWithRetry(ctx, mgr, segments, segType, searchReq, DeleteSearchResults, waitSegmentReadGateRetry)
}

func searchSegmentsWithRetry(
	ctx context.Context,
	mgr *Manager,
	segments []Segment,
	segType SegmentType,
	searchReq *SearchRequest,
	cleanup searchResultsCleanup,
	waitRetry segmentReadGateRetryWait,
) ([]*SearchResult, error) {
	grouped, err := searchSegmentsGroupedWithRetry(ctx, mgr, segments, segType,
		[]*SearchRequest{searchReq}, cleanup, waitRetry)
	if err != nil {
		return nil, err
	}
	return grouped[0], nil
}

// searchSegmentsGrouped runs several branches that share one filter predicate
// over the given segments. The result is branch-major -- results[b][i] is
// branch b's result on segments[i] -- because that is the shape the reduce
// pipeline consumes, one branch at a time.
func searchSegmentsGrouped(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, searchReqs []*SearchRequest) ([][]*SearchResult, error) {
	return searchSegmentsGroupedWithRetry(ctx, mgr, segments, segType, searchReqs, DeleteSearchResults, waitSegmentReadGateRetry)
}

func searchSegmentsGroupedWithRetry(
	ctx context.Context,
	mgr *Manager,
	segments []Segment,
	segType SegmentType,
	searchReqs []*SearchRequest,
	cleanup searchResultsCleanup,
	waitRetry segmentReadGateRetryWait,
) ([][]*SearchResult, error) {
	retryCount := 0
	for {
		searchResults, err := searchSegmentsGroupedAttempt(ctx, mgr, segments, segType, searchReqs)
		if err == nil {
			return searchResults, nil
		}

		validResults := make([]*SearchResult, 0, len(segments)*len(searchReqs))
		for _, perBranch := range searchResults {
			for _, result := range perBranch {
				if result != nil {
					validResults = append(validResults, result)
				}
			}
		}
		cleanup(validResults)

		if segType != SegmentTypeSealed || !segcoreutil.IsSegmentReadGateBusy(err) {
			return nil, err
		}

		retryCount++
		mlog.Debug(ctx, "retry sealed segment search after publish gate rejection",
			mlog.Int("retryCount", retryCount))
		if err := waitRetry(ctx, retryCount); err != nil {
			return nil, err
		}
	}
}

func searchSegmentsGroupedAttempt(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, searchReqs []*SearchRequest) ([][]*SearchResult, error) {
	searchLabel := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		searchLabel = metrics.GrowingSegmentLabel
	}

	nodeIDStr := paramtable.GetStringNodeID()
	// Branch-major so the caller can hand one branch's slice straight to the
	// reduce pipeline; the per-segment call still produces branch-minor.
	searchResults := make([][]*SearchResult, len(searchReqs))
	for b := range searchResults {
		searchResults[b] = make([]*SearchResult, len(segments))
	}

	searcher := func(ctx context.Context, s Segment, idx int) error {
		tr := timerecord.NewTimeRecorder("searchOnSegments")
		results, err := s.SearchGrouped(ctx, searchReqs)
		if err != nil {
			return err
		}
		if len(results) != len(searchReqs) {
			return merr.WrapErrServiceInternalMsg("grouped search returned %d results for %d branches",
				len(results), len(searchReqs))
		}
		for b, result := range results {
			searchResults[b][idx] = result
		}
		elapsed := float64(tr.ElapseSpan().Microseconds()) / 1000.0
		metrics.QueryNodeSQSegmentLatency.WithLabelValues(nodeIDStr,
			metrics.SearchLabel, searchLabel).Observe(elapsed)
		metrics.QueryNodeSegmentSearchLatencyPerVector.WithLabelValues(nodeIDStr,
			metrics.SearchLabel, searchLabel).Observe(elapsed / float64(searchReqs[0].GetNumOfQuery()))
		return nil
	}

	executeSegment := func(ctx context.Context, seg Segment, idx int) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var err error
		accessRecord := metricsutil.NewSearchSegmentAccessRecord(getSegmentMetricLabel(seg))
		defer func() {
			accessRecord.Finish(err)
		}()

		return searcher(ctx, seg, idx)
	}

	segmentsWithoutIndex := make([]int64, 0, len(segments))
	for _, seg := range segments {
		if !seg.ExistIndex(searchReqs[0].SearchFieldID()) {
			segmentsWithoutIndex = append(segmentsWithoutIndex, seg.ID())
		}
	}

	var err error
	if len(segments) == 1 {
		// Single segment fast path: skip errgroup/goroutine overhead
		err = executeSegment(ctx, segments[0], 0)
	} else {
		errGroup, groupCtx := errgroup.WithContext(ctx)
		for i, segment := range segments {
			segIdx := i
			seg := segment
			errGroup.Go(func() error {
				return executeSegment(groupCtx, seg, segIdx)
			})
		}
		err = errGroup.Wait()
	}

	if err != nil {
		return searchResults, err
	}

	if len(segmentsWithoutIndex) > 0 {
		mlog.Debug(ctx, "search growing/sealed segments without indexes", mlog.Int64s("segmentIDs", segmentsWithoutIndex))
	}

	return searchResults, nil
}

// search will search on the historical segments the target segments in historical.
// if segIDs is not specified, it will search on all the historical segments speficied by partIDs.
// if segIDs is specified, it will only search on the segments specified by the segIDs.
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func SearchHistorical(ctx context.Context, manager *Manager, searchReq *SearchRequest, collID int64, partIDs []int64, segIDs []int64) ([]*SearchResult, []Segment, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	segments, err := validateOnHistorical(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return nil, nil, err
	}
	searchResults, err := searchSegments(ctx, manager, segments, SegmentTypeSealed, searchReq)
	return searchResults, segments, err
}

// searchStreaming will search all the target segments in streaming
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func SearchStreaming(ctx context.Context, manager *Manager, searchReq *SearchRequest, collID int64, partIDs []int64, segIDs []int64) ([]*SearchResult, []Segment, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	segments, err := validateOnStream(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return nil, nil, err
	}
	searchResults, err := searchSegments(ctx, manager, segments, SegmentTypeGrowing, searchReq)
	return searchResults, segments, err
}

// SearchHistoricalGrouped is SearchHistorical for a set of branches that share
// one filter predicate. Results are branch-major.
func SearchHistoricalGrouped(ctx context.Context, manager *Manager, searchReqs []*SearchRequest, collID int64, partIDs []int64, segIDs []int64) ([][]*SearchResult, []Segment, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	segments, err := validateOnHistorical(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return nil, nil, err
	}
	searchResults, err := searchSegmentsGrouped(ctx, manager, segments, SegmentTypeSealed, searchReqs)
	return searchResults, segments, err
}

// SearchStreamingGrouped is SearchStreaming for a set of branches that share
// one filter predicate. Results are branch-major.
func SearchStreamingGrouped(ctx context.Context, manager *Manager, searchReqs []*SearchRequest, collID int64, partIDs []int64, segIDs []int64) ([][]*SearchResult, []Segment, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	segments, err := validateOnStream(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return nil, nil, err
	}
	searchResults, err := searchSegmentsGrouped(ctx, manager, segments, SegmentTypeGrowing, searchReqs)
	return searchResults, segments, err
}
