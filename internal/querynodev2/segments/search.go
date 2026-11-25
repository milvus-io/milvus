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
	"github.com/milvus-io/milvus/internal/util/querymeter"
	segcoreutil "github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type searchResultsCleanup func([]*SearchResult)

// SearchReadUsage receives the planned column usage of a successful segment
// fan-out. A failed attempt leaves the accumulated value empty.
type SearchReadUsage struct {
	fields []int64
	Usage  *internalpb.QueryReadUsage
}

func NewSearchReadUsage(fields []int64) *SearchReadUsage {
	return &SearchReadUsage{fields: fields}
}

// searchOnSegments performs search on listed segments
// all segment ids are validated before calling this function
func searchSegments(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, searchReq *SearchRequest, metering ...*SearchReadUsage) ([]*SearchResult, error) {
	return searchSegmentsWithRetry(ctx, mgr, segments, segType, searchReq, DeleteSearchResults, waitSegmentReadGateRetry, metering...)
}

func searchSegmentsWithRetry(
	ctx context.Context,
	mgr *Manager,
	segments []Segment,
	segType SegmentType,
	searchReq *SearchRequest,
	cleanup searchResultsCleanup,
	waitRetry segmentReadGateRetryWait,
	metering ...*SearchReadUsage,
) ([]*SearchResult, error) {
	retryCount := 0
	for {
		searchResults, err := searchSegmentsAttempt(ctx, mgr, segments, segType, searchReq, metering...)
		if err == nil {
			return searchResults, nil
		}

		validResults := make([]*SearchResult, 0, len(searchResults))
		for _, result := range searchResults {
			if result != nil {
				validResults = append(validResults, result)
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

func searchSegmentsAttempt(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, searchReq *SearchRequest, metering ...*SearchReadUsage) ([]*SearchResult, error) {
	searchLabel := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		searchLabel = metrics.GrowingSegmentLabel
	}

	nodeIDStr := paramtable.GetStringNodeID()
	searchResults := make([]*SearchResult, len(segments))
	var meter *SearchReadUsage
	var parts []*internalpb.QueryReadUsage
	if len(metering) > 0 && metering[0] != nil {
		meter = metering[0]
		parts = make([]*internalpb.QueryReadUsage, len(segments))
	}
	searcher := func(ctx context.Context, s Segment, idx int) error {
		if meter != nil {
			usage, unlock, err := collectPlannedReadUsage(s, meter.fields)
			defer unlock()
			if err != nil {
				return err
			}
			parts[idx] = usage
		}
		tr := timerecord.NewTimeRecorder("searchOnSegments")
		searchResult, err := s.Search(ctx, searchReq)
		if err != nil {
			return err
		}
		searchResults[idx] = searchResult
		elapsed := float64(tr.ElapseSpan().Microseconds()) / 1000.0
		metrics.QueryNodeSQSegmentLatency.WithLabelValues(nodeIDStr,
			metrics.SearchLabel, searchLabel).Observe(elapsed)
		metrics.QueryNodeSegmentSearchLatencyPerVector.WithLabelValues(nodeIDStr,
			metrics.SearchLabel, searchLabel).Observe(elapsed / float64(searchReq.GetNumOfQuery()))
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
		if !seg.ExistIndex(searchReq.SearchFieldID()) {
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

	if meter != nil {
		usage := &internalpb.QueryReadUsage{}
		for _, part := range parts {
			if part != nil {
				if err := querymeter.AddReadUsage(usage, part); err != nil {
					return searchResults, err
				}
			}
		}
		meter.Usage = usage
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
func SearchHistorical(ctx context.Context, manager *Manager, searchReq *SearchRequest, collID int64, partIDs []int64, segIDs []int64, metering ...*SearchReadUsage) ([]*SearchResult, []Segment, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	segments, err := validateOnHistorical(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return nil, nil, err
	}
	searchResults, err := searchSegments(ctx, manager, segments, SegmentTypeSealed, searchReq, metering...)
	return searchResults, segments, err
}

// searchStreaming will search all the target segments in streaming
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func SearchStreaming(ctx context.Context, manager *Manager, searchReq *SearchRequest, collID int64, partIDs []int64, segIDs []int64, metering ...*SearchReadUsage) ([]*SearchResult, []Segment, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	segments, err := validateOnStream(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return nil, nil, err
	}
	searchResults, err := searchSegments(ctx, manager, segments, SegmentTypeGrowing, searchReq, metering...)
	return searchResults, segments, err
}
