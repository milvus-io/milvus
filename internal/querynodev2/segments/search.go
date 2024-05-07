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

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/metricsutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

// searchOnSegments performs search on listed segments
// all segment ids are validated before calling this function
func searchSegments(ctx context.Context, mgr *Manager, segments []Segment, segType SegmentType, searchReq *SearchRequest) ([]*SearchResult, error) {
	var (
		// results variables
		futures  = make([]*conc.Future[any], 0, len(segments))
		resultCh = make(chan *SearchResult, len(segments))

		// For log only
		segmentsWithoutIndex []int64
	)

	searchLabel := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		searchLabel = metrics.GrowingSegmentLabel
	}

	searcher := func(ctx context.Context, s Segment) error {
		// record search time
		tr := timerecord.NewTimeRecorder("searchOnSegments")
		searchResult, err := s.Search(ctx, searchReq)
		if err != nil {
			return err
		}
		resultCh <- searchResult
		// update metrics
		elapsed := tr.ElapseSpan().Milliseconds()
		metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			metrics.SearchLabel, searchLabel).Observe(float64(elapsed))
		metrics.QueryNodeSegmentSearchLatencyPerVector.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			metrics.SearchLabel, searchLabel).Observe(float64(elapsed) / float64(searchReq.getNumOfQuery()))
		return nil
	}

	// calling segment search in goroutines
	for _, segment := range segments {
		seg := segment
		if !seg.ExistIndex(searchReq.searchFieldID) {
			segmentsWithoutIndex = append(segmentsWithoutIndex, seg.ID())
		}
		future := GetSQPool().Submit(func() (any, error) {
			if ctx.Err() != nil {
				return ctx.Err(), ctx.Err()
			}

			var err error
			accessRecord := metricsutil.NewSearchSegmentAccessRecord(getSegmentMetricLabel(seg))
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
				missing, err = mgr.DiskCache.DoWait(ctx, seg.ID(), timeout, searcher)
				if missing {
					accessRecord.CacheMissing()
				}
				return err, err
			}
			err = searcher(ctx, seg)
			return err, err
		})
		futures = append(futures, future)
	}
	err := conc.AwaitAll(futures...)
	close(resultCh)

	searchResults := make([]*SearchResult, 0, len(segments))
	for result := range resultCh {
		searchResults = append(searchResults, result)
	}

	if err != nil {
		DeleteSearchResults(searchResults)
		return nil, err
	}

	if len(segmentsWithoutIndex) > 0 {
		log.Ctx(ctx).Debug("search growing/sealed segments without indexes", zap.Int64s("segmentIDs", segmentsWithoutIndex))
	}

	return searchResults, nil
}

// searchSegmentsStreamly performs search on listed segments in a stream mode instead of a batch mode
// all segment ids are validated before calling this function
func searchSegmentsStreamly(ctx context.Context,
	mgr *Manager,
	segments []Segment,
	searchReq *SearchRequest,
	streamReduce func(result *SearchResult) error,
) error {
	searchLabel := metrics.SealedSegmentLabel
	searchResultsToClear := make([]*SearchResult, 0)
	var reduceMutex sync.Mutex
	var sumReduceDuration atomic.Duration
	searcher := func(ctx context.Context, seg Segment) error {
		// record search time
		tr := timerecord.NewTimeRecorder("searchOnSegments")
		searchResult, searchErr := seg.Search(ctx, searchReq)
		searchDuration := tr.RecordSpan().Milliseconds()
		if searchErr != nil {
			return searchErr
		}
		reduceMutex.Lock()
		searchResultsToClear = append(searchResultsToClear, searchResult)
		reducedErr := streamReduce(searchResult)
		reduceMutex.Unlock()
		reduceDuration := tr.RecordSpan()
		if reducedErr != nil {
			return reducedErr
		}
		sumReduceDuration.Add(reduceDuration)
		// update metrics
		metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			metrics.SearchLabel, searchLabel).Observe(float64(searchDuration))
		metrics.QueryNodeSegmentSearchLatencyPerVector.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			metrics.SearchLabel, searchLabel).Observe(float64(searchDuration) / float64(searchReq.getNumOfQuery()))
		return nil
	}

	// calling segment search in goroutines
	errGroup, ctx := errgroup.WithContext(ctx)
	log := log.Ctx(ctx)
	for _, segment := range segments {
		seg := segment
		errGroup.Go(func() error {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			var err error
			accessRecord := metricsutil.NewSearchSegmentAccessRecord(getSegmentMetricLabel(seg))
			defer func() {
				accessRecord.Finish(err)
			}()
			if seg.IsLazyLoad() {
				log.Debug("before doing stream search in DiskCache", zap.Int64("segID", seg.ID()))
				var timeout time.Duration
				timeout, err = lazyloadWaitTimeout(ctx)
				if err != nil {
					return err
				}
				var missing bool
				missing, err = mgr.DiskCache.DoWait(ctx, seg.ID(), timeout, searcher)
				if missing {
					accessRecord.CacheMissing()
				}
				if err != nil {
					log.Error("failed to do search for disk cache", zap.Int64("seg_id", seg.ID()), zap.Error(err))
				}
				log.Debug("after doing stream search in DiskCache", zap.Int64("segID", seg.ID()), zap.Error(err))
				return err
			}
			return searcher(ctx, seg)
		})
	}
	err := errGroup.Wait()
	DeleteSearchResults(searchResultsToClear)
	if err != nil {
		return err
	}
	metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
		metrics.SearchLabel,
		metrics.ReduceSegments,
		metrics.StreamReduce).Observe(float64(sumReduceDuration.Load().Milliseconds()))
	log.Debug("stream reduce sum duration:", zap.Duration("duration", sumReduceDuration.Load()))
	return nil
}

func lazyloadWaitTimeout(ctx context.Context) (time.Duration, error) {
	timeout := params.Params.QueryNodeCfg.LazyLoadWaitTimeout.GetAsDuration(time.Millisecond)
	deadline, ok := ctx.Deadline()
	if ok {
		remain := time.Until(deadline)
		if remain <= 0 {
			return -1, merr.WrapErrServiceInternal("search context deadline exceeded")
		} else if remain < timeout {
			timeout = remain
		}
	}
	return timeout, nil
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

func SearchHistoricalStreamly(ctx context.Context, manager *Manager, searchReq *SearchRequest,
	collID int64, partIDs []int64, segIDs []int64, streamReduce func(result *SearchResult) error,
) ([]Segment, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	segments, err := validateOnHistorical(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return segments, err
	}
	err = searchSegmentsStreamly(ctx, manager, segments, searchReq, streamReduce)
	if err != nil {
		return segments, err
	}
	return segments, nil
}
