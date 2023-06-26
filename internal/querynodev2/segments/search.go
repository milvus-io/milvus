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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

// searchOnSegments performs search on listed segments
// all segment ids are validated before calling this function
func searchSegments(ctx context.Context, manager *Manager, segType SegmentType, searchReq *SearchRequest, segIDs []int64) ([]*SearchResult, error) {
	var (
		// results variables
		resultCh = make(chan *SearchResult, len(segIDs))
		errs     = make([]error, len(segIDs))
		wg       sync.WaitGroup

		// For log only
		mu                   sync.Mutex
		segmentsWithoutIndex []int64
	)

	searchLabel := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		searchLabel = metrics.GrowingSegmentLabel
	}

	// calling segment search in goroutines
	for i, segID := range segIDs {
		wg.Add(1)
		go func(segID int64, i int) {
			defer wg.Done()
			seg, _ := manager.Segment.GetWithType(segID, segType).(*LocalSegment)
			if seg == nil {
				log.Warn("segment released while searching", zap.Int64("segmentID", segID))
				return
			}

			if !seg.ExistIndex(searchReq.searchFieldID) {
				mu.Lock()
				segmentsWithoutIndex = append(segmentsWithoutIndex, segID)
				mu.Unlock()
			}
			// record search time
			tr := timerecord.NewTimeRecorder("searchOnSegments")
			searchResult, err := seg.Search(ctx, searchReq)
			errs[i] = err
			resultCh <- searchResult
			// update metrics
			elapsed := tr.ElapseSpan().Milliseconds()
			metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
				metrics.SearchLabel, searchLabel).Observe(float64(elapsed))
			metrics.QueryNodeSegmentSearchLatencyPerVector.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
				metrics.SearchLabel, searchLabel).Observe(float64(elapsed) / float64(searchReq.getNumOfQuery()))
		}(segID, i)
	}
	wg.Wait()
	close(resultCh)

	searchResults := make([]*SearchResult, 0, len(segIDs))
	for result := range resultCh {
		searchResults = append(searchResults, result)
	}

	for _, err := range errs {
		if err != nil {
			DeleteSearchResults(searchResults)
			return nil, err
		}
	}

	if len(segmentsWithoutIndex) > 0 {
		log.Ctx(ctx).Info("search growing/sealed segments without indexes", zap.Int64s("segmentIDs", segmentsWithoutIndex))
	}

	return searchResults, nil
}

// search will search on the historical segments the target segments in historical.
// if segIDs is not specified, it will search on all the historical segments speficied by partIDs.
// if segIDs is specified, it will only search on the segments specified by the segIDs.
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func SearchHistorical(ctx context.Context, manager *Manager, searchReq *SearchRequest, collID int64, partIDs []int64, segIDs []int64) ([]*SearchResult, []int64, []int64, error) {
	var err error
	var searchResults []*SearchResult
	var searchSegmentIDs []int64
	var searchPartIDs []int64
	searchPartIDs, searchSegmentIDs, err = validateOnHistorical(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return searchResults, searchSegmentIDs, searchPartIDs, err
	}
	searchResults, err = searchSegments(ctx, manager, SegmentTypeSealed, searchReq, searchSegmentIDs)
	return searchResults, searchPartIDs, searchSegmentIDs, err
}

// searchStreaming will search all the target segments in streaming
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func SearchStreaming(ctx context.Context, manager *Manager, searchReq *SearchRequest, collID int64, partIDs []int64, segIDs []int64) ([]*SearchResult, []int64, []int64, error) {
	var err error
	var searchResults []*SearchResult
	var searchPartIDs []int64
	var searchSegmentIDs []int64

	searchPartIDs, searchSegmentIDs, err = validateOnStream(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return searchResults, searchSegmentIDs, searchPartIDs, err
	}
	searchResults, err = searchSegments(ctx, manager, SegmentTypeGrowing, searchReq, searchSegmentIDs)
	return searchResults, searchPartIDs, searchSegmentIDs, err
}
