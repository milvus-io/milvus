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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

// searchOnSegments performs search on listed segments
// all segment ids are validated before calling this function
func searchSegments(ctx context.Context, segments []Segment, segType SegmentType, searchReq *SearchRequest) ([]*SearchResult, error) {
	var segmentsWithoutIndex []int64
	searchLabel := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		searchLabel = metrics.GrowingSegmentLabel
	}

	tr := timerecord.NewTimeRecorder("searchOnSegments")
	// calling segment search in goroutines
	searchFutures := make([]*conc.Future[any], 0, len(segments))
	for _, segment := range segments {
		if !segment.ExistIndex(searchReq.searchFieldID) {
			segmentsWithoutIndex = append(segmentsWithoutIndex, segment.ID())
		}
		searchFuture := segment.Search(ctx, searchReq)
		searchFutures = append(searchFutures, searchFuture)
	}

	searchResults := make([]*SearchResult, 0, len(segments))

	var errorList error
	for _, future := range searchFutures {
		searchResult, err := future.Await()
		if err != nil {
			// can not early out because we have to handle search result here
			errorList = merr.Combine(errorList, err)
			continue
		}
		searchResults = append(searchResults, searchResult.(*SearchResult))
	}

	if errorList != nil {
		DeleteSearchResults(searchResults)
		metrics.QueryNodeSQFailCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			metrics.SearchLabel, searchLabel).Inc()
		return nil, errorList
	}

	// reported metrics, this metrics is the overall time consumption
	elapsed := tr.ElapseSpan().Milliseconds()
	metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
		metrics.SearchLabel, searchLabel).Observe(float64(elapsed))
	metrics.QueryNodeSegmentSearchLatencyPerVector.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
		metrics.SearchLabel, searchLabel).Observe(float64(elapsed) / float64(searchReq.getNumOfQuery()))

	if len(segmentsWithoutIndex) > 0 {
		log.Ctx(ctx).Debug("search growing/sealed segments without indexes", zap.Int64s("segmentIDs", segmentsWithoutIndex))
	}

	return searchResults, nil
}

// search will search on the historical segments the target segments in historical.
// if segIDs is not specified, it will search on all the historical segments speficied by partIDs.
// if segIDs is specified, it will only search on the segments specified by the segIDs.
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func SearchHistorical(ctx context.Context, manager *Manager, searchReq *SearchRequest, collID int64, partIDs []int64, segIDs []int64) ([]*SearchResult, []Segment, error) {
	segments, err := validateOnHistorical(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return nil, nil, err
	}
	searchResults, err := searchSegments(ctx, segments, SegmentTypeSealed, searchReq)
	return searchResults, segments, err
}

// searchStreaming will search all the target segments in streaming
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func SearchStreaming(ctx context.Context, manager *Manager, searchReq *SearchRequest, collID int64, partIDs []int64, segIDs []int64) ([]*SearchResult, []Segment, error) {
	segments, err := validateOnStream(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return nil, nil, err
	}
	searchResults, err := searchSegments(ctx, segments, SegmentTypeGrowing, searchReq)
	return searchResults, segments, err
}
