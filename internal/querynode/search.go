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

package querynode

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

// searchOnSegments performs search on listed segments
// all segment ids are validated before calling this function
func searchOnSegments(ctx context.Context, replica ReplicaInterface, segType segmentType, searchReq *searchRequest, segIDs []UniqueID) ([]*SearchResult, error) {
	// results variables
	searchResults := make([]*SearchResult, len(segIDs))
	errs := make([]error, len(segIDs))
	searchLabel := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		searchLabel = metrics.GrowingSegmentLabel
	}

	// calling segment search in goroutines
	var wg sync.WaitGroup
	for i, segID := range segIDs {
		wg.Add(1)
		go func(segID UniqueID, i int) {
			defer wg.Done()
			seg, err := replica.getSegmentByID(segID, segType)
			if err != nil {
				log.Error(err.Error()) // should not happen but still ignore it since the result is still correct
				return
			}
			// record search time
			tr := timerecord.NewTimeRecorder("searchOnSegments")
			searchResult, err := seg.search(searchReq)
			errs[i] = err
			searchResults[i] = searchResult
			// update metrics
			metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
				metrics.SearchLabel, searchLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
		}(segID, i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			deleteSearchResults(searchResults)
			return nil, err
		}
	}
	return searchResults, nil
}

// search will search on the historical segments the target segments in historical.
// if segIDs is not specified, it will search on all the historical segments speficied by partIDs.
// if segIDs is specified, it will only search on the segments specified by the segIDs.
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func searchHistorical(ctx context.Context, replica ReplicaInterface, searchReq *searchRequest, collID UniqueID, partIDs []UniqueID, segIDs []UniqueID) ([]*SearchResult, []UniqueID, []UniqueID, error) {
	var err error
	var searchResults []*SearchResult
	var searchSegmentIDs []UniqueID
	var searchPartIDs []UniqueID
	searchPartIDs, searchSegmentIDs, err = validateOnHistoricalReplica(ctx, replica, collID, partIDs, segIDs)
	if err != nil {
		return searchResults, searchSegmentIDs, searchPartIDs, err
	}
	searchResults, err = searchOnSegments(ctx, replica, segmentTypeSealed, searchReq, searchSegmentIDs)
	return searchResults, searchPartIDs, searchSegmentIDs, err
}

// searchStreaming will search all the target segments in streaming
// if partIDs is empty, it means all the partitions of the loaded collection or all the partitions loaded.
func searchStreaming(ctx context.Context, replica ReplicaInterface, searchReq *searchRequest, collID UniqueID, partIDs []UniqueID, vChannel Channel) ([]*SearchResult, []UniqueID, []UniqueID, error) {
	var err error
	var searchResults []*SearchResult
	var searchPartIDs []UniqueID
	var searchSegmentIDs []UniqueID

	searchPartIDs, searchSegmentIDs, err = validateOnStreamReplica(ctx, replica, collID, partIDs, vChannel)
	if err != nil {
		return searchResults, searchSegmentIDs, searchPartIDs, err
	}
	searchResults, err = searchOnSegments(ctx, replica, segmentTypeGrowing, searchReq, searchSegmentIDs)
	return searchResults, searchPartIDs, searchSegmentIDs, err
}
