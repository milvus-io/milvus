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
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

// historical is in charge of historical data in query node
type historical struct {
	ctx context.Context

	replica      ReplicaInterface
	tSafeReplica TSafeReplicaInterface
}

// newHistorical returns a new historical
func newHistorical(ctx context.Context,
	replica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface) *historical {

	return &historical{
		ctx:          ctx,
		replica:      replica,
		tSafeReplica: tSafeReplica,
	}
}

// close would release all resources in historical
func (h *historical) close() {
	// free collectionReplica
	h.replica.freeAll()
}

// // retrieve will retrieve from the segments in historical
func (h *historical) retrieve(collID UniqueID, partIDs []UniqueID, vcm storage.ChunkManager,
	plan *RetrievePlan) (retrieveResults []*segcorepb.RetrieveResults, retrieveSegmentIDs []UniqueID, retrievePartIDs []UniqueID, err error) {

	// get historical partition ids
	retrievePartIDs, err = h.getTargetPartIDs(collID, partIDs)
	if err != nil {
		return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
	}

	log.Debug("retrieve target partitions", zap.Int64("collectionID", collID), zap.Int64s("partitionIDs", retrievePartIDs))

	for _, partID := range retrievePartIDs {
		segIDs, err := h.replica.getSegmentIDs(partID)
		if err != nil {
			return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
		}
		for _, segID := range segIDs {
			seg, err := h.replica.getSegmentByID(segID)
			if err != nil {
				return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
			}
			result, err := seg.retrieve(plan)
			if err != nil {
				return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
			}

			if err = seg.fillVectorFieldsData(collID, vcm, result); err != nil {
				return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
			}
			retrieveResults = append(retrieveResults, result)
			retrieveSegmentIDs = append(retrieveSegmentIDs, segID)
		}
	}

	return retrieveResults, retrieveSegmentIDs, retrievePartIDs, nil
}

// search will search all the target segments in historical
func (h *historical) search(searchReqs []*searchRequest, collID UniqueID, partIDs []UniqueID, plan *SearchPlan,
	searchTs Timestamp) (searchResults []*SearchResult, searchSegmentIDs []UniqueID, searchPartIDs []UniqueID, err error) {

	searchPartIDs, err = h.getTargetPartIDs(collID, partIDs)
	if err != nil {
		return searchResults, searchSegmentIDs, searchPartIDs, err
	}

	log.Debug("search target partitions", zap.Int64("collectionID", collID), zap.Int64s("partitionIDs", searchPartIDs))

	col, err := h.replica.getCollectionByID(collID)
	if err != nil {
		return searchResults, searchSegmentIDs, searchPartIDs, err
	}

	// all partitions have been released
	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypePartition {
		return nil, nil, searchPartIDs, errors.New("partitions have been released , collectionID = " +
			fmt.Sprintln(collID) + "target partitionIDs = " + fmt.Sprintln(partIDs))
	}

	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypeCollection {
		return searchResults, searchSegmentIDs, searchPartIDs, nil
	}

	var segmentIDs []UniqueID
	for _, partID := range searchPartIDs {
		segIDs, err := h.replica.getSegmentIDs(partID)
		if err != nil {
			return searchResults, searchSegmentIDs, searchPartIDs, err
		}

		segmentIDs = append(segmentIDs, segIDs...)
	}

	searchResults, searchSegmentIDs, err = h.searchSegments(segmentIDs, searchReqs, plan, searchTs)

	return searchResults, searchSegmentIDs, searchPartIDs, err
}

// getSearchPartIDs fetchs the partition ids to search from the request ids
func (h *historical) getTargetPartIDs(collID UniqueID, partIDs []UniqueID) ([]UniqueID, error) {
	// no partition id specified, get all partition ids in collection
	if len(partIDs) == 0 {
		return h.replica.getPartitionIDs(collID)
	}

	var targetPartIDs []UniqueID
	for _, id := range partIDs {
		_, err := h.replica.getPartitionByID(id)
		if err != nil {
			return nil, err
		}
		targetPartIDs = append(targetPartIDs, id)
	}
	return targetPartIDs, nil
}

// searchSegments performs search on listed segments
// all segment ids are validated before calling this function
func (h *historical) searchSegments(segIDs []UniqueID, searchReqs []*searchRequest, plan *SearchPlan, searchTs Timestamp) ([]*SearchResult, []UniqueID, error) {
	// pre-fetch all the segment
	// if error found, return before executing segment search
	segments := make([]*Segment, 0, len(segIDs))
	for _, segID := range segIDs {
		seg, err := h.replica.getSegmentByID(segID)
		if err != nil {
			return nil, nil, err
		}
		segments = append(segments, seg)
	}

	// results variables
	var searchResults []*SearchResult
	var searchSegmentIDs []UniqueID
	var lock sync.Mutex
	var serr error

	// calling segment search in goroutines
	var wg sync.WaitGroup
	for _, seg := range segments {
		wg.Add(1)
		go func(seg *Segment) {
			defer wg.Done()
			if !seg.getOnService() {
				log.Warn("segment no on service", zap.Int64("segmentID", seg.segmentID))
				return
			}
			// record search time
			tr := timerecord.NewTimeRecorder("searchOnSealed")
			searchResult, err := seg.search(plan, searchReqs, []Timestamp{searchTs})

			// update metrics
			metrics.QueryNodeSQSegmentLatency.WithLabelValues(metrics.SearchLabel,
				metrics.SealedSegmentLabel,
				fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Observe(float64(tr.ElapseSpan().Milliseconds()))

			// write back result into list
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				serr = err
				return
			}
			searchResults = append(searchResults, searchResult)
			searchSegmentIDs = append(searchSegmentIDs, seg.segmentID)
		}(seg)
	}
	wg.Wait()
	if serr != nil {
		return nil, nil, serr
	}
	return searchResults, searchSegmentIDs, nil
}
