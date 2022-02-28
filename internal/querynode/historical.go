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
	plan *RetrievePlan) ([]*segcorepb.RetrieveResults, []UniqueID, []UniqueID, error) {

	retrieveResults := make([]*segcorepb.RetrieveResults, 0)
	retrieveSegmentIDs := make([]UniqueID, 0)

	// get historical partition ids
	var retrievePartIDs []UniqueID
	if len(partIDs) == 0 {
		hisPartIDs, err := h.replica.getPartitionIDs(collID)
		if err != nil {
			return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
		}
		retrievePartIDs = hisPartIDs
	} else {
		for _, id := range partIDs {
			_, err := h.replica.getPartitionByID(id)
			if err == nil {
				retrievePartIDs = append(retrievePartIDs, id)
			}
		}
	}

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
	searchTs Timestamp) ([]*SearchResult, []UniqueID, []UniqueID, error) {

	searchResults := make([]*SearchResult, 0)
	searchSegmentIDs := make([]UniqueID, 0)

	// get historical partition ids
	var searchPartIDs []UniqueID
	if len(partIDs) == 0 {
		hisPartIDs, err := h.replica.getPartitionIDs(collID)
		if err != nil {
			return searchResults, searchSegmentIDs, searchPartIDs, err
		}
		log.Debug("no partition specified, search all partitions",
			zap.Any("collectionID", collID),
			zap.Any("all partitions", hisPartIDs),
		)
		searchPartIDs = hisPartIDs
	} else {
		for _, id := range partIDs {
			_, err := h.replica.getPartitionByID(id)
			if err == nil {
				log.Debug("append search partition id",
					zap.Any("collectionID", collID),
					zap.Any("partitionID", id),
				)
				searchPartIDs = append(searchPartIDs, id)
			}
		}
	}

	col, err := h.replica.getCollectionByID(collID)
	if err != nil {
		return searchResults, searchSegmentIDs, searchPartIDs, err
	}

	// all partitions have been released
	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypePartition {
		return searchResults, searchSegmentIDs, searchPartIDs, errors.New("partitions have been released , collectionID = " +
			fmt.Sprintln(collID) + "target partitionIDs = " + fmt.Sprintln(partIDs))
	}

	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypeCollection {
		if err = col.checkReleasedPartitions(partIDs); err != nil {
			return searchResults, searchSegmentIDs, searchPartIDs, err
		}
		return searchResults, searchSegmentIDs, searchPartIDs, nil
	}

	var segmentLock sync.RWMutex
	for _, partID := range searchPartIDs {
		segIDs, err := h.replica.getSegmentIDs(partID)
		if err != nil {
			return searchResults, searchSegmentIDs, searchPartIDs, err
		}

		var err2 error
		var wg sync.WaitGroup
		for _, segID := range segIDs {
			segID2 := segID
			wg.Add(1)
			go func() {
				defer wg.Done()
				seg, err := h.replica.getSegmentByID(segID2)
				if err != nil {
					err2 = err
					return
				}
				if !seg.getOnService() {
					return
				}
				tr := timerecord.NewTimeRecorder("searchOnSealed")
				searchResult, err := seg.search(plan, searchReqs, []Timestamp{searchTs})
				if err != nil {
					err2 = err
					return
				}
				metrics.QueryNodeSQSegmentLatency.WithLabelValues(metrics.QueryNodeQueryTypeSearch,
					metrics.QueryNodeSegTypeSealed,
					fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Observe(float64(tr.ElapseSpan().Milliseconds()))

				segmentLock.Lock()
				searchResults = append(searchResults, searchResult)
				searchSegmentIDs = append(searchSegmentIDs, seg.segmentID)
				segmentLock.Unlock()
			}()

		}
		wg.Wait()
		if err2 != nil {
			return searchResults, searchSegmentIDs, searchPartIDs, err2
		}
	}

	return searchResults, searchSegmentIDs, searchPartIDs, nil
}
