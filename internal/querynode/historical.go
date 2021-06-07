// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/types"
)

type historical struct {
	replica      ReplicaInterface
	loadService  *loadService
	statsService *statsService
}

func newHistorical(ctx context.Context,
	masterService types.MasterService,
	dataService types.DataService,
	indexService types.IndexService,
	factory msgstream.Factory) *historical {
	replica := newCollectionReplica()
	ls := newLoadService(ctx, masterService, dataService, indexService, replica)
	ss := newStatsService(ctx, replica, ls.segLoader.indexLoader.fieldStatsChan, factory)

	return &historical{
		replica:      replica,
		loadService:  ls,
		statsService: ss,
	}
}

func (h *historical) start() {
	h.loadService.start()
	h.statsService.start()
}

func (h *historical) close() {
	h.loadService.close()
	h.statsService.close()

	// free collectionReplica
	h.replica.freeAll()
}

func (h *historical) search(searchReqs []*searchRequest, collID UniqueID, partIDs []UniqueID, plan *Plan, ts Timestamp) ([]*SearchResult, []*Segment, error) {
	searchResults := make([]*SearchResult, 0)
	segmentResults := make([]*Segment, 0)

	// get historical partition ids
	var searchPartIDs []UniqueID
	if len(partIDs) == 0 {
		hisPartIDs, err := h.replica.getPartitionIDs(collID)
		if err != nil {
			return searchResults, segmentResults, err
		}
		searchPartIDs = hisPartIDs
	} else {
		for _, id := range partIDs {
			_, err1 := h.replica.getPartitionByID(id)
			if err1 == nil {
				searchPartIDs = append(searchPartIDs, id)
			}
		}
	}

	for _, partID := range searchPartIDs {
		segIDs, err := h.replica.getSegmentIDs(partID)
		if err != nil {
			return searchResults, segmentResults, err
		}
		for _, segID := range segIDs {
			seg, err := h.replica.getSegmentByID(segID)
			if err != nil {
				return searchResults, segmentResults, err
			}
			searchResult, err := seg.segmentSearch(plan, searchReqs, []Timestamp{ts})
			if err != nil {
				return searchResults, segmentResults, err
			}
			searchResults = append(searchResults, searchResult)
			segmentResults = append(segmentResults, seg)
		}
	}

	return searchResults, segmentResults, nil
}
