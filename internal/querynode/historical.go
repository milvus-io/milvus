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
	"errors"
	"fmt"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/types"
)

type historical struct {
	replica      ReplicaInterface
	loader       *segmentLoader
	statsService *statsService

	//TODO
	globalSealedSegments []UniqueID
}

func newHistorical(ctx context.Context,
	rootCoord types.RootCoord,
	indexCoord types.IndexCoord,
	factory msgstream.Factory,
	etcdKV *etcdkv.EtcdKV) *historical {
	replica := newCollectionReplica(etcdKV)
	loader := newSegmentLoader(ctx, rootCoord, indexCoord, replica, etcdKV)
	ss := newStatsService(ctx, replica, loader.indexLoader.fieldStatsChan, factory)

	return &historical{
		replica:      replica,
		loader:       loader,
		statsService: ss,
	}
}

func (h *historical) start() {
	h.statsService.start()
}

func (h *historical) close() {
	h.statsService.close()

	// free collectionReplica
	h.replica.freeAll()
}

func (h *historical) search(searchReqs []*searchRequest,
	collID UniqueID,
	partIDs []UniqueID,
	plan *SearchPlan,
	searchTs Timestamp) ([]*SearchResult, []*Segment, error) {

	searchResults := make([]*SearchResult, 0)
	segmentResults := make([]*Segment, 0)

	// get historical partition ids
	var searchPartIDs []UniqueID
	if len(partIDs) == 0 {
		hisPartIDs, err := h.replica.getPartitionIDs(collID)
		if len(hisPartIDs) == 0 {
			// no partitions in collection, do empty search
			return nil, nil, nil
		}
		if err != nil {
			return searchResults, segmentResults, err
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
		return nil, nil, err
	}

	// all partitions have been released
	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypePartition {
		return nil, nil, errors.New("partitions have been released , collectionID = " +
			fmt.Sprintln(collID) +
			"target partitionIDs = " +
			fmt.Sprintln(partIDs))
	}

	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypeCollection {
		if err = col.checkReleasedPartitions(partIDs); err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
	}

	log.Debug("doing search in historical",
		zap.Any("collectionID", collID),
		zap.Any("reqPartitionIDs", partIDs),
		zap.Any("searchPartitionIDs", searchPartIDs),
	)

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
			if !seg.getOnService() {
				continue
			}
			searchResult, err := seg.search(plan, searchReqs, []Timestamp{searchTs})
			if err != nil {
				return searchResults, segmentResults, err
			}
			searchResults = append(searchResults, searchResult)
			segmentResults = append(segmentResults, seg)
		}
	}

	return searchResults, segmentResults, nil
}
