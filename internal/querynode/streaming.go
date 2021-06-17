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

	"github.com/milvus-io/milvus/internal/msgstream"
)

type streaming struct {
	ctx context.Context

	replica      ReplicaInterface
	tSafeReplica TSafeReplicaInterface

	dataSyncService *dataSyncService
	msFactory       msgstream.Factory
}

func newStreaming(ctx context.Context, factory msgstream.Factory) *streaming {
	replica := newCollectionReplica()
	tReplica := newTSafeReplica()
	newDS := newDataSyncService(ctx, replica, tReplica, factory)

	return &streaming{
		replica:         replica,
		tSafeReplica:    tReplica,
		dataSyncService: newDS,
	}
}

func (s *streaming) start() {
	// TODO: start stats
}

func (s *streaming) close() {
	// TODO: stop stats

	if s.dataSyncService != nil {
		s.dataSyncService.close()
	}

	// free collectionReplica
	s.replica.freeAll()
}

func (s *streaming) search(searchReqs []*searchRequest,
	collID UniqueID,
	partIDs []UniqueID,
	vChannel Channel,
	plan *Plan,
	searchTs Timestamp) ([]*SearchResult, []*Segment, error) {

	searchResults := make([]*SearchResult, 0)
	segmentResults := make([]*Segment, 0)

	// get streaming partition ids
	var searchPartIDs []UniqueID
	if len(partIDs) == 0 {
		strPartIDs, err := s.replica.getPartitionIDs(collID)
		if len(strPartIDs) == 0 {
			// no partitions in collection, do empty search
			return nil, nil, nil
		}
		if err != nil {
			return searchResults, segmentResults, err
		}
		searchPartIDs = strPartIDs
	} else {
		for _, id := range partIDs {
			_, err := s.replica.getPartitionByID(id)
			if err == nil {
				searchPartIDs = append(searchPartIDs, id)
			}
		}
	}

	// all partitions have been released
	if len(searchPartIDs) == 0 {
		return nil, nil, errors.New("partitions have been released , collectionID = " +
			fmt.Sprintln(collID) +
			"target partitionIDs = " +
			fmt.Sprintln(partIDs))
	}

	for _, partID := range searchPartIDs {
		segIDs, err := s.replica.getSegmentIDsByVChannel(partID, vChannel)
		if err != nil {
			return searchResults, segmentResults, err
		}
		for _, segID := range segIDs {
			seg, err := s.replica.getSegmentByID(segID)
			if err != nil {
				return searchResults, segmentResults, err
			}

			// TSafe less than searchTs means this vChannel is not available
			ts := s.tSafeReplica.getTSafe(seg.vChannelID)
			if ts < searchTs {
				continue
			}

			searchResult, err := seg.segmentSearch(plan, searchReqs, []Timestamp{searchTs})
			if err != nil {
				return searchResults, segmentResults, err
			}
			searchResults = append(searchResults, searchResult)
			segmentResults = append(segmentResults, seg)
		}
	}

	return searchResults, segmentResults, nil
}
