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

	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
)

// streaming is in charge of streaming data in query node
type streaming struct {
	ctx context.Context

	replica      ReplicaInterface
	tSafeReplica TSafeReplicaInterface

	dataSyncService *dataSyncService
	msFactory       msgstream.Factory
}

func newStreaming(ctx context.Context, factory msgstream.Factory, etcdKV *etcdkv.EtcdKV) *streaming {
	replica := newCollectionReplica(etcdKV)
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

func (s *streaming) retrieve(collID UniqueID, partIDs []UniqueID, plan *RetrievePlan) ([]*segcorepb.RetrieveResults, []UniqueID, error) {
	retrieveResults := make([]*segcorepb.RetrieveResults, 0)
	retrieveSegmentIDs := make([]UniqueID, 0)

	var retrievePartIDs []UniqueID
	if len(partIDs) == 0 {
		strPartIDs, err := s.replica.getPartitionIDs(collID)
		if err != nil {
			return retrieveResults, retrieveSegmentIDs, err
		}
		retrievePartIDs = strPartIDs
	} else {
		for _, id := range partIDs {
			_, err := s.replica.getPartitionByID(id)
			if err == nil {
				retrievePartIDs = append(retrievePartIDs, id)
			}
		}
	}

	for _, partID := range retrievePartIDs {
		segIDs, err := s.replica.getSegmentIDs(partID)
		if err != nil {
			return retrieveResults, retrieveSegmentIDs, err
		}
		for _, segID := range segIDs {
			seg, err := s.replica.getSegmentByID(segID)
			if err != nil {
				return retrieveResults, retrieveSegmentIDs, err
			}
			result, err := seg.getEntityByIds(plan)
			if err != nil {
				return retrieveResults, retrieveSegmentIDs, err
			}

			retrieveResults = append(retrieveResults, result)
			retrieveSegmentIDs = append(retrieveSegmentIDs, segID)
		}
	}
	return retrieveResults, retrieveSegmentIDs, nil
}

// search will search all the target segments in streaming
func (s *streaming) search(searchReqs []*searchRequest, collID UniqueID, partIDs []UniqueID, vChannel Channel,
	plan *SearchPlan, searchTs Timestamp) ([]*SearchResult, error) {

	searchResults := make([]*SearchResult, 0)

	// get streaming partition ids
	var searchPartIDs []UniqueID
	if len(partIDs) == 0 {
		strPartIDs, err := s.replica.getPartitionIDs(collID)
		if len(strPartIDs) == 0 {
			// no partitions in collection, do empty search
			return nil, nil
		}
		if err != nil {
			return searchResults, err
		}
		log.Debug("no partition specified, search all partitions",
			zap.Any("collectionID", collID),
			zap.Any("vChannel", vChannel),
			zap.Any("all partitions", strPartIDs),
		)
		searchPartIDs = strPartIDs
	} else {
		for _, id := range partIDs {
			_, err := s.replica.getPartitionByID(id)
			if err == nil {
				log.Debug("append search partition id",
					zap.Any("collectionID", collID),
					zap.Any("vChannel", vChannel),
					zap.Any("partitionID", id),
				)
				searchPartIDs = append(searchPartIDs, id)
			}
		}
	}

	col, err := s.replica.getCollectionByID(collID)
	if err != nil {
		return nil, err
	}

	// all partitions have been released
	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypePartition {
		err = errors.New("partitions have been released , collectionID = " + fmt.Sprintln(collID) + "target partitionIDs = " + fmt.Sprintln(partIDs))
		return nil, err
	}

	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypeCollection {
		if err = col.checkReleasedPartitions(partIDs); err != nil {
			return nil, err
		}
		return nil, nil
	}

	log.Debug("doing search in streaming",
		zap.Any("collectionID", collID),
		zap.Any("vChannel", vChannel),
		zap.Any("reqPartitionIDs", partIDs),
		zap.Any("searchPartitionIDs", searchPartIDs),
	)

	log.Debug("print streaming replica when searching...",
		zap.Any("collectionID", collID),
	)
	s.replica.printReplica()

	for _, partID := range searchPartIDs {
		segIDs, err := s.replica.getSegmentIDsByVChannel(partID, vChannel)
		log.Debug("get segmentIDs by vChannel",
			zap.Any("collectionID", collID),
			zap.Any("vChannel", vChannel),
			zap.Any("partitionID", partID),
			zap.Any("segmentIDs", segIDs),
		)
		if err != nil {
			log.Warn(err.Error())
			return searchResults, err
		}
		for _, segID := range segIDs {
			seg, err := s.replica.getSegmentByID(segID)
			if err != nil {
				log.Warn(err.Error())
				return searchResults, err
			}

			// TSafe less than searchTs means this vChannel is not available
			//ts := s.tSafeReplica.getTSafe(seg.vChannelID)
			//gracefulTimeInMilliSecond := Params.GracefulTime
			//if gracefulTimeInMilliSecond > 0 {
			//	gracefulTime := tsoutil.ComposeTS(gracefulTimeInMilliSecond, 0)
			//	ts += gracefulTime
			//}
			//tsp, _ := tsoutil.ParseTS(ts)
			//stp, _ := tsoutil.ParseTS(searchTs)
			//log.Debug("timestamp check in streaming search",
			//	zap.Any("collectionID", collID),
			//	zap.Any("serviceTime_l", ts),
			//	zap.Any("searchTime_l", searchTs),
			//	zap.Any("serviceTime_p", tsp),
			//	zap.Any("searchTime_p", stp),
			//)
			//if ts < searchTs {
			//	continue
			//}

			searchResult, err := seg.search(plan, searchReqs, []Timestamp{searchTs})
			if err != nil {
				return searchResults, err
			}
			searchResults = append(searchResults, searchResult)
		}
	}

	return searchResults, nil
}
