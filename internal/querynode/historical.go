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

//func (h *historical) search(searchRequests []*searchRequest, collectionID UniqueID, partitionIDsInQuery []UniqueID, plan *Plan, ts Timestamp) error {
//	searchResults := make([]*SearchResult, 0)
//	matchedSegments := make([]*Segment, 0)
//
//	var searchPartitionIDsInHistorical []UniqueID
//	if len(partitionIDsInQuery) == 0 {
//		partitionIDsInHistoricalCol, err1 := h.replica.getPartitionIDs(collectionID)
//		if err1 != nil {
//			return err1
//		}
//		if len(partitionIDsInHistoricalCol) == 0 {
//			return errors.New("none of this collection's partition has been loaded")
//		}
//		searchPartitionIDsInHistorical = partitionIDsInHistoricalCol
//	} else {
//		for _, id := range partitionIDsInQuery {
//			_, err := h.replica.getPartitionByID(id)
//			if err == nil {
//				searchPartitionIDsInHistorical = append(searchPartitionIDsInHistorical, id)
//			}
//			if err != nil {
//				return err
//			}
//		}
//	}
//
//	sealedSegmentSearched := make([]UniqueID, 0)
//	for _, partitionID := range searchPartitionIDsInHistorical {
//		segmentIDs, err := s.historicalReplica.getSegmentIDs(partitionID)
//		if err != nil {
//			return err
//		}
//		for _, segmentID := range segmentIDs {
//			segment, err := s.historicalReplica.getSegmentByID(segmentID)
//			if err != nil {
//				return err
//			}
//			searchResult, err := segment.segmentSearch(plan, searchRequests, []Timestamp{searchTimestamp})
//
//			if err != nil {
//				return err
//			}
//			searchResults = append(searchResults, searchResult)
//			matchedSegments = append(matchedSegments, segment)
//			sealedSegmentSearched = append(sealedSegmentSearched, segmentID)
//		}
//	}
//
//	inReduced := make([]bool, len(searchResults))
//	numSegment := int64(len(searchResults))
//	var marshaledHits *MarshaledHits = nil
//	if numSegment == 1 {
//		inReduced[0] = true
//		err = fillTargetEntry(plan, searchResults, matchedSegments, inReduced)
//		sp.LogFields(oplog.String("statistical time", "fillTargetEntry end"))
//		if err != nil {
//			return err
//		}
//		marshaledHits, err = reorganizeSingleQueryResult(plan, searchRequests, searchResults[0])
//		sp.LogFields(oplog.String("statistical time", "reorganizeSingleQueryResult end"))
//		if err != nil {
//			return err
//		}
//	} else {
//		err = reduceSearchResults(searchResults, numSegment, inReduced)
//		sp.LogFields(oplog.String("statistical time", "reduceSearchResults end"))
//		if err != nil {
//			return err
//		}
//		err = fillTargetEntry(plan, searchResults, matchedSegments, inReduced)
//		sp.LogFields(oplog.String("statistical time", "fillTargetEntry end"))
//		if err != nil {
//			return err
//		}
//		marshaledHits, err = reorganizeQueryResults(plan, searchRequests, searchResults, numSegment, inReduced)
//		sp.LogFields(oplog.String("statistical time", "reorganizeQueryResults end"))
//		if err != nil {
//			return err
//		}
//	}
//	hitsBlob, err := marshaledHits.getHitsBlob()
//	sp.LogFields(oplog.String("statistical time", "getHitsBlob end"))
//	if err != nil {
//		return err
//	}
//
//	var offset int64 = 0
//	for index := range searchRequests {
//		hitBlobSizePeerQuery, err := marshaledHits.hitBlobSizeInGroup(int64(index))
//		if err != nil {
//			return err
//		}
//		hits := make([][]byte, len(hitBlobSizePeerQuery))
//		for i, len := range hitBlobSizePeerQuery {
//			hits[i] = hitsBlob[offset : offset+len]
//			//test code to checkout marshaled hits
//			//marshaledHit := hitsBlob[offset:offset+len]
//			//unMarshaledHit := milvuspb.Hits{}
//			//err = proto.Unmarshal(marshaledHit, &unMarshaledHit)
//			//if err != nil {
//			//	return err
//			//}
//			//log.Debug("hits msg  = ", unMarshaledHit)
//			offset += len
//		}
//		resultChannelInt := 0
//		searchResultMsg := &msgstream.SearchResultMsg{
//			BaseMsg: msgstream.BaseMsg{Ctx: searchMsg.Ctx, HashValues: []uint32{uint32(resultChannelInt)}},
//			SearchResults: internalpb.SearchResults{
//				Base: &commonpb.MsgBase{
//					MsgType:   commonpb.MsgType_SearchResult,
//					MsgID:     searchMsg.Base.MsgID,
//					Timestamp: searchTimestamp,
//					SourceID:  searchMsg.Base.SourceID,
//				},
//				Status:                   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
//				ResultChannelID:          searchMsg.ResultChannelID,
//				Hits:                     hits,
//				MetricType:               plan.getMetricType(),
//				SealedSegmentIDsSearched: sealedSegmentSearched,
//				ChannelIDsSearched:       collection.getWatchedDmChannels(),
//				//TODO:: get global sealed segment from etcd
//				GlobalSealedSegmentIDs: sealedSegmentSearched,
//			},
//		}
//
//		// For debugging, please don't delete.
//		//fmt.Println("==================== search result ======================")
//		//for i := 0; i < len(hits); i++ {
//		//	testHits := milvuspb.Hits{}
//		//	err := proto.Unmarshal(hits[i], &testHits)
//		//	if err != nil {
//		//		panic(err)
//		//	}
//		//	fmt.Println(testHits.IDs)
//		//	fmt.Println(testHits.Scores)
//		//}
//		err = s.publishSearchResult(searchResultMsg, searchMsg.CollectionID)
//		if err != nil {
//			return err
//		}
//	}
//
//	sp.LogFields(oplog.String("statistical time", "before free c++ memory"))
//	deleteSearchResults(searchResults)
//	deleteMarshaledHits(marshaledHits)
//	sp.LogFields(oplog.String("statistical time", "stats done"))
//	plan.delete()
//	searchReq.delete()
//	return nil
//}
