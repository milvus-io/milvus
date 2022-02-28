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

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

// streaming is in charge of streaming data in query node
type streaming struct {
	ctx context.Context

	replica      ReplicaInterface
	tSafeReplica TSafeReplicaInterface

	msFactory msgstream.Factory
}

func newStreaming(ctx context.Context, replica ReplicaInterface, factory msgstream.Factory, etcdKV *etcdkv.EtcdKV, tSafeReplica TSafeReplicaInterface) *streaming {

	return &streaming{
		replica:      replica,
		tSafeReplica: tSafeReplica,
	}
}

func (s *streaming) start() {
	// TODO: start stats
}

func (s *streaming) close() {
	// TODO: stop stats

	// free collectionReplica
	s.replica.freeAll()
}

func (s *streaming) retrieve(collID UniqueID, partIDs []UniqueID, plan *RetrievePlan) ([]*segcorepb.RetrieveResults, []UniqueID, []UniqueID, error) {
	retrieveResults := make([]*segcorepb.RetrieveResults, 0)
	retrieveSegmentIDs := make([]UniqueID, 0)

	var retrievePartIDs []UniqueID
	if len(partIDs) == 0 {
		strPartIDs, err := s.replica.getPartitionIDs(collID)
		if err != nil {
			return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
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
			return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
		}
		for _, segID := range segIDs {
			seg, err := s.replica.getSegmentByID(segID)
			if err != nil {
				return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
			}
			result, err := seg.retrieve(plan)
			if err != nil {
				return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
			}

			retrieveResults = append(retrieveResults, result)
			retrieveSegmentIDs = append(retrieveSegmentIDs, segID)
		}
	}

	return retrieveResults, retrieveSegmentIDs, retrievePartIDs, nil
}

// search will search all the target segments in streaming
func (s *streaming) search(searchReqs []*searchRequest, collID UniqueID, partIDs []UniqueID, vChannel Channel,
	plan *SearchPlan, searchTs Timestamp) ([]*SearchResult, []UniqueID, []UniqueID, error) {

	searchResults := make([]*SearchResult, 0)
	searchSegmentIDs := make([]UniqueID, 0)

	// get streaming partition ids
	var searchPartIDs []UniqueID
	if len(partIDs) == 0 {
		strPartIDs, err := s.replica.getPartitionIDs(collID)
		if len(strPartIDs) == 0 {
			// no partitions in collection, do empty search
			return searchResults, searchSegmentIDs, searchPartIDs, nil
		}
		if err != nil {
			return searchResults, searchSegmentIDs, searchPartIDs, err
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
		return searchResults, searchSegmentIDs, searchPartIDs, err
	}

	// all partitions have been released
	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypePartition {
		err = errors.New("partitions have been released , collectionID = " + fmt.Sprintln(collID) + "target partitionIDs = " + fmt.Sprintln(partIDs))
		return searchResults, searchSegmentIDs, searchPartIDs, err
	}

	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypeCollection {
		if err = col.checkReleasedPartitions(partIDs); err != nil {
			return searchResults, searchSegmentIDs, searchPartIDs, err
		}
		return searchResults, searchSegmentIDs, searchPartIDs, nil
	}

	var segmentLock sync.RWMutex
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
			return searchResults, searchSegmentIDs, searchPartIDs, err
		}

		var err2 error
		var wg sync.WaitGroup
		for _, segID := range segIDs {
			segID2 := segID
			wg.Add(1)
			go func() {
				defer wg.Done()
				seg, err := s.replica.getSegmentByID(segID2)
				if err != nil {
					log.Warn(err.Error())
					err2 = err
					return
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

				tr := timerecord.NewTimeRecorder("searchOnGrowing")
				searchResult, err := seg.search(plan, searchReqs, []Timestamp{searchTs})
				if err != nil {
					err2 = err
					return
				}
				metrics.QueryNodeSQSegmentLatency.WithLabelValues(metrics.QueryNodeQueryTypeSearch,
					metrics.QueryNodeSegTypeGrowing,
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
