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
	"path/filepath"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
)

const (
	segmentMetaPrefix = "queryCoord-segmentMeta"
)

// historical is in charge of historical data in query node
type historical struct {
	ctx context.Context

	replica      ReplicaInterface
	loader       *segmentLoader
	statsService *statsService

	mu                   sync.Mutex // guards globalSealedSegments
	globalSealedSegments map[UniqueID]*querypb.SegmentInfo

	etcdKV *etcdkv.EtcdKV
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
		ctx:                  ctx,
		replica:              replica,
		loader:               loader,
		statsService:         ss,
		globalSealedSegments: make(map[UniqueID]*querypb.SegmentInfo),
		etcdKV:               etcdKV,
	}
}

func (h *historical) start() {
	go h.statsService.start()
	go h.watchGlobalSegmentMeta()
}

func (h *historical) close() {
	h.statsService.close()

	// free collectionReplica
	h.replica.freeAll()
}

func (h *historical) watchGlobalSegmentMeta() {
	log.Debug("query node watchGlobalSegmentMeta start")
	watchChan := h.etcdKV.WatchWithPrefix(segmentMetaPrefix)

	for {
		select {
		case <-h.ctx.Done():
			log.Debug("query node watchGlobalSegmentMeta close")
			return
		case resp := <-watchChan:
			for _, event := range resp.Events {
				segmentID, err := strconv.ParseInt(filepath.Base(string(event.Kv.Key)), 10, 64)
				if err != nil {
					log.Warn("watchGlobalSegmentMeta failed", zap.Any("error", err.Error()))
					continue
				}
				switch event.Type {
				case mvccpb.PUT:
					log.Debug("globalSealedSegments add segment",
						zap.Any("segmentID", segmentID),
					)
					segmentInfo := &querypb.SegmentInfo{}
					err = proto.UnmarshalText(string(event.Kv.Value), segmentInfo)
					if err != nil {
						log.Warn("watchGlobalSegmentMeta failed", zap.Any("error", err.Error()))
						continue
					}
					h.addGlobalSegmentInfo(segmentID, segmentInfo)
				case mvccpb.DELETE:
					log.Debug("globalSealedSegments delete segment",
						zap.Any("segmentID", segmentID),
					)
					h.removeGlobalSegmentInfo(segmentID)
				}
			}
		}
	}
}

func (h *historical) addGlobalSegmentInfo(segmentID UniqueID, segmentInfo *querypb.SegmentInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.globalSealedSegments[segmentID] = segmentInfo
}

func (h *historical) removeGlobalSegmentInfo(segmentID UniqueID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.globalSealedSegments, segmentID)
}

func (h *historical) getGlobalSegmentIDsByCollectionID(collectionID UniqueID) []UniqueID {
	h.mu.Lock()
	defer h.mu.Unlock()
	resIDs := make([]UniqueID, 0)
	for _, v := range h.globalSealedSegments {
		if v.CollectionID == collectionID {
			resIDs = append(resIDs, v.SegmentID)
		}
	}
	return resIDs
}

func (h *historical) getGlobalSegmentIDsByPartitionIds(partitionIDs []UniqueID) []UniqueID {
	h.mu.Lock()
	defer h.mu.Unlock()
	resIDs := make([]UniqueID, 0)
	for _, v := range h.globalSealedSegments {
		for _, partitionID := range partitionIDs {
			if v.PartitionID == partitionID {
				resIDs = append(resIDs, v.SegmentID)
			}
		}
	}
	return resIDs
}

func (h *historical) removeGlobalSegmentIDsByCollectionID(collectionID UniqueID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, v := range h.globalSealedSegments {
		if v.CollectionID == collectionID {
			delete(h.globalSealedSegments, v.SegmentID)
		}
	}
}

func (h *historical) removeGlobalSegmentIDsByPartitionIds(partitionIDs []UniqueID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, v := range h.globalSealedSegments {
		for _, partitionID := range partitionIDs {
			if v.PartitionID == partitionID {
				delete(h.globalSealedSegments, v.SegmentID)
			}
		}
	}
}

func (h *historical) retrieve(collID UniqueID, partIDs []UniqueID, vcm storage.ChunkManager,
	plan *RetrievePlan) ([]*segcorepb.RetrieveResults, []UniqueID, error) {

	retrieveResults := make([]*segcorepb.RetrieveResults, 0)
	retrieveSegmentIDs := make([]UniqueID, 0)

	// get historical partition ids
	var retrievePartIDs []UniqueID
	if len(partIDs) == 0 {
		hisPartIDs, err := h.replica.getPartitionIDs(collID)
		if err != nil {
			return retrieveResults, retrieveSegmentIDs, err
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
			return retrieveResults, retrieveSegmentIDs, err
		}
		for _, segID := range segIDs {
			seg, err := h.replica.getSegmentByID(segID)
			if err != nil {
				return retrieveResults, retrieveSegmentIDs, err
			}
			result, err := seg.getEntityByIds(plan)
			if err != nil {
				return retrieveResults, retrieveSegmentIDs, err
			}

			if err = seg.fillVectorFieldsData(collID, vcm, result); err != nil {
				return retrieveResults, retrieveSegmentIDs, err
			}
			retrieveResults = append(retrieveResults, result)
			retrieveSegmentIDs = append(retrieveSegmentIDs, segID)
		}
	}
	return retrieveResults, retrieveSegmentIDs, nil
}

func (h *historical) search(searchReqs []*searchRequest, collID UniqueID, partIDs []UniqueID, plan *SearchPlan,
	searchTs Timestamp) ([]*SearchResult, []UniqueID, error) {

	searchResults := make([]*SearchResult, 0)
	searchSegmentIDs := make([]UniqueID, 0)

	// get historical partition ids
	var searchPartIDs []UniqueID
	if len(partIDs) == 0 {
		hisPartIDs, err := h.replica.getPartitionIDs(collID)
		if err != nil {
			return searchResults, searchSegmentIDs, err
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
			fmt.Sprintln(collID) + "target partitionIDs = " + fmt.Sprintln(partIDs))
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
			return searchResults, searchSegmentIDs, err
		}
		for _, segID := range segIDs {
			seg, err := h.replica.getSegmentByID(segID)
			if err != nil {
				return searchResults, searchSegmentIDs, err
			}
			if !seg.getOnService() {
				continue
			}
			searchResult, err := seg.search(plan, searchReqs, []Timestamp{searchTs})
			if err != nil {
				return searchResults, searchSegmentIDs, err
			}
			searchResults = append(searchResults, searchResult)
			searchSegmentIDs = append(searchSegmentIDs, seg.segmentID)
		}
	}

	return searchResults, searchSegmentIDs, nil
}
