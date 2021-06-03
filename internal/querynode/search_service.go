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

import "C"
import (
	"context"
	"errors"
	"strconv"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/util/trace"
	"go.uber.org/zap"
)

type searchService struct {
	ctx    context.Context
	cancel context.CancelFunc

	historicalReplica ReplicaInterface
	streamingReplica  ReplicaInterface
	tSafeReplica      TSafeReplicaInterface

	searchMsgStream       msgstream.MsgStream
	searchResultMsgStream msgstream.MsgStream

	queryNodeID           UniqueID
	searchCollections     map[UniqueID]*searchCollection
	emptySearchCollection *searchCollection
}

func newSearchService(ctx context.Context,
	historicalReplica ReplicaInterface,
	streamingReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	factory msgstream.Factory) *searchService {

	searchStream, _ := factory.NewQueryMsgStream(ctx)
	searchResultStream, _ := factory.NewQueryMsgStream(ctx)

	if len(Params.SearchChannelNames) > 0 && len(Params.SearchResultChannelNames) > 0 {
		// query node need to consume search channels and produce search result channels when init.
		consumeChannels := Params.SearchChannelNames
		consumeSubName := Params.MsgChannelSubName
		searchStream.AsConsumer(consumeChannels, consumeSubName)
		log.Debug("query node AsConsumer", zap.Any("searchChannels", consumeChannels), zap.Any("consumeSubName", consumeSubName))
		producerChannels := Params.SearchResultChannelNames
		searchResultStream.AsProducer(producerChannels)
		log.Debug("query node AsProducer", zap.Any("searchResultChannels", producerChannels))
	}

	searchServiceCtx, searchServiceCancel := context.WithCancel(ctx)
	return &searchService{
		ctx:    searchServiceCtx,
		cancel: searchServiceCancel,

		historicalReplica: historicalReplica,
		streamingReplica:  streamingReplica,
		tSafeReplica:      tSafeReplica,

		searchMsgStream:       searchStream,
		searchResultMsgStream: searchResultStream,

		queryNodeID:       Params.QueryNodeID,
		searchCollections: make(map[UniqueID]*searchCollection),
	}
}

func (s *searchService) start() {
	s.searchMsgStream.Start()
	s.searchResultMsgStream.Start()
	s.startEmptySearchCollection()
	s.consumeSearch()
}

func (s *searchService) collectionCheck(collectionID UniqueID) error {
	// check if collection exists
	if ok := s.historicalReplica.hasCollection(collectionID); !ok {
		err := errors.New("no collection found, collectionID = " + strconv.FormatInt(collectionID, 10))
		log.Error(err.Error())
		return err
	}

	return nil
}

func (s *searchService) consumeSearch() {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			msgPack := s.searchMsgStream.Consume()
			if msgPack == nil || len(msgPack.Msgs) <= 0 {
				continue
			}
			for _, msg := range msgPack.Msgs {
				log.Debug("consume search message", zap.Int64("msgID", msg.ID()))
				sm, ok := msg.(*msgstream.SearchMsg)
				if !ok {
					continue
				}
				sp, ctx := trace.StartSpanFromContext(sm.TraceCtx())
				sm.SetTraceCtx(ctx)
				err := s.collectionCheck(sm.CollectionID)
				if err != nil {
					s.emptySearchCollection.emptySearch(sm)
					log.Debug("cannot found collection, do empty search done",
						zap.Int64("msgID", sm.ID()),
						zap.Int64("collectionID", sm.CollectionID))
					continue
				}
				_, ok = s.searchCollections[sm.CollectionID]
				if !ok {
					s.startSearchCollection(sm.CollectionID)
					log.Debug("new search collection, start search collection service",
						zap.Int64("collectionID", sm.CollectionID))
				}
				s.searchCollections[sm.CollectionID].msgBuffer <- sm
				sp.Finish()
			}
		}
	}
}

func (s *searchService) close() {
	if s.searchMsgStream != nil {
		s.searchMsgStream.Close()
	}
	if s.searchResultMsgStream != nil {
		s.searchResultMsgStream.Close()
	}
	for collectionID := range s.searchCollections {
		s.stopSearchCollection(collectionID)
	}
	s.searchCollections = make(map[UniqueID]*searchCollection)
	s.cancel()
}

func (s *searchService) startSearchCollection(collectionID UniqueID) {
	ctx1, cancel := context.WithCancel(s.ctx)
	sc := newSearchCollection(ctx1,
		cancel,
		collectionID,
		s.historicalReplica,
		s.streamingReplica,
		s.tSafeReplica,
		s.searchResultMsgStream)
	s.searchCollections[collectionID] = sc
	sc.start()
}

func (s *searchService) startEmptySearchCollection() {
	ctx1, cancel := context.WithCancel(s.ctx)
	sc := newSearchCollection(ctx1,
		cancel,
		UniqueID(-1),
		s.historicalReplica,
		s.streamingReplica,
		s.tSafeReplica,
		s.searchResultMsgStream)
	s.emptySearchCollection = sc
	sc.start()
}

func (s *searchService) hasSearchCollection(collectionID UniqueID) bool {
	_, ok := s.searchCollections[collectionID]
	return ok
}

func (s *searchService) stopSearchCollection(collectionID UniqueID) {
	sc, ok := s.searchCollections[collectionID]
	if !ok {
		log.Error("stopSearchCollection failed, collection doesn't exist", zap.Int64("collectionID", collectionID))
	}
	sc.cancel()
	delete(s.searchCollections, collectionID)
}
