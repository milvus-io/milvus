package querynode

import "C"
import (
	"context"
	"errors"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

type searchService struct {
	ctx    context.Context
	cancel context.CancelFunc

	replica ReplicaInterface

	searchMsgStream       msgstream.MsgStream
	searchResultMsgStream msgstream.MsgStream

	queryNodeID           UniqueID
	searchCollections     map[UniqueID]*searchCollection
	emptySearchCollection *searchCollection
}

func newSearchService(ctx context.Context, replica ReplicaInterface, factory msgstream.Factory) *searchService {
	searchStream, _ := factory.NewQueryMsgStream(ctx)
	searchResultStream, _ := factory.NewQueryMsgStream(ctx)

	// query node doesn't need to consumer any search or search result channel actively.
	consumeChannels := Params.SearchChannelNames
	consumeSubName := Params.MsgChannelSubName
	searchStream.AsConsumer(consumeChannels, consumeSubName)
	log.Debug("query node AsConsumer: " + strings.Join(consumeChannels, ", ") + " : " + consumeSubName)
	producerChannels := Params.SearchResultChannelNames
	searchResultStream.AsProducer(producerChannels)
	log.Debug("query node AsProducer: " + strings.Join(producerChannels, ", "))

	searchServiceCtx, searchServiceCancel := context.WithCancel(ctx)
	return &searchService{
		ctx:    searchServiceCtx,
		cancel: searchServiceCancel,

		replica: replica,

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
	if ok := s.replica.hasCollection(collectionID); !ok {
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
			msgPack, _ := s.searchMsgStream.Consume()
			if msgPack == nil || len(msgPack.Msgs) <= 0 {
				continue
			}
			emptySearchNum := 0
			for _, msg := range msgPack.Msgs {
				sm, ok := msg.(*msgstream.SearchMsg)
				if !ok {
					continue
				}
				err := s.collectionCheck(sm.CollectionID)
				if err != nil {
					s.emptySearchCollection.emptySearch(sm)
					emptySearchNum++
					continue
				}
				sc, ok := s.searchCollections[sm.CollectionID]
				if !ok {
					s.startSearchCollection(sm.CollectionID)
				}
				sc.msgBuffer <- sm
			}
			log.Debug("do empty search done", zap.Int("num of searchMsg", emptySearchNum))
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
	sc := newSearchCollection(ctx1, cancel, collectionID, s.replica, s.searchResultMsgStream)
	s.searchCollections[collectionID] = sc
	sc.start()
}

func (s *searchService) startEmptySearchCollection() {
	ctx1, cancel := context.WithCancel(s.ctx)
	sc := newSearchCollection(ctx1, cancel, UniqueID(-1), s.replica, s.searchResultMsgStream)
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
