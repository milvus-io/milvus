package querynode

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

const indexCheckInterval = 3

type loadService struct {
	ctx    context.Context
	cancel context.CancelFunc

	segLoader *segmentLoader
}

// -------------------------------------------- load index -------------------------------------------- //
func (s *loadService) start() {
	wg := &sync.WaitGroup{}
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(indexCheckInterval * time.Second):
			wg.Add(2)
			go s.segLoader.indexLoader.doLoadIndex(wg)
			go s.loadSegmentActively(wg)
			wg.Wait()
		}
	}
}

func (s *loadService) close() {
	s.cancel()
}

func (s *loadService) loadSegmentActively(wg *sync.WaitGroup) {
	collectionIDs, partitionIDs, segmentIDs := s.segLoader.replica.getSealedSegmentsBySegmentType(segTypeGrowing)
	if len(collectionIDs) <= 0 {
		wg.Done()
		return
	}
	fmt.Println("do load segment for growing segments:", segmentIDs)
	for i := range collectionIDs {
		fieldIDs, err := s.segLoader.replica.getFieldIDsByCollectionID(collectionIDs[i])
		if err != nil {
			log.Println(err)
			continue
		}
		err = s.loadSegmentInternal(collectionIDs[i], partitionIDs[i], segmentIDs[i], fieldIDs)
		if err != nil {
			log.Println(err)
		}
	}
	// sendQueryNodeStats
	err := s.segLoader.indexLoader.sendQueryNodeStats()
	if err != nil {
		log.Println(err)
		wg.Done()
		return
	}

	wg.Done()
}

// load segment passively
func (s *loadService) loadSegment(collectionID UniqueID, partitionID UniqueID, segmentIDs []UniqueID, fieldIDs []int64) error {
	// TODO: interim solution
	if len(fieldIDs) == 0 {
		var err error
		fieldIDs, err = s.segLoader.replica.getFieldIDsByCollectionID(collectionID)
		if err != nil {
			return err
		}
	}
	for _, segmentID := range segmentIDs {
		err := s.loadSegmentInternal(collectionID, partitionID, segmentID, fieldIDs)
		if err != nil {
			log.Println(err)
			continue
		}
	}
	return nil
}

func (s *loadService) loadSegmentInternal(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, fieldIDs []int64) error {
	// we don't need index id yet
	_, buildID, errIndex := s.segLoader.indexLoader.getIndexInfo(collectionID, segmentID)
	if errIndex == nil {
		// we don't need load to vector fields
		vectorFields, err := s.segLoader.replica.getVecFieldIDsByCollectionID(collectionID)
		if err != nil {
			return err
		}
		fieldIDs = s.segLoader.filterOutVectorFields(fieldIDs, vectorFields)
	}
	paths, srcFieldIDs, err := s.segLoader.getInsertBinlogPaths(segmentID)
	if err != nil {
		return err
	}

	targetFields := s.segLoader.getTargetFields(paths, srcFieldIDs, fieldIDs)
	collection, err := s.segLoader.replica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}
	segment := newSegment(collection, segmentID, partitionID, collectionID, segTypeSealed)
	err = s.segLoader.loadSegmentFieldsData(segment, targetFields)
	if err != nil {
		return err
	}
	// replace segment
	err = s.segLoader.replica.replaceGrowingSegmentBySealedSegment(segment)
	if err != nil {
		return err
	}
	if errIndex == nil {
		fmt.Println("loading index...")
		indexPaths, err := s.segLoader.indexLoader.getIndexPaths(buildID)
		if err != nil {
			return err
		}
		err = s.segLoader.indexLoader.loadIndexImmediate(segment, indexPaths)
		if err != nil {
			return err
		}
	}
	return nil
}

func newLoadService(ctx context.Context, masterClient MasterServiceInterface, dataClient DataServiceInterface, indexClient IndexServiceInterface, replica collectionReplica, dmStream msgstream.MsgStream) *loadService {
	ctx1, cancel := context.WithCancel(ctx)

	segLoader := newSegmentLoader(ctx1, masterClient, indexClient, dataClient, replica, dmStream)

	return &loadService{
		ctx:    ctx1,
		cancel: cancel,

		segLoader: segLoader,
	}
}
