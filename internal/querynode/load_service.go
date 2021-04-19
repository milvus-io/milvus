package querynode

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/types"
)

const loadingCheckInterval = 3

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
		case <-time.After(loadingCheckInterval * time.Second):
			wg.Add(2)
			//go s.segLoader.indexLoader.doLoadIndex(wg)
			go s.loadSegmentActively(wg)
			wg.Wait()
		}
	}
}

func (s *loadService) close() {
	s.cancel()
}

func (s *loadService) loadSegmentActively(wg *sync.WaitGroup) {
	collectionIDs, partitionIDs, segmentIDs := s.segLoader.replica.getSegmentsToLoadBySegmentType(segmentTypeGrowing)
	if len(collectionIDs) <= 0 {
		wg.Done()
		return
	}
	log.Debug("do load segment for growing segments:", zap.String("segmentIDs", fmt.Sprintln(segmentIDs)))
	for i := range collectionIDs {
		collection, err := s.segLoader.replica.getCollectionByID(collectionIDs[i])
		if err != nil {
			log.Warn(err.Error())
		}

		fieldIDs, err := s.segLoader.replica.getFieldIDsByCollectionID(collectionIDs[i])
		if err != nil {
			log.Error(err.Error())
			continue
		}
		segment := newSegment(collection, segmentIDs[i], partitionIDs[i], collectionIDs[i], segmentTypeSealed)
		segment.setLoadBinLogEnable(true)
		err = s.loadSegmentInternal(collectionIDs[i], segment, fieldIDs)
		if err == nil {
			// replace segment
			err = s.segLoader.replica.replaceGrowingSegmentBySealedSegment(segment)
		}
		if err != nil {
			deleteSegment(segment)
			log.Error(err.Error())
		}
	}
	// sendQueryNodeStats
	err := s.segLoader.indexLoader.sendQueryNodeStats()
	if err != nil {
		log.Error(err.Error())
		wg.Done()
		return
	}

	wg.Done()
}

// load segment passively
func (s *loadService) loadSegmentPassively(collectionID UniqueID, partitionID UniqueID, segmentIDs []UniqueID, fieldIDs []int64) error {
	// TODO: interim solution
	if len(fieldIDs) == 0 {
		var err error
		fieldIDs, err = s.segLoader.replica.getFieldIDsByCollectionID(collectionID)
		if err != nil {
			return err
		}
	}
	for _, segmentID := range segmentIDs {
		collection, err := s.segLoader.replica.getCollectionByID(collectionID)
		if err != nil {
			return err
		}
		_, err = s.segLoader.replica.getPartitionByID(partitionID)
		if err != nil {
			return err
		}

		segment := newSegment(collection, segmentID, partitionID, collectionID, segmentTypeSealed)
		segment.setLoadBinLogEnable(true)
		err = s.loadSegmentInternal(collectionID, segment, fieldIDs)
		if err == nil {
			err = s.segLoader.replica.setSegment(segment)
		}
		if err != nil {
			log.Warn(err.Error())
			err = s.addSegmentToLoadBuffer(segment)
			if err != nil {
				log.Warn(err.Error())
			}
		}
	}
	return nil
}

func (s *loadService) addSegmentToLoadBuffer(segment *Segment) error {
	segmentID := segment.segmentID
	partitionID := segment.partitionID
	collectionID := segment.collectionID
	deleteSegment(segment)
	err := s.segLoader.replica.addSegment(segmentID, partitionID, collectionID, segmentTypeGrowing)
	if err != nil {
		return err
	}
	err = s.segLoader.replica.setSegmentEnableLoadBinLog(segmentID, true)
	if err != nil {
		s.segLoader.replica.removeSegment(segmentID)
	}

	return err
}

func (s *loadService) loadSegmentInternal(collectionID UniqueID, segment *Segment, fieldIDs []int64) error {
	// create segment
	statesResp, err := s.segLoader.GetSegmentStates(segment.segmentID)
	if err != nil {
		return err
	}
	if statesResp.States[0].State != commonpb.SegmentState_Flushed {
		return errors.New("segment not flush done")
	}

	insertBinlogPaths, srcFieldIDs, err := s.segLoader.getInsertBinlogPaths(segment.segmentID)
	if err != nil {
		return err
	}
	vectorFieldIDs, err := s.segLoader.replica.getVecFieldIDsByCollectionID(collectionID)
	if err != nil {
		return err
	}

	loadIndexFieldIDs := make([]int64, 0)
	for _, vecFieldID := range vectorFieldIDs {
		err = s.segLoader.indexLoader.setIndexInfo(collectionID, segment, vecFieldID)
		if err != nil {
			log.Warn(err.Error())
			continue
		}
		loadIndexFieldIDs = append(loadIndexFieldIDs, vecFieldID)
	}
	// we don't need load to vector fields
	fieldIDs = s.segLoader.filterOutVectorFields(fieldIDs, loadIndexFieldIDs)

	//log.Debug("srcFieldIDs in internal:", srcFieldIDs)
	//log.Debug("dstFieldIDs in internal:", fieldIDs)
	targetFields, err := s.segLoader.checkTargetFields(insertBinlogPaths, srcFieldIDs, fieldIDs)
	if err != nil {
		return err
	}
	log.Debug("loading insert...")
	err = s.segLoader.loadSegmentFieldsData(segment, targetFields)
	if err != nil {
		return err
	}
	for _, id := range loadIndexFieldIDs {
		log.Debug("loading index...")
		err = s.segLoader.indexLoader.loadIndex(segment, id)
		if err != nil {
			return err
		}
	}

	return nil
}

func newLoadService(ctx context.Context, masterService types.MasterService, dataService types.DataService, indexService types.IndexService, replica ReplicaInterface) *loadService {
	ctx1, cancel := context.WithCancel(ctx)

	segLoader := newSegmentLoader(ctx1, masterService, indexService, dataService, replica)

	return &loadService{
		ctx:    ctx1,
		cancel: cancel,

		segLoader: segLoader,
	}
}
