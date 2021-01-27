package datanode

import (
	"log"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type collectionReplica interface {

	// collection
	getCollectionNum() int
	addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error
	removeCollection(collectionID UniqueID) error
	getCollectionByID(collectionID UniqueID) (*Collection, error)
	getCollectionByName(collectionName string) (*Collection, error)
	getCollectionIDByName(collectionName string) (UniqueID, error)
	hasCollection(collectionID UniqueID) bool

	// segment
	addSegment(segmentID UniqueID, collID UniqueID, partitionID UniqueID,
		positions []*internalpb2.MsgPosition) error
	removeSegment(segmentID UniqueID) error
	hasSegment(segmentID UniqueID) bool
	updateStatistics(segmentID UniqueID, numRows int64) error
	getSegmentStatisticsUpdates(segmentID UniqueID) (*internalpb2.SegmentStatisticsUpdates, error)
	getSegmentByID(segmentID UniqueID) (*Segment, error)
}

type (
	Segment struct {
		segmentID      UniqueID
		collectionID   UniqueID
		partitionID    UniqueID
		numRows        int64
		memorySize     int64
		isNew          bool
		createTime     Timestamp // not using
		endTime        Timestamp // not using
		startPositions []*internalpb2.MsgPosition
		endPositions   []*internalpb2.MsgPosition // not using
	}

	collectionReplicaImpl struct {
		mu          sync.RWMutex
		collections []*Collection
		segments    []*Segment
	}
)

func newReplica() collectionReplica {
	collections := make([]*Collection, 0)
	segments := make([]*Segment, 0)

	var replica collectionReplica = &collectionReplicaImpl{
		collections: collections,
		segments:    segments,
	}
	return replica
}

func (colReplica *collectionReplicaImpl) getSegmentByID(segmentID UniqueID) (*Segment, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	for _, segment := range colReplica.segments {
		if segment.segmentID == segmentID {
			return segment, nil
		}
	}
	return nil, errors.Errorf("Cannot find segment, id = %v", segmentID)
}

func (colReplica *collectionReplicaImpl) addSegment(segmentID UniqueID, collID UniqueID,
	partitionID UniqueID, positions []*internalpb2.MsgPosition) error {

	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	log.Println("Add Segment", segmentID)

	seg := &Segment{
		segmentID:      segmentID,
		collectionID:   collID,
		partitionID:    partitionID,
		isNew:          true,
		createTime:     0,
		startPositions: positions,
		endPositions:   make([]*internalpb2.MsgPosition, 0),
	}
	colReplica.segments = append(colReplica.segments, seg)
	return nil
}

func (colReplica *collectionReplicaImpl) removeSegment(segmentID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	for index, ele := range colReplica.segments {
		if ele.segmentID == segmentID {
			log.Println("Removing segment:", segmentID)
			numOfSegs := len(colReplica.segments)
			colReplica.segments[index] = colReplica.segments[numOfSegs-1]
			colReplica.segments = colReplica.segments[:numOfSegs-1]
			return nil
		}
	}
	return errors.Errorf("Error, there's no segment %v", segmentID)
}

func (colReplica *collectionReplicaImpl) hasSegment(segmentID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	for _, ele := range colReplica.segments {
		if ele.segmentID == segmentID {
			return true
		}
	}
	return false
}

func (colReplica *collectionReplicaImpl) updateStatistics(segmentID UniqueID, numRows int64) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	for _, ele := range colReplica.segments {
		if ele.segmentID == segmentID {
			log.Printf("updating segment(%v) row nums: (%v)", segmentID, numRows)
			ele.memorySize = 0
			ele.numRows += numRows
			return nil
		}
	}
	return errors.Errorf("Error, there's no segment %v", segmentID)
}

func (colReplica *collectionReplicaImpl) getSegmentStatisticsUpdates(segmentID UniqueID) (*internalpb2.SegmentStatisticsUpdates, error) {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	for _, ele := range colReplica.segments {
		if ele.segmentID == segmentID {
			updates := &internalpb2.SegmentStatisticsUpdates{
				SegmentID:    segmentID,
				MemorySize:   ele.memorySize,
				NumRows:      ele.numRows,
				IsNewSegment: ele.isNew,
			}

			if ele.isNew {
				updates.StartPositions = ele.startPositions
				ele.isNew = false
			}
			return updates, nil
		}
	}
	return nil, errors.Errorf("Error, there's no segment %v", segmentID)
}

func (colReplica *collectionReplicaImpl) getCollectionNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	return len(colReplica.collections)
}

func (colReplica *collectionReplicaImpl) addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	var newCollection = newCollection(collectionID, schema)
	colReplica.collections = append(colReplica.collections, newCollection)
	log.Println("Create collection:", newCollection.Name())

	return nil
}

func (colReplica *collectionReplicaImpl) getCollectionIDByName(collName string) (UniqueID, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	for _, collection := range colReplica.collections {
		if collection.Name() == collName {
			return collection.ID(), nil
		}
	}
	return 0, errors.Errorf("Cannot get collection ID by name %s: not exist", collName)

}

func (colReplica *collectionReplicaImpl) removeCollection(collectionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	length := len(colReplica.collections)
	for index, col := range colReplica.collections {
		if col.ID() == collectionID {
			log.Println("Drop collection: ", col.Name())
			colReplica.collections[index] = colReplica.collections[length-1]
			colReplica.collections = colReplica.collections[:length-1]
			return nil
		}
	}

	return errors.Errorf("Cannot remove collection %d: not exist", collectionID)
}

func (colReplica *collectionReplicaImpl) getCollectionByID(collectionID UniqueID) (*Collection, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	for _, collection := range colReplica.collections {
		if collection.ID() == collectionID {
			return collection, nil
		}
	}
	return nil, errors.Errorf("Cannot get collection %d by ID: not exist", collectionID)
}

func (colReplica *collectionReplicaImpl) getCollectionByName(collectionName string) (*Collection, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	for _, collection := range colReplica.collections {
		if collection.Name() == collectionName {
			return collection, nil
		}
	}

	return nil, errors.Errorf("Cannot found collection: %v", collectionName)
}

func (colReplica *collectionReplicaImpl) hasCollection(collectionID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	for _, col := range colReplica.collections {
		if col.ID() == collectionID {
			return true
		}
	}
	return false
}
