package datanode

import (
	"log"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type collectionReplica interface {

	// collection
	getCollectionNum() int
	addCollection(collectionID UniqueID, schemaBlob string) error
	removeCollection(collectionID UniqueID) error
	getCollectionByID(collectionID UniqueID) (*Collection, error)
	getCollectionByName(collectionName string) (*Collection, error)
	getCollectionIDByName(collectionName string) (UniqueID, error)
	hasCollection(collectionID UniqueID) bool

	// segment
	addSegment(segmentID UniqueID, collID UniqueID, partitionID UniqueID) error
	removeSegment(segmentID UniqueID) error
	hasSegment(segmentID UniqueID) bool
	updateSegmentRowNums(segmentID UniqueID, numRows int64) error
	getSegmentStatisticsUpdates(segmentID UniqueID) (*internalpb2.SegmentStatisticsUpdates, error)
	getSegmentByID(segmentID UniqueID) (*Segment, error)
}

type (
	Segment struct {
		segmentID    UniqueID
		collectionID UniqueID
		partitionID  UniqueID
		numRows      int64
		memorySize   int64
	}

	collectionReplicaImpl struct {
		mu          sync.RWMutex
		collections []*Collection
		segments    []*Segment
	}
)

//----------------------------------------------------------------------------------------------------- collection

func (colReplica *collectionReplicaImpl) getSegmentByID(segmentID UniqueID) (*Segment, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	for _, segment := range colReplica.segments {
		if segment.segmentID == segmentID {
			return segment, nil
		}
	}
	return nil, errors.Errorf("cannot find segment, id = %v", segmentID)
}

func (colReplica *collectionReplicaImpl) addSegment(segmentID UniqueID, collID UniqueID, partitionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	log.Println("Add Segment", segmentID)

	seg := &Segment{
		segmentID:    segmentID,
		collectionID: collID,
		partitionID:  partitionID,
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

func (colReplica *collectionReplicaImpl) updateSegmentRowNums(segmentID UniqueID, numRows int64) error {
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
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	for _, ele := range colReplica.segments {
		if ele.segmentID == segmentID {
			updates := &internalpb2.SegmentStatisticsUpdates{
				SegmentID:  segmentID,
				MemorySize: ele.memorySize,
				NumRows:    ele.numRows,
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

func (colReplica *collectionReplicaImpl) addCollection(collectionID UniqueID, schemaBlob string) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	var newCollection = newCollection(collectionID, schemaBlob)
	colReplica.collections = append(colReplica.collections, newCollection)
	log.Println("Create collection: ", newCollection.Name())

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
	return 0, errors.Errorf("There is no collection name=%v", collName)

}

func (colReplica *collectionReplicaImpl) removeCollection(collectionID UniqueID) error {
	// GOOSE TODO: optimize
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	tmpCollections := make([]*Collection, 0)
	for _, col := range colReplica.collections {
		if col.ID() != collectionID {
			tmpCollections = append(tmpCollections, col)
		} else {
			log.Println("Drop collection : ", col.Name())
		}
	}
	colReplica.collections = tmpCollections
	return nil
}

func (colReplica *collectionReplicaImpl) getCollectionByID(collectionID UniqueID) (*Collection, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	for _, collection := range colReplica.collections {
		if collection.ID() == collectionID {
			return collection, nil
		}
	}
	return nil, errors.Errorf("cannot find collection, id = %v", collectionID)
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
