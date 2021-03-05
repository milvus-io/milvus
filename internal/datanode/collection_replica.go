package datanode

import (
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type Replica interface {

	// collection
	getCollectionNum() int
	addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error
	removeCollection(collectionID UniqueID) error
	getCollectionByID(collectionID UniqueID) (*Collection, error)
	hasCollection(collectionID UniqueID) bool

	// segment
	addSegment(segmentID UniqueID, collID UniqueID, partitionID UniqueID, channelName string) error
	removeSegment(segmentID UniqueID) error
	hasSegment(segmentID UniqueID) bool
	updateStatistics(segmentID UniqueID, numRows int64) error
	getSegmentStatisticsUpdates(segmentID UniqueID) (*internalpb2.SegmentStatisticsUpdates, error)
	getSegmentByID(segmentID UniqueID) (*Segment, error)
}

type Segment struct {
	segmentID     UniqueID
	collectionID  UniqueID
	partitionID   UniqueID
	numRows       int64
	memorySize    int64
	isNew         bool
	createTime    Timestamp // not using
	endTime       Timestamp // not using
	startPosition *internalpb2.MsgPosition
	endPosition   *internalpb2.MsgPosition // not using
}

type CollectionSegmentReplica struct {
	mu          sync.RWMutex
	segments    []*Segment
	collections map[UniqueID]*Collection
}

func newReplica() Replica {
	segments := make([]*Segment, 0)
	collections := make(map[UniqueID]*Collection)

	var replica Replica = &CollectionSegmentReplica{
		segments:    segments,
		collections: collections,
	}
	return replica
}

// --- segment ---
func (replica *CollectionSegmentReplica) getSegmentByID(segmentID UniqueID) (*Segment, error) {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	for _, segment := range replica.segments {
		if segment.segmentID == segmentID {
			return segment, nil
		}
	}
	return nil, fmt.Errorf("Cannot find segment, id = %v", segmentID)
}

func (replica *CollectionSegmentReplica) addSegment(
	segmentID UniqueID,
	collID UniqueID,
	partitionID UniqueID,
	channelName string) error {

	replica.mu.Lock()
	defer replica.mu.Unlock()
	log.Debug("Add Segment", zap.Int64("Segment ID", segmentID))

	position := &internalpb2.MsgPosition{
		ChannelName: channelName,
	}

	seg := &Segment{
		segmentID:     segmentID,
		collectionID:  collID,
		partitionID:   partitionID,
		isNew:         true,
		createTime:    0,
		startPosition: position,
		endPosition:   new(internalpb2.MsgPosition),
	}
	replica.segments = append(replica.segments, seg)
	return nil
}

func (replica *CollectionSegmentReplica) removeSegment(segmentID UniqueID) error {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	for index, ele := range replica.segments {
		if ele.segmentID == segmentID {
			log.Debug("Removing segment", zap.Int64("Segment ID", segmentID))
			numOfSegs := len(replica.segments)
			replica.segments[index] = replica.segments[numOfSegs-1]
			replica.segments = replica.segments[:numOfSegs-1]
			return nil
		}
	}
	return fmt.Errorf("Error, there's no segment %v", segmentID)
}

func (replica *CollectionSegmentReplica) hasSegment(segmentID UniqueID) bool {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	for _, ele := range replica.segments {
		if ele.segmentID == segmentID {
			return true
		}
	}
	return false
}

func (replica *CollectionSegmentReplica) updateStatistics(segmentID UniqueID, numRows int64) error {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	for _, ele := range replica.segments {
		if ele.segmentID == segmentID {
			log.Debug("updating segment", zap.Int64("Segment ID", segmentID), zap.Int64("numRows", numRows))
			ele.memorySize = 0
			ele.numRows += numRows
			return nil
		}
	}
	return fmt.Errorf("Error, there's no segment %v", segmentID)
}

func (replica *CollectionSegmentReplica) getSegmentStatisticsUpdates(segmentID UniqueID) (*internalpb2.SegmentStatisticsUpdates, error) {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	for _, ele := range replica.segments {
		if ele.segmentID == segmentID {
			updates := &internalpb2.SegmentStatisticsUpdates{
				SegmentID:     segmentID,
				MemorySize:    ele.memorySize,
				NumRows:       ele.numRows,
				IsNewSegment:  ele.isNew,
				StartPosition: new(internalpb2.MsgPosition),
			}

			if ele.isNew {
				updates.StartPosition = ele.startPosition
				ele.isNew = false
			}
			return updates, nil
		}
	}
	return nil, fmt.Errorf("Error, there's no segment %v", segmentID)
}

// --- collection ---
func (replica *CollectionSegmentReplica) getCollectionNum() int {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	return len(replica.collections)
}

func (replica *CollectionSegmentReplica) addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	if _, ok := replica.collections[collectionID]; ok {
		return fmt.Errorf("Create an existing collection=%s", schema.GetName())
	}

	newCollection, err := newCollection(collectionID, schema)
	if err != nil {
		return err
	}

	replica.collections[collectionID] = newCollection
	log.Debug("Create collection", zap.String("collection name", newCollection.GetName()))

	return nil
}

func (replica *CollectionSegmentReplica) removeCollection(collectionID UniqueID) error {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	delete(replica.collections, collectionID)

	return nil
}

func (replica *CollectionSegmentReplica) getCollectionByID(collectionID UniqueID) (*Collection, error) {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	coll, ok := replica.collections[collectionID]
	if !ok {
		return nil, fmt.Errorf("Cannot get collection %d by ID: not exist", collectionID)
	}

	return coll, nil
}

func (replica *CollectionSegmentReplica) hasCollection(collectionID UniqueID) bool {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	_, ok := replica.collections[collectionID]
	return ok
}
