package datanode

import (
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
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
	setIsFlushed(segmentID UniqueID) error
	setStartPosition(segmentID UniqueID, startPos *internalpb.MsgPosition) error
	setEndPosition(segmentID UniqueID, endPos *internalpb.MsgPosition) error
	updateStatistics(segmentID UniqueID, numRows int64) error
	getSegmentStatisticsUpdates(segmentID UniqueID) (*internalpb.SegmentStatisticsUpdates, error)
	getSegmentByID(segmentID UniqueID) (*Segment, error)
}

type Segment struct {
	segmentID    UniqueID
	collectionID UniqueID
	partitionID  UniqueID
	numRows      int64
	memorySize   int64
	isNew        atomic.Value // bool
	isFlushed    bool

	createTime    Timestamp // not using
	endTime       Timestamp // not using
	startPosition *internalpb.MsgPosition
	endPosition   *internalpb.MsgPosition // not using
}

type CollectionSegmentReplica struct {
	mu          sync.RWMutex
	segments    map[UniqueID]*Segment
	collections map[UniqueID]*Collection
}

func newReplica() Replica {
	segments := make(map[UniqueID]*Segment)
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

	if seg, ok := replica.segments[segmentID]; ok {
		return seg, nil
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

	position := &internalpb.MsgPosition{
		ChannelName: channelName,
	}

	seg := &Segment{
		segmentID:     segmentID,
		collectionID:  collID,
		partitionID:   partitionID,
		isFlushed:     false,
		createTime:    0,
		startPosition: position,
		endPosition:   new(internalpb.MsgPosition),
	}

	seg.isNew.Store(true)

	replica.segments[segmentID] = seg
	return nil
}

func (replica *CollectionSegmentReplica) removeSegment(segmentID UniqueID) error {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	delete(replica.segments, segmentID)

	return nil
}

func (replica *CollectionSegmentReplica) hasSegment(segmentID UniqueID) bool {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	_, ok := replica.segments[segmentID]
	return ok
}

func (replica *CollectionSegmentReplica) setIsFlushed(segmentID UniqueID) error {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	if seg, ok := replica.segments[segmentID]; ok {
		seg.isFlushed = true
		return nil
	}

	return fmt.Errorf("There's no segment %v", segmentID)
}

func (replica *CollectionSegmentReplica) setStartPosition(segmentID UniqueID, startPos *internalpb.MsgPosition) error {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	if startPos == nil {
		return fmt.Errorf("Nil MsgPosition")
	}

	if seg, ok := replica.segments[segmentID]; ok {
		seg.startPosition = startPos
		return nil
	}
	return fmt.Errorf("There's no segment %v", segmentID)
}

func (replica *CollectionSegmentReplica) setEndPosition(segmentID UniqueID, endPos *internalpb.MsgPosition) error {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	if endPos == nil {
		return fmt.Errorf("Nil MsgPosition")
	}

	if seg, ok := replica.segments[segmentID]; ok {
		seg.endPosition = endPos
		return nil
	}
	return fmt.Errorf("There's no segment %v", segmentID)
}

func (replica *CollectionSegmentReplica) updateStatistics(segmentID UniqueID, numRows int64) error {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	if seg, ok := replica.segments[segmentID]; ok {
		log.Debug("updating segment", zap.Int64("Segment ID", segmentID), zap.Int64("numRows", numRows))
		seg.memorySize = 0
		seg.numRows += numRows
		return nil
	}

	return fmt.Errorf("There's no segment %v", segmentID)
}

func (replica *CollectionSegmentReplica) getSegmentStatisticsUpdates(segmentID UniqueID) (*internalpb.SegmentStatisticsUpdates, error) {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	if seg, ok := replica.segments[segmentID]; ok {
		updates := &internalpb.SegmentStatisticsUpdates{
			SegmentID:  segmentID,
			MemorySize: seg.memorySize,
			NumRows:    seg.numRows,
		}

		if seg.isNew.Load() == true {
			updates.StartPosition = seg.startPosition
			seg.isNew.Store(false)
		}

		if seg.isFlushed {
			updates.EndPosition = seg.endPosition
		}

		return updates, nil
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
