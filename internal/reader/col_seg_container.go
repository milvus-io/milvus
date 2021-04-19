package reader

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "collection_c.h"
#include "segment_c.h"

*/
import "C"
import (
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
)

// TODO: rename
type ColSegContainer struct {
	collections []*Collection
	segments    map[UniqueID]*Segment
}

//----------------------------------------------------------------------------------------------------- collection
func (container *ColSegContainer) addCollection(collMeta *etcdpb.CollectionMeta, collMetaBlob string) *Collection {
	var newCollection = newCollection(collMeta, collMetaBlob)
	container.collections = append(container.collections, newCollection)

	return newCollection
}

func (container *ColSegContainer) removeCollection(collection *Collection) error {
	if collection == nil {
		return errors.New("null collection")
	}

	deleteCollection(collection)

	collectionID := collection.ID()
	tmpCollections := make([]*Collection, 0)
	for _, col := range container.collections {
		if col.ID() == collectionID {
			for _, p := range *collection.Partitions() {
				for _, s := range *p.Segments() {
					delete(container.segments, s.ID())
				}
			}
		} else {
			tmpCollections = append(tmpCollections, col)
		}
	}

	container.collections = tmpCollections
	return nil
}

func (container *ColSegContainer) getCollectionByID(collectionID int64) (*Collection, error) {
	for _, collection := range container.collections {
		if collection.ID() == collectionID {
			return collection, nil
		}
	}

	return nil, errors.New("cannot find collection, id = " + strconv.FormatInt(collectionID, 10))
}

func (container *ColSegContainer) getCollectionByName(collectionName string) (*Collection, error) {
	for _, collection := range container.collections {
		if collection.Name() == collectionName {
			return collection, nil
		}
	}

	return nil, errors.New("Cannot found collection: " + collectionName)
}

//----------------------------------------------------------------------------------------------------- partition
func (container *ColSegContainer) addPartition(collection *Collection, partitionTag string) (*Partition, error) {
	if collection == nil {
		return nil, errors.New("null collection")
	}

	var newPartition = newPartition(partitionTag)

	for _, col := range container.collections {
		if col.Name() == collection.Name() {
			*col.Partitions() = append(*col.Partitions(), newPartition)
			return newPartition, nil
		}
	}

	return nil, errors.New("cannot find collection, name = " + collection.Name())
}

func (container *ColSegContainer) removePartition(partition *Partition) error {
	if partition == nil {
		return errors.New("null partition")
	}

	var targetCollection *Collection
	var tmpPartitions = make([]*Partition, 0)
	var hasPartition = false

	for _, col := range container.collections {
		for _, p := range *col.Partitions() {
			if p.Tag() == partition.partitionTag {
				targetCollection = col
				hasPartition = true
				for _, s := range *p.Segments() {
					delete(container.segments, s.ID())
				}
			} else {
				tmpPartitions = append(tmpPartitions, p)
			}
		}
	}

	if hasPartition && targetCollection != nil {
		*targetCollection.Partitions() = tmpPartitions
		return nil
	}

	return errors.New("cannot found partition, tag = " + partition.Tag())
}

func (container *ColSegContainer) getPartitionByTag(collectionName string, partitionTag string) (*Partition, error) {
	targetCollection, err := container.getCollectionByName(collectionName)
	if err != nil {
		return nil, err
	}
	for _, p := range *targetCollection.Partitions() {
		if p.Tag() == partitionTag {
			return p, nil
		}
	}

	return nil, errors.New("cannot find partition, tag = " + partitionTag)
}

//----------------------------------------------------------------------------------------------------- segment
func (container *ColSegContainer) addSegment(collection *Collection, partition *Partition, segmentID int64) (*Segment, error) {
	if collection == nil {
		return nil, errors.New("null collection")
	}

	if partition == nil {
		return nil, errors.New("null partition")
	}

	var newSegment = newSegment(collection, segmentID)
	container.segments[segmentID] = newSegment

	for _, col := range container.collections {
		if col.ID() == collection.ID() {
			for _, p := range *col.Partitions() {
				if p.Tag() == partition.Tag() {
					*p.Segments() = append(*p.Segments(), newSegment)
					return newSegment, nil
				}
			}
		}
	}

	return nil, errors.New("cannot find collection or segment")
}

func (container *ColSegContainer) removeSegment(segment *Segment) error {
	var targetPartition *Partition
	var tmpSegments = make([]*Segment, 0)
	var hasSegment = false

	for _, col := range container.collections {
		for _, p := range *col.Partitions() {
			for _, s := range *p.Segments() {
				if s.ID() == segment.ID() {
					targetPartition = p
					hasSegment = true
					delete(container.segments, segment.ID())
				} else {
					tmpSegments = append(tmpSegments, s)
				}
			}
		}
	}

	if hasSegment && targetPartition != nil {
		*targetPartition.Segments() = tmpSegments
		return nil
	}

	return errors.New("cannot found segment, id = " + strconv.FormatInt(segment.ID(), 10))
}

func (container *ColSegContainer) getSegmentByID(segmentID int64) (*Segment, error) {
	targetSegment, ok := container.segments[segmentID]

	if !ok {
		return nil, errors.New("cannot found segment with id = " + strconv.FormatInt(segmentID, 10))
	}

	return targetSegment, nil
}

func (container *ColSegContainer) hasSegment(segmentID int64) bool {
	_, ok := container.segments[segmentID]

	return ok
}
