package reader

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "collection_c.h"
#include "segment_c.h"

*/
import "C"
import (
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
)

type container interface {
	// collection
	getCollectionNum() int
	addCollection(collMeta *etcdpb.CollectionMeta, collMetaBlob string) error
	removeCollection(collectionID UniqueID) error
	getCollectionByID(collectionID UniqueID) (*Collection, error)
	getCollectionByName(collectionName string) (*Collection, error)

	// partition
	// Partition tags in different collections are not unique,
	// so partition api should specify the target collection.
	addPartition(collectionID UniqueID, partitionTag string) error
	removePartition(collectionID UniqueID, partitionTag string) error
	getPartitionByTag(collectionID UniqueID, partitionTag string) (*Partition, error)

	// segment
	getSegmentNum() int
	getSegmentStatistics() *internalpb.QueryNodeSegStats
	addSegment(segmentID UniqueID, partitionTag string, collectionID UniqueID) error
	removeSegment(segmentID UniqueID) error
	getSegmentByID(segmentID UniqueID) (*Segment, error)
	hasSegment(segmentID UniqueID) bool
}

// TODO: rename
type colSegContainer struct {
	mu          sync.RWMutex
	collections []*Collection
	segments    map[UniqueID]*Segment
}

//----------------------------------------------------------------------------------------------------- collection
func (container *colSegContainer) getCollectionNum() int {
	container.mu.RLock()
	defer container.mu.RUnlock()

	return len(container.collections)
}

func (container *colSegContainer) addCollection(collMeta *etcdpb.CollectionMeta, collMetaBlob string) error {
	container.mu.Lock()
	defer container.mu.Unlock()

	var newCollection = newCollection(collMeta, collMetaBlob)
	container.collections = append(container.collections, newCollection)

	return nil
}

func (container *colSegContainer) removeCollection(collectionID UniqueID) error {
	collection, err := container.getCollectionByID(collectionID)

	container.mu.Lock()
	defer container.mu.Unlock()

	if err != nil {
		return err
	}

	deleteCollection(collection)

	tmpCollections := make([]*Collection, 0)
	for _, col := range container.collections {
		if col.ID() == collectionID {
			for _, p := range *col.Partitions() {
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

func (container *colSegContainer) getCollectionByID(collectionID UniqueID) (*Collection, error) {
	container.mu.RLock()
	defer container.mu.RUnlock()

	for _, collection := range container.collections {
		if collection.ID() == collectionID {
			return collection, nil
		}
	}

	return nil, errors.New("cannot find collection, id = " + strconv.FormatInt(collectionID, 10))
}

func (container *colSegContainer) getCollectionByName(collectionName string) (*Collection, error) {
	container.mu.RLock()
	defer container.mu.RUnlock()

	for _, collection := range container.collections {
		if collection.Name() == collectionName {
			return collection, nil
		}
	}

	return nil, errors.New("Cannot found collection: " + collectionName)
}

//----------------------------------------------------------------------------------------------------- partition
func (container *colSegContainer) addPartition(collectionID UniqueID, partitionTag string) error {
	collection, err := container.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	container.mu.Lock()
	defer container.mu.Unlock()

	var newPartition = newPartition(partitionTag)

	*collection.Partitions() = append(*collection.Partitions(), newPartition)
	return nil
}

func (container *colSegContainer) removePartition(collectionID UniqueID, partitionTag string) error {
	collection, err := container.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	container.mu.Lock()
	defer container.mu.Unlock()

	var tmpPartitions = make([]*Partition, 0)
	for _, p := range *collection.Partitions() {
		if p.Tag() == partitionTag {
			for _, s := range *p.Segments() {
				delete(container.segments, s.ID())
			}
		} else {
			tmpPartitions = append(tmpPartitions, p)
		}
	}

	*collection.Partitions() = tmpPartitions
	return nil
}

func (container *colSegContainer) getPartitionByTag(collectionID UniqueID, partitionTag string) (*Partition, error) {
	collection, err := container.getCollectionByID(collectionID)
	if err != nil {
		return nil, err
	}

	container.mu.RLock()
	defer container.mu.RUnlock()

	for _, p := range *collection.Partitions() {
		if p.Tag() == partitionTag {
			return p, nil
		}
	}

	return nil, errors.New("cannot find partition, tag = " + partitionTag)
}

//----------------------------------------------------------------------------------------------------- segment
func (container *colSegContainer) getSegmentNum() int {
	container.mu.RLock()
	defer container.mu.RUnlock()

	return len(container.segments)
}

func (container *colSegContainer) getSegmentStatistics() *internalpb.QueryNodeSegStats {
	var statisticData = make([]*internalpb.SegmentStats, 0)

	for segmentID, segment := range container.segments {
		currentMemSize := segment.getMemSize()
		segment.lastMemSize = currentMemSize
		segmentNumOfRows := segment.getRowCount()

		stat := internalpb.SegmentStats{
			SegmentID:        segmentID,
			MemorySize:       currentMemSize,
			NumRows:          segmentNumOfRows,
			RecentlyModified: segment.recentlyModified,
		}

		statisticData = append(statisticData, &stat)
		segment.recentlyModified = false
	}

	return &internalpb.QueryNodeSegStats{
		MsgType:  internalpb.MsgType_kQueryNodeSegStats,
		SegStats: statisticData,
	}
}

func (container *colSegContainer) addSegment(segmentID UniqueID, partitionTag string, collectionID UniqueID) error {
	collection, err := container.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	partition, err := container.getPartitionByTag(collectionID, partitionTag)
	if err != nil {
		return err
	}

	container.mu.Lock()
	defer container.mu.Unlock()

	var newSegment = newSegment(collection, segmentID)

	container.segments[segmentID] = newSegment
	*partition.Segments() = append(*partition.Segments(), newSegment)

	return nil
}

func (container *colSegContainer) removeSegment(segmentID UniqueID) error {
	container.mu.Lock()
	defer container.mu.Unlock()

	var targetPartition *Partition
	var segmentIndex = -1

	for _, col := range container.collections {
		for _, p := range *col.Partitions() {
			for i, s := range *p.Segments() {
				if s.ID() == segmentID {
					targetPartition = p
					segmentIndex = i
				}
			}
		}
	}

	delete(container.segments, segmentID)

	if targetPartition != nil && segmentIndex > 0 {
		targetPartition.segments = append(targetPartition.segments[:segmentIndex], targetPartition.segments[segmentIndex+1:]...)
	}

	return nil
}

func (container *colSegContainer) getSegmentByID(segmentID UniqueID) (*Segment, error) {
	container.mu.RLock()
	defer container.mu.RUnlock()

	targetSegment, ok := container.segments[segmentID]

	if !ok {
		return nil, errors.New("cannot found segment with id = " + strconv.FormatInt(segmentID, 10))
	}

	return targetSegment, nil
}

func (container *colSegContainer) hasSegment(segmentID UniqueID) bool {
	container.mu.RLock()
	defer container.mu.RUnlock()

	_, ok := container.segments[segmentID]

	return ok
}
