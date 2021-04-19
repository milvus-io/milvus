package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"
import (
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

/*
 * collectionReplica contains a in-memory local copy of persistent collections.
 * In common cases, the system has multiple query nodes. Data of a collection will be
 * distributed across all the available query nodes, and each query node's collectionReplica
 * will maintain its own share (only part of the collection).
 * Every replica tracks a value called tSafe which is the maximum timestamp that the replica
 * is up-to-date.
 */
type collectionReplica interface {
	getTSafe() tSafe

	// collection
	getCollectionNum() int
	addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error
	removeCollection(collectionID UniqueID) error
	getCollectionByID(collectionID UniqueID) (*Collection, error)
	getCollectionByName(collectionName string) (*Collection, error)
	hasCollection(collectionID UniqueID) bool

	// partition
	// Partition tags in different collections are not unique,
	// so partition api should specify the target collection.
	getPartitionNum(collectionID UniqueID) (int, error)
	addPartition(collectionID UniqueID, partitionTag string) error
	removePartition(collectionID UniqueID, partitionTag string) error
	addPartitionsByCollectionMeta(colMeta *etcdpb.CollectionMeta) error
	removePartitionsByCollectionMeta(colMeta *etcdpb.CollectionMeta) error
	getPartitionByTag(collectionID UniqueID, partitionTag string) (*Partition, error)
	hasPartition(collectionID UniqueID, partitionTag string) bool

	// segment
	getSegmentNum() int
	getSegmentStatistics() []*internalpb.SegmentStats
	addSegment(segmentID UniqueID, partitionTag string, collectionID UniqueID) error
	removeSegment(segmentID UniqueID) error
	getSegmentByID(segmentID UniqueID) (*Segment, error)
	hasSegment(segmentID UniqueID) bool
	getVecFieldIDsBySegmentID(segmentID UniqueID) ([]int64, error)

	freeAll()
}

type collectionReplicaImpl struct {
	tSafe tSafe

	mu          sync.RWMutex // guards collections and segments
	collections []*Collection
	segments    map[UniqueID]*Segment
}

//----------------------------------------------------------------------------------------------------- tSafe
func (colReplica *collectionReplicaImpl) getTSafe() tSafe {
	return colReplica.tSafe
}

//----------------------------------------------------------------------------------------------------- collection
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

	return nil
}

func (colReplica *collectionReplicaImpl) removeCollection(collectionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}

	deleteCollection(collection)

	tmpCollections := make([]*Collection, 0)
	for _, col := range colReplica.collections {
		if col.ID() == collectionID {
			for _, p := range *col.Partitions() {
				for _, s := range *p.Segments() {
					deleteSegment(colReplica.segments[s.ID()])
					delete(colReplica.segments, s.ID())
				}
			}
		} else {
			tmpCollections = append(tmpCollections, col)
		}
	}

	colReplica.collections = tmpCollections
	return nil
}

func (colReplica *collectionReplicaImpl) getCollectionByID(collectionID UniqueID) (*Collection, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	return colReplica.getCollectionByIDPrivate(collectionID)
}

func (colReplica *collectionReplicaImpl) getCollectionByIDPrivate(collectionID UniqueID) (*Collection, error) {
	for _, collection := range colReplica.collections {
		if collection.ID() == collectionID {
			return collection, nil
		}
	}

	return nil, errors.New("cannot find collection, id = " + strconv.FormatInt(collectionID, 10))
}

func (colReplica *collectionReplicaImpl) getCollectionByName(collectionName string) (*Collection, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	for _, collection := range colReplica.collections {
		if collection.Name() == collectionName {
			return collection, nil
		}
	}

	return nil, errors.New("Cannot found collection: " + collectionName)
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

//----------------------------------------------------------------------------------------------------- partition
func (colReplica *collectionReplicaImpl) getPartitionNum(collectionID UniqueID) (int, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return -1, err
	}

	return len(collection.partitions), nil
}

func (colReplica *collectionReplicaImpl) addPartition(collectionID UniqueID, partitionTag string) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}

	var newPartition = newPartition(partitionTag)

	*collection.Partitions() = append(*collection.Partitions(), newPartition)
	return nil
}

func (colReplica *collectionReplicaImpl) removePartition(collectionID UniqueID, partitionTag string) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	return colReplica.removePartitionPrivate(collectionID, partitionTag)
}

func (colReplica *collectionReplicaImpl) removePartitionPrivate(collectionID UniqueID, partitionTag string) error {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}

	var tmpPartitions = make([]*Partition, 0)
	for _, p := range *collection.Partitions() {
		if p.Tag() == partitionTag {
			for _, s := range *p.Segments() {
				deleteSegment(colReplica.segments[s.ID()])
				delete(colReplica.segments, s.ID())
			}
		} else {
			tmpPartitions = append(tmpPartitions, p)
		}
	}

	*collection.Partitions() = tmpPartitions
	return nil
}

// deprecated
func (colReplica *collectionReplicaImpl) addPartitionsByCollectionMeta(colMeta *etcdpb.CollectionMeta) error {
	if !colReplica.hasCollection(colMeta.ID) {
		err := errors.New("Cannot find collection, id = " + strconv.FormatInt(colMeta.ID, 10))
		return err
	}
	pToAdd := make([]string, 0)
	for _, partitionTag := range colMeta.PartitionTags {
		if !colReplica.hasPartition(colMeta.ID, partitionTag) {
			pToAdd = append(pToAdd, partitionTag)
		}
	}

	for _, tag := range pToAdd {
		err := colReplica.addPartition(colMeta.ID, tag)
		if err != nil {
			log.Println(err)
		}
		fmt.Println("add partition: ", tag)
	}

	return nil
}

func (colReplica *collectionReplicaImpl) removePartitionsByCollectionMeta(colMeta *etcdpb.CollectionMeta) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	col, err := colReplica.getCollectionByIDPrivate(colMeta.ID)
	if err != nil {
		return err
	}

	pToDel := make([]string, 0)
	for _, partition := range col.partitions {
		hasPartition := false
		for _, tag := range colMeta.PartitionTags {
			if partition.partitionTag == tag {
				hasPartition = true
			}
		}
		if !hasPartition {
			pToDel = append(pToDel, partition.partitionTag)
		}
	}

	for _, tag := range pToDel {
		err := colReplica.removePartitionPrivate(col.ID(), tag)
		if err != nil {
			log.Println(err)
		}
		fmt.Println("delete partition: ", tag)
	}

	return nil
}

func (colReplica *collectionReplicaImpl) getPartitionByTag(collectionID UniqueID, partitionTag string) (*Partition, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	return colReplica.getPartitionByTagPrivate(collectionID, partitionTag)
}

func (colReplica *collectionReplicaImpl) getPartitionByTagPrivate(collectionID UniqueID, partitionTag string) (*Partition, error) {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return nil, err
	}

	for _, p := range *collection.Partitions() {
		if p.Tag() == partitionTag {
			return p, nil
		}
	}

	return nil, errors.New("cannot find partition, tag = " + partitionTag)
}

func (colReplica *collectionReplicaImpl) hasPartition(collectionID UniqueID, partitionTag string) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		log.Println(err)
		return false
	}

	for _, p := range *collection.Partitions() {
		if p.Tag() == partitionTag {
			return true
		}
	}

	return false
}

//----------------------------------------------------------------------------------------------------- segment
func (colReplica *collectionReplicaImpl) getSegmentNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	return len(colReplica.segments)
}

func (colReplica *collectionReplicaImpl) getSegmentStatistics() []*internalpb.SegmentStats {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	var statisticData = make([]*internalpb.SegmentStats, 0)

	for segmentID, segment := range colReplica.segments {
		currentMemSize := segment.getMemSize()
		segment.lastMemSize = currentMemSize
		segmentNumOfRows := segment.getRowCount()

		stat := internalpb.SegmentStats{
			SegmentID:        segmentID,
			MemorySize:       currentMemSize,
			NumRows:          segmentNumOfRows,
			RecentlyModified: segment.GetRecentlyModified(),
		}

		statisticData = append(statisticData, &stat)
		segment.SetRecentlyModified(false)
	}

	return statisticData
}

func (colReplica *collectionReplicaImpl) addSegment(segmentID UniqueID, partitionTag string, collectionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}

	partition, err2 := colReplica.getPartitionByTagPrivate(collectionID, partitionTag)
	if err2 != nil {
		return err2
	}

	var newSegment = newSegment(collection, segmentID, partitionTag, collectionID)

	colReplica.segments[segmentID] = newSegment
	*partition.Segments() = append(*partition.Segments(), newSegment)

	return nil
}

func (colReplica *collectionReplicaImpl) removeSegment(segmentID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	var targetPartition *Partition
	var segmentIndex = -1

	for _, col := range colReplica.collections {
		for _, p := range *col.Partitions() {
			for i, s := range *p.Segments() {
				if s.ID() == segmentID {
					targetPartition = p
					segmentIndex = i
				}
			}
		}
	}

	delete(colReplica.segments, segmentID)

	if targetPartition != nil && segmentIndex > 0 {
		targetPartition.segments = append(targetPartition.segments[:segmentIndex], targetPartition.segments[segmentIndex+1:]...)
	}

	return nil
}

func (colReplica *collectionReplicaImpl) getSegmentByID(segmentID UniqueID) (*Segment, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	return colReplica.getSegmentByIDPrivate(segmentID)
}

func (colReplica *collectionReplicaImpl) getSegmentByIDPrivate(segmentID UniqueID) (*Segment, error) {
	targetSegment, ok := colReplica.segments[segmentID]

	if !ok {
		return nil, errors.New("cannot found segment with id = " + strconv.FormatInt(segmentID, 10))
	}

	return targetSegment, nil
}

func (colReplica *collectionReplicaImpl) hasSegment(segmentID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	_, ok := colReplica.segments[segmentID]

	return ok
}

func (colReplica *collectionReplicaImpl) getVecFieldIDsBySegmentID(segmentID UniqueID) ([]int64, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	seg, err := colReplica.getSegmentByIDPrivate(segmentID)
	if err != nil {
		return nil, err
	}
	col, err := colReplica.getCollectionByIDPrivate(seg.collectionID)
	if err != nil {
		return nil, err
	}

	vecFields := make([]int64, 0)
	for _, field := range col.Schema().Fields {
		if field.DataType == schemapb.DataType_VECTOR_BINARY || field.DataType == schemapb.DataType_VECTOR_FLOAT {
			vecFields = append(vecFields, field.FieldID)
		}
	}

	if len(vecFields) <= 0 {
		return nil, errors.New("no vector field in segment " + strconv.FormatInt(segmentID, 10))
	}
	return vecFields, nil
}

//-----------------------------------------------------------------------------------------------------
func (colReplica *collectionReplicaImpl) freeAll() {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	for _, seg := range colReplica.segments {
		deleteSegment(seg)
	}
	for _, col := range colReplica.collections {
		deleteCollection(col)
	}
}
