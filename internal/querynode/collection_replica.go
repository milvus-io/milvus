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
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"log"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
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
	getTSafe() *tSafe

	// collection
	getCollectionNum() int
	addCollection(collMeta *etcdpb.CollectionMeta, colMetaBlob string) error
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
	getSegmentStatistics() *internalpb.QueryNodeSegStats
	addSegment(segmentID UniqueID, partitionTag string, collectionID UniqueID) error
	removeSegment(segmentID UniqueID) error
	getSegmentByID(segmentID UniqueID) (*Segment, error)
	hasSegment(segmentID UniqueID) bool

	freeAll()
}

type collectionReplicaImpl struct {
	mu          sync.RWMutex
	collections []*Collection
	segments    map[UniqueID]*Segment

	tSafe *tSafe
}

//----------------------------------------------------------------------------------------------------- tSafe
func (colReplica *collectionReplicaImpl) getTSafe() *tSafe {
	return colReplica.tSafe
}

//----------------------------------------------------------------------------------------------------- collection
func (colReplica *collectionReplicaImpl) getCollectionNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	return len(colReplica.collections)
}

func (colReplica *collectionReplicaImpl) addCollection(collMeta *etcdpb.CollectionMeta, colMetaBlob string) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	var newCollection = newCollection(collMeta, colMetaBlob)
	colReplica.collections = append(colReplica.collections, newCollection)

	return nil
}

func (colReplica *collectionReplicaImpl) removeCollection(collectionID UniqueID) error {
	collection, err := colReplica.getCollectionByID(collectionID)

	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	if err != nil {
		return err
	}

	deleteCollection(collection)

	tmpCollections := make([]*Collection, 0)
	for _, col := range colReplica.collections {
		if col.ID() == collectionID {
			for _, p := range *col.Partitions() {
				for _, s := range *p.Segments() {
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
	collection, err := colReplica.getCollectionByID(collectionID)
	if err != nil {
		return -1, err
	}

	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	return len(collection.partitions), nil
}

func (colReplica *collectionReplicaImpl) addPartition(collectionID UniqueID, partitionTag string) error {
	collection, err := colReplica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	var newPartition = newPartition(partitionTag)

	*collection.Partitions() = append(*collection.Partitions(), newPartition)
	return nil
}

func (colReplica *collectionReplicaImpl) removePartition(collectionID UniqueID, partitionTag string) error {
	collection, err := colReplica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	var tmpPartitions = make([]*Partition, 0)
	for _, p := range *collection.Partitions() {
		if p.Tag() == partitionTag {
			for _, s := range *p.Segments() {
				delete(colReplica.segments, s.ID())
			}
		} else {
			tmpPartitions = append(tmpPartitions, p)
		}
	}

	*collection.Partitions() = tmpPartitions
	return nil
}

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
	col, err := colReplica.getCollectionByID(colMeta.ID)
	if err != nil {
		return err
	}

	colReplica.mu.Lock()

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

	colReplica.mu.Unlock()

	for _, tag := range pToDel {
		err := colReplica.removePartition(col.ID(), tag)
		if err != nil {
			log.Println(err)
		}
		fmt.Println("delete partition: ", tag)
	}

	return nil
}

func (colReplica *collectionReplicaImpl) getPartitionByTag(collectionID UniqueID, partitionTag string) (*Partition, error) {
	collection, err := colReplica.getCollectionByID(collectionID)
	if err != nil {
		return nil, err
	}

	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	for _, p := range *collection.Partitions() {
		if p.Tag() == partitionTag {
			return p, nil
		}
	}

	return nil, errors.New("cannot find partition, tag = " + partitionTag)
}

func (colReplica *collectionReplicaImpl) hasPartition(collectionID UniqueID, partitionTag string) bool {
	collection, err := colReplica.getCollectionByID(collectionID)
	if err != nil {
		log.Println(err)
		return false
	}

	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

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

func (colReplica *collectionReplicaImpl) getSegmentStatistics() *internalpb.QueryNodeSegStats {
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

func (colReplica *collectionReplicaImpl) addSegment(segmentID UniqueID, partitionTag string, collectionID UniqueID) error {
	collection, err := colReplica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	partition, err := colReplica.getPartitionByTag(collectionID, partitionTag)
	if err != nil {
		return err
	}

	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	var newSegment = newSegment(collection, segmentID)

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
