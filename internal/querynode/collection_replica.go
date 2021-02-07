package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"
import (
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
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
	// collection
	getCollectionIDs() []UniqueID
	addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error
	removeCollection(collectionID UniqueID) error
	getCollectionByID(collectionID UniqueID) (*Collection, error)
	hasCollection(collectionID UniqueID) bool
	getCollectionNum() int
	getPartitionIDs(collectionID UniqueID) ([]UniqueID, error)

	getVecFieldIDsByCollectionID(collectionID UniqueID) ([]int64, error)
	getFieldIDsByCollectionID(collectionID UniqueID) ([]int64, error)

	// partition
	addPartition(collectionID UniqueID, partitionID UniqueID) error
	removePartition(partitionID UniqueID) error
	getPartitionByID(partitionID UniqueID) (*Partition, error)
	hasPartition(partitionID UniqueID) bool
	getPartitionNum() int
	getSegmentIDs(partitionID UniqueID) ([]UniqueID, error)

	enablePartition(partitionID UniqueID) error
	disablePartition(partitionID UniqueID) error

	// segment
	addSegment(segmentID UniqueID, partitionID UniqueID, collectionID UniqueID, segType segmentType) error
	removeSegment(segmentID UniqueID) error
	getSegmentByID(segmentID UniqueID) (*Segment, error)
	hasSegment(segmentID UniqueID) bool
	getSegmentNum() int

	getSegmentStatistics() []*internalpb2.SegmentStats
	getEnabledSealedSegmentsBySegmentType(segType segmentType) ([]UniqueID, []UniqueID, []UniqueID)
	getSealedSegmentsBySegmentType(segType segmentType) ([]UniqueID, []UniqueID, []UniqueID)
	replaceGrowingSegmentBySealedSegment(segment *Segment) error

	getTSafe() tSafe
	freeAll()
}

type collectionReplicaImpl struct {
	tSafe tSafe

	mu          sync.RWMutex // guards all
	collections map[UniqueID]*Collection
	partitions  map[UniqueID]*Partition
	segments    map[UniqueID]*Segment
}

//----------------------------------------------------------------------------------------------------- collection
func (colReplica *collectionReplicaImpl) getCollectionIDs() []UniqueID {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	collectionIDs := make([]UniqueID, 0)
	for id := range colReplica.collections {
		collectionIDs = append(collectionIDs, id)
	}
	return collectionIDs
}

func (colReplica *collectionReplicaImpl) addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	if ok := colReplica.hasCollectionPrivate(collectionID); ok {
		return errors.New("collection has been existed, id = " + strconv.FormatInt(collectionID, 10))
	}

	var newCollection = newCollection(collectionID, schema)
	colReplica.collections[collectionID] = newCollection

	return nil
}

func (colReplica *collectionReplicaImpl) removeCollection(collectionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.removeCollectionPrivate(collectionID)
}

func (colReplica *collectionReplicaImpl) removeCollectionPrivate(collectionID UniqueID) error {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}

	deleteCollection(collection)
	delete(colReplica.collections, collectionID)

	// delete partitions
	for _, partitionID := range collection.partitionIDs {
		// ignore error, try to delete
		_ = colReplica.removePartitionPrivate(partitionID)
	}

	return nil
}

func (colReplica *collectionReplicaImpl) getCollectionByID(collectionID UniqueID) (*Collection, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getCollectionByIDPrivate(collectionID)
}

func (colReplica *collectionReplicaImpl) getCollectionByIDPrivate(collectionID UniqueID) (*Collection, error) {
	collection, ok := colReplica.collections[collectionID]
	if !ok {
		return nil, errors.New("cannot find collection, id = " + strconv.FormatInt(collectionID, 10))
	}

	return collection, nil
}

func (colReplica *collectionReplicaImpl) hasCollection(collectionID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.hasCollectionPrivate(collectionID)
}

func (colReplica *collectionReplicaImpl) hasCollectionPrivate(collectionID UniqueID) bool {
	_, ok := colReplica.collections[collectionID]
	return ok
}

func (colReplica *collectionReplicaImpl) getCollectionNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return len(colReplica.collections)
}

func (colReplica *collectionReplicaImpl) getPartitionIDs(collectionID UniqueID) ([]UniqueID, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return nil, err
	}

	return collection.partitionIDs, nil
}

func (colReplica *collectionReplicaImpl) getVecFieldIDsByCollectionID(collectionID UniqueID) ([]int64, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	fields, err := colReplica.getFieldsByCollectionIDPrivate(collectionID)
	if err != nil {
		return nil, err
	}

	vecFields := make([]int64, 0)
	for _, field := range fields {
		if field.DataType == schemapb.DataType_VECTOR_BINARY || field.DataType == schemapb.DataType_VECTOR_FLOAT {
			vecFields = append(vecFields, field.FieldID)
		}
	}

	if len(vecFields) <= 0 {
		return nil, errors.New("no vector field in collection " + strconv.FormatInt(collectionID, 10))
	}

	return vecFields, nil
}

func (colReplica *collectionReplicaImpl) getFieldIDsByCollectionID(collectionID UniqueID) ([]int64, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	fields, err := colReplica.getFieldsByCollectionIDPrivate(collectionID)
	if err != nil {
		return nil, err
	}

	targetFields := make([]int64, 0)
	for _, field := range fields {
		targetFields = append(targetFields, field.FieldID)
	}

	return targetFields, nil
}

func (colReplica *collectionReplicaImpl) getFieldsByCollectionIDPrivate(collectionID UniqueID) ([]*schemapb.FieldSchema, error) {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return nil, err
	}

	if len(collection.Schema().Fields) <= 0 {
		return nil, errors.New("no field in collection " + strconv.FormatInt(collectionID, 10))
	}

	return collection.Schema().Fields, nil
}

//----------------------------------------------------------------------------------------------------- partition
func (colReplica *collectionReplicaImpl) addPartition(collectionID UniqueID, partitionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.addPartitionPrivate(collectionID, partitionID)
}

func (colReplica *collectionReplicaImpl) addPartitionPrivate(collectionID UniqueID, partitionID UniqueID) error {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}

	collection.addPartitionID(partitionID)
	var newPartition = newPartition(collectionID, partitionID)
	colReplica.partitions[partitionID] = newPartition
	return nil
}

func (colReplica *collectionReplicaImpl) removePartition(partitionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.removePartitionPrivate(partitionID)
}

func (colReplica *collectionReplicaImpl) removePartitionPrivate(partitionID UniqueID) error {
	partition, err := colReplica.getPartitionByIDPrivate(partitionID)
	if err != nil {
		return err
	}

	collection, err := colReplica.getCollectionByIDPrivate(partition.collectionID)
	if err != nil {
		return err
	}

	collection.removePartitionID(partitionID)
	delete(colReplica.partitions, partitionID)

	// delete segments
	for _, segmentID := range partition.segmentIDs {
		// try to delete, ignore error
		_ = colReplica.removeSegmentPrivate(segmentID)
	}
	return nil
}

func (colReplica *collectionReplicaImpl) getPartitionByID(partitionID UniqueID) (*Partition, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getPartitionByIDPrivate(partitionID)
}

func (colReplica *collectionReplicaImpl) getPartitionByIDPrivate(partitionID UniqueID) (*Partition, error) {
	partition, ok := colReplica.partitions[partitionID]
	if !ok {
		return nil, errors.New("cannot find partition, id = " + strconv.FormatInt(partitionID, 10))
	}

	return partition, nil
}

func (colReplica *collectionReplicaImpl) hasPartition(partitionID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.hasPartitionPrivate(partitionID)
}

func (colReplica *collectionReplicaImpl) hasPartitionPrivate(partitionID UniqueID) bool {
	_, ok := colReplica.partitions[partitionID]
	return ok
}

func (colReplica *collectionReplicaImpl) getPartitionNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return len(colReplica.partitions)
}

func (colReplica *collectionReplicaImpl) getSegmentIDs(partitionID UniqueID) ([]UniqueID, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getSegmentIDsPrivate(partitionID)
}

func (colReplica *collectionReplicaImpl) getSegmentIDsPrivate(partitionID UniqueID) ([]UniqueID, error) {
	partition, err2 := colReplica.getPartitionByIDPrivate(partitionID)
	if err2 != nil {
		return nil, err2
	}
	return partition.segmentIDs, nil
}

func (colReplica *collectionReplicaImpl) enablePartition(partitionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	partition, err := colReplica.getPartitionByIDPrivate(partitionID)
	if err != nil {
		return err
	}

	partition.enable = true
	return nil
}

func (colReplica *collectionReplicaImpl) disablePartition(partitionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	partition, err := colReplica.getPartitionByIDPrivate(partitionID)
	if err != nil {
		return err
	}

	partition.enable = false
	return nil
}

func (colReplica *collectionReplicaImpl) getEnabledPartitionIDsPrivate() []UniqueID {
	partitionIDs := make([]UniqueID, 0)
	for _, partition := range colReplica.partitions {
		if partition.enable {
			partitionIDs = append(partitionIDs, partition.partitionID)
		}
	}
	return partitionIDs
}

//----------------------------------------------------------------------------------------------------- segment
func (colReplica *collectionReplicaImpl) addSegment(segmentID UniqueID, partitionID UniqueID, collectionID UniqueID, segType segmentType) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.addSegmentPrivate(segmentID, partitionID, collectionID, segType)
}

func (colReplica *collectionReplicaImpl) addSegmentPrivate(segmentID UniqueID, partitionID UniqueID, collectionID UniqueID, segType segmentType) error {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}

	partition, err := colReplica.getPartitionByIDPrivate(partitionID)
	if err != nil {
		return err
	}

	partition.addSegmentID(segmentID)
	var newSegment = newSegment(collection, segmentID, partitionID, collectionID, segType)
	colReplica.segments[segmentID] = newSegment

	return nil
}

func (colReplica *collectionReplicaImpl) removeSegment(segmentID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.removeSegmentPrivate(segmentID)
}

func (colReplica *collectionReplicaImpl) removeSegmentPrivate(segmentID UniqueID) error {
	segment, err := colReplica.getSegmentByIDPrivate(segmentID)
	if err != nil {
		return err
	}

	partition, err := colReplica.getPartitionByIDPrivate(segment.partitionID)
	if err != nil {
		return err
	}

	partition.removeSegmentID(segmentID)
	delete(colReplica.segments, segmentID)
	deleteSegment(segment)

	return nil
}

func (colReplica *collectionReplicaImpl) getSegmentByID(segmentID UniqueID) (*Segment, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getSegmentByIDPrivate(segmentID)
}

func (colReplica *collectionReplicaImpl) getSegmentByIDPrivate(segmentID UniqueID) (*Segment, error) {
	segment, ok := colReplica.segments[segmentID]
	if !ok {
		return nil, errors.New("cannot find segment, id = " + strconv.FormatInt(segmentID, 10))
	}

	return segment, nil
}

func (colReplica *collectionReplicaImpl) hasSegment(segmentID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.hasSegmentPrivate(segmentID)
}

func (colReplica *collectionReplicaImpl) hasSegmentPrivate(segmentID UniqueID) bool {
	_, ok := colReplica.segments[segmentID]
	return ok
}

func (colReplica *collectionReplicaImpl) getSegmentNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return len(colReplica.segments)
}

func (colReplica *collectionReplicaImpl) getSegmentStatistics() []*internalpb2.SegmentStats {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	var statisticData = make([]*internalpb2.SegmentStats, 0)

	for segmentID, segment := range colReplica.segments {
		currentMemSize := segment.getMemSize()
		segment.lastMemSize = currentMemSize
		segmentNumOfRows := segment.getRowCount()

		stat := internalpb2.SegmentStats{
			SegmentID:        segmentID,
			MemorySize:       currentMemSize,
			NumRows:          segmentNumOfRows,
			RecentlyModified: segment.getRecentlyModified(),
		}

		statisticData = append(statisticData, &stat)
		segment.setRecentlyModified(false)
	}

	return statisticData
}

func (colReplica *collectionReplicaImpl) getEnabledSealedSegmentsBySegmentType(segType segmentType) ([]UniqueID, []UniqueID, []UniqueID) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	targetCollectionIDs := make([]UniqueID, 0)
	targetPartitionIDs := make([]UniqueID, 0)
	targetSegmentIDs := make([]UniqueID, 0)

	for _, partitionID := range colReplica.getEnabledPartitionIDsPrivate() {
		segmentIDs, err := colReplica.getSegmentIDsPrivate(partitionID)
		if err != nil {
			continue
		}
		for _, segmentID := range segmentIDs {
			segment, err := colReplica.getSegmentByIDPrivate(segmentID)
			if err != nil {
				continue
			}
			if segment.getType() == segType {
				targetCollectionIDs = append(targetCollectionIDs, segment.collectionID)
				targetPartitionIDs = append(targetPartitionIDs, segment.partitionID)
				targetSegmentIDs = append(targetSegmentIDs, segment.segmentID)
			}
		}
	}

	return targetCollectionIDs, targetPartitionIDs, targetSegmentIDs
}

func (colReplica *collectionReplicaImpl) getSealedSegmentsBySegmentType(segType segmentType) ([]UniqueID, []UniqueID, []UniqueID) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	targetCollectionIDs := make([]UniqueID, 0)
	targetPartitionIDs := make([]UniqueID, 0)
	targetSegmentIDs := make([]UniqueID, 0)

	for _, segment := range colReplica.segments {
		if segment.getType() == segType {
			targetCollectionIDs = append(targetCollectionIDs, segment.collectionID)
			targetPartitionIDs = append(targetPartitionIDs, segment.partitionID)
			targetSegmentIDs = append(targetSegmentIDs, segment.segmentID)
		}
	}

	return targetCollectionIDs, targetPartitionIDs, targetSegmentIDs
}

func (colReplica *collectionReplicaImpl) replaceGrowingSegmentBySealedSegment(segment *Segment) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	if segment.segmentType != segTypeSealed && segment.segmentType != segTypeIndexing {
		return errors.New("unexpected segment type")
	}
	targetSegment, err := colReplica.getSegmentByIDPrivate(segment.ID())
	if err == nil && targetSegment != nil {
		if targetSegment.segmentType != segTypeGrowing {
			// target segment has been a sealed segment
			return nil
		}
		deleteSegment(targetSegment)
	}

	colReplica.segments[segment.ID()] = segment
	return nil
}

//-----------------------------------------------------------------------------------------------------
func (colReplica *collectionReplicaImpl) getTSafe() tSafe {
	return colReplica.tSafe
}

func (colReplica *collectionReplicaImpl) freeAll() {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	for id := range colReplica.collections {
		_ = colReplica.removeCollectionPrivate(id)
	}

	colReplica.collections = make(map[UniqueID]*Collection)
	colReplica.partitions = make(map[UniqueID]*Partition)
	colReplica.segments = make(map[UniqueID]*Segment)
}

func newCollectionReplicaImpl() collectionReplica {
	collections := make(map[int64]*Collection)
	partitions := make(map[int64]*Partition)
	segments := make(map[int64]*Segment)

	tSafe := newTSafe()

	var replica collectionReplica = &collectionReplicaImpl{
		collections: collections,
		partitions:  partitions,
		segments:    segments,

		tSafe: tSafe,
	}

	return replica
}
