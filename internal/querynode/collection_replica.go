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
	"strconv"
	"sync"

	"errors"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
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
type ReplicaInterface interface {
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
	setSegmentEnableIndex(segmentID UniqueID, enable bool) error
	setSegmentEnableLoadBinLog(segmentID UniqueID, enable bool) error
	getSegmentsToLoadBySegmentType(segType segmentType) ([]UniqueID, []UniqueID, []UniqueID)
	getSegmentStatistics() []*internalpb.SegmentStats

	getEnabledSegmentsBySegmentType(segType segmentType) ([]UniqueID, []UniqueID, []UniqueID)
	getSegmentsBySegmentType(segType segmentType) ([]UniqueID, []UniqueID, []UniqueID)
	replaceGrowingSegmentBySealedSegment(segment *Segment) error

	getTSafe() tSafer
	freeAll()
}

type collectionReplica struct {
	tSafe tSafer

	mu          sync.RWMutex // guards all
	collections map[UniqueID]*Collection
	partitions  map[UniqueID]*Partition
	segments    map[UniqueID]*Segment
}

//----------------------------------------------------------------------------------------------------- collection
func (colReplica *collectionReplica) getCollectionIDs() []UniqueID {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	collectionIDs := make([]UniqueID, 0)
	for id := range colReplica.collections {
		collectionIDs = append(collectionIDs, id)
	}
	return collectionIDs
}

func (colReplica *collectionReplica) addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	if ok := colReplica.hasCollectionPrivate(collectionID); ok {
		return fmt.Errorf("collection has been existed, id %d", collectionID)
	}

	var newCollection = newCollection(collectionID, schema)
	colReplica.collections[collectionID] = newCollection

	return nil
}

func (colReplica *collectionReplica) removeCollection(collectionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.removeCollectionPrivate(collectionID)
}

func (colReplica *collectionReplica) removeCollectionPrivate(collectionID UniqueID) error {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}

	// delete partitions
	for _, partitionID := range collection.partitionIDs {
		// ignore error, try to delete
		_ = colReplica.removePartitionPrivate(partitionID)
	}

	deleteCollection(collection)
	delete(colReplica.collections, collectionID)

	return nil
}

func (colReplica *collectionReplica) getCollectionByID(collectionID UniqueID) (*Collection, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getCollectionByIDPrivate(collectionID)
}

func (colReplica *collectionReplica) getCollectionByIDPrivate(collectionID UniqueID) (*Collection, error) {
	collection, ok := colReplica.collections[collectionID]
	if !ok {
		return nil, fmt.Errorf("cannot find collection, id = %d", collectionID)
	}

	return collection, nil
}

func (colReplica *collectionReplica) hasCollection(collectionID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.hasCollectionPrivate(collectionID)
}

func (colReplica *collectionReplica) hasCollectionPrivate(collectionID UniqueID) bool {
	_, ok := colReplica.collections[collectionID]
	return ok
}

func (colReplica *collectionReplica) getCollectionNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return len(colReplica.collections)
}

func (colReplica *collectionReplica) getPartitionIDs(collectionID UniqueID) ([]UniqueID, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return nil, err
	}

	return collection.partitionIDs, nil
}

func (colReplica *collectionReplica) getVecFieldIDsByCollectionID(collectionID UniqueID) ([]int64, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	fields, err := colReplica.getFieldsByCollectionIDPrivate(collectionID)
	if err != nil {
		return nil, err
	}

	vecFields := make([]int64, 0)
	for _, field := range fields {
		if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
			vecFields = append(vecFields, field.FieldID)
		}
	}

	if len(vecFields) <= 0 {
		return nil, fmt.Errorf("no vector field in collection %d", collectionID)
	}

	return vecFields, nil
}

func (colReplica *collectionReplica) getFieldIDsByCollectionID(collectionID UniqueID) ([]int64, error) {
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

	// add row id field
	targetFields = append(targetFields, rowIDFieldID)

	return targetFields, nil
}

func (colReplica *collectionReplica) getFieldsByCollectionIDPrivate(collectionID UniqueID) ([]*schemapb.FieldSchema, error) {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return nil, err
	}

	if len(collection.Schema().Fields) <= 0 {
		return nil, fmt.Errorf("no field in collection %d", collectionID)
	}

	return collection.Schema().Fields, nil
}

//----------------------------------------------------------------------------------------------------- partition
func (colReplica *collectionReplica) addPartition(collectionID UniqueID, partitionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.addPartitionPrivate(collectionID, partitionID)
}

func (colReplica *collectionReplica) addPartitionPrivate(collectionID UniqueID, partitionID UniqueID) error {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}

	collection.addPartitionID(partitionID)
	var newPartition = newPartition(collectionID, partitionID)
	colReplica.partitions[partitionID] = newPartition
	return nil
}

func (colReplica *collectionReplica) removePartition(partitionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.removePartitionPrivate(partitionID)
}

func (colReplica *collectionReplica) removePartitionPrivate(partitionID UniqueID) error {
	partition, err := colReplica.getPartitionByIDPrivate(partitionID)
	if err != nil {
		return err
	}

	collection, err := colReplica.getCollectionByIDPrivate(partition.collectionID)
	if err != nil {
		return err
	}

	// delete segments
	for _, segmentID := range partition.segmentIDs {
		// try to delete, ignore error
		_ = colReplica.removeSegmentPrivate(segmentID)
	}

	collection.removePartitionID(partitionID)
	delete(colReplica.partitions, partitionID)

	return nil
}

func (colReplica *collectionReplica) getPartitionByID(partitionID UniqueID) (*Partition, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getPartitionByIDPrivate(partitionID)
}

func (colReplica *collectionReplica) getPartitionByIDPrivate(partitionID UniqueID) (*Partition, error) {
	partition, ok := colReplica.partitions[partitionID]
	if !ok {
		return nil, fmt.Errorf("cannot find partition, id = %d", partitionID)
	}

	return partition, nil
}

func (colReplica *collectionReplica) hasPartition(partitionID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.hasPartitionPrivate(partitionID)
}

func (colReplica *collectionReplica) hasPartitionPrivate(partitionID UniqueID) bool {
	_, ok := colReplica.partitions[partitionID]
	return ok
}

func (colReplica *collectionReplica) getPartitionNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return len(colReplica.partitions)
}

func (colReplica *collectionReplica) getSegmentIDs(partitionID UniqueID) ([]UniqueID, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getSegmentIDsPrivate(partitionID)
}

func (colReplica *collectionReplica) getSegmentIDsPrivate(partitionID UniqueID) ([]UniqueID, error) {
	partition, err2 := colReplica.getPartitionByIDPrivate(partitionID)
	if err2 != nil {
		return nil, err2
	}
	return partition.segmentIDs, nil
}

func (colReplica *collectionReplica) enablePartition(partitionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	partition, err := colReplica.getPartitionByIDPrivate(partitionID)
	if err != nil {
		return err
	}

	partition.enable = true
	return nil
}

func (colReplica *collectionReplica) disablePartition(partitionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	partition, err := colReplica.getPartitionByIDPrivate(partitionID)
	if err != nil {
		return err
	}

	partition.enable = false
	return nil
}

func (colReplica *collectionReplica) getEnabledPartitionIDsPrivate() []UniqueID {
	partitionIDs := make([]UniqueID, 0)
	for _, partition := range colReplica.partitions {
		if partition.enable {
			partitionIDs = append(partitionIDs, partition.partitionID)
		}
	}
	return partitionIDs
}

//----------------------------------------------------------------------------------------------------- segment
func (colReplica *collectionReplica) addSegment(segmentID UniqueID, partitionID UniqueID, collectionID UniqueID, segType segmentType) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.addSegmentPrivate(segmentID, partitionID, collectionID, segType)
}

func (colReplica *collectionReplica) addSegmentPrivate(segmentID UniqueID, partitionID UniqueID, collectionID UniqueID, segType segmentType) error {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}

	partition, err := colReplica.getPartitionByIDPrivate(partitionID)
	if err != nil {
		return err
	}

	if colReplica.hasSegmentPrivate(segmentID) {
		return nil
	}
	partition.addSegmentID(segmentID)
	var newSegment = newSegment(collection, segmentID, partitionID, collectionID, segType)
	colReplica.segments[segmentID] = newSegment

	return nil
}

func (colReplica *collectionReplica) removeSegment(segmentID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.removeSegmentPrivate(segmentID)
}

func (colReplica *collectionReplica) removeSegmentPrivate(segmentID UniqueID) error {
	log.Debug("remove segment", zap.Int64("segmentID", segmentID))
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

func (colReplica *collectionReplica) getSegmentByID(segmentID UniqueID) (*Segment, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getSegmentByIDPrivate(segmentID)
}

func (colReplica *collectionReplica) getSegmentByIDPrivate(segmentID UniqueID) (*Segment, error) {
	segment, ok := colReplica.segments[segmentID]
	if !ok {
		return nil, errors.New("cannot find segment, id = " + strconv.FormatInt(segmentID, 10))
	}

	return segment, nil
}

func (colReplica *collectionReplica) hasSegment(segmentID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.hasSegmentPrivate(segmentID)
}

func (colReplica *collectionReplica) hasSegmentPrivate(segmentID UniqueID) bool {
	_, ok := colReplica.segments[segmentID]
	return ok
}

func (colReplica *collectionReplica) getSegmentNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return len(colReplica.segments)
}

func (colReplica *collectionReplica) getSegmentStatistics() []*internalpb.SegmentStats {
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
			RecentlyModified: segment.getRecentlyModified(),
		}

		statisticData = append(statisticData, &stat)
		segment.setRecentlyModified(false)
	}

	return statisticData
}

func (colReplica *collectionReplica) getEnabledSegmentsBySegmentType(segType segmentType) ([]UniqueID, []UniqueID, []UniqueID) {
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

func (colReplica *collectionReplica) getSegmentsBySegmentType(segType segmentType) ([]UniqueID, []UniqueID, []UniqueID) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	targetCollectionIDs := make([]UniqueID, 0)
	targetPartitionIDs := make([]UniqueID, 0)
	targetSegmentIDs := make([]UniqueID, 0)

	for _, segment := range colReplica.segments {
		if segment.getType() == segType {
			if segType == segmentTypeSealed && !segment.getEnableIndex() {
				continue
			}

			targetCollectionIDs = append(targetCollectionIDs, segment.collectionID)
			targetPartitionIDs = append(targetPartitionIDs, segment.partitionID)
			targetSegmentIDs = append(targetSegmentIDs, segment.segmentID)
		}
	}

	return targetCollectionIDs, targetPartitionIDs, targetSegmentIDs
}

func (colReplica *collectionReplica) replaceGrowingSegmentBySealedSegment(segment *Segment) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	if segment.segmentType != segmentTypeSealed && segment.segmentType != segTypeIndexing {
		return errors.New("unexpected segment type")
	}
	targetSegment, err := colReplica.getSegmentByIDPrivate(segment.ID())
	if err == nil && targetSegment != nil {
		if targetSegment.segmentType != segmentTypeGrowing {
			// target segment has been a sealed segment
			return nil
		}
		deleteSegment(targetSegment)
	}

	colReplica.segments[segment.ID()] = segment
	return nil
}

func (colReplica *collectionReplica) setSegmentEnableIndex(segmentID UniqueID, enable bool) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	targetSegment, err := colReplica.getSegmentByIDPrivate(segmentID)
	if targetSegment.segmentType != segmentTypeSealed {
		return errors.New("unexpected segment type")
	}
	if err == nil && targetSegment != nil {
		targetSegment.setEnableIndex(enable)
	}
	return nil
}

func (colReplica *collectionReplica) setSegmentEnableLoadBinLog(segmentID UniqueID, enable bool) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	targetSegment, err := colReplica.getSegmentByIDPrivate(segmentID)
	if targetSegment.segmentType != segmentTypeGrowing {
		return errors.New("unexpected segment type")
	}
	if err == nil && targetSegment != nil {
		targetSegment.setLoadBinLogEnable(enable)
	}
	return nil
}

//-----------------------------------------------------------------------------------------------------
func (colReplica *collectionReplica) getTSafe() tSafer {
	return colReplica.tSafe
}

func (colReplica *collectionReplica) freeAll() {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	for id := range colReplica.collections {
		_ = colReplica.removeCollectionPrivate(id)
	}

	colReplica.collections = make(map[UniqueID]*Collection)
	colReplica.partitions = make(map[UniqueID]*Partition)
	colReplica.segments = make(map[UniqueID]*Segment)
}

func newCollectionReplica() ReplicaInterface {
	collections := make(map[int64]*Collection)
	partitions := make(map[int64]*Partition)
	segments := make(map[int64]*Segment)

	tSafe := newTSafe()

	var replica ReplicaInterface = &collectionReplica{
		collections: collections,
		partitions:  partitions,
		segments:    segments,

		tSafe: tSafe,
	}

	return replica
}

func (colReplica *collectionReplica) getSegmentsToLoadBySegmentType(segType segmentType) ([]UniqueID, []UniqueID, []UniqueID) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	targetCollectionIDs := make([]UniqueID, 0)
	targetPartitionIDs := make([]UniqueID, 0)
	targetSegmentIDs := make([]UniqueID, 0)

	for _, segment := range colReplica.segments {
		if !segment.enableLoadBinLog {
			continue
		}
		if segment.getType() == segType {
			if segType == segmentTypeSealed && !segment.getEnableIndex() {
				continue
			}

			targetCollectionIDs = append(targetCollectionIDs, segment.collectionID)
			targetPartitionIDs = append(targetPartitionIDs, segment.partitionID)
			targetSegmentIDs = append(targetSegmentIDs, segment.segmentID)
		}
	}

	return targetCollectionIDs, targetPartitionIDs, targetSegmentIDs
}
