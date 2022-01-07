// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"
import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

// ReplicaInterface specifies all the methods that the Collection object needs to implement in QueryNode.
// In common cases, the system has multiple query nodes. The full data of a collection will be distributed
// across multiple query nodes, and each query node's collectionReplica will maintain its own part.
type ReplicaInterface interface {
	// collection
	// getCollectionIDs returns all collection ids in the collectionReplica
	getCollectionIDs() []UniqueID
	// addCollection creates a new collection and add it to collectionReplica
	addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error
	// removeCollection removes the collection from collectionReplica
	removeCollection(collectionID UniqueID) error
	// getCollectionByID gets the collection which id is collectionID
	getCollectionByID(collectionID UniqueID) (*Collection, error)
	// hasCollection checks if collectionReplica has the collection which id is collectionID
	hasCollection(collectionID UniqueID) bool
	// getCollectionNum returns num of collections in collectionReplica
	getCollectionNum() int
	// getPartitionIDs returns partition ids of collection
	getPartitionIDs(collectionID UniqueID) ([]UniqueID, error)
	// getVecFieldIDsByCollectionID returns vector field ids of collection
	getVecFieldIDsByCollectionID(collectionID UniqueID) ([]FieldID, error)
	// getPKFieldIDsByCollectionID returns vector field ids of collection
	getPKFieldIDByCollectionID(collectionID UniqueID) (FieldID, error)
	// getSegmentInfosByColID return segments info by collectionID
	getSegmentInfosByColID(collectionID UniqueID) ([]*querypb.SegmentInfo, error)

	// partition
	// addPartition adds a new partition to collection
	addPartition(collectionID UniqueID, partitionID UniqueID) error
	// removePartition removes the partition from collectionReplica
	removePartition(partitionID UniqueID) error
	// getPartitionByID returns the partition which id is partitionID
	getPartitionByID(partitionID UniqueID) (*Partition, error)
	// hasPartition returns true if collectionReplica has the partition, false otherwise
	hasPartition(partitionID UniqueID) bool
	// getPartitionNum returns num of partitions
	getPartitionNum() int
	// getSegmentIDs returns segment ids
	getSegmentIDs(partitionID UniqueID) ([]UniqueID, error)
	// getSegmentIDsByVChannel returns segment ids which virtual channel is vChannel
	getSegmentIDsByVChannel(partitionID UniqueID, vChannel Channel) ([]UniqueID, error)

	// segment
	// addSegment add a new segment to collectionReplica
	addSegment(segmentID UniqueID, partitionID UniqueID, collectionID UniqueID, vChannelID Channel, segType segmentType, onService bool) error
	// setSegment adds a segment to collectionReplica
	setSegment(segment *Segment) error
	// removeSegment removes a segment from collectionReplica
	removeSegment(segmentID UniqueID) error
	// getSegmentByID returns the segment which id is segmentID
	getSegmentByID(segmentID UniqueID) (*Segment, error)
	// hasSegment returns true if collectionReplica has the segment, false otherwise
	hasSegment(segmentID UniqueID) bool
	// getSegmentNum returns num of segments in collectionReplica
	getSegmentNum() int
	//  getSegmentStatistics returns the statistics of segments in collectionReplica
	getSegmentStatistics() []*internalpb.SegmentStats

	// excluded segments
	//  removeExcludedSegments will remove excludedSegments from collectionReplica
	removeExcludedSegments(collectionID UniqueID)
	// addExcludedSegments will add excludedSegments to collectionReplica
	addExcludedSegments(collectionID UniqueID, segmentInfos []*datapb.SegmentInfo)
	// getExcludedSegments returns excludedSegments of collectionReplica
	getExcludedSegments(collectionID UniqueID) ([]*datapb.SegmentInfo, error)

	// query mu
	// queryLock guards query and delete operations
	queryLock()
	// queryUnlock guards query and delete segment operations
	queryUnlock()
	// queryRLock guards query and delete segment operations
	queryRLock()
	// queryRUnlock guards query and delete segment operations
	queryRUnlock()

	// getSegmentsMemSize get the memory size in bytes of all the Segments
	getSegmentsMemSize() int64
	// freeAll will free all meta info from collectionReplica
	freeAll()
	// printReplica prints the collections, partitions and segments in the collectionReplica
	printReplica()
}

// collectionReplica is the data replication of memory data in query node.
// It implements `ReplicaInterface` interface.
type collectionReplica struct {
	mu          sync.RWMutex // guards all
	collections map[UniqueID]*Collection
	partitions  map[UniqueID]*Partition
	segments    map[UniqueID]*Segment

	queryMu          sync.RWMutex
	excludedSegments map[UniqueID][]*datapb.SegmentInfo // map[collectionID]segmentIDs

	etcdKV *etcdkv.EtcdKV
}

// queryLock guards query and delete operations
func (colReplica *collectionReplica) queryLock() {
	colReplica.queryMu.Lock()
}

// queryUnlock guards query and delete segment operations
func (colReplica *collectionReplica) queryUnlock() {
	colReplica.queryMu.Unlock()
}

// queryRLock guards query and delete segment operations
func (colReplica *collectionReplica) queryRLock() {
	colReplica.queryMu.RLock()
}

// queryRUnlock guards query and delete segment operations
func (colReplica *collectionReplica) queryRUnlock() {
	colReplica.queryMu.RUnlock()
}

// getSegmentsMemSize get the memory size in bytes of all the Segments
func (colReplica *collectionReplica) getSegmentsMemSize() int64 {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	memSize := int64(0)
	for _, segment := range colReplica.segments {
		memSize += segment.getMemSize()
	}
	return memSize
}

// printReplica prints the collections, partitions and segments in the collectionReplica
func (colReplica *collectionReplica) printReplica() {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	log.Debug("collections in collectionReplica", zap.Any("info", colReplica.collections))
	log.Debug("partitions in collectionReplica", zap.Any("info", colReplica.partitions))
	log.Debug("segments in collectionReplica", zap.Any("info", colReplica.segments))
	log.Debug("excludedSegments in collectionReplica", zap.Any("info", colReplica.excludedSegments))
}

//----------------------------------------------------------------------------------------------------- collection
// getCollectionIDs gets all the collection ids in the collectionReplica
func (colReplica *collectionReplica) getCollectionIDs() []UniqueID {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	collectionIDs := make([]UniqueID, 0)
	for id := range colReplica.collections {
		collectionIDs = append(collectionIDs, id)
	}
	return collectionIDs
}

// addCollection creates a new collection and add it to collectionReplica
func (colReplica *collectionReplica) addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	if ok := colReplica.hasCollectionPrivate(collectionID); ok {
		return fmt.Errorf("collection has been loaded, id %d", collectionID)
	}

	var newCollection = newCollection(collectionID, schema)
	colReplica.collections[collectionID] = newCollection

	return nil
}

// removeCollection removes the collection from collectionReplica
func (colReplica *collectionReplica) removeCollection(collectionID UniqueID) error {
	colReplica.queryMu.Lock()
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	defer colReplica.queryMu.Unlock()
	return colReplica.removeCollectionPrivate(collectionID)
}

// removeCollectionPrivate is the private function in collectionReplica, to remove collection from collectionReplica
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

// getCollectionByID gets the collection which id is collectionID
func (colReplica *collectionReplica) getCollectionByID(collectionID UniqueID) (*Collection, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getCollectionByIDPrivate(collectionID)
}

// getCollectionByIDPrivate is the private function in collectionReplica, to get collection from collectionReplica
func (colReplica *collectionReplica) getCollectionByIDPrivate(collectionID UniqueID) (*Collection, error) {
	collection, ok := colReplica.collections[collectionID]
	if !ok {
		return nil, fmt.Errorf("collection hasn't been loaded or has been released, collection id = %d", collectionID)
	}

	return collection, nil
}

// hasCollection checks if collectionReplica has the collection which id is collectionID
func (colReplica *collectionReplica) hasCollection(collectionID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.hasCollectionPrivate(collectionID)
}

// hasCollectionPrivate is the private function in collectionReplica, to check collection in collectionReplica
func (colReplica *collectionReplica) hasCollectionPrivate(collectionID UniqueID) bool {
	_, ok := colReplica.collections[collectionID]
	return ok
}

// getCollectionNum returns num of collections in collectionReplica
func (colReplica *collectionReplica) getCollectionNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return len(colReplica.collections)
}

// getPartitionIDs returns partition ids of collection
func (colReplica *collectionReplica) getPartitionIDs(collectionID UniqueID) ([]UniqueID, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return nil, err
	}

	return collection.partitionIDs, nil
}

// getVecFieldIDsByCollectionID returns vector field ids of collection
func (colReplica *collectionReplica) getVecFieldIDsByCollectionID(collectionID UniqueID) ([]FieldID, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	fields, err := colReplica.getFieldsByCollectionIDPrivate(collectionID)
	if err != nil {
		return nil, err
	}

	vecFields := make([]FieldID, 0)
	for _, field := range fields {
		if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
			vecFields = append(vecFields, field.FieldID)
		}
	}
	return vecFields, nil
}

// getPKFieldIDsByCollectionID returns vector field ids of collection
func (colReplica *collectionReplica) getPKFieldIDByCollectionID(collectionID UniqueID) (FieldID, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	fields, err := colReplica.getFieldsByCollectionIDPrivate(collectionID)
	if err != nil {
		return common.InvalidFieldID, err
	}

	for _, field := range fields {
		if field.IsPrimaryKey {
			return field.FieldID, nil
		}
	}
	return common.InvalidFieldID, nil
}

// getFieldsByCollectionIDPrivate is the private function in collectionReplica, to return vector field ids of collection
func (colReplica *collectionReplica) getFieldsByCollectionIDPrivate(collectionID UniqueID) ([]*schemapb.FieldSchema, error) {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return nil, err
	}

	if len(collection.Schema().Fields) <= 0 {
		return nil, errors.New("no field in collection %d" + strconv.FormatInt(collectionID, 10))
	}

	return collection.Schema().Fields, nil
}

// getSegmentInfosByColID return segments info by collectionID
func (colReplica *collectionReplica) getSegmentInfosByColID(collectionID UniqueID) ([]*querypb.SegmentInfo, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	segmentInfos := make([]*querypb.SegmentInfo, 0)
	collection, ok := colReplica.collections[collectionID]
	if !ok {
		// collection not exist, so result segmentInfos is empty
		return segmentInfos, nil
	}

	for _, partitionID := range collection.partitionIDs {
		partition, ok := colReplica.partitions[partitionID]
		if !ok {
			return nil, errors.New("the meta of collection and partition are inconsistent in QueryNode")
		}
		for _, segmentID := range partition.segmentIDs {
			segment, ok := colReplica.segments[segmentID]
			if !ok {
				return nil, errors.New("the meta of partition and segment are inconsistent in QueryNode")
			}
			segmentInfo := getSegmentInfo(segment)
			segmentInfos = append(segmentInfos, segmentInfo)
		}
	}

	return segmentInfos, nil
}

//----------------------------------------------------------------------------------------------------- partition
// addPartition adds a new partition to collection
func (colReplica *collectionReplica) addPartition(collectionID UniqueID, partitionID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.addPartitionPrivate(collectionID, partitionID)
}

// addPartitionPrivate is the private function in collectionReplica, to add a new partition to collection
func (colReplica *collectionReplica) addPartitionPrivate(collectionID UniqueID, partitionID UniqueID) error {
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}

	if !colReplica.hasPartitionPrivate(partitionID) {
		collection.addPartitionID(partitionID)
		var newPartition = newPartition(collectionID, partitionID)
		colReplica.partitions[partitionID] = newPartition
	}
	return nil
}

// removePartition removes the partition from collectionReplica
func (colReplica *collectionReplica) removePartition(partitionID UniqueID) error {
	colReplica.queryMu.Lock()
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	defer colReplica.queryMu.Unlock()
	return colReplica.removePartitionPrivate(partitionID)
}

// removePartitionPrivate is the private function in collectionReplica, to remove the partition from collectionReplica
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

// getPartitionByID returns the partition which id is partitionID
func (colReplica *collectionReplica) getPartitionByID(partitionID UniqueID) (*Partition, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getPartitionByIDPrivate(partitionID)
}

// getPartitionByIDPrivate is the private function in collectionReplica, to get the partition
func (colReplica *collectionReplica) getPartitionByIDPrivate(partitionID UniqueID) (*Partition, error) {
	partition, ok := colReplica.partitions[partitionID]
	if !ok {
		return nil, errors.New("partition hasn't been loaded or has been released, partition id = %d" + strconv.FormatInt(partitionID, 10))
	}

	return partition, nil
}

// hasPartition returns true if collectionReplica has the partition, false otherwise
func (colReplica *collectionReplica) hasPartition(partitionID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.hasPartitionPrivate(partitionID)
}

// hasPartitionPrivate is the private function in collectionReplica, to check if collectionReplica has the partition
func (colReplica *collectionReplica) hasPartitionPrivate(partitionID UniqueID) bool {
	_, ok := colReplica.partitions[partitionID]
	return ok
}

// getPartitionNum returns num of partitions
func (colReplica *collectionReplica) getPartitionNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return len(colReplica.partitions)
}

// getSegmentIDs returns segment ids
func (colReplica *collectionReplica) getSegmentIDs(partitionID UniqueID) ([]UniqueID, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getSegmentIDsPrivate(partitionID)
}

// getSegmentIDsByVChannel returns segment ids which virtual channel is vChannel
func (colReplica *collectionReplica) getSegmentIDsByVChannel(partitionID UniqueID, vChannel Channel) ([]UniqueID, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	segmentIDs, err := colReplica.getSegmentIDsPrivate(partitionID)
	if err != nil {
		return nil, err
	}
	segmentIDsTmp := make([]UniqueID, 0)
	for _, segmentID := range segmentIDs {
		segment, err := colReplica.getSegmentByIDPrivate(segmentID)
		if err != nil {
			return nil, err
		}
		if segment.vChannelID == vChannel {
			segmentIDsTmp = append(segmentIDsTmp, segment.ID())
		}
	}

	return segmentIDsTmp, nil
}

// getSegmentIDsPrivate is private function in collectionReplica, it returns segment ids
func (colReplica *collectionReplica) getSegmentIDsPrivate(partitionID UniqueID) ([]UniqueID, error) {
	partition, err2 := colReplica.getPartitionByIDPrivate(partitionID)
	if err2 != nil {
		return nil, err2
	}
	return partition.segmentIDs, nil
}

//----------------------------------------------------------------------------------------------------- segment
// addSegment add a new segment to collectionReplica
func (colReplica *collectionReplica) addSegment(segmentID UniqueID, partitionID UniqueID, collectionID UniqueID, vChannelID Channel, segType segmentType, onService bool) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	collection, err := colReplica.getCollectionByIDPrivate(collectionID)
	if err != nil {
		return err
	}
	seg := newSegment(collection, segmentID, partitionID, collectionID, vChannelID, segType, onService)
	return colReplica.addSegmentPrivate(segmentID, partitionID, seg)
}

// addSegmentPrivate is private function in collectionReplica, to add a new segment to collectionReplica
func (colReplica *collectionReplica) addSegmentPrivate(segmentID UniqueID, partitionID UniqueID, segment *Segment) error {
	partition, err := colReplica.getPartitionByIDPrivate(partitionID)
	if err != nil {
		return err
	}

	if colReplica.hasSegmentPrivate(segmentID) {
		return nil
	}
	partition.addSegmentID(segmentID)
	colReplica.segments[segmentID] = segment

	return nil
}

// setSegment adds a segment to collectionReplica
func (colReplica *collectionReplica) setSegment(segment *Segment) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	_, err := colReplica.getCollectionByIDPrivate(segment.collectionID)
	if err != nil {
		return err
	}
	return colReplica.addSegmentPrivate(segment.segmentID, segment.partitionID, segment)
}

// removeSegment removes a segment from collectionReplica
func (colReplica *collectionReplica) removeSegment(segmentID UniqueID) error {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	return colReplica.removeSegmentPrivate(segmentID)
}

// removeSegmentPrivate is private function in collectionReplica, to remove a segment from collectionReplica
func (colReplica *collectionReplica) removeSegmentPrivate(segmentID UniqueID) error {
	segment, err := colReplica.getSegmentByIDPrivate(segmentID)
	if err != nil {
		return err
	}

	partition, err2 := colReplica.getPartitionByIDPrivate(segment.partitionID)
	if err2 != nil {
		return err
	}

	partition.removeSegmentID(segmentID)
	delete(colReplica.segments, segmentID)
	deleteSegment(segment)
	return nil
}

// getSegmentByID returns the segment which id is segmentID
func (colReplica *collectionReplica) getSegmentByID(segmentID UniqueID) (*Segment, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.getSegmentByIDPrivate(segmentID)
}

// getSegmentByIDPrivate is private function in collectionReplica, it returns the segment which id is segmentID
func (colReplica *collectionReplica) getSegmentByIDPrivate(segmentID UniqueID) (*Segment, error) {
	segment, ok := colReplica.segments[segmentID]
	if !ok {
		return nil, fmt.Errorf("cannot find segment %d in QueryNode", segmentID)
	}

	return segment, nil
}

// hasSegment returns true if collectionReplica has the segment, false otherwise
func (colReplica *collectionReplica) hasSegment(segmentID UniqueID) bool {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return colReplica.hasSegmentPrivate(segmentID)
}

// hasSegmentPrivate is private function in collectionReplica, to check if collectionReplica has the segment
func (colReplica *collectionReplica) hasSegmentPrivate(segmentID UniqueID) bool {
	_, ok := colReplica.segments[segmentID]
	return ok
}

// getSegmentNum returns num of segments in collectionReplica
func (colReplica *collectionReplica) getSegmentNum() int {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()
	return len(colReplica.segments)
}

//  getSegmentStatistics returns the statistics of segments in collectionReplica
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

//  removeExcludedSegments will remove excludedSegments from collectionReplica
func (colReplica *collectionReplica) removeExcludedSegments(collectionID UniqueID) {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	delete(colReplica.excludedSegments, collectionID)
}

// addExcludedSegments will add excludedSegments to collectionReplica
func (colReplica *collectionReplica) addExcludedSegments(collectionID UniqueID, segmentInfos []*datapb.SegmentInfo) {
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()

	if _, ok := colReplica.excludedSegments[collectionID]; !ok {
		colReplica.excludedSegments[collectionID] = make([]*datapb.SegmentInfo, 0)
	}

	colReplica.excludedSegments[collectionID] = append(colReplica.excludedSegments[collectionID], segmentInfos...)
}

// getExcludedSegments returns excludedSegments of collectionReplica
func (colReplica *collectionReplica) getExcludedSegments(collectionID UniqueID) ([]*datapb.SegmentInfo, error) {
	colReplica.mu.RLock()
	defer colReplica.mu.RUnlock()

	if _, ok := colReplica.excludedSegments[collectionID]; !ok {
		return nil, errors.New("getExcludedSegments failed, cannot found collection, id =" + fmt.Sprintln(collectionID))
	}

	return colReplica.excludedSegments[collectionID], nil
}

// freeAll will free all meta info from collectionReplica
func (colReplica *collectionReplica) freeAll() {
	colReplica.queryMu.Lock() // wait for current search/query finish
	colReplica.mu.Lock()
	defer colReplica.mu.Unlock()
	defer colReplica.queryMu.Unlock()

	for id := range colReplica.collections {
		_ = colReplica.removeCollectionPrivate(id)
	}

	colReplica.collections = make(map[UniqueID]*Collection)
	colReplica.partitions = make(map[UniqueID]*Partition)
	colReplica.segments = make(map[UniqueID]*Segment)
}

// newCollectionReplica returns a new ReplicaInterface
func newCollectionReplica(etcdKv *etcdkv.EtcdKV) ReplicaInterface {
	collections := make(map[UniqueID]*Collection)
	partitions := make(map[UniqueID]*Partition)
	segments := make(map[UniqueID]*Segment)
	excludedSegments := make(map[UniqueID][]*datapb.SegmentInfo)

	var replica ReplicaInterface = &collectionReplica{
		collections: collections,
		partitions:  partitions,
		segments:    segments,

		excludedSegments: excludedSegments,
		etcdKV:           etcdKv,
	}

	return replica
}

// trans segment to queryPb.segmentInfo
func getSegmentInfo(segment *Segment) *querypb.SegmentInfo {
	var indexName string
	var indexID int64
	// TODO:: segment has multi vec column
	for fieldID := range segment.indexInfos {
		indexName = segment.getIndexName(fieldID)
		indexID = segment.getIndexID(fieldID)
		break
	}
	info := &querypb.SegmentInfo{
		SegmentID:    segment.ID(),
		CollectionID: segment.collectionID,
		PartitionID:  segment.partitionID,
		NodeID:       Params.QueryNodeCfg.QueryNodeID,
		MemSize:      segment.getMemSize(),
		NumRows:      segment.getRowCount(),
		IndexName:    indexName,
		IndexID:      indexID,
		DmChannel:    segment.vChannelID,
		SegmentState: getSegmentStateBySegmentType(segment.segmentType),
	}
	return info
}

// TODO: remove segmentType and use queryPb.SegmentState instead
func getSegmentStateBySegmentType(segType segmentType) commonpb.SegmentState {
	switch segType {
	case segmentTypeGrowing:
		return commonpb.SegmentState_Growing
	case segmentTypeSealed:
		return commonpb.SegmentState_Sealed
	// TODO: remove segmentTypeIndexing
	case segmentTypeIndexing:
		return commonpb.SegmentState_Sealed
	default:
		return commonpb.SegmentState_NotExist
	}
}
