// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datanode

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
)

const (
	// TODO silverxia maybe need set from config
	bloomFilterSize       uint    = 100000
	maxBloomFalsePositive float64 = 0.005
)

// Replica is DataNode unique replication
type Replica interface {
	getCollectionID() UniqueID
	getCollectionSchema(collectionID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error)
	getCollectionAndPartitionID(segID UniqueID) (collID, partitionID UniqueID, err error)

	addNewSegment(segID, collID, partitionID UniqueID, channelName string, startPos, endPos *internalpb.MsgPosition) error
	addNormalSegment(segID, collID, partitionID UniqueID, channelName string, numOfRows int64, cp *segmentCheckPoint) error
	filterSegments(channelName string, partitionID UniqueID) []*Segment
	addFlushedSegment(segID, collID, partitionID UniqueID, channelName string, numOfRows int64) error
	listNewSegmentsStartPositions() []*datapb.SegmentStartPosition
	listSegmentsCheckPoints() map[UniqueID]segmentCheckPoint
	updateSegmentEndPosition(segID UniqueID, endPos *internalpb.MsgPosition)
	updateSegmentCheckPoint(segID UniqueID)
	updateSegmentPKRange(segID UniqueID, rowIDs []int64)
	hasSegment(segID UniqueID, countFlushed bool) bool

	updateStatistics(segID UniqueID, numRows int64)
	getSegmentStatisticsUpdates(segID UniqueID) (*internalpb.SegmentStatisticsUpdates, error)
	segmentFlushed(segID UniqueID)
}

// Segment is the data structure of segments in data node replica.
type Segment struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	numRows      int64
	memorySize   int64
	isNew        atomic.Value // bool
	isFlushed    atomic.Value // bool
	channelName  string

	checkPoint segmentCheckPoint
	startPos   *internalpb.MsgPosition // TODO readonly
	endPos     *internalpb.MsgPosition

	pkFilter *bloom.BloomFilter //  bloom filter of pk inside a segment
	// TODO silverxia, needs to change to interface to support `string` type PK
	minPK int64 //	minimal pk value, shortcut for checking whether a pk is inside this segment
	maxPK int64 //  maximal pk value, same above
}

// SegmentReplica is the data replication of persistent data in datanode.
// It implements `Replica` interface.
type SegmentReplica struct {
	collectionID UniqueID
	collSchema   *schemapb.CollectionSchema

	segMu           sync.RWMutex
	newSegments     map[UniqueID]*Segment
	normalSegments  map[UniqueID]*Segment
	flushedSegments map[UniqueID]*Segment

	metaService *metaService
}

func (s *Segment) updatePKRange(rowIDs []int64) {
	buf := make([]byte, 8)
	for _, rowID := range rowIDs {
		binary.BigEndian.PutUint64(buf, uint64(rowID))
		s.pkFilter.Add(buf)
		if rowID > s.maxPK {
			s.maxPK = rowID
		}
		if rowID < s.minPK {
			s.minPK = rowID
		}
	}
}

var _ Replica = &SegmentReplica{}

func newReplica(rc types.RootCoord, collID UniqueID) Replica {
	metaService := newMetaService(rc, collID)

	var replica Replica = &SegmentReplica{
		collectionID: collID,

		newSegments:     make(map[UniqueID]*Segment),
		normalSegments:  make(map[UniqueID]*Segment),
		flushedSegments: make(map[UniqueID]*Segment),

		metaService: metaService,
	}
	return replica
}

// segmentFlushed transfers a segment from *New* or *Normal* into *Flushed*.
func (replica *SegmentReplica) segmentFlushed(segID UniqueID) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	if _, ok := replica.newSegments[segID]; ok {
		replica.new2FlushedSegment(segID)
	}

	if _, ok := replica.normalSegments[segID]; ok {
		replica.normal2FlushedSegment(segID)
	}
}

func (replica *SegmentReplica) new2NormalSegment(segID UniqueID) {
	var seg Segment = *replica.newSegments[segID]

	seg.isNew.Store(false)
	replica.normalSegments[segID] = &seg

	delete(replica.newSegments, segID)
}

func (replica *SegmentReplica) new2FlushedSegment(segID UniqueID) {
	var seg Segment = *replica.newSegments[segID]

	seg.isNew.Store(false)
	seg.isFlushed.Store(true)
	replica.flushedSegments[segID] = &seg

	delete(replica.newSegments, segID)
}

// normal2FlushedSegment transfers a segment from *normal* to *flushed* by changing *isFlushed*
//  flag into true, and mv the segment from normalSegments map to flushedSegments map.
func (replica *SegmentReplica) normal2FlushedSegment(segID UniqueID) {
	var seg Segment = *replica.normalSegments[segID]

	seg.isFlushed.Store(true)
	replica.flushedSegments[segID] = &seg

	delete(replica.normalSegments, segID)
}

func (replica *SegmentReplica) getCollectionAndPartitionID(segID UniqueID) (collID, partitionID UniqueID, err error) {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	if seg, ok := replica.newSegments[segID]; ok {
		return seg.collectionID, seg.partitionID, nil
	}

	if seg, ok := replica.normalSegments[segID]; ok {
		return seg.collectionID, seg.partitionID, nil
	}

	return 0, 0, fmt.Errorf("Cannot find segment, id = %v", segID)
}

// addNewSegment adds a *New* and *NotFlushed* new segment. Before add, please make sure there's no
// such segment by `hasSegment`
func (replica *SegmentReplica) addNewSegment(segID, collID, partitionID UniqueID, channelName string,
	startPos, endPos *internalpb.MsgPosition) error {

	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	if collID != replica.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("input ID", collID),
			zap.Int64("expected ID", replica.collectionID))
		return fmt.Errorf("Mismatch collection, ID=%d", collID)
	}

	log.Debug("Add new segment",
		zap.Int64("segment ID", segID),
		zap.Int64("collection ID", collID),
		zap.Int64("partition ID", partitionID),
		zap.String("channel name", channelName),
	)

	seg := &Segment{
		collectionID: collID,
		partitionID:  partitionID,
		segmentID:    segID,
		channelName:  channelName,

		checkPoint: segmentCheckPoint{0, *startPos},
		startPos:   startPos,
		endPos:     endPos,

		pkFilter: bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive),
		minPK:    math.MaxInt64, // use max value, represents no value
		maxPK:    math.MinInt64, // use min value represents no value
	}

	seg.isNew.Store(true)
	seg.isFlushed.Store(false)

	replica.newSegments[segID] = seg
	return nil
}

// filterSegments return segments with same channelName and partition ID
func (replica *SegmentReplica) filterSegments(channelName string, partitionID UniqueID) []*Segment {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()
	results := make([]*Segment, 0)

	isMatched := func(segment *Segment, chanName string, partID UniqueID) bool {
		return segment.channelName == chanName && (partID == 0 || segment.partitionID == partID)
	}
	for _, seg := range replica.newSegments {
		if isMatched(seg, channelName, partitionID) {
			results = append(results, seg)
		}
	}
	for _, seg := range replica.normalSegments {
		if isMatched(seg, channelName, partitionID) {
			results = append(results, seg)
		}
	}
	for _, seg := range replica.flushedSegments {
		if isMatched(seg, channelName, partitionID) {
			results = append(results, seg)
		}
	}
	return results
}

// addNormalSegment adds a *NotNew* and *NotFlushed* segment. Before add, please make sure there's no
// such segment by `hasSegment`
func (replica *SegmentReplica) addNormalSegment(segID, collID, partitionID UniqueID, channelName string, numOfRows int64, cp *segmentCheckPoint) error {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	if collID != replica.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("input ID", collID),
			zap.Int64("expected ID", replica.collectionID))
		return fmt.Errorf("Mismatch collection, ID=%d", collID)
	}

	log.Debug("Add Normal segment",
		zap.Int64("segment ID", segID),
		zap.Int64("collection ID", collID),
		zap.Int64("partition ID", partitionID),
		zap.String("channel name", channelName),
	)

	seg := &Segment{
		collectionID: collID,
		partitionID:  partitionID,
		segmentID:    segID,
		channelName:  channelName,
		numRows:      numOfRows,

		checkPoint: *cp,
		endPos:     &cp.pos,

		//TODO silverxia, normal segments bloom filter and pk range should be loaded from serialized files
		pkFilter: bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive),
		minPK:    math.MaxInt64, // use max value, represents no value
		maxPK:    math.MinInt64, // use min value represents no value
	}

	seg.isNew.Store(false)
	seg.isFlushed.Store(false)

	replica.normalSegments[segID] = seg
	return nil
}

// addFlushedSegment adds a *Flushed* segment. Before add, please make sure there's no
// such segment by `hasSegment`
func (replica *SegmentReplica) addFlushedSegment(segID, collID, partitionID UniqueID, channelName string, numOfRows int64) error {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	if collID != replica.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("input ID", collID),
			zap.Int64("expected ID", replica.collectionID))
		return fmt.Errorf("Mismatch collection, ID=%d", collID)
	}

	log.Debug("Add Normal segment",
		zap.Int64("segment ID", segID),
		zap.Int64("collection ID", collID),
		zap.Int64("partition ID", partitionID),
		zap.String("channel name", channelName),
	)

	seg := &Segment{
		collectionID: collID,
		partitionID:  partitionID,
		segmentID:    segID,
		channelName:  channelName,
		numRows:      numOfRows,

		//TODO silverxia, normal segments bloom filter and pk range should be loaded from serialized files
		pkFilter: bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive),
		minPK:    math.MaxInt64, // use max value, represents no value
		maxPK:    math.MinInt64, // use min value represents no value
	}

	seg.isNew.Store(false)
	seg.isFlushed.Store(true)

	replica.flushedSegments[segID] = seg
	return nil
}

// listNewSegmentsStartPositions gets all *New Segments* start positions and
//   transfer segments states from *New* to *Normal*.
func (replica *SegmentReplica) listNewSegmentsStartPositions() []*datapb.SegmentStartPosition {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	result := make([]*datapb.SegmentStartPosition, 0, len(replica.newSegments))
	for id, seg := range replica.newSegments {

		result = append(result, &datapb.SegmentStartPosition{
			SegmentID:     id,
			StartPosition: seg.startPos,
		})

		// transfer states
		replica.new2NormalSegment(id)
	}
	return result
}

// listSegmentsCheckPoints gets check points from both *New* and *Normal* segments.
func (replica *SegmentReplica) listSegmentsCheckPoints() map[UniqueID]segmentCheckPoint {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	result := make(map[UniqueID]segmentCheckPoint)

	for id, seg := range replica.newSegments {
		result[id] = seg.checkPoint
	}

	for id, seg := range replica.normalSegments {
		result[id] = seg.checkPoint
	}

	return result
}

// updateSegmentEndPosition updates *New* or *Normal* segment's end position.
func (replica *SegmentReplica) updateSegmentEndPosition(segID UniqueID, endPos *internalpb.MsgPosition) {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	seg, ok := replica.newSegments[segID]
	if ok {
		seg.endPos = endPos
		return
	}

	seg, ok = replica.normalSegments[segID]
	if ok {
		seg.endPos = endPos
		return
	}

	log.Warn("No match segment", zap.Int64("ID", segID))
}

func (replica *SegmentReplica) updateSegmentPKRange(segID UniqueID, rowIDs []int64) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	seg, ok := replica.newSegments[segID]
	if ok {
		seg.updatePKRange(rowIDs)
		return
	}

	seg, ok = replica.normalSegments[segID]
	if ok {
		seg.updatePKRange(rowIDs)
		return
	}

	log.Warn("No match segment to update PK range", zap.Int64("ID", segID))
}

func (replica *SegmentReplica) removeSegment(segID UniqueID) error {
	return nil
}

// hasSegment checks whether this replica has a segment according to segment ID.
func (replica *SegmentReplica) hasSegment(segID UniqueID, countFlushed bool) bool {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	_, inNew := replica.newSegments[segID]
	_, inNormal := replica.normalSegments[segID]

	inFlush := false
	if countFlushed {
		_, inFlush = replica.flushedSegments[segID]
	}

	return inNew || inNormal || inFlush
}

// updateStatistics updates the number of rows of a segment in replica.
func (replica *SegmentReplica) updateStatistics(segID UniqueID, numRows int64) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	log.Debug("updating segment", zap.Int64("Segment ID", segID), zap.Int64("numRows", numRows))
	if seg, ok := replica.newSegments[segID]; ok {
		seg.memorySize = 0
		seg.numRows += numRows
		return
	}

	if seg, ok := replica.normalSegments[segID]; ok {
		seg.memorySize = 0
		seg.numRows += numRows
		return
	}

	log.Warn("update segment num row not exist", zap.Int64("segID", segID))
}

// getSegmentStatisticsUpdates gives current segment's statistics updates.
func (replica *SegmentReplica) getSegmentStatisticsUpdates(segID UniqueID) (*internalpb.SegmentStatisticsUpdates, error) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()
	updates := &internalpb.SegmentStatisticsUpdates{
		SegmentID: segID,
	}

	if seg, ok := replica.newSegments[segID]; ok {
		updates.NumRows = seg.numRows
		return updates, nil
	}

	if seg, ok := replica.normalSegments[segID]; ok {
		updates.NumRows = seg.numRows
		return updates, nil
	}

	return nil, fmt.Errorf("Error, there's no segment %v", segID)
}

// --- collection ---
func (replica *SegmentReplica) getCollectionID() UniqueID {
	return replica.collectionID
}

// getCollectionSchema gets collection schema from rootcoord for a certain timestamp.
//   If you want the latest collection schema, ts should be 0.
func (replica *SegmentReplica) getCollectionSchema(collID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	if !replica.validCollection(collID) {
		log.Warn("Mismatch collection for the replica",
			zap.Int64("Want", replica.collectionID),
			zap.Int64("Actual", collID),
		)
		return nil, fmt.Errorf("Not supported collection %v", collID)
	}

	sch, err := replica.metaService.getCollectionSchema(context.Background(), collID, ts)
	if err != nil {
		log.Error("Grpc error", zap.Error(err))
		return nil, err
	}

	return sch, nil
}

func (replica *SegmentReplica) validCollection(collID UniqueID) bool {
	return collID == replica.collectionID
}

// updateSegmentCheckPoint is called when auto flush or mannul flush is done.
func (replica *SegmentReplica) updateSegmentCheckPoint(segID UniqueID) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	if seg, ok := replica.newSegments[segID]; ok {
		seg.checkPoint = segmentCheckPoint{seg.numRows, *seg.endPos}
		return
	}

	if seg, ok := replica.normalSegments[segID]; ok {
		seg.checkPoint = segmentCheckPoint{seg.numRows, *seg.endPos}
		return
	}

	log.Warn("There's no segment", zap.Int64("ID", segID))
}
