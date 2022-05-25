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

package datanode

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/bits-and-blooms/bloom/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
)

const (
	// TODO silverxia maybe need set from config
	bloomFilterSize       uint    = 100000
	maxBloomFalsePositive float64 = 0.005
)

type primaryKey = storage.PrimaryKey
type int64PrimaryKey = storage.Int64PrimaryKey
type varCharPrimaryKey = storage.VarCharPrimaryKey

var newInt64PrimaryKey = storage.NewInt64PrimaryKey
var newVarCharPrimaryKey = storage.NewVarCharPrimaryKey

// Replica is DataNode unique replication
type Replica interface {
	getCollectionID() UniqueID
	getCollectionSchema(collectionID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error)
	getCollectionAndPartitionID(segID UniqueID) (collID, partitionID UniqueID, err error)

	listAllSegmentIDs() []UniqueID
	listNotFlushedSegmentIDs() []UniqueID
	addNewSegment(segID, collID, partitionID UniqueID, channelName string, startPos, endPos *internalpb.MsgPosition) error
	addNormalSegment(segID, collID, partitionID UniqueID, channelName string, numOfRows int64, statsBinlog []*datapb.FieldBinlog, cp *segmentCheckPoint, recoverTs Timestamp) error
	filterSegments(channelName string, partitionID UniqueID) []*Segment
	addFlushedSegment(segID, collID, partitionID UniqueID, channelName string, numOfRows int64, statsBinlog []*datapb.FieldBinlog, recoverTs Timestamp) error
	listNewSegmentsStartPositions() []*datapb.SegmentStartPosition
	listSegmentsCheckPoints() map[UniqueID]segmentCheckPoint
	updateSegmentEndPosition(segID UniqueID, endPos *internalpb.MsgPosition)
	updateSegmentCheckPoint(segID UniqueID)
	updateSegmentPKRange(segID UniqueID, ids storage.FieldData)
	mergeFlushedSegments(segID, collID, partID, planID UniqueID, compactedFrom []UniqueID, channelName string, numOfRows int64) error
	hasSegment(segID UniqueID, countFlushed bool) bool
	removeSegments(segID ...UniqueID)
	listCompactedSegmentIDs() map[UniqueID][]UniqueID

	updateStatistics(segID UniqueID, numRows int64)
	refreshFlushedSegStatistics(segID UniqueID, numRows int64)
	getSegmentStatisticsUpdates(segID UniqueID) (*datapb.SegmentStats, error)
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
	compactedTo  UniqueID

	checkPoint segmentCheckPoint
	startPos   *internalpb.MsgPosition // TODO readonly
	endPos     *internalpb.MsgPosition

	pkFilter *bloom.BloomFilter //  bloom filter of pk inside a segment
	// TODO silverxia, needs to change to interface to support `string` type PK
	minPK primaryKey //	minimal pk value, shortcut for checking whether a pk is inside this segment
	maxPK primaryKey //  maximal pk value, same above
}

// SegmentReplica is the data replication of persistent data in datanode.
// It implements `Replica` interface.
type SegmentReplica struct {
	collectionID UniqueID
	collSchema   *schemapb.CollectionSchema

	segMu             sync.RWMutex
	newSegments       map[UniqueID]*Segment
	normalSegments    map[UniqueID]*Segment
	flushedSegments   map[UniqueID]*Segment
	compactedSegments map[UniqueID]*Segment

	metaService  *metaService
	chunkManager storage.ChunkManager
}

func (s *Segment) updatePk(pk primaryKey) error {
	if s.minPK == nil {
		s.minPK = pk
	} else if s.minPK.GT(pk) {
		s.minPK = pk
	}

	if s.maxPK == nil {
		s.maxPK = pk
	} else if s.maxPK.LT(pk) {
		s.maxPK = pk
	}

	return nil
}

func (s *Segment) updatePKRange(ids storage.FieldData) error {
	switch pks := ids.(type) {
	case *storage.Int64FieldData:
		buf := make([]byte, 8)
		for _, pk := range pks.Data {
			id := newInt64PrimaryKey(pk)
			err := s.updatePk(id)
			if err != nil {
				return err
			}
			common.Endian.PutUint64(buf, uint64(pk))
			s.pkFilter.Add(buf)
		}
	case *storage.StringFieldData:
		for _, pk := range pks.Data {
			id := newVarCharPrimaryKey(pk)
			err := s.updatePk(id)
			if err != nil {
				return err
			}
			s.pkFilter.AddString(pk)
		}
	default:
		//TODO::
	}

	log.Info("update pk range",
		zap.Int64("collectionID", s.collectionID), zap.Int64("partitionID", s.partitionID), zap.Int64("segmentID", s.segmentID),
		zap.String("channel", s.channelName),
		zap.Int64("num_rows", s.numRows), zap.Any("minPK", s.minPK), zap.Any("maxPK", s.maxPK))

	return nil
}

var _ Replica = &SegmentReplica{}

func newReplica(ctx context.Context, rc types.RootCoord, cm storage.ChunkManager, collID UniqueID) (*SegmentReplica, error) {
	metaService := newMetaService(rc, collID)

	replica := &SegmentReplica{
		collectionID: collID,

		newSegments:       make(map[UniqueID]*Segment),
		normalSegments:    make(map[UniqueID]*Segment),
		flushedSegments:   make(map[UniqueID]*Segment),
		compactedSegments: make(map[UniqueID]*Segment),

		metaService:  metaService,
		chunkManager: cm,
	}

	return replica, nil
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
	var seg = *replica.newSegments[segID]

	seg.isNew.Store(false)
	replica.normalSegments[segID] = &seg

	delete(replica.newSegments, segID)
}

func (replica *SegmentReplica) new2FlushedSegment(segID UniqueID) {
	var seg = *replica.newSegments[segID]

	seg.isNew.Store(false)
	seg.isFlushed.Store(true)
	replica.flushedSegments[segID] = &seg

	delete(replica.newSegments, segID)
	metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.NodeID)).Dec()
}

// normal2FlushedSegment transfers a segment from *normal* to *flushed* by changing *isFlushed*
//  flag into true, and mv the segment from normalSegments map to flushedSegments map.
func (replica *SegmentReplica) normal2FlushedSegment(segID UniqueID) {
	var seg = *replica.normalSegments[segID]

	seg.isFlushed.Store(true)
	replica.flushedSegments[segID] = &seg

	delete(replica.normalSegments, segID)
	metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.NodeID)).Dec()
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

	if seg, ok := replica.flushedSegments[segID]; ok {
		return seg.collectionID, seg.partitionID, nil
	}

	return 0, 0, fmt.Errorf("cannot find segment, id = %v", segID)
}

// addNewSegment adds a *New* and *NotFlushed* new segment. Before add, please make sure there's no
// such segment by `hasSegment`
func (replica *SegmentReplica) addNewSegment(segID, collID, partitionID UniqueID, channelName string,
	startPos, endPos *internalpb.MsgPosition) error {

	if collID != replica.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("input ID", collID),
			zap.Int64("expected ID", replica.collectionID))
		return fmt.Errorf("mismatch collection, ID=%d", collID)
	}

	log.Info("Add new segment",
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
	}

	seg.isNew.Store(true)
	seg.isFlushed.Store(false)

	replica.segMu.Lock()
	defer replica.segMu.Unlock()
	replica.newSegments[segID] = seg
	metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.NodeID)).Inc()
	return nil
}

func (replica *SegmentReplica) listCompactedSegmentIDs() map[UniqueID][]UniqueID {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	compactedTo2From := make(map[UniqueID][]UniqueID)

	for segID, seg := range replica.compactedSegments {
		var from []UniqueID
		from, ok := compactedTo2From[seg.compactedTo]
		if !ok {
			from = []UniqueID{}
		}

		from = append(from, segID)
		compactedTo2From[seg.compactedTo] = from
	}

	return compactedTo2From
}

// filterSegments return segments with same channelName and partition ID
// get all segments
func (replica *SegmentReplica) filterSegments(channelName string, partitionID UniqueID) []*Segment {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()
	results := make([]*Segment, 0)

	isMatched := func(segment *Segment, chanName string, partID UniqueID) bool {
		return segment.channelName == chanName && (partID == common.InvalidPartitionID || segment.partitionID == partID)
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
func (replica *SegmentReplica) addNormalSegment(segID, collID, partitionID UniqueID, channelName string, numOfRows int64, statsBinlogs []*datapb.FieldBinlog, cp *segmentCheckPoint, recoverTs Timestamp) error {
	if collID != replica.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("input ID", collID),
			zap.Int64("expected ID", replica.collectionID))
		return fmt.Errorf("mismatch collection, ID=%d", collID)
	}

	log.Info("Add Normal segment",
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

		pkFilter: bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive),
	}

	if cp != nil {
		seg.checkPoint = *cp
		seg.endPos = &cp.pos
	}
	err := replica.initPKBloomFilter(seg, statsBinlogs, recoverTs)
	if err != nil {
		return err
	}

	seg.isNew.Store(false)
	seg.isFlushed.Store(false)

	replica.segMu.Lock()
	replica.normalSegments[segID] = seg
	replica.segMu.Unlock()
	metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.NodeID)).Inc()

	return nil
}

// addFlushedSegment adds a *Flushed* segment. Before add, please make sure there's no
// such segment by `hasSegment`
func (replica *SegmentReplica) addFlushedSegment(segID, collID, partitionID UniqueID, channelName string, numOfRows int64, statsBinlogs []*datapb.FieldBinlog, recoverTs Timestamp) error {

	if collID != replica.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("input ID", collID),
			zap.Int64("expected ID", replica.collectionID))
		return fmt.Errorf("mismatch collection, ID=%d", collID)
	}

	log.Info("Add Flushed segment",
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
	}

	err := replica.initPKBloomFilter(seg, statsBinlogs, recoverTs)
	if err != nil {
		return err
	}

	seg.isNew.Store(false)
	seg.isFlushed.Store(true)

	replica.segMu.Lock()
	replica.flushedSegments[segID] = seg
	replica.segMu.Unlock()

	return nil
}

func (replica *SegmentReplica) initPKBloomFilter(s *Segment, statsBinlogs []*datapb.FieldBinlog, ts Timestamp) error {
	log.Info("begin to init pk bloom filter", zap.Int("stats bin logs", len(statsBinlogs)))
	schema, err := replica.getCollectionSchema(s.collectionID, ts)
	if err != nil {
		return err
	}

	// get pkfield id
	pkField := int64(-1)
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			pkField = field.FieldID
			break
		}
	}

	// filter stats binlog files which is pk field stats log
	var bloomFilterFiles []string
	for _, binlog := range statsBinlogs {
		if binlog.FieldID != pkField {
			continue
		}
		for _, log := range binlog.GetBinlogs() {
			bloomFilterFiles = append(bloomFilterFiles, log.GetLogPath())
		}
	}

	values, err := replica.chunkManager.MultiRead(bloomFilterFiles)
	if err != nil {
		log.Warn("failed to load bloom filter files", zap.Error(err))
		return err
	}
	blobs := make([]*Blob, 0)
	for i := 0; i < len(values); i++ {
		blobs = append(blobs, &Blob{Value: values[i]})
	}

	stats, err := storage.DeserializeStats(blobs)
	if err != nil {
		log.Warn("failed to deserialize bloom filter files", zap.Error(err))
		return err
	}
	for _, stat := range stats {
		err = s.pkFilter.Merge(stat.BF)
		if err != nil {
			return err
		}
		s.updatePk(stat.MinPk)
		s.updatePk(stat.MaxPk)
	}
	return nil
}

// listNewSegmentsStartPositions gets all *New Segments* start positions and
//   transfer segments states from *New* to *Normal*.
func (replica *SegmentReplica) listNewSegmentsStartPositions() []*datapb.SegmentStartPosition {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

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

func (replica *SegmentReplica) updateSegmentPKRange(segID UniqueID, ids storage.FieldData) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	seg, ok := replica.newSegments[segID]
	if ok {
		seg.updatePKRange(ids)
		return
	}

	seg, ok = replica.normalSegments[segID]
	if ok {
		seg.updatePKRange(ids)
		return
	}

	seg, ok = replica.flushedSegments[segID]
	if ok {
		seg.updatePKRange(ids)
		return
	}

	log.Warn("No match segment to update PK range", zap.Int64("ID", segID))
}

func (replica *SegmentReplica) removeSegments(segIDs ...UniqueID) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	log.Info("remove segments if exist", zap.Int64s("segmentIDs", segIDs))
	cnt := 0
	for _, segID := range segIDs {
		if _, ok := replica.newSegments[segID]; ok {
			cnt++
		} else if _, ok := replica.normalSegments[segID]; ok {
			cnt++
		}
	}
	metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.NodeID)).Sub(float64(cnt))

	for _, segID := range segIDs {
		delete(replica.newSegments, segID)
		delete(replica.normalSegments, segID)
		delete(replica.flushedSegments, segID)
		delete(replica.compactedSegments, segID)
	}
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
func (replica *SegmentReplica) refreshFlushedSegStatistics(segID UniqueID, numRows int64) {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	if seg, ok := replica.flushedSegments[segID]; ok {
		seg.memorySize = 0
		seg.numRows = numRows
		return
	}

	log.Warn("refresh numRow on not exists segment", zap.Int64("segID", segID))
}

// updateStatistics updates the number of rows of a segment in replica.
func (replica *SegmentReplica) updateStatistics(segID UniqueID, numRows int64) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	log.Info("updating segment", zap.Int64("Segment ID", segID), zap.Int64("numRows", numRows))
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
func (replica *SegmentReplica) getSegmentStatisticsUpdates(segID UniqueID) (*datapb.SegmentStats, error) {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()
	updates := &datapb.SegmentStats{SegmentID: segID}

	if seg, ok := replica.newSegments[segID]; ok {
		updates.NumRows = seg.numRows
		return updates, nil
	}

	if seg, ok := replica.normalSegments[segID]; ok {
		updates.NumRows = seg.numRows
		return updates, nil
	}

	if seg, ok := replica.flushedSegments[segID]; ok {
		updates.NumRows = seg.numRows
		return updates, nil
	}

	return nil, fmt.Errorf("error, there's no segment %v", segID)
}

// --- collection ---
func (replica *SegmentReplica) getCollectionID() UniqueID {
	return replica.collectionID
}

// getCollectionSchema gets collection schema from rootcoord for a certain timestamp.
//   If you want the latest collection schema, ts should be 0.
func (replica *SegmentReplica) getCollectionSchema(collID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error) {
	if !replica.validCollection(collID) {
		log.Warn("Mismatch collection for the replica",
			zap.Int64("Want", replica.collectionID),
			zap.Int64("Actual", collID),
		)
		return nil, fmt.Errorf("not supported collection %v", collID)
	}

	if replica.collSchema == nil {
		sch, err := replica.metaService.getCollectionSchema(context.Background(), collID, ts)
		if err != nil {
			log.Error("Grpc error", zap.Error(err))
			return nil, err
		}
		replica.collSchema = sch
	}

	return replica.collSchema, nil
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

func (replica *SegmentReplica) mergeFlushedSegments(segID, collID, partID, planID UniqueID, compactedFrom []UniqueID, channelName string, numOfRows int64) error {
	if collID != replica.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("input ID", collID),
			zap.Int64("expected ID", replica.collectionID))
		return fmt.Errorf("mismatch collection, ID=%d", collID)
	}

	log.Info("merge flushed segments",
		zap.Int64("planID", planID),
		zap.Int64("compacted To segmentID", segID),
		zap.Int64s("compacted From segmentIDs", compactedFrom),
		zap.Int64("partition ID", partID),
		zap.String("channel name", channelName),
	)

	seg := &Segment{
		collectionID: collID,
		partitionID:  partID,
		segmentID:    segID,
		channelName:  channelName,
		numRows:      numOfRows,

		pkFilter: bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive),
	}

	replica.segMu.Lock()
	for _, ID := range compactedFrom {
		s, ok := replica.flushedSegments[ID]

		if !ok {
			log.Warn("no match flushed segment to merge from", zap.Int64("segmentID", ID))
			continue
		}

		s.compactedTo = segID
		replica.compactedSegments[ID] = s
		delete(replica.flushedSegments, ID)

		seg.pkFilter.Merge(s.pkFilter)
	}
	replica.segMu.Unlock()

	seg.isNew.Store(false)
	seg.isFlushed.Store(true)

	replica.segMu.Lock()
	replica.flushedSegments[segID] = seg
	replica.segMu.Unlock()

	return nil
}

// for tests only
func (replica *SegmentReplica) addFlushedSegmentWithPKs(segID, collID, partID UniqueID, channelName string, numOfRows int64, ids storage.FieldData) error {
	if collID != replica.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("input ID", collID),
			zap.Int64("expected ID", replica.collectionID))
		return fmt.Errorf("mismatch collection, ID=%d", collID)
	}

	log.Info("Add Flushed segment",
		zap.Int64("segment ID", segID),
		zap.Int64("collection ID", collID),
		zap.Int64("partition ID", partID),
		zap.String("channel name", channelName),
	)

	seg := &Segment{
		collectionID: collID,
		partitionID:  partID,
		segmentID:    segID,
		channelName:  channelName,
		numRows:      numOfRows,

		pkFilter: bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive),
	}

	seg.updatePKRange(ids)

	seg.isNew.Store(false)
	seg.isFlushed.Store(true)

	replica.segMu.Lock()
	replica.flushedSegments[segID] = seg
	replica.segMu.Unlock()

	return nil
}

func (replica *SegmentReplica) listAllSegmentIDs() []UniqueID {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	var segIDs []UniqueID

	for _, seg := range replica.newSegments {
		segIDs = append(segIDs, seg.segmentID)
	}

	for _, seg := range replica.normalSegments {
		segIDs = append(segIDs, seg.segmentID)
	}

	for _, seg := range replica.flushedSegments {
		segIDs = append(segIDs, seg.segmentID)
	}

	return segIDs
}

func (replica *SegmentReplica) listNotFlushedSegmentIDs() []UniqueID {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	var segIDs []UniqueID

	for _, seg := range replica.newSegments {
		segIDs = append(segIDs, seg.segmentID)
	}

	for _, seg := range replica.normalSegments {
		segIDs = append(segIDs, seg.segmentID)
	}

	return segIDs
}
