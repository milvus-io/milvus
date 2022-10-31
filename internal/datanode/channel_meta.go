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
	"time"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type (
	primaryKey        = storage.PrimaryKey
	int64PrimaryKey   = storage.Int64PrimaryKey
	varCharPrimaryKey = storage.VarCharPrimaryKey
)

var (
	newInt64PrimaryKey   = storage.NewInt64PrimaryKey
	newVarCharPrimaryKey = storage.NewVarCharPrimaryKey
)

// Channel is DataNode unique replication
type Channel interface {
	getCollectionID() UniqueID
	getCollectionSchema(collectionID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error)
	getCollectionAndPartitionID(segID UniqueID) (collID, partitionID UniqueID, err error)
	getChannelName(segID UniqueID) string

	listAllSegmentIDs() []UniqueID
	listNotFlushedSegmentIDs() []UniqueID
	addSegment(req addSegmentReq) error
	listPartitionSegments(partID UniqueID) []UniqueID
	filterSegments(partitionID UniqueID) []*Segment
	listNewSegmentsStartPositions() []*datapb.SegmentStartPosition
	transferNewSegments(segmentIDs []UniqueID)
	updateSegmentEndPosition(segID UniqueID, endPos *internalpb.MsgPosition)
	updateSegmentPKRange(segID UniqueID, ids storage.FieldData)
	mergeFlushedSegments(seg *Segment, planID UniqueID, compactedFrom []UniqueID) error
	hasSegment(segID UniqueID, countFlushed bool) bool
	removeSegments(segID ...UniqueID)
	listCompactedSegmentIDs() map[UniqueID][]UniqueID

	updateStatistics(segID UniqueID, numRows int64)
	InitPKstats(ctx context.Context, s *Segment, statsBinlogs []*datapb.FieldBinlog, ts Timestamp) error
	RollPKstats(segID UniqueID, stats []*storage.PrimaryKeyStats)
	getSegmentStatisticsUpdates(segID UniqueID) (*datapb.SegmentStats, error)
	segmentFlushed(segID UniqueID)
}

// ChannelMeta contains channel meta and the latest segments infos of the channel.
type ChannelMeta struct {
	collectionID UniqueID
	channelName  string
	collSchema   *schemapb.CollectionSchema
	schemaMut    sync.RWMutex

	segMu    sync.RWMutex
	segments map[UniqueID]*Segment

	metaService  *metaService
	chunkManager storage.ChunkManager
}

var _ Channel = &ChannelMeta{}

func newChannel(channelName string, collID UniqueID, schema *schemapb.CollectionSchema, rc types.RootCoord, cm storage.ChunkManager) *ChannelMeta {
	metaService := newMetaService(rc, collID)

	channel := ChannelMeta{
		collectionID: collID,
		collSchema:   schema,
		channelName:  channelName,

		segments: make(map[UniqueID]*Segment),

		metaService:  metaService,
		chunkManager: cm,
	}

	return &channel
}

// segmentFlushed transfers a segment from *New* or *Normal* into *Flushed*.
func (c *ChannelMeta) segmentFlushed(segID UniqueID) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	if seg, ok := c.segments[segID]; ok {
		seg.setType(datapb.SegmentType_Flushed)
	}
	metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Dec()
}

// new2NormalSegment transfers a segment from *New* to *Normal*.
// make sure the segID is in the channel before call this func
func (c *ChannelMeta) new2NormalSegment(segID UniqueID) {
	seg := c.segments[segID]
	if seg.getType() == datapb.SegmentType_New {
		seg.setType(datapb.SegmentType_Normal)
	}
}

func (c *ChannelMeta) getCollectionAndPartitionID(segID UniqueID) (collID, partitionID UniqueID, err error) {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	if seg, ok := c.segments[segID]; ok && seg.isValid() {
		return seg.collectionID, seg.partitionID, nil
	}
	return 0, 0, fmt.Errorf("cannot find segment, id = %d", segID)
}

func (c *ChannelMeta) getChannelName(segID UniqueID) string {
	return c.channelName
}

// maxRowCountPerSegment returns max row count for a segment based on estimation of row size.
func (c *ChannelMeta) maxRowCountPerSegment(ts Timestamp) (int64, error) {
	log := log.With(zap.Int64("collectionID", c.collectionID), zap.Uint64("timpstamp", ts))
	schema, err := c.getCollectionSchema(c.collectionID, ts)
	if err != nil {
		log.Warn("failed to get collection schema", zap.Error(err))
		return 0, err
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		log.Warn("failed to estimate size per record", zap.Error(err))
		return 0, err
	}
	threshold := Params.DataCoordCfg.SegmentMaxSize * 1024 * 1024
	return int64(threshold / float64(sizePerRecord)), nil
}

// addSegment adds the segment to current channel. Segments can be added as *new*, *normal* or *flushed*.
// Make sure to verify `channel.hasSegment(segID)` == false before calling `channel.addSegment()`.
func (c *ChannelMeta) addSegment(req addSegmentReq) error {
	if req.collID != c.collectionID {
		log.Warn("collection mismatch",
			zap.Int64("current collection ID", req.collID),
			zap.Int64("expected collection ID", c.collectionID))
		return fmt.Errorf("mismatch collection, ID=%d", req.collID)
	}
	log.Info("adding segment",
		zap.String("type", req.segType.String()),
		zap.Int64("segmentID", req.segID),
		zap.Int64("collectionID", req.collID),
		zap.Int64("partitionID", req.partitionID),
		zap.String("channel", c.channelName),
		zap.Any("startPosition", req.startPos),
		zap.Any("endPosition", req.endPos),
		zap.Uint64("recoverTs", req.recoverTs),
		zap.Bool("importing", req.importing),
	)
	seg := &Segment{
		collectionID: req.collID,
		partitionID:  req.partitionID,
		segmentID:    req.segID,
		numRows:      req.numOfRows, // 0 if segType == NEW
		startPos:     req.startPos,
		endPos:       req.endPos,
	}
	seg.sType.Store(req.segType)
	// Set up pk stats
	err := c.InitPKstats(context.TODO(), seg, req.statsBinLogs, req.recoverTs)
	if err != nil {
		log.Error("failed to init bloom filter",
			zap.Int64("segment ID", req.segID),
			zap.Error(err))
		return err
	}

	c.segMu.Lock()
	c.segments[req.segID] = seg
	c.segMu.Unlock()
	if req.segType == datapb.SegmentType_New || req.segType == datapb.SegmentType_Normal {
		metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Inc()
	}
	return nil
}

func (c *ChannelMeta) listCompactedSegmentIDs() map[UniqueID][]UniqueID {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	compactedTo2From := make(map[UniqueID][]UniqueID)

	for segID, seg := range c.segments {
		if !seg.isValid() {
			compactedTo2From[seg.compactedTo] = append(compactedTo2From[seg.compactedTo], segID)
		}
	}
	return compactedTo2From
}

// filterSegments return segments with same partitionID for all segments
// get all segments
func (c *ChannelMeta) filterSegments(partitionID UniqueID) []*Segment {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	var results []*Segment
	for _, seg := range c.segments {
		if seg.isValid() &&
			partitionID == common.InvalidPartitionID || seg.partitionID == partitionID {
			results = append(results, seg)
		}
	}
	return results
}

func (c *ChannelMeta) InitPKstats(ctx context.Context, s *Segment, statsBinlogs []*datapb.FieldBinlog, ts Timestamp) error {
	startTs := time.Now()
	log := log.With(zap.Int64("segmentID", s.segmentID))
	log.Info("begin to init pk bloom filter", zap.Int("stats bin logs", len(statsBinlogs)))
	schema, err := c.getCollectionSchema(s.collectionID, ts)
	if err != nil {
		log.Warn("failed to initPKBloomFilter, get schema return error", zap.Error(err))
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

	// no stats log to parse, initialize a new BF
	if len(bloomFilterFiles) == 0 {
		log.Warn("no stats files to load")
		return nil
	}

	// read historical PK filter
	values, err := c.chunkManager.MultiRead(ctx, bloomFilterFiles)
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
	var size uint
	for _, stat := range stats {
		pkStat := &storage.PkStatistics{
			PkFilter: stat.BF,
			MinPK:    stat.MinPk,
			MaxPK:    stat.MaxPk,
		}
		size += stat.BF.Cap()
		s.historyStats = append(s.historyStats, pkStat)
	}
	log.Info("Successfully load pk stats", zap.Any("time", time.Since(startTs)), zap.Uint("size", size))

	return nil
}

func (c *ChannelMeta) RollPKstats(segID UniqueID, stats []*storage.PrimaryKeyStats) {
	c.segMu.Lock()
	defer c.segMu.Unlock()
	seg, ok := c.segments[segID]
	log.Info("roll pk stats", zap.Int64("segment id", segID))
	if ok && seg.notFlushed() {
		for _, stat := range stats {
			pkStat := &storage.PkStatistics{
				PkFilter: stat.BF,
				MinPK:    stat.MinPk,
				MaxPK:    stat.MaxPk,
			}
			seg.historyStats = append(seg.historyStats, pkStat)
		}
		seg.currentStat = nil
		return
	}
	// should not happen at all
	if ok {
		log.Warn("only growing segment should roll PK stats", zap.Int64("segment", segID), zap.Any("type", seg.sType))
	} else {
		log.Warn("can not find segment", zap.Int64("segment", segID))
	}
}

// listNewSegmentsStartPositions gets all *New Segments* start positions and
//   transfer segments states from *New* to *Normal*.
func (c *ChannelMeta) listNewSegmentsStartPositions() []*datapb.SegmentStartPosition {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	var result []*datapb.SegmentStartPosition
	for id, seg := range c.segments {
		if seg.getType() == datapb.SegmentType_New {
			result = append(result, &datapb.SegmentStartPosition{
				SegmentID:     id,
				StartPosition: seg.startPos,
			})
		}
	}
	return result
}

// transferNewSegments make new segment transfer to normal segments.
func (c *ChannelMeta) transferNewSegments(segmentIDs []UniqueID) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	for _, segmentID := range segmentIDs {
		c.new2NormalSegment(segmentID)
	}
}

// updateSegmentEndPosition updates *New* or *Normal* segment's end position.
func (c *ChannelMeta) updateSegmentEndPosition(segID UniqueID, endPos *internalpb.MsgPosition) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	seg, ok := c.segments[segID]
	if ok && seg.notFlushed() {
		seg.endPos = endPos
		return
	}

	log.Warn("No match segment", zap.Int64("ID", segID))
}

func (c *ChannelMeta) updateSegmentPKRange(segID UniqueID, ids storage.FieldData) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	seg, ok := c.segments[segID]
	if ok && seg.isValid() {
		seg.updatePKRange(ids)
		return
	}

	log.Warn("No match segment to update PK range", zap.Int64("ID", segID))
}

func (c *ChannelMeta) removeSegments(segIDs ...UniqueID) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	log.Info("remove segments if exist", zap.Int64s("segmentIDs", segIDs))
	cnt := 0
	for _, segID := range segIDs {
		seg, ok := c.segments[segID]
		if ok &&
			(seg.getType() == datapb.SegmentType_New || seg.getType() == datapb.SegmentType_Normal) {
			cnt++
		}

		delete(c.segments, segID)
	}
	metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Sub(float64(cnt))
}

// hasSegment checks whether this channel has a segment according to segment ID.
func (c *ChannelMeta) hasSegment(segID UniqueID, countFlushed bool) bool {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	seg, ok := c.segments[segID]
	if !ok {
		return false
	}

	if !seg.isValid() ||
		(!countFlushed && seg.getType() == datapb.SegmentType_Flushed) {
		return false
	}

	return true
}

// updateStatistics updates the number of rows of a segment in channel.
func (c *ChannelMeta) updateStatistics(segID UniqueID, numRows int64) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	log.Info("updating segment", zap.Int64("Segment ID", segID), zap.Int64("numRows", numRows))
	seg, ok := c.segments[segID]
	if ok && seg.notFlushed() {
		seg.memorySize = 0
		seg.numRows += numRows
		return
	}

	log.Warn("update segment num row not exist", zap.Int64("segID", segID))
}

// getSegmentStatisticsUpdates gives current segment's statistics updates.
func (c *ChannelMeta) getSegmentStatisticsUpdates(segID UniqueID) (*datapb.SegmentStats, error) {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	if seg, ok := c.segments[segID]; ok && seg.isValid() {
		return &datapb.SegmentStats{SegmentID: segID, NumRows: seg.numRows}, nil
	}

	return nil, fmt.Errorf("error, there's no segment %d", segID)
}

func (c *ChannelMeta) getCollectionID() UniqueID {
	return c.collectionID
}

// getCollectionSchema gets collection schema from rootcoord for a certain timestamp.
//   If you want the latest collection schema, ts should be 0.
func (c *ChannelMeta) getCollectionSchema(collID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error) {
	if !c.validCollection(collID) {
		return nil, fmt.Errorf("mismatch collection, want %d, actual %d", c.collectionID, collID)
	}

	c.schemaMut.RLock()
	if c.collSchema == nil {
		c.schemaMut.RUnlock()

		c.schemaMut.Lock()
		defer c.schemaMut.Unlock()
		if c.collSchema == nil {
			sch, err := c.metaService.getCollectionSchema(context.Background(), collID, ts)
			if err != nil {
				return nil, err
			}
			c.collSchema = sch
		}
	} else {
		defer c.schemaMut.RUnlock()
	}

	return c.collSchema, nil
}

func (c *ChannelMeta) validCollection(collID UniqueID) bool {
	return collID == c.collectionID
}

func (c *ChannelMeta) mergeFlushedSegments(seg *Segment, planID UniqueID, compactedFrom []UniqueID) error {

	log := log.With(
		zap.Int64("segment ID", seg.segmentID),
		zap.Int64("collection ID", seg.collectionID),
		zap.Int64("partition ID", seg.partitionID),
		zap.Int64s("compacted from", compactedFrom),
		zap.Int64("planID", planID),
		zap.String("channel name", c.channelName))

	if seg.collectionID != c.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("expected collectionID", c.collectionID))
		return fmt.Errorf("mismatch collection, ID=%d", seg.collectionID)
	}

	var inValidSegments []UniqueID
	for _, ID := range compactedFrom {
		// no such segments in channel or the segments are unflushed.
		if !c.hasSegment(ID, true) || c.hasSegment(ID, false) {
			inValidSegments = append(inValidSegments, ID)
		}
	}

	if len(inValidSegments) > 0 {
		log.Warn("no match flushed segments to merge from", zap.Int64s("invalid segmentIDs", inValidSegments))
		return fmt.Errorf("invalid compactedFrom segments: %v", inValidSegments)
	}

	log.Info("merge flushed segments")
	c.segMu.Lock()
	for _, ID := range compactedFrom {
		// the existent of the segments are already checked
		s := c.segments[ID]
		s.compactedTo = seg.segmentID
		s.setType(datapb.SegmentType_Compacted)
		// release bloom filter
		s.currentStat = nil
		s.historyStats = nil
	}
	c.segMu.Unlock()

	// only store segments with numRows > 0
	if seg.numRows > 0 {
		seg.setType(datapb.SegmentType_Flushed)

		c.segMu.Lock()
		c.segments[seg.segmentID] = seg
		c.segMu.Unlock()
	}

	return nil
}

// for tests only
func (c *ChannelMeta) addFlushedSegmentWithPKs(segID, collID, partID UniqueID, numOfRows int64, ids storage.FieldData) error {
	if collID != c.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("input ID", collID),
			zap.Int64("expected ID", c.collectionID))
		return fmt.Errorf("mismatch collection, ID=%d", collID)
	}

	log.Info("Add Flushed segment",
		zap.Int64("segment ID", segID),
		zap.Int64("collection ID", collID),
		zap.Int64("partition ID", partID),
		zap.String("channel name", c.channelName),
	)

	seg := &Segment{
		collectionID: collID,
		partitionID:  partID,
		segmentID:    segID,
		numRows:      numOfRows,
	}

	seg.updatePKRange(ids)
	seg.setType(datapb.SegmentType_Flushed)

	c.segMu.Lock()
	c.segments[segID] = seg
	c.segMu.Unlock()

	return nil
}

func (c *ChannelMeta) listAllSegmentIDs() []UniqueID {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	var segIDs []UniqueID
	for _, seg := range c.segments {
		if seg.isValid() {
			segIDs = append(segIDs, seg.segmentID)
		}
	}
	return segIDs
}

func (c *ChannelMeta) listPartitionSegments(partID UniqueID) []UniqueID {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	var segIDs []UniqueID
	for _, seg := range c.segments {
		if seg.isValid() && seg.partitionID == partID {
			segIDs = append(segIDs, seg.segmentID)
		}
	}
	return segIDs
}

func (c *ChannelMeta) listNotFlushedSegmentIDs() []UniqueID {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	var segIDs []UniqueID
	for sID, seg := range c.segments {
		if seg.notFlushed() {
			segIDs = append(segIDs, sID)
		}
	}

	return segIDs
}
