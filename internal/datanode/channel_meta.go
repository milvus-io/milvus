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
	"errors"
	"fmt"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
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
	getChannelName() string

	listAllSegmentIDs() []UniqueID
	listNotFlushedSegmentIDs() []UniqueID
	addSegment(req addSegmentReq) error
	listPartitionSegments(partID UniqueID) []UniqueID
	filterSegments(partitionID UniqueID) []*Segment
	listNewSegmentsStartPositions() []*datapb.SegmentStartPosition
	transferNewSegments(segmentIDs []UniqueID)
	updateSegmentPKRange(segID UniqueID, ids storage.FieldData)
	mergeFlushedSegments(ctx context.Context, seg *Segment, planID UniqueID, compactedFrom []UniqueID) error
	getSegment(segID UniqueID) *Segment
	hasSegment(segID UniqueID, countFlushed bool) bool
	removeSegments(segID ...UniqueID)

	listCompactedSegmentIDs() map[UniqueID][]UniqueID
	listSegmentIDsToSync(ts Timestamp) []UniqueID
	setSegmentLastSyncTs(segID UniqueID, ts Timestamp)

	updateSegmentRowNumber(segID UniqueID, numRows int64)
	updateSegmentMemorySize(segID UniqueID, memorySize int64)
	InitPKstats(ctx context.Context, s *Segment, statsBinlogs []*datapb.FieldBinlog, ts Timestamp) error
	RollPKstats(segID UniqueID, stats []*storage.PrimaryKeyStats)
	getSegmentStatisticsUpdates(segID UniqueID) (*datapb.SegmentStats, error)
	segmentFlushed(segID UniqueID)

	getChannelCheckpoint(ttPos *internalpb.MsgPosition) *internalpb.MsgPosition

	getCurInsertBuffer(segmentID UniqueID) (*BufferData, bool)
	setCurInsertBuffer(segmentID UniqueID, buf *BufferData)
	rollInsertBuffer(segmentID UniqueID)
	evictHistoryInsertBuffer(segmentID UniqueID, endPos *internalpb.MsgPosition)

	getCurDeleteBuffer(segmentID UniqueID) (*DelDataBuf, bool)
	setCurDeleteBuffer(segmentID UniqueID, buf *DelDataBuf)
	rollDeleteBuffer(segmentID UniqueID)
	evictHistoryDeleteBuffer(segmentID UniqueID, endPos *internalpb.MsgPosition)

	// getTotalMemorySize returns the sum of memory sizes of segments.
	getTotalMemorySize() int64
	forceToSync()

	close()
}

// ChannelMeta contains channel meta and the latest segments infos of the channel.
type ChannelMeta struct {
	collectionID UniqueID
	channelName  string
	collSchema   *schemapb.CollectionSchema
	schemaMut    sync.RWMutex

	segMu    sync.RWMutex
	segments map[UniqueID]*Segment

	needToSync   *atomic.Bool
	syncPolicies []segmentSyncPolicy

	metaService  *metaService
	chunkManager storage.ChunkManager
	workerPool   *concurrency.Pool

	closed *atomic.Bool
}

var _ Channel = &ChannelMeta{}

func newChannel(channelName string, collID UniqueID, schema *schemapb.CollectionSchema, rc types.RootCoord, cm storage.ChunkManager) *ChannelMeta {
	metaService := newMetaService(rc, collID)

	channel := ChannelMeta{
		collectionID: collID,
		collSchema:   schema,
		channelName:  channelName,

		segments: make(map[UniqueID]*Segment),

		needToSync: atomic.NewBool(false),
		syncPolicies: []segmentSyncPolicy{
			syncPeriodically(),
			syncMemoryTooHigh(),
			syncCPLagTooBehind(),
		},

		metaService:  metaService,
		chunkManager: cm,
		workerPool:   getOrCreateStatsPool(),
		closed:       atomic.NewBool(false),
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
}

// new2NormalSegment transfers a segment from *New* to *Normal*.
// make sure the segID is in the channel before call this func
func (c *ChannelMeta) new2NormalSegment(segID UniqueID) {
	if seg, ok := c.segments[segID]; ok && seg.getType() == datapb.SegmentType_New {
		seg.setType(datapb.SegmentType_Normal)
	}
}

func (c *ChannelMeta) getCollectionAndPartitionID(segID UniqueID) (collID, partitionID UniqueID, err error) {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	if seg, ok := c.segments[segID]; ok && seg.isValid() {
		return seg.collectionID, seg.partitionID, nil
	}
	return 0, 0, merr.WrapErrSegmentNotFound(segID)
}

func (c *ChannelMeta) getChannelName() string {
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
		log.Warn("failed to addSegment, collection mismatch",
			zap.Int64("current collection ID", req.collID),
			zap.Int64("expected collection ID", c.collectionID))
		return fmt.Errorf("failed to addSegment, mismatch collection, ID=%d", req.collID)
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
		collectionID:     req.collID,
		partitionID:      req.partitionID,
		segmentID:        req.segID,
		numRows:          req.numOfRows, // 0 if segType == NEW
		historyInsertBuf: make([]*BufferData, 0),
		historyDeleteBuf: make([]*DelDataBuf, 0),
		startPos:         req.startPos,
		lastSyncTs:       tsoutil.GetCurrentTime(),
	}
	seg.setType(req.segType)
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
	return nil
}

func (c *ChannelMeta) getSegment(segID UniqueID) *Segment {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	seg, ok := c.segments[segID]
	if !ok {
		return nil
	}
	return seg
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

func (c *ChannelMeta) listSegmentIDsToSync(ts Timestamp) []UniqueID {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	validSegs := make([]*Segment, 0)
	for _, seg := range c.segments {
		// flushed segments also need to update cp
		if !seg.isValid() {
			continue
		}
		validSegs = append(validSegs, seg)
	}

	segIDsToSync := typeutil.NewUniqueSet()
	for _, policy := range c.syncPolicies {
		segments := policy(validSegs, ts, c.needToSync)
		for _, segID := range segments {
			segIDsToSync.Insert(segID)
		}
	}
	return segIDsToSync.Collect()
}

func (c *ChannelMeta) setSegmentLastSyncTs(segID UniqueID, ts Timestamp) {
	c.segMu.Lock()
	defer c.segMu.Unlock()
	if _, ok := c.segments[segID]; ok {
		c.segments[segID].lastSyncTs = ts
	}
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
	if Params.DataNodeCfg.SkipBFStatsLoad {
		// mark segment lazy loading
		s.setLoadingLazy(true)
		c.submitLoadStatsTask(s, statsBinlogs, ts)
		return nil
	}
	return c.initPKstats(ctx, s, statsBinlogs, ts)
}

func (c *ChannelMeta) submitLoadStatsTask(s *Segment, statsBinlogs []*datapb.FieldBinlog, ts Timestamp) {
	log := log.Ctx(context.TODO()).With(
		zap.Int64("segmentID", s.segmentID),
		zap.Int64("collectionID", s.collectionID),
	)
	if c.closed.Load() {
		// stop retry and resubmit if channel meta closed
		return
	}
	// do submitting in a goroutine in case of task pool is full
	go func() {
		c.workerPool.Submit(func() (any, error) {
			stats, err := c.loadStats(context.Background(), s.segmentID, s.collectionID, statsBinlogs, ts)
			if err != nil {
				// TODO if not retryable, add rebuild statslog logic
				log.Warn("failed to lazy load statslog for segment", zap.Int64("segment", s.segmentID), zap.Error(err))
				if c.retryableLoadError(err) {
					time.Sleep(100 * time.Millisecond)
					log.Warn("failed to lazy load statslog for segment, retrying...", zap.Int64("segment", s.segmentID), zap.Error(err))
					c.submitLoadStatsTask(s, statsBinlogs, ts)
				}
				return nil, err
			}
			// get segment lock here
			// it's ok that segment is dropped here
			c.segMu.Lock()
			defer c.segMu.Unlock()
			s.historyStats = append(s.historyStats, stats...)
			s.setLoadingLazy(false)

			log.Info("lazy loading segment statslog complete")

			return nil, nil
		})
	}()
}

func (c *ChannelMeta) retryableLoadError(err error) bool {
	switch {
	case errors.Is(err, errBinlogCorrupted):
		return false
	case errors.Is(err, storage.ErrNoSuchKey):
		return false
	default:
		return true
	}
}

func (c *ChannelMeta) loadStats(ctx context.Context, segmentID int64, collectionID int64, statsBinlogs []*datapb.FieldBinlog, ts Timestamp) ([]*storage.PkStatistics, error) {
	startTs := time.Now()
	log := log.With(zap.Int64("segmentID", segmentID))
	log.Info("begin to init pk bloom filter", zap.Int("statsBinLogsLen", len(statsBinlogs)))
	schema, err := c.getCollectionSchema(collectionID, ts)
	if err != nil {
		log.Warn("failed to initPKBloomFilter, get schema return error", zap.Error(err))
		return nil, err
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
	bloomFilterFiles := filterPKStatsBinlogs(pkField, statsBinlogs)
	// no stats log to parse, initialize a new BF
	if len(bloomFilterFiles) == 0 {
		log.Warn("no stats files to load")
		return nil, nil
	}

	// read historical PK filter
	values, err := c.chunkManager.MultiRead(ctx, bloomFilterFiles)
	if err != nil {
		log.Warn("failed to load bloom filter files", zap.Error(err))
		return nil, err
	}
	blobs := make([]*Blob, 0)
	for i := 0; i < len(values); i++ {
		blobs = append(blobs, &Blob{Value: values[i]})
	}

	stats, err := storage.DeserializeStats(blobs)
	if err != nil {
		log.Warn("failed to deserialize bloom filter files", zap.Error(err))
		return nil, WrapBinlogCorruptedErr(strings.Join(bloomFilterFiles, ","))
	}
	var size uint
	result := make([]*storage.PkStatistics, 0, len(stats))
	for _, stat := range stats {
		pkStat := &storage.PkStatistics{
			PkFilter: stat.BF,
			MinPK:    stat.MinPk,
			MaxPK:    stat.MaxPk,
		}
		size += stat.BF.Cap()
		result = append(result, pkStat)
	}

	log.Info("Successfully load pk stats", zap.Any("time", time.Since(startTs)), zap.Uint("size", size))
	return result, nil
}

func (c *ChannelMeta) initPKstats(ctx context.Context, s *Segment, statsBinlogs []*datapb.FieldBinlog, ts Timestamp) error {
	stats, err := c.loadStats(ctx, s.segmentID, s.collectionID, statsBinlogs, ts)
	if err != nil {
		return err
	}
	s.historyStats = stats

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
//
//	transfer segments states from *New* to *Normal*.
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
		if seg, ok := c.segments[segID]; ok {
			if seg.notFlushed() {
				cnt++
			}
			// free memory
			seg.curInsertBuf = nil
			seg.curDeleteBuf = nil
			seg.historyInsertBuf = nil
			seg.historyDeleteBuf = nil
		}

		delete(c.segments, segID)
	}
}

// hasSegment checks whether this channel has a segment according to segment ID.
func (c *ChannelMeta) hasSegment(segID UniqueID, countFlushed bool) bool {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	return c.hasSegmentInternal(segID, countFlushed)
}

func (c *ChannelMeta) hasSegmentInternal(segID UniqueID, countFlushed bool) bool {
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
func (c *ChannelMeta) updateSegmentRowNumber(segID UniqueID, numRows int64) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	log.Info("updating segment num row", zap.Int64("Segment ID", segID), zap.Int64("numRows", numRows))
	seg, ok := c.segments[segID]
	if ok && seg.notFlushed() {
		seg.numRows += numRows
		return
	}

	log.Warn("update segment num row not exist", zap.Int64("segID", segID))
}

// updateStatistics updates the number of rows of a segment in channel.
func (c *ChannelMeta) updateSegmentMemorySize(segID UniqueID, memorySize int64) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	log.Info("updating segment memorySize", zap.Int64("Segment ID", segID), zap.Int64("memorySize", memorySize))
	seg, ok := c.segments[segID]
	if ok && seg.notFlushed() {
		seg.memorySize = memorySize
		return
	}

	log.Warn("update segment memorySize not exist", zap.Int64("segID", segID))
}

// getSegmentStatisticsUpdates gives current segment's statistics updates.
func (c *ChannelMeta) getSegmentStatisticsUpdates(segID UniqueID) (*datapb.SegmentStats, error) {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	if seg, ok := c.segments[segID]; ok && seg.isValid() {
		return &datapb.SegmentStats{SegmentID: segID, NumRows: seg.numRows}, nil
	}

	return nil, merr.WrapErrSegmentNotFound(segID)
}

func (c *ChannelMeta) getCollectionID() UniqueID {
	return c.collectionID
}

// getCollectionSchema gets collection schema from rootcoord for a certain timestamp.
//
//	If you want the latest collection schema, ts should be 0.
func (c *ChannelMeta) getCollectionSchema(collID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error) {
	if collID != c.collectionID {
		log.Warn("failed to getCollectionSchema, collection mismatch",
			zap.Int64("current collection ID", collID),
			zap.Int64("expected collection ID", c.collectionID))
		return nil, fmt.Errorf("failed to getCollectionSchema, mismatch collection, want %d, actual %d", c.collectionID, collID)
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

func (c *ChannelMeta) mergeFlushedSegments(ctx context.Context, seg *Segment, planID UniqueID, compactedFrom []UniqueID) error {
	log := log.Ctx(ctx).With(
		zap.Int64("segment ID", seg.segmentID),
		zap.Int64("collection ID", seg.collectionID),
		zap.Int64("partition ID", seg.partitionID),
		zap.Int64s("compacted from", compactedFrom),
		zap.Int64("planID", planID),
		zap.String("channel name", c.channelName))

	c.segMu.Lock()
	defer c.segMu.Unlock()
	var inValidSegments []UniqueID
	for _, segID := range compactedFrom {
		seg, ok := c.segments[segID]
		if !ok {
			inValidSegments = append(inValidSegments, segID)
			continue
		}

		// compacted
		if !seg.isValid() {
			inValidSegments = append(inValidSegments, segID)
			continue
		}

		if seg.notFlushed() {
			log.Warn("segment is not flushed, skip mergeFlushedSegments",
				zap.Int64("segmentID", segID),
				zap.String("segType", seg.getType().String()),
			)
			return merr.WrapErrSegmentNotFound(segID, "segment in flush state not found")
		}

	}

	if len(inValidSegments) > 0 {
		log.Warn("no match flushed segments to merge from", zap.Int64s("invalid segmentIDs", inValidSegments))
		compactedFrom = lo.Without(compactedFrom, inValidSegments...)
	}

	log.Info("merge flushed segments")

	for _, ID := range compactedFrom {
		// the existent of the segments are already checked
		s := c.segments[ID]
		s.compactedTo = seg.segmentID
		s.setType(datapb.SegmentType_Compacted)
		// release bloom filter
		s.currentStat = nil
		s.historyStats = nil

		// set correct lastSyncTs for 10-mins channelCP force sync.
		if s.lastSyncTs < seg.lastSyncTs {
			seg.lastSyncTs = s.lastSyncTs
		}
	}

	// only store segments with numRows > 0
	if seg.numRows > 0 {
		seg.setType(datapb.SegmentType_Flushed)
		c.segments[seg.segmentID] = seg
	}

	return nil
}

// for tests only
func (c *ChannelMeta) addFlushedSegmentWithPKs(segID, collID, partID UniqueID, numOfRows int64, ids storage.FieldData) error {
	if collID != c.collectionID {
		log.Warn("failed to addFlushedSegmentWithPKs, collection mismatch",
			zap.Int64("current collection ID", collID),
			zap.Int64("expected collection ID", c.collectionID))
		return fmt.Errorf("failed to addFlushedSegmentWithPKs, mismatch collection, ID=%d", collID)
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

func (c *ChannelMeta) getChannelCheckpoint(ttPos *internalpb.MsgPosition) *internalpb.MsgPosition {
	c.segMu.RLock()
	defer c.segMu.RUnlock()
	channelCP := &internalpb.MsgPosition{Timestamp: math.MaxUint64}
	// 1. find the earliest startPos in current buffer and history buffer
	for _, seg := range c.segments {
		if seg.curInsertBuf != nil && seg.curInsertBuf.startPos != nil && seg.curInsertBuf.startPos.Timestamp < channelCP.Timestamp {
			channelCP = seg.curInsertBuf.startPos
		}
		if seg.curDeleteBuf != nil && seg.curDeleteBuf.startPos != nil && seg.curDeleteBuf.startPos.Timestamp < channelCP.Timestamp {
			channelCP = seg.curDeleteBuf.startPos
		}
		for _, ib := range seg.historyInsertBuf {
			if ib != nil && ib.startPos != nil && ib.startPos.Timestamp < channelCP.Timestamp {
				channelCP = ib.startPos
			}
		}
		for _, db := range seg.historyDeleteBuf {
			if db != nil && db.startPos != nil && db.startPos.Timestamp < channelCP.Timestamp {
				channelCP = db.startPos
			}
		}
		// TODO: maybe too many logs would print
		log.Debug("getChannelCheckpoint for segment", zap.Int64("segmentID", seg.segmentID),
			zap.Bool("isCurIBEmpty", seg.curInsertBuf == nil),
			zap.Bool("isCurDBEmpty", seg.curDeleteBuf == nil),
			zap.Int("len(hisIB)", len(seg.historyInsertBuf)),
			zap.Int("len(hisDB)", len(seg.historyDeleteBuf)),
			zap.Any("newChannelCpTs", channelCP.GetTimestamp()))
	}
	// 2. if no data in buffer, use the current tt as channelCP
	if channelCP.MsgID == nil {
		channelCP = ttPos
	}
	return channelCP
}

func (c *ChannelMeta) getCurInsertBuffer(segmentID UniqueID) (*BufferData, bool) {
	c.segMu.RLock()
	defer c.segMu.RUnlock()
	seg, ok := c.segments[segmentID]
	if ok {
		return seg.curInsertBuf, seg.curInsertBuf != nil
	}
	return nil, false
}

func (c *ChannelMeta) setCurInsertBuffer(segmentID UniqueID, buf *BufferData) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	seg, ok := c.segments[segmentID]
	if ok {
		seg.setInsertBuffer(buf)
		return
	}
	log.Warn("cannot find segment when setCurInsertBuffer", zap.Int64("segmentID", segmentID))
}

func (c *ChannelMeta) rollInsertBuffer(segmentID UniqueID) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	seg, ok := c.segments[segmentID]
	if ok {
		seg.rollInsertBuffer()
		return
	}
	log.Warn("cannot find segment when rollInsertBuffer", zap.Int64("segmentID", segmentID))
}

func (c *ChannelMeta) evictHistoryInsertBuffer(segmentID UniqueID, endPos *internalpb.MsgPosition) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	seg, ok := c.segments[segmentID]
	if ok {
		seg.evictHistoryInsertBuffer(endPos)
		return
	}
	log.Warn("cannot find segment when evictHistoryInsertBuffer", zap.Int64("segmentID", segmentID))
}

func (c *ChannelMeta) getCurDeleteBuffer(segmentID UniqueID) (*DelDataBuf, bool) {
	c.segMu.RLock()
	defer c.segMu.RUnlock()

	seg, ok := c.segments[segmentID]
	if ok {
		return seg.curDeleteBuf, seg.curDeleteBuf != nil
	}
	return nil, false
}

func (c *ChannelMeta) setCurDeleteBuffer(segmentID UniqueID, buf *DelDataBuf) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	seg, ok := c.segments[segmentID]
	if ok {
		seg.curDeleteBuf = buf
		return
	}
	log.Warn("cannot find segment when setCurDeleteBuffer", zap.Int64("segmentID", segmentID))
}

func (c *ChannelMeta) rollDeleteBuffer(segmentID UniqueID) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	seg, ok := c.segments[segmentID]
	if ok {
		seg.rollDeleteBuffer()
		return
	}
	log.Warn("cannot find segment when rollDeleteBuffer", zap.Int64("segmentID", segmentID))
}

func (c *ChannelMeta) evictHistoryDeleteBuffer(segmentID UniqueID, endPos *internalpb.MsgPosition) {
	c.segMu.Lock()
	defer c.segMu.Unlock()

	seg, ok := c.segments[segmentID]
	if ok {
		seg.evictHistoryDeleteBuffer(endPos)
		return
	}
	log.Warn("cannot find segment when evictHistoryDeleteBuffer", zap.Int64("segmentID", segmentID))
}

func (c *ChannelMeta) forceToSync() {
	c.needToSync.Store(true)
}

func (c *ChannelMeta) getTotalMemorySize() int64 {
	c.segMu.RLock()
	defer c.segMu.RUnlock()
	var res int64
	for _, segment := range c.segments {
		res += segment.memorySize
	}
	return res
}

func (c *ChannelMeta) close() {
	c.closed.Store(true)
}

func filterPKStatsBinlogs(pkField int64, statsBinlogs []*datapb.FieldBinlog) []string {
	bloomFilterFiles := []string{}
	for _, binlog := range statsBinlogs {
		if binlog.FieldID != pkField {
			continue
		}
		for _, binlog := range binlog.GetBinlogs() {
			_, logID := path.Split(binlog.GetLogPath())
			// ignore CompoundStatsLog for upward compatibility
			if logID != fmt.Sprint(storage.CompoundStatsType.GetLogID()) {
				bloomFilterFiles = append(bloomFilterFiles, binlog.GetLogPath())
			}
		}
	}
	return bloomFilterFiles
}
