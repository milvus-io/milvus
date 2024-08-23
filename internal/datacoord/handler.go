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

package datacoord

import (
	"context"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Handler handles some channel method for ChannelManager
type Handler interface {
	// GetQueryVChanPositions gets the information recovery needed of a channel for QueryCoord
	GetQueryVChanPositions(ch RWChannel, partitionIDs ...UniqueID) *datapb.VchannelInfo
	// GetDataVChanPositions gets the information recovery needed of a channel for DataNode
	GetDataVChanPositions(ch RWChannel, partitionID UniqueID) *datapb.VchannelInfo
	CheckShouldDropChannel(ch string) bool
	FinishDropChannel(ch string, collectionID int64) error
	GetCollection(ctx context.Context, collectionID UniqueID) (*collectionInfo, error)
}

// ServerHandler is a helper of Server
type ServerHandler struct {
	s *Server
}

// newServerHandler creates a new ServerHandler
func newServerHandler(s *Server) *ServerHandler {
	return &ServerHandler{s: s}
}

// GetDataVChanPositions gets vchannel latest positions with provided dml channel names for DataNode.
func (h *ServerHandler) GetDataVChanPositions(channel RWChannel, partitionID UniqueID) *datapb.VchannelInfo {
	segments := h.s.meta.GetRealSegmentsForChannel(channel.GetName())
	log.Info("GetDataVChanPositions",
		zap.Int64("collectionID", channel.GetCollectionID()),
		zap.String("channel", channel.GetName()),
		zap.Int("numOfSegments", len(segments)),
	)
	var (
		flushedIDs   = make(typeutil.UniqueSet)
		unflushedIDs = make(typeutil.UniqueSet)
		droppedIDs   = make(typeutil.UniqueSet)
	)
	for _, s := range segments {
		if (partitionID > allPartitionID && s.PartitionID != partitionID) ||
			(s.GetStartPosition() == nil && s.GetDmlPosition() == nil) {
			continue
		}
		if s.GetIsImporting() {
			// Skip bulk insert segments.
			continue
		}

		if s.GetState() == commonpb.SegmentState_Dropped {
			droppedIDs.Insert(s.GetID())
			continue
		} else if s.GetState() == commonpb.SegmentState_Flushing || s.GetState() == commonpb.SegmentState_Flushed {
			flushedIDs.Insert(s.GetID())
		} else {
			unflushedIDs.Insert(s.GetID())
		}
	}

	return &datapb.VchannelInfo{
		CollectionID:        channel.GetCollectionID(),
		ChannelName:         channel.GetName(),
		SeekPosition:        h.GetChannelSeekPosition(channel, partitionID),
		FlushedSegmentIds:   flushedIDs.Collect(),
		UnflushedSegmentIds: unflushedIDs.Collect(),
		DroppedSegmentIds:   droppedIDs.Collect(),
	}
}

// GetQueryVChanPositions gets vchannel latest positions with provided dml channel names for QueryCoord,
// we expect QueryCoord gets the indexed segments to load, so the flushed segments below are actually the indexed segments,
// the unflushed segments are actually the segments without index, even they are flushed.
func (h *ServerHandler) GetQueryVChanPositions(channel RWChannel, partitionIDs ...UniqueID) *datapb.VchannelInfo {
	// cannot use GetSegmentsByChannel since dropped segments are needed here
	validPartitions := lo.Filter(partitionIDs, func(partitionID int64, _ int) bool { return partitionID > allPartitionID })
	if len(validPartitions) <= 0 {
		collInfo, err := h.s.handler.GetCollection(h.s.ctx, channel.GetCollectionID())
		if err != nil || collInfo == nil {
			log.Warn("collectionInfo is nil")
			return nil
		}
		validPartitions = collInfo.Partitions
	}
	partStatsVersionsMap := make(map[int64]int64)
	var (
		indexedIDs   = make(typeutil.UniqueSet)
		droppedIDs   = make(typeutil.UniqueSet)
		growingIDs   = make(typeutil.UniqueSet)
		levelZeroIDs = make(typeutil.UniqueSet)
	)

	segments := h.s.meta.GetRealSegmentsForChannel(channel.GetName())

	segmentInfos := make(map[int64]*SegmentInfo)
	indexedSegments := FilterInIndexedSegments(h, h.s.meta, segments...)
	indexed := make(typeutil.UniqueSet)
	for _, segment := range indexedSegments {
		indexed.Insert(segment.GetID())
	}

	unIndexedIDs := make(typeutil.UniqueSet)

	for _, s := range segments {
		if s.GetStartPosition() == nil && s.GetDmlPosition() == nil {
			continue
		}
		if s.GetIsImporting() {
			// Skip bulk insert segments.
			continue
		}

		currentPartitionStatsVersion := h.s.meta.partitionStatsMeta.GetCurrentPartitionStatsVersion(channel.GetCollectionID(), s.GetPartitionID(), channel.GetName())
		if s.GetLevel() == datapb.SegmentLevel_L2 && s.GetPartitionStatsVersion() != currentPartitionStatsVersion {
			// in the process of L2 compaction, newly generated segment may be visible before the whole L2 compaction Plan
			// is finished, we have to skip these fast-finished segment because all segments in one L2 Batch must be
			// seen atomically, otherwise users will see intermediate result
			continue
		}

		segmentInfos[s.GetID()] = s
		switch {
		case s.GetState() == commonpb.SegmentState_Dropped:
			if s.GetLevel() == datapb.SegmentLevel_L2 && s.GetPartitionStatsVersion() == currentPartitionStatsVersion {
				// if segment.partStatsVersion is equal to currentPartitionStatsVersion,
				// it must have been indexed, this is guaranteed by clustering compaction process
				// this is to ensure that the current valid L2 compaction produce is available to search/query
				// to avoid insufficient data
				indexedIDs.Insert(s.GetID())
				continue
			}
			droppedIDs.Insert(s.GetID())
		case !isFlushState(s.GetState()):
			growingIDs.Insert(s.GetID())
		case s.GetLevel() == datapb.SegmentLevel_L0:
			levelZeroIDs.Insert(s.GetID())
		case indexed.Contain(s.GetID()):
			indexedIDs.Insert(s.GetID())
		case s.GetNumOfRows() < Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64(): // treat small flushed segment as indexed
			indexedIDs.Insert(s.GetID())
		default:
			unIndexedIDs.Insert(s.GetID())
		}
	}

	// ================================================
	// Segments blood relationship:
	//          a   b
	//           \ /
	//            c   d
	//             \ /
	//              e
	//
	// GC:        a, b
	// Indexed:   c, d, e
	//              ||
	//              || (Index dropped and creating new index and not finished)
	//              \/
	// UnIndexed: c, d, e
	//
	// Retrieve unIndexed expected result:
	// unIndexed: c, d
	// ================================================
	isValid := func(ids ...UniqueID) bool {
		for _, id := range ids {
			if seg, ok := segmentInfos[id]; !ok || seg == nil {
				return false
			}
		}
		return true
	}
	retrieveUnIndexed := func() bool {
		continueRetrieve := false
		for id := range unIndexedIDs {
			compactionFrom := segmentInfos[id].GetCompactionFrom()
			if len(compactionFrom) > 0 && isValid(compactionFrom...) {
				for _, fromID := range compactionFrom {
					if indexed.Contain(fromID) {
						indexedIDs.Insert(fromID)
					} else {
						unIndexedIDs.Insert(fromID)
						continueRetrieve = true
					}
				}
				unIndexedIDs.Remove(id)
				droppedIDs.Remove(compactionFrom...)
			}
		}
		return continueRetrieve
	}
	for retrieveUnIndexed() {
	}

	// unindexed is flushed segments as well
	indexedIDs.Insert(unIndexedIDs.Collect()...)

	for _, partitionID := range validPartitions {
		currentPartitionStatsVersion := h.s.meta.partitionStatsMeta.GetCurrentPartitionStatsVersion(channel.GetCollectionID(), partitionID, channel.GetName())
		partStatsVersionsMap[partitionID] = currentPartitionStatsVersion
	}

	log.Info("GetQueryVChanPositions",
		zap.Int64("collectionID", channel.GetCollectionID()),
		zap.String("channel", channel.GetName()),
		zap.Int("numOfSegments", len(segments)),
		zap.Int("indexed segment", len(indexedSegments)),
		zap.Int("result indexed", len(indexedIDs)),
		zap.Int("result unIndexed", len(unIndexedIDs)),
		zap.Int("result growing", len(growingIDs)),
		zap.Any("partition stats", partStatsVersionsMap),
	)

	return &datapb.VchannelInfo{
		CollectionID:           channel.GetCollectionID(),
		ChannelName:            channel.GetName(),
		SeekPosition:           h.GetChannelSeekPosition(channel, partitionIDs...),
		FlushedSegmentIds:      indexedIDs.Collect(),
		UnflushedSegmentIds:    growingIDs.Collect(),
		DroppedSegmentIds:      droppedIDs.Collect(),
		LevelZeroSegmentIds:    levelZeroIDs.Collect(),
		PartitionStatsVersions: partStatsVersionsMap,
	}
}

// getEarliestSegmentDMLPos returns the earliest dml position of segments,
// this is mainly for COMPATIBILITY with old version <=2.1.x
func (h *ServerHandler) getEarliestSegmentDMLPos(channel string, partitionIDs ...UniqueID) *msgpb.MsgPosition {
	var minPos *msgpb.MsgPosition
	var minPosSegID int64
	var minPosTs uint64
	segments := h.s.meta.SelectSegments(WithChannel(channel))

	validPartitions := lo.Filter(partitionIDs, func(partitionID int64, _ int) bool { return partitionID > allPartitionID })
	partitionSet := typeutil.NewUniqueSet(validPartitions...)
	for _, s := range segments {
		if (partitionSet.Len() > 0 && !partitionSet.Contain(s.PartitionID)) ||
			(s.GetStartPosition() == nil && s.GetDmlPosition() == nil) {
			continue
		}
		if s.GetIsImporting() {
			// Skip bulk insert segments.
			continue
		}
		if s.GetState() == commonpb.SegmentState_Dropped {
			continue
		}

		var segmentPosition *msgpb.MsgPosition
		if s.GetDmlPosition() != nil {
			segmentPosition = s.GetDmlPosition()
		} else {
			segmentPosition = s.GetStartPosition()
		}
		if minPos == nil || segmentPosition.Timestamp < minPos.Timestamp {
			minPosSegID = s.GetID()
			minPosTs = segmentPosition.GetTimestamp()
			minPos = segmentPosition
		}
	}
	if minPos != nil {
		log.Info("getEarliestSegmentDMLPos done",
			zap.Int64("segmentID", minPosSegID),
			zap.Uint64("posTs", minPosTs),
			zap.Time("posTime", tsoutil.PhysicalTime(minPosTs)))
	}
	return minPos
}

// getCollectionStartPos returns collection start position.
func (h *ServerHandler) getCollectionStartPos(channel RWChannel) *msgpb.MsgPosition {
	log := log.With(zap.String("channel", channel.GetName()))
	// use collection start position when segment position is not found
	var startPosition *msgpb.MsgPosition
	if channel.GetStartPositions() == nil {
		collection, err := h.GetCollection(h.s.ctx, channel.GetCollectionID())
		if collection != nil && err == nil {
			startPosition = getCollectionStartPosition(channel.GetName(), collection)
		}
		log.Info("NEITHER segment position or channel start position are found, setting channel seek position to collection start position",
			zap.Uint64("posTs", startPosition.GetTimestamp()),
			zap.Time("posTime", tsoutil.PhysicalTime(startPosition.GetTimestamp())),
		)
	} else {
		// use passed start positions, skip to ask RootCoord.
		startPosition = toMsgPosition(channel.GetName(), channel.GetStartPositions())
		if startPosition != nil {
			startPosition.Timestamp = channel.GetCreateTimestamp()
		}
		log.Info("segment position not found, setting channel seek position to channel start position",
			zap.Uint64("posTs", startPosition.GetTimestamp()),
			zap.Time("posTime", tsoutil.PhysicalTime(startPosition.GetTimestamp())),
		)
	}
	return startPosition
}

// GetChannelSeekPosition gets channel seek position from:
//  1. Channel checkpoint meta;
//  2. Segments earliest dml position;
//  3. Collection start position;
//     And would return if any position is valid.
func (h *ServerHandler) GetChannelSeekPosition(channel RWChannel, partitionIDs ...UniqueID) *msgpb.MsgPosition {
	log := log.With(zap.String("channel", channel.GetName()))
	var seekPosition *msgpb.MsgPosition
	seekPosition = h.s.meta.GetChannelCheckpoint(channel.GetName())
	if seekPosition != nil {
		log.Info("channel seek position set from channel checkpoint meta",
			zap.Uint64("posTs", seekPosition.Timestamp),
			zap.Time("posTime", tsoutil.PhysicalTime(seekPosition.GetTimestamp())))
		return seekPosition
	}

	seekPosition = h.getEarliestSegmentDMLPos(channel.GetName(), partitionIDs...)
	if seekPosition != nil {
		log.Info("channel seek position set from earliest segment dml position",
			zap.Uint64("posTs", seekPosition.Timestamp),
			zap.Time("posTime", tsoutil.PhysicalTime(seekPosition.GetTimestamp())))
		return seekPosition
	}

	seekPosition = h.getCollectionStartPos(channel)
	if seekPosition != nil {
		log.Info("channel seek position set from collection start position",
			zap.Uint64("posTs", seekPosition.Timestamp),
			zap.Time("posTime", tsoutil.PhysicalTime(seekPosition.GetTimestamp())))
		return seekPosition
	}

	log.Warn("get channel checkpoint failed, channelCPMeta and earliestSegDMLPos and collStartPos are all invalid")
	return nil
}

func getCollectionStartPosition(channel string, collectionInfo *collectionInfo) *msgpb.MsgPosition {
	position := toMsgPosition(channel, collectionInfo.StartPositions)
	if position != nil {
		position.Timestamp = collectionInfo.CreatedAt
	}
	return position
}

func toMsgPosition(channel string, startPositions []*commonpb.KeyDataPair) *msgpb.MsgPosition {
	for _, sp := range startPositions {
		if sp.GetKey() != funcutil.ToPhysicalChannel(channel) {
			continue
		}
		return &msgpb.MsgPosition{
			ChannelName: channel,
			MsgID:       sp.GetData(),
		}
	}
	return nil
}

// trimSegmentInfo returns a shallow copy of datapb.SegmentInfo and sets ALL binlog info to nil
func trimSegmentInfo(info *datapb.SegmentInfo) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:             info.ID,
		CollectionID:   info.CollectionID,
		PartitionID:    info.PartitionID,
		InsertChannel:  info.InsertChannel,
		NumOfRows:      info.NumOfRows,
		State:          info.State,
		MaxRowNum:      info.MaxRowNum,
		LastExpireTime: info.LastExpireTime,
		StartPosition:  info.StartPosition,
		DmlPosition:    info.DmlPosition,
	}
}

// HasCollection returns whether the collection exist from user's perspective.
func (h *ServerHandler) HasCollection(ctx context.Context, collectionID UniqueID) (bool, error) {
	var hasCollection bool
	ctx2, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if err := retry.Do(ctx2, func() error {
		has, err := h.s.broker.HasCollection(ctx2, collectionID)
		if err != nil {
			log.RatedInfo(60, "datacoord ServerHandler HasCollection retry failed", zap.Error(err))
			return err
		}
		hasCollection = has
		return nil
	}, retry.Attempts(5)); err != nil {
		log.Ctx(ctx2).Error("datacoord ServerHandler HasCollection finally failed",
			zap.Int64("collectionID", collectionID),
			zap.Error(err))
		// A workaround for https://github.com/milvus-io/milvus/issues/26863. The collection may be considered as not
		// dropped when any exception happened, but there are chances that finally the collection will be cleaned.
		return true, nil
	}
	return hasCollection, nil
}

// GetCollection returns collection info with specified collection id
func (h *ServerHandler) GetCollection(ctx context.Context, collectionID UniqueID) (*collectionInfo, error) {
	coll := h.s.meta.GetCollection(collectionID)
	if coll != nil {
		return coll, nil
	}
	ctx2, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if err := retry.Do(ctx2, func() error {
		err := h.s.loadCollectionFromRootCoord(ctx2, collectionID)
		if err != nil {
			log.Warn("failed to load collection from rootcoord", zap.Int64("collectionID", collectionID), zap.Error(err))
			return err
		}
		return nil
	}, retry.Attempts(5)); err != nil {
		log.Ctx(ctx2).Warn("datacoord ServerHandler GetCollection finally failed",
			zap.Int64("collectionID", collectionID),
			zap.Error(err))
		return nil, err
	}

	return h.s.meta.GetCollection(collectionID), nil
}

// CheckShouldDropChannel returns whether specified channel is marked to be removed
func (h *ServerHandler) CheckShouldDropChannel(channel string) bool {
	return h.s.meta.catalog.ShouldDropChannel(h.s.ctx, channel)
}

// FinishDropChannel cleans up the remove flag for channels
// this function is a wrapper of server.meta.FinishDropChannel
func (h *ServerHandler) FinishDropChannel(channel string, collectionID int64) error {
	err := h.s.meta.catalog.DropChannel(h.s.ctx, channel)
	if err != nil {
		log.Warn("DropChannel failed", zap.String("vChannel", channel), zap.Error(err))
		return err
	}
	err = h.s.meta.DropChannelCheckpoint(channel)
	if err != nil {
		log.Warn("DropChannel failed to drop channel checkpoint", zap.String("channel", channel), zap.Error(err))
		return err
	}
	log.Info("DropChannel succeeded", zap.String("channel", channel))
	// Channel checkpoints are cleaned up during garbage collection.

	// clean collection info cache when meet drop collection info
	h.s.meta.DropCollection(collectionID)

	return nil
}
