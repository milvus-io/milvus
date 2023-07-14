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

	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Handler handles some channel method for ChannelManager
type Handler interface {
	// GetQueryVChanPositions gets the information recovery needed of a channel for QueryCoord
	GetQueryVChanPositions(ch *channel, partitionIDs ...UniqueID) *datapb.VchannelInfo
	// GetDataVChanPositions gets the information recovery needed of a channel for DataNode
	GetDataVChanPositions(ch *channel, partitionID UniqueID) *datapb.VchannelInfo
	CheckShouldDropChannel(channel string, collectionID UniqueID) bool
	FinishDropChannel(channel string) error
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
func (h *ServerHandler) GetDataVChanPositions(channel *channel, partitionID UniqueID) *datapb.VchannelInfo {
	segments := h.s.meta.SelectSegments(func(s *SegmentInfo) bool {
		return s.InsertChannel == channel.Name && !s.GetIsFake()
	})
	log.Info("GetDataVChanPositions",
		zap.Int64("collectionID", channel.CollectionID),
		zap.String("channel", channel.Name),
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
		CollectionID:        channel.CollectionID,
		ChannelName:         channel.Name,
		SeekPosition:        h.GetChannelSeekPosition(channel, partitionID),
		FlushedSegmentIds:   flushedIDs.Collect(),
		UnflushedSegmentIds: unflushedIDs.Collect(),
		DroppedSegmentIds:   droppedIDs.Collect(),
	}
}

// GetQueryVChanPositions gets vchannel latest positions with provided dml channel names for QueryCoord,
// we expect QueryCoord gets the indexed segments to load, so the flushed segments below are actually the indexed segments,
// the unflushed segments are actually the segments without index, even they are flushed.
func (h *ServerHandler) GetQueryVChanPositions(channel *channel, partitionIDs ...UniqueID) *datapb.VchannelInfo {
	// cannot use GetSegmentsByChannel since dropped segments are needed here
	segments := h.s.meta.SelectSegments(func(s *SegmentInfo) bool {
		return s.InsertChannel == channel.Name && !s.GetIsFake()
	})
	segmentInfos := make(map[int64]*SegmentInfo)
	indexedSegments := FilterInIndexedSegments(h, h.s.meta, segments...)
	indexed := make(typeutil.UniqueSet)
	for _, segment := range indexedSegments {
		indexed.Insert(segment.GetID())
	}
	log.Info("GetQueryVChanPositions",
		zap.Int64("collectionID", channel.CollectionID),
		zap.String("channel", channel.Name),
		zap.Int("numOfSegments", len(segments)),
		zap.Int("indexed segment", len(indexedSegments)),
	)
	var (
		indexedIDs   = make(typeutil.UniqueSet)
		unIndexedIDs = make(typeutil.UniqueSet)
		droppedIDs   = make(typeutil.UniqueSet)
		growingIDs   = make(typeutil.UniqueSet)
	)

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
		segmentInfos[s.GetID()] = s
		switch {
		case s.GetState() == commonpb.SegmentState_Dropped:
			droppedIDs.Insert(s.GetID())
		case !isFlushState(s.GetState()):
			growingIDs.Insert(s.GetID())
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

	for segId := range unIndexedIDs {
		segInfo := segmentInfos[segId]
		if segInfo.GetState() == commonpb.SegmentState_Dropped {
			unIndexedIDs.Remove(segId)
			indexedIDs.Insert(segId)
		}
	}

	unIndexedIDs.Insert(growingIDs.Collect()...)

	return &datapb.VchannelInfo{
		CollectionID:        channel.CollectionID,
		ChannelName:         channel.Name,
		SeekPosition:        h.GetChannelSeekPosition(channel, partitionIDs...),
		FlushedSegmentIds:   indexedIDs.Collect(),
		UnflushedSegmentIds: unIndexedIDs.Collect(),
		DroppedSegmentIds:   droppedIDs.Collect(),
		// IndexedSegmentIds: indexed.Collect(),
	}
}

// getEarliestSegmentDMLPos returns the earliest dml position of segments,
// this is mainly for COMPATIBILITY with old version <=2.1.x
func (h *ServerHandler) getEarliestSegmentDMLPos(channel *channel, partitionIDs ...UniqueID) *msgpb.MsgPosition {
	var minPos *msgpb.MsgPosition
	var minPosSegID int64
	var minPosTs uint64
	segments := h.s.meta.SelectSegments(func(s *SegmentInfo) bool {
		return s.InsertChannel == channel.Name
	})

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
func (h *ServerHandler) getCollectionStartPos(channel *channel) *msgpb.MsgPosition {
	// use collection start position when segment position is not found
	var startPosition *msgpb.MsgPosition
	if channel.StartPositions == nil {
		collection, err := h.GetCollection(h.s.ctx, channel.CollectionID)
		if collection != nil && err == nil {
			startPosition = getCollectionStartPosition(channel.Name, collection)
		}
		log.Info("NEITHER segment position or channel start position are found, setting channel seek position to collection start position",
			zap.String("channel", channel.Name),
			zap.Uint64("posTs", startPosition.GetTimestamp()),
			zap.Time("posTime", tsoutil.PhysicalTime(startPosition.GetTimestamp())),
		)
	} else {
		// use passed start positions, skip to ask RootCoord.
		startPosition = toMsgPosition(channel.Name, channel.StartPositions)
		log.Info("segment position not found, setting channel seek position to channel start position",
			zap.String("channel", channel.Name),
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
func (h *ServerHandler) GetChannelSeekPosition(channel *channel, partitionIDs ...UniqueID) *msgpb.MsgPosition {
	var seekPosition *msgpb.MsgPosition
	seekPosition = h.s.meta.GetChannelCheckpoint(channel.Name)
	if seekPosition != nil {
		log.Info("channel seek position set from channel checkpoint meta",
			zap.String("channel", channel.Name),
			zap.Uint64("posTs", seekPosition.Timestamp),
			zap.Time("posTime", tsoutil.PhysicalTime(seekPosition.GetTimestamp())))
		return seekPosition
	}

	seekPosition = h.getEarliestSegmentDMLPos(channel, partitionIDs...)
	if seekPosition != nil {
		log.Info("channel seek position set from earliest segment dml position",
			zap.String("channel", channel.Name),
			zap.Uint64("posTs", seekPosition.Timestamp),
			zap.Time("posTime", tsoutil.PhysicalTime(seekPosition.GetTimestamp())))
		return seekPosition
	}

	seekPosition = h.getCollectionStartPos(channel)
	if seekPosition != nil {
		log.Info("channel seek position set from collection start position",
			zap.String("channel", channel.Name),
			zap.Uint64("posTs", seekPosition.Timestamp),
			zap.Time("posTime", tsoutil.PhysicalTime(seekPosition.GetTimestamp())))
		return seekPosition
	}

	log.Warn("get channel checkpoint failed, channelCPMeta and earliestSegDMLPos and collStartPos are all invalid",
		zap.String("channel", channel.Name))
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
	ctx2, cancel := context.WithTimeout(ctx, time.Minute*30)
	defer cancel()
	if err := retry.Do(ctx2, func() error {
		has, err := h.s.broker.HasCollection(ctx2, collectionID)
		if err != nil {
			log.RatedInfo(60, "datacoord ServerHandler HasCollection retry failed", zap.Error(err))
			return err
		}
		hasCollection = has
		return nil
	}, retry.Attempts(500)); err != nil {
		log.Error("datacoord ServerHandler HasCollection finally failed")
		panic("datacoord ServerHandler HasCollection finally failed")
	}
	return hasCollection, nil
}

// GetCollection returns collection info with specified collection id
func (h *ServerHandler) GetCollection(ctx context.Context, collectionID UniqueID) (*collectionInfo, error) {
	coll := h.s.meta.GetCollection(collectionID)
	if coll != nil {
		return coll, nil
	}
	err := h.s.loadCollectionFromRootCoord(ctx, collectionID)
	if err != nil {
		log.Warn("failed to load collection from rootcoord", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}

	return h.s.meta.GetCollection(collectionID), nil
}

// CheckShouldDropChannel returns whether specified channel is marked to be removed
func (h *ServerHandler) CheckShouldDropChannel(channel string, collectionID UniqueID) bool {
	if h.s.meta.catalog.ShouldDropChannel(h.s.ctx, channel) {
		return true
	}
	// collectionID parse from channelName
	has, err := h.HasCollection(h.s.ctx, collectionID)
	if err != nil {
		log.Info("datacoord ServerHandler CheckShouldDropChannel hasCollection failed", zap.Error(err))
		return false
	}
	log.Info("datacoord ServerHandler CheckShouldDropChannel hasCollection", zap.Bool("shouldDropChannel", !has),
		zap.String("channel", channel))

	return !has
}

// FinishDropChannel cleans up the remove flag for channels
// this function is a wrapper of server.meta.FinishDropChannel
func (h *ServerHandler) FinishDropChannel(channel string) error {
	err := h.s.meta.catalog.DropChannel(h.s.ctx, channel)
	if err != nil {
		log.Warn("DropChannel failed", zap.String("vChannel", channel), zap.Error(err))
		return err
	}
	log.Info("DropChannel succeeded", zap.String("vChannel", channel))
	// Channel checkpoints are cleaned up during garbage collection.
	return nil
}
