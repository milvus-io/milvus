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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// Handler handles some channel method for ChannelManager
type Handler interface {
	// GetQueryVChanPositions gets the information recovery needed of a channel for QueryCoord
	GetQueryVChanPositions(channel *channel, partitionID UniqueID) *datapb.VchannelInfo
	// GetDataVChanPositions gets the information recovery needed of a channel for DataNode
	GetDataVChanPositions(channel *channel, partitionID UniqueID) *datapb.VchannelInfo
	CheckShouldDropChannel(channel string) bool
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

// GetDataVChanPositions gets vchannel latest postitions with provided dml channel names for DataNode.
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

// GetQueryVChanPositions gets vchannel latest postitions with provided dml channel names for QueryCoord,
// we expect QueryCoord gets the indexed segments to load, so the flushed segments below are actually the indexed segments,
// the unflushed segments are actually the segments without index, even they are flushed.
func (h *ServerHandler) GetQueryVChanPositions(channel *channel, partitionID UniqueID) *datapb.VchannelInfo {
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
		segmentInfos[s.GetID()] = s
		if s.GetState() == commonpb.SegmentState_Dropped {
			droppedIDs.Insert(s.GetID())
		} else if indexed.Contain(s.GetID()) {
			indexedIDs.Insert(s.GetID())
		} else {
			unIndexedIDs.Insert(s.GetID())
		}
	}
	hasUnIndexed := true
	for hasUnIndexed {
		hasUnIndexed = false
		for id := range unIndexedIDs {
			// Indexed segments are compacted to a raw segment,
			// replace it with the indexed ones
			if len(segmentInfos[id].GetCompactionFrom()) > 0 {
				unIndexedIDs.Remove(id)
				for _, segID := range segmentInfos[id].GetCompactionFrom() {
					if indexed.Contain(segID) {
						indexedIDs.Insert(segID)
					} else {
						unIndexedIDs.Insert(segID)
						hasUnIndexed = true
					}
				}
				droppedIDs.Remove(segmentInfos[id].GetCompactionFrom()...)
			}
		}
	}

	return &datapb.VchannelInfo{
		CollectionID:        channel.CollectionID,
		ChannelName:         channel.Name,
		SeekPosition:        h.GetChannelSeekPosition(channel, partitionID),
		FlushedSegmentIds:   indexedIDs.Collect(),
		UnflushedSegmentIds: unIndexedIDs.Collect(),
		DroppedSegmentIds:   droppedIDs.Collect(),
		// IndexedSegmentIds: indexed.Collect(),
	}
}

// getEarliestSegmentDMLPos returns the earliest dml position of segments,
// this is mainly for COMPATIBILITY with old version <=2.1.x
func (h *ServerHandler) getEarliestSegmentDMLPos(channel *channel, partitionID UniqueID) *msgpb.MsgPosition {
	var minPos *msgpb.MsgPosition
	var minPosSegID int64
	var minPosTs uint64
	segments := h.s.meta.SelectSegments(func(s *SegmentInfo) bool {
		return s.InsertChannel == channel.Name
	})
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
			zap.Int64("segment ID", minPosSegID),
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
func (h *ServerHandler) GetChannelSeekPosition(channel *channel, partitionID UniqueID) *msgpb.MsgPosition {
	var seekPosition *msgpb.MsgPosition
	seekPosition = h.s.meta.GetChannelCheckpoint(channel.Name)
	if seekPosition != nil {
		log.Info("channel seek position set from channel checkpoint meta",
			zap.String("channel", channel.Name),
			zap.Uint64("posTs", seekPosition.Timestamp),
			zap.Time("posTime", tsoutil.PhysicalTime(seekPosition.GetTimestamp())))
		return seekPosition
	}

	seekPosition = h.getEarliestSegmentDMLPos(channel, partitionID)
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
func (h *ServerHandler) CheckShouldDropChannel(channel string) bool {
	/*
		segments := h.s.meta.GetSegmentsByChannel(channel)
		for _, segment := range segments {
			if segment.GetStartPosition() != nil && // filter empty segment
				// FIXME: we filter compaction generated segments
				// because datanode may not know the segment due to the network lag or
				// datacoord crash when handling CompleteCompaction.
				// FIXME: cancel this limitation for #12265
				// need to change a unified DropAndFlush to solve the root problem
				//len(segment.CompactionFrom) == 0 &&
				segment.GetState() != commonpb.SegmentState_Dropped {
				return false
			}
		}
		return false*/
	return h.s.meta.catalog.ShouldDropChannel(h.s.ctx, channel)
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
