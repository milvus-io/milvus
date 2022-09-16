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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

// Handler handles some channel method for ChannelManager
type Handler interface {
	// GetVChanPositions gets the information recovery needed of a channel
	GetVChanPositions(channel *channel, partitionID UniqueID) *datapb.VchannelInfo
	CheckShouldDropChannel(channel string) bool
	FinishDropChannel(channel string)
}

// ServerHandler is a helper of Server
type ServerHandler struct {
	s *Server
}

// newServerHandler creates a new ServerHandler
func newServerHandler(s *Server) *ServerHandler {
	return &ServerHandler{s: s}
}

// GetVChanPositions gets vchannel latest postitions with provided dml channel names,
// we expect QueryCoord gets the indexed segments to load, so the flushed segments below are actually the indexed segments,
// the unflushed segments are actually the segments without index, even they are flushed.
func (h *ServerHandler) GetVChanPositions(channel *channel, partitionID UniqueID) *datapb.VchannelInfo {
	// cannot use GetSegmentsByChannel since dropped segments are needed here
	segments := h.s.meta.SelectSegments(func(s *SegmentInfo) bool {
		return s.InsertChannel == channel.Name
	})
	segmentInfos := make(map[int64]*SegmentInfo)
	indexedSegments := FilterInIndexedSegments(h.s.meta, h.s.indexCoord, segments...)
	indexed := make(typeutil.UniqueSet)
	for _, segment := range indexedSegments {
		indexed.Insert(segment.GetID())
	}
	log.Info("GetSegmentsByChannel",
		zap.Any("collectionID", channel.CollectionID),
		zap.Any("channel", channel),
		zap.Any("numOfSegments", len(segments)),
	)
	var (
		flushedIds   = make(typeutil.UniqueSet)
		unflushedIds = make(typeutil.UniqueSet)
		droppedIds   = make(typeutil.UniqueSet)
		seekPosition *internalpb.MsgPosition
	)
	for _, s := range segments {
		if (partitionID > allPartitionID && s.PartitionID != partitionID) ||
			(s.GetStartPosition() == nil && s.GetDmlPosition() == nil) {
			continue
		}
		segmentInfos[s.GetID()] = s
		if s.GetState() == commonpb.SegmentState_Dropped {
			droppedIds.Insert(s.GetID())
		} else if indexed.Contain(s.GetID()) {
			flushedIds.Insert(s.GetID())
		} else {
			unflushedIds.Insert(s.GetID())
		}
	}
	for id := range unflushedIds {
		// Indexed segments are compacted to a raw segment,
		// replace it with the indexed ones
		if !indexed.Contain(id) &&
			len(segmentInfos[id].GetCompactionFrom()) > 0 &&
			indexed.Contain(segmentInfos[id].GetCompactionFrom()...) {
			flushedIds.Insert(segmentInfos[id].GetCompactionFrom()...)
			unflushedIds.Remove(id)
			droppedIds.Remove(segmentInfos[id].GetCompactionFrom()...)
		}
	}

	for id := range flushedIds {
		var segmentPosition *internalpb.MsgPosition
		segment := segmentInfos[id]
		if segment.GetDmlPosition() != nil {
			segmentPosition = segment.GetDmlPosition()
		} else {
			segmentPosition = segment.GetStartPosition()
		}

		if seekPosition == nil || segmentPosition.Timestamp < seekPosition.Timestamp {
			seekPosition = segmentPosition
		}
	}
	for id := range unflushedIds {
		var segmentPosition *internalpb.MsgPosition
		segment := segmentInfos[id]
		if segment.GetDmlPosition() != nil {
			segmentPosition = segment.GetDmlPosition()
		} else {
			segmentPosition = segment.GetStartPosition()
		}

		if seekPosition == nil || segmentPosition.Timestamp < seekPosition.Timestamp {
			seekPosition = segmentPosition
		}
	}

	// use collection start position when segment position is not found
	if seekPosition == nil {
		if channel.StartPositions == nil {
			collection := h.GetCollection(h.s.ctx, channel.CollectionID)
			if collection != nil {
				seekPosition = getCollectionStartPosition(channel.Name, collection)
			}
		} else {
			// use passed start positions, skip to ask rootcoord.
			seekPosition = toMsgPosition(channel.Name, channel.StartPositions)
		}
	}

	return &datapb.VchannelInfo{
		CollectionID:        channel.CollectionID,
		ChannelName:         channel.Name,
		SeekPosition:        seekPosition,
		FlushedSegmentIds:   flushedIds.Collect(),
		UnflushedSegmentIds: unflushedIds.Collect(),
		DroppedSegmentIds:   droppedIds.Collect(),
	}
}

func getCollectionStartPosition(channel string, collectionInfo *datapb.CollectionInfo) *internalpb.MsgPosition {
	return toMsgPosition(channel, collectionInfo.GetStartPositions())
}

func toMsgPosition(channel string, startPositions []*commonpb.KeyDataPair) *internalpb.MsgPosition {
	for _, sp := range startPositions {
		if sp.GetKey() != funcutil.ToPhysicalChannel(channel) {
			continue
		}
		return &internalpb.MsgPosition{
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
func (h *ServerHandler) GetCollection(ctx context.Context, collectionID UniqueID) *datapb.CollectionInfo {
	coll := h.s.meta.GetCollection(collectionID)
	if coll != nil {
		return coll
	}
	err := h.s.loadCollectionFromRootCoord(ctx, collectionID)
	if err != nil {
		log.Warn("failed to load collection from rootcoord", zap.Int64("collectionID", collectionID), zap.Error(err))
	}

	return h.s.meta.GetCollection(collectionID)
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
	return h.s.meta.catalog.IsChannelDropped(h.s.ctx, channel)
}

// FinishDropChannel cleans up the remove flag for channels
// this function is a wrapper of server.meta.FinishDropChannel
func (h *ServerHandler) FinishDropChannel(channel string) {
	h.s.meta.catalog.DropChannel(h.s.ctx, channel)
}
