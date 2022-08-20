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
	"go.uber.org/zap"
)

// Handler handles some channel method for ChannelManager
type Handler interface {
	// GetVChanPositions gets the information recovery needed of a channel
	GetVChanPositions(channel string, collectionID UniqueID, partitionID UniqueID) *datapb.VchannelInfo
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

// GetVChanPositions gets vchannel latest postitions with provided dml channel names
func (h *ServerHandler) GetVChanPositions(channel string, collectionID UniqueID, partitionID UniqueID) *datapb.VchannelInfo {
	// cannot use GetSegmentsByChannel since dropped segments are needed here
	segments := h.s.meta.SelectSegments(func(s *SegmentInfo) bool {
		return s.InsertChannel == channel
	})
	log.Info("GetSegmentsByChannel",
		zap.Any("collectionID", collectionID),
		zap.Any("channel", channel),
		zap.Any("numOfSegments", len(segments)),
	)
	var flushedIds []int64
	var unflushedIds []int64
	var droppedIds []int64
	var seekPosition *internalpb.MsgPosition
	for _, s := range segments {
		if (partitionID > allPartitionID && s.PartitionID != partitionID) ||
			(s.GetStartPosition() == nil && s.GetDmlPosition() == nil) {
			continue
		}

		if s.GetState() == commonpb.SegmentState_Dropped {
			droppedIds = append(droppedIds, trimSegmentInfo(s.SegmentInfo).GetID())
			continue
		}

		if s.GetState() == commonpb.SegmentState_Flushing || s.GetState() == commonpb.SegmentState_Flushed {
			flushedIds = append(flushedIds, trimSegmentInfo(s.SegmentInfo).GetID())
		} else {
			unflushedIds = append(unflushedIds, s.SegmentInfo.GetID())
		}

		var segmentPosition *internalpb.MsgPosition
		if s.GetDmlPosition() != nil {
			segmentPosition = s.GetDmlPosition()
		} else {
			segmentPosition = s.GetStartPosition()
		}

		if seekPosition == nil || segmentPosition.Timestamp < seekPosition.Timestamp {
			seekPosition = segmentPosition
		}
	}
	// use collection start position when segment position is not found
	if seekPosition == nil {
		collection := h.GetCollection(h.s.ctx, collectionID)
		if collection != nil {
			seekPosition = getCollectionStartPosition(channel, collection)
		}
	}

	return &datapb.VchannelInfo{
		CollectionID:        collectionID,
		ChannelName:         channel,
		SeekPosition:        seekPosition,
		FlushedSegmentIds:   flushedIds,
		UnflushedSegmentIds: unflushedIds,
		DroppedSegmentIds:   droppedIds,
	}
}

func getCollectionStartPosition(channel string, collectionInfo *datapb.CollectionInfo) *internalpb.MsgPosition {
	for _, sp := range collectionInfo.GetStartPositions() {
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
