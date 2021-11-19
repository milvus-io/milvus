package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"go.uber.org/zap"
)

// Handler handles some channel method for ChannelManager
type Handler interface {
	GetVChanPositions(channel string, collectionID UniqueID, partitionID UniqueID) *datapb.VchannelInfo
	CheckShouldDropChannel(channel string) bool
}

// Handler is a helper of Server
type ServerHandler struct {
	s *Server
}

func newServerHandler(s *Server) *ServerHandler {
	return &ServerHandler{s: s}
}

// GetVChanPositions get vchannel latest postitions with provided dml channel names
func (h *ServerHandler) GetVChanPositions(channel string, collectionID UniqueID, partitionID UniqueID) *datapb.VchannelInfo {
	segments := h.s.meta.GetSegmentsByChannel(channel)
	log.Debug("GetSegmentsByChannel",
		zap.Any("collectionID", collectionID),
		zap.Any("channel", channel),
		zap.Any("numOfSegments", len(segments)),
	)
	var flushed []*datapb.SegmentInfo
	var unflushed []*datapb.SegmentInfo
	var seekPosition *internalpb.MsgPosition
	for _, s := range segments {
		if (partitionID > allPartitionID && s.PartitionID != partitionID) ||
			(s.GetStartPosition() == nil && s.GetDmlPosition() == nil) {
			continue
		}

		if s.GetState() == commonpb.SegmentState_Flushing || s.GetState() == commonpb.SegmentState_Flushed {
			flushed = append(flushed, trimSegmentInfo(s.SegmentInfo))
		} else {
			unflushed = append(unflushed, s.SegmentInfo)
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
		CollectionID:      collectionID,
		ChannelName:       channel,
		SeekPosition:      seekPosition,
		FlushedSegments:   flushed,
		UnflushedSegments: unflushed,
	}
}

func getCollectionStartPosition(channel string, collectionInfo *datapb.CollectionInfo) *internalpb.MsgPosition {
	for _, sp := range collectionInfo.GetStartPositions() {
		if sp.GetKey() != rootcoord.ToPhysicalChannel(channel) {
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

func (h *ServerHandler) CheckShouldDropChannel(channel string) bool {
	segments := h.s.meta.GetSegmentsByChannel(channel)
	for _, segment := range segments {
		if segment.GetStartPosition() != nil && // fitler empty segment
			// FIXME: we filter compaction generated segments
			// because datanode may not know the segment due to the network lag or
			// datacoord crash when handling CompleteCompaction.
			len(segment.CompactionFrom) == 0 &&
			segment.GetState() != commonpb.SegmentState_Dropped {
			return false
		}
	}
	return true
}
