package dataservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type statsHandler struct {
	meta *meta
}

func newStatsHandler(meta *meta) *statsHandler {
	return &statsHandler{
		meta: meta,
	}
}

func (handler *statsHandler) HandleSegmentStat(segStats *internalpb2.SegmentStatisticsUpdates) error {
	segMeta, err := handler.meta.GetSegment(segStats.SegmentID)
	if err != nil {
		return err
	}

	if segStats.IsNewSegment {
		segMeta.OpenTime = segStats.CreateTime
		segMeta.StartPosition = append(segMeta.StartPosition, segStats.StartPositions...)
	}
	segMeta.SealedTime = segStats.EndTime
	for _, pos := range segStats.EndPositions {
		isNew := true
		for _, epos := range segMeta.EndPosition {
			if epos.ChannelName == pos.ChannelName {
				epos.Timestamp = pos.Timestamp
				epos.MsgID = pos.MsgID
				isNew = false
				break
			}
		}
		if isNew {
			segMeta.EndPosition = append(segMeta.EndPosition, pos)
		}
	}
	segMeta.NumRows = segStats.NumRows
	segMeta.MemSize = segStats.MemorySize

	return handler.meta.UpdateSegment(segMeta)
}
