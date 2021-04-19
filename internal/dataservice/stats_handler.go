package dataservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type statsHandler struct {
	meta *meta
}

func newStatsHandler(meta *meta) *statsHandler {
	return &statsHandler{
		meta: meta,
	}
}

func (handler *statsHandler) HandleSegmentStat(segStats *internalpb.SegmentStatisticsUpdates) error {
	segMeta, err := handler.meta.GetSegment(segStats.SegmentID)
	if err != nil {
		return err
	}

	if segStats.StartPosition != nil {
		segMeta.OpenTime = segStats.CreateTime
		segMeta.StartPosition = segStats.StartPosition
	}

	if segStats.EndPosition != nil {
		segMeta.EndPosition = segStats.EndPosition
	}

	segMeta.SealedTime = segStats.EndTime
	segMeta.NumRows = segStats.NumRows
	segMeta.MemSize = segStats.MemorySize

	return handler.meta.UpdateSegment(segMeta)
}
