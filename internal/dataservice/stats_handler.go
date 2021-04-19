package dataservice

import (
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"go.uber.org/zap"
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
	log.Debug("stats_handler update segment", zap.Any("segmentID", segMeta.ID), zap.Any("State", segMeta.State))
	return handler.meta.UpdateSegment(segMeta)
}
