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

	//if segStats.IsNewSegment {
	//	segMeta.OpenTime = segStats.CreateTime
	//	segMeta.segStats.StartPositions
	//}
	segMeta.NumRows = segStats.NumRows
	segMeta.MemSize = segStats.MemorySize

	return handler.meta.UpdateSegment(segMeta)
}
