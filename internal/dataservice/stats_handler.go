package dataservice

import (
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
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

func (handler *statsHandler) HandleQueryNodeStats(msgPack *msgstream.MsgPack) error {
	for _, msg := range msgPack.Msgs {
		statsMsg, ok := msg.(*msgstream.QueryNodeStatsMsg)
		if !ok {
			return errors.Errorf("Type of message is not QueryNodeSegStatsMsg")
		}

		for _, segStat := range statsMsg.GetSegStats() {
			if err := handler.handleSegmentStat(segStat); err != nil {
				return err
			}
		}
	}

	return nil
}

func (handler *statsHandler) handleSegmentStat(segStats *internalpb2.SegmentStats) error {
	if !segStats.GetRecentlyModified() {
		return nil
	}

	segMeta, err := handler.meta.GetSegment(segStats.SegmentID)
	if err != nil {
		return err
	}

	segMeta.NumRows = segStats.NumRows
	segMeta.MemSize = segStats.MemorySize

	return handler.meta.UpdateSegment(segMeta)
}
