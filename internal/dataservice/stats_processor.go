package dataservice

import (
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type statsProcessor struct {
	meta                   *meta
	segmentThreshold       float64
	segmentThresholdFactor float64
}

func newStatsProcessor(meta *meta) *statsProcessor {
	return &statsProcessor{
		meta:                   meta,
		segmentThreshold:       Params.SegmentSize * 1024 * 1024,
		segmentThresholdFactor: Params.SegmentSizeFactor,
	}
}

func (processor *statsProcessor) ProcessQueryNodeStats(msgPack *msgstream.MsgPack) error {
	for _, msg := range msgPack.Msgs {
		statsMsg, ok := msg.(*msgstream.QueryNodeStatsMsg)
		if !ok {
			return errors.Errorf("Type of message is not QueryNodeSegStatsMsg")
		}

		for _, segStat := range statsMsg.GetSegStats() {
			if err := processor.processSegmentStat(segStat); err != nil {
				return err
			}
		}
	}

	return nil
}

func (processor *statsProcessor) processSegmentStat(segStats *internalpb2.SegmentStats) error {
	if !segStats.GetRecentlyModified() {
		return nil
	}

	segID := segStats.GetSegmentID()
	segMeta, err := processor.meta.GetSegment(segID)
	if err != nil {
		return err
	}

	segMeta.NumRows = segStats.NumRows
	segMeta.MemSize = segStats.MemorySize

	return processor.meta.UpdateSegment(segMeta)
}
