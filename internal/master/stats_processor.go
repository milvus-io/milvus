package master

import (
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type StatsProcessor struct {
	metaTable              *metaTable
	segmentThreshold       float64
	segmentThresholdFactor float64
	globalTSOAllocator     func() (Timestamp, error)
}

func (processor *StatsProcessor) ProcessQueryNodeStats(msgPack *msgstream.MsgPack) error {
	for _, msg := range msgPack.Msgs {
		statsMsg, ok := msg.(*msgstream.QueryNodeStatsMsg)
		if !ok {
			return errors.Errorf("Type of message is not QueryNodeSegStatsMsg")
		}

		for _, segStat := range statsMsg.GetSegStats() {
			err := processor.processSegmentStat(segStat)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (processor *StatsProcessor) processSegmentStat(segStats *internalpb.SegmentStats) error {
	if !segStats.GetRecentlyModified() {
		return nil
	}

	segID := segStats.GetSegmentID()
	segMeta, err := processor.metaTable.GetSegmentByID(segID)
	if err != nil {
		return err
	}

	segMeta.NumRows = segStats.NumRows
	segMeta.MemSize = segStats.MemorySize

	return processor.metaTable.UpdateSegment(segMeta)
}

func NewStatsProcessor(mt *metaTable, globalTSOAllocator func() (Timestamp, error)) *StatsProcessor {
	return &StatsProcessor{
		metaTable:              mt,
		segmentThreshold:       Params.SegmentSize * 1024 * 1024,
		segmentThresholdFactor: Params.SegmentSizeFactor,
		globalTSOAllocator:     globalTSOAllocator,
	}
}
