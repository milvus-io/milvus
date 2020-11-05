package reader

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type segmentService struct {
	ctx       context.Context
	msgStream *msgstream.PulsarMsgStream
	node      *QueryNode
}

func newSegmentService(ctx context.Context, node *QueryNode, pulsarAddress string) *segmentService {
	// TODO: add pulsar message stream init

	return &segmentService{
		ctx:  ctx,
		node: node,
	}
}

func (sService *segmentService) start() {
	sleepMillisecondTime := 1000
	fmt.Println("do segments statistic in ", strconv.Itoa(sleepMillisecondTime), "ms")
	for {
		select {
		case <-sService.ctx.Done():
			return
		default:
			time.Sleep(time.Duration(sleepMillisecondTime) * time.Millisecond)
			sService.sendSegmentStatistic()
		}
	}
}

func (sService *segmentService) sendSegmentStatistic() {
	var statisticData = make([]internalpb.SegmentStatistics, 0)

	for segmentID, segment := range sService.node.SegmentsMap {
		currentMemSize := segment.getMemSize()
		segment.LastMemSize = currentMemSize

		segmentNumOfRows := segment.getRowCount()

		stat := internalpb.SegmentStatistics{
			// TODO: set master pb's segment id type from uint64 to int64
			SegmentId:  segmentID,
			MemorySize: currentMemSize,
			NumRows:    segmentNumOfRows,
		}

		statisticData = append(statisticData, stat)
	}

	// fmt.Println("Publish segment statistic")
	// fmt.Println(statisticData)
	sService.publicStatistic(&statisticData)
}

func (sService *segmentService) publicStatistic(statistic *[]internalpb.SegmentStatistics) {
	// TODO: publish statistic
}
