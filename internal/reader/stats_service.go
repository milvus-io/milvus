package reader

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type statsService struct {
	ctx       context.Context
	msgStream *msgstream.PulsarMsgStream
	container *ColSegContainer
}

func newStatsService(ctx context.Context, container *ColSegContainer, pulsarAddress string) *statsService {
	// TODO: add pulsar message stream init

	return &statsService{
		ctx:       ctx,
		container: container,
	}
}

func (sService *statsService) start() {
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

func (sService *statsService) sendSegmentStatistic() {
	var statisticData = make([]internalpb.SegmentStats, 0)

	for segmentID, segment := range sService.container.segments {
		currentMemSize := segment.getMemSize()
		segment.lastMemSize = currentMemSize

		segmentNumOfRows := segment.getRowCount()

		stat := internalpb.SegmentStats{
			// TODO: set master pb's segment id type from uint64 to int64
			SegmentID:  segmentID,
			MemorySize: currentMemSize,
			NumRows:    segmentNumOfRows,
		}

		statisticData = append(statisticData, stat)
	}

	// fmt.Println("Publish segment statistic")
	// fmt.Println(statisticData)
	sService.publicStatistic(&statisticData)
}

func (sService *statsService) publicStatistic(statistic *[]internalpb.SegmentStats) {
	// TODO: publish statistic
}
