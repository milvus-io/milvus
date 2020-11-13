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
	container *container
}

func newStatsService(ctx context.Context, container *container, pulsarAddress string) *statsService {
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
	var statisticData = (*sService.container).getSegmentStatistics()

	// fmt.Println("Publish segment statistic")
	// fmt.Println(statisticData)
	sService.publicStatistic(statisticData)
}

func (sService *statsService) publicStatistic(statistic *internalpb.QueryNodeSegStats) {
	// TODO: publish statistic
}
