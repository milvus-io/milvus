package reader

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type statsService struct {
	ctx       context.Context
	pulsarURL string

	msgStream *msgstream.MsgStream

	container *container
}

func newStatsService(ctx context.Context, container *container, pulsarURL string) *statsService {

	return &statsService{
		ctx:       ctx,
		pulsarURL: pulsarURL,
		msgStream: nil,
		container: container,
	}
}

func (sService *statsService) start() {
	const (
		receiveBufSize       = 1024
		sleepMillisecondTime = 1000
	)

	// start pulsar
	producerChannels := []string{"statistic"}

	statsStream := msgstream.NewPulsarMsgStream(sService.ctx, receiveBufSize)
	statsStream.SetPulsarCient(sService.pulsarURL)
	statsStream.CreatePulsarProducers(producerChannels)

	var statsMsgStream msgstream.MsgStream = statsStream

	sService.msgStream = &statsMsgStream
	(*sService.msgStream).Start()

	// start service
	fmt.Println("do segments statistic in ", strconv.Itoa(sleepMillisecondTime), "ms")
	for {
		select {
		case <-sService.ctx.Done():
			return
		case <-time.After(sleepMillisecondTime * time.Millisecond):
			sService.sendSegmentStatistic()
		}
	}
}

func (sService *statsService) sendSegmentStatistic() {
	statisticData := (*sService.container).getSegmentStatistics()

	// fmt.Println("Publish segment statistic")
	// fmt.Println(statisticData)
	sService.publicStatistic(statisticData)
}

func (sService *statsService) publicStatistic(statistic *internalpb.QueryNodeSegStats) {
	var msg msgstream.TsMsg = &msgstream.QueryNodeSegStatsMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []int32{0},
		},
		QueryNodeSegStats: *statistic,
	}

	var msgPack = msgstream.MsgPack{
		Msgs: []*msgstream.TsMsg{&msg},
	}
	err := (*sService.msgStream).Produce(&msgPack)
	if err != nil {
		log.Println(err)
	}
}
