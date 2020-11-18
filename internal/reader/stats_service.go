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
	ctx         context.Context
	statsStream *msgstream.MsgStream
	replica     *collectionReplica
}

func newStatsService(ctx context.Context, replica *collectionReplica) *statsService {

	return &statsService{
		ctx:         ctx,
		statsStream: nil,
		replica:     replica,
	}
}

func (sService *statsService) start() {
	sleepTimeInterval := Params.statsServiceTimeInterval()
	receiveBufSize := Params.statsMsgStreamReceiveBufSize()

	// start pulsar
	msgStreamURL, err := Params.PulsarAddress()
	if err != nil {
		log.Fatal(err)
	}
	producerChannels := []string{"statistic"}

	statsStream := msgstream.NewPulsarMsgStream(sService.ctx, receiveBufSize)
	statsStream.SetPulsarClient(msgStreamURL)
	statsStream.CreatePulsarProducers(producerChannels)

	var statsMsgStream msgstream.MsgStream = statsStream

	sService.statsStream = &statsMsgStream
	(*sService.statsStream).Start()

	// start service
	fmt.Println("do segments statistic in ", strconv.Itoa(sleepTimeInterval), "ms")
	for {
		select {
		case <-sService.ctx.Done():
			return
		case <-time.After(time.Duration(sleepTimeInterval) * time.Millisecond):
			sService.sendSegmentStatistic()
		}
	}
}

func (sService *statsService) sendSegmentStatistic() {
	statisticData := (*sService.replica).getSegmentStatistics()

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
		Msgs: []msgstream.TsMsg{msg},
	}
	err := (*sService.statsStream).Produce(&msgPack)
	if err != nil {
		log.Println(err)
	}
}
