package querynode

import (
	"context"
	"strings"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type statsService struct {
	ctx context.Context

	replica ReplicaInterface

	fieldStatsChan chan []*internalpb2.FieldStats
	statsStream    msgstream.MsgStream
	msFactory      msgstream.Factory
}

func newStatsService(ctx context.Context, replica ReplicaInterface, fieldStatsChan chan []*internalpb2.FieldStats, factory msgstream.Factory) *statsService {

	return &statsService{
		ctx: ctx,

		replica: replica,

		fieldStatsChan: fieldStatsChan,
		statsStream:    nil,

		msFactory: factory,
	}
}

func (sService *statsService) start() {
	sleepTimeInterval := Params.StatsPublishInterval

	// start pulsar
	producerChannels := []string{Params.StatsChannelName}

	statsStream, _ := sService.msFactory.NewMsgStream(sService.ctx)
	statsStream.AsProducer(producerChannels)
	log.Debug("querynode AsProducer: " + strings.Join(producerChannels, ", "))

	var statsMsgStream msgstream.MsgStream = statsStream

	sService.statsStream = statsMsgStream
	sService.statsStream.Start()

	// start service
	for {
		select {
		case <-sService.ctx.Done():
			return
		case <-time.After(time.Duration(sleepTimeInterval) * time.Millisecond):
			sService.publicStatistic(nil)
		case fieldStats := <-sService.fieldStatsChan:
			sService.publicStatistic(fieldStats)
		}
	}
}

func (sService *statsService) close() {
	if sService.statsStream != nil {
		sService.statsStream.Close()
	}
}

func (sService *statsService) publicStatistic(fieldStats []*internalpb2.FieldStats) {
	segStats := sService.replica.getSegmentStatistics()

	queryNodeStats := internalpb2.QueryNodeStats{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_kQueryNodeStats,
			SourceID: Params.QueryNodeID,
		},
		SegStats:   segStats,
		FieldStats: fieldStats,
	}

	var msg msgstream.TsMsg = &msgstream.QueryNodeStatsMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		QueryNodeStats: queryNodeStats,
	}

	var msgPack = msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{msg},
	}
	err := sService.statsStream.Produce(context.TODO(), &msgPack)
	if err != nil {
		log.Error(err.Error())
	}
}
