package proxy

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	pb "github.com/czs007/suvlim/internal/proto/message"
	"github.com/golang/protobuf/proto"
	"log"
	"time"
)

type timeTick struct {
	lastTick             Timestamp
	currentTick          Timestamp
	interval             uint64
	pulsarProducer       pulsar.Producer
	peer_id              int64
	ctx                  context.Context
	areRequestsDelivered func(ts Timestamp) bool
	getTimestamp         func() (Timestamp, pb.Status)
}

func (tt *timeTick) tick() pb.Status {
	if tt.lastTick == tt.currentTick {
		ts, s := tt.getTimestamp()
		if s.ErrorCode != pb.ErrorCode_SUCCESS {
			return s
		}
		tt.currentTick = ts
	}
	if tt.areRequestsDelivered(tt.currentTick) == false {
		return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
	}
	tsm := pb.TimeSyncMsg{
		Timestamp: uint64(tt.currentTick),
		Peer_Id:   tt.peer_id,
		SyncType:  pb.SyncType_READ,
	}
	payload, err := proto.Marshal(&tsm)
	if err != nil {
		return pb.Status{ErrorCode: pb.ErrorCode_UNEXPECTED_ERROR, Reason: fmt.Sprintf("marshal TimeSync failed, error = %v", err)}
	}
	if _, err := tt.pulsarProducer.Send(tt.ctx, &pulsar.ProducerMessage{Payload: payload}); err != nil {
		return pb.Status{ErrorCode: pb.ErrorCode_UNEXPECTED_ERROR, Reason: fmt.Sprintf("send into pulsar failed, error = %v", err)}
	}
	tt.lastTick = tt.currentTick
	return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}

func (tt *timeTick) Restart() pb.Status {
	tt.lastTick = 0
	ts, s := tt.getTimestamp()
	if s.ErrorCode != pb.ErrorCode_SUCCESS {
		return s
	}
	tt.currentTick = ts
	tick := time.Tick(time.Millisecond * time.Duration(tt.interval))

	go func() {
		for {
			select {
			case <-tick:
				if s := tt.tick(); s.ErrorCode != pb.ErrorCode_SUCCESS {
					log.Printf("timeTick error ,status = %d", int(s.ErrorCode))
				}
			case <-tt.ctx.Done():
				tt.pulsarProducer.Close()
				return
			}
		}
	}()
	return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}
