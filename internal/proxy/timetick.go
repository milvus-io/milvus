package proxy

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"log"
	"time"
)

type timeTick struct {
	lastTick             typeutil.Timestamp
	currentTick          typeutil.Timestamp
	interval             uint64
	pulsarProducer       pulsar.Producer
	peer_id              int64
	ctx                  context.Context
	areRequestsDelivered func(ts typeutil.Timestamp) bool
	getTimestamp         func() (typeutil.Timestamp, error)
}

func (tt *timeTick) tick() error {
	if tt.lastTick == tt.currentTick {
		ts, err := tt.getTimestamp()
		if err != nil {
			return err
		}
		tt.currentTick = ts
	}
	if tt.areRequestsDelivered(tt.currentTick) == false {
		return errors.New("Failed")
	}
	tsm := pb.TimeSyncMsg{
		Timestamp: uint64(tt.currentTick),
		Peer_Id:   tt.peer_id,
		SyncType:  pb.SyncType_READ,
	}
	payload, err := proto.Marshal(&tsm)
	if err != nil {
		return err
	}
	if _, err := tt.pulsarProducer.Send(tt.ctx, &pulsar.ProducerMessage{Payload: payload}); err != nil {
		return err
	}
	tt.lastTick = tt.currentTick
	return nil
}

func (tt *timeTick) Restart() error{
	tt.lastTick = 0
	ts, err := tt.getTimestamp()
	if err != nil{
		return err
	}

	tt.currentTick = ts
	tick := time.Tick(time.Millisecond * time.Duration(tt.interval))

	go func() {
		for {
			select {
			case <-tick:
				if err := tt.tick(); err != nil {
					log.Printf("timeTick error")
				}
			case <-tt.ctx.Done():
				tt.pulsarProducer.Close()
				return
			}
		}
	}()
	return nil
}
