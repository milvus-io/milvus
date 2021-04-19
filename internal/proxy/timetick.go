package proxy

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"

	"github.com/apache/pulsar-client-go/pulsar"
)

type timeTick struct {
	lastTick    Timestamp
	currentTick Timestamp
	interval    uint64

	pulsarProducer pulsar.Producer

	tsoAllocator  *allocator.TimestampAllocator
	tickMsgStream *msgstream.PulsarMsgStream

	peerID UniqueID
	wg     sync.WaitGroup
	ctx    context.Context
	cancel func()
	timer  *time.Ticker

	areRequestsDelivered func(ts Timestamp) bool
}

func newTimeTick(ctx context.Context, tsoAllocator *allocator.TimestampAllocator) *timeTick {
	ctx1, cancel := context.WithCancel(ctx)
	t := &timeTick{
		ctx:          ctx1,
		cancel:       cancel,
		tsoAllocator: tsoAllocator,
	}

	bufSzie := int64(1000)
	t.tickMsgStream = msgstream.NewPulsarMsgStream(t.ctx, bufSzie)

	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"timeTick"}
	t.tickMsgStream.SetPulsarCient(pulsarAddress)
	t.tickMsgStream.CreatePulsarProducers(producerChannels)
	return t
}

func (tt *timeTick) tick() error {

	if tt.lastTick == tt.currentTick {
		ts, err := tt.tsoAllocator.AllocOne()
		if err != nil {
			return err
		}
		tt.currentTick = ts
	}

	if tt.areRequestsDelivered(tt.currentTick) == false {
		return nil
	}
	msgPack := msgstream.MsgPack{}
	var timeTickMsg msgstream.TsMsg = &msgstream.TimeTickMsg{
		TimeTickMsg: internalpb.TimeTickMsg{
			MsgType:   internalpb.MsgType_kTimeTick,
			PeerId:    tt.peerID,
			Timestamp: tt.currentTick,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, &timeTickMsg)
	tt.tickMsgStream.Produce(&msgPack)
	tt.lastTick = tt.currentTick
	return nil
}

func (tt *timeTick) tickLoop() {
	defer tt.wg.Done()
	tt.timer = time.NewTicker(time.Millisecond * time.Duration(tt.interval))
	for {
		select {
		case <-tt.timer.C:
			if err := tt.tick(); err != nil {
				log.Printf("timeTick error")
			}
		case <-tt.ctx.Done():
			return
		}
	}
}

func (tt *timeTick) Start() error {
	tt.lastTick = 0
	ts, err := tt.tsoAllocator.AllocOne()
	if err != nil {
		return err
	}

	tt.currentTick = ts
	tt.tickMsgStream.Start()
	tt.wg.Add(1)
	go tt.tickLoop()
	return nil
}

func (tt *timeTick) Close() {
	if tt.timer != nil {
		tt.timer.Stop()
	}
	tt.cancel()
	tt.tickMsgStream.Close()
	tt.wg.Wait()
}
