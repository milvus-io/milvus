package proxy

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type tickCheckFunc = func(Timestamp) bool

type timeTick struct {
	lastTick    Timestamp
	currentTick Timestamp
	interval    time.Duration

	pulsarProducer pulsar.Producer

	tsoAllocator  *allocator.TimestampAllocator
	tickMsgStream *msgstream.PulsarMsgStream

	peerID UniqueID
	wg     sync.WaitGroup
	ctx    context.Context
	cancel func()
	timer  *time.Ticker

	checkFunc tickCheckFunc
}

func newTimeTick(ctx context.Context,
	tsoAllocator *allocator.TimestampAllocator,
	interval time.Duration,
	checkFunc tickCheckFunc) *timeTick {
	ctx1, cancel := context.WithCancel(ctx)
	t := &timeTick{
		ctx:          ctx1,
		cancel:       cancel,
		tsoAllocator: tsoAllocator,
		interval:     interval,
		peerID:       1,
		checkFunc:    checkFunc,
	}

	t.tickMsgStream = msgstream.NewPulsarMsgStream(t.ctx, Params.MsgStreamTimeTickBufSize())
	pulsarAddress := Params.PulsarAddress()

	t.tickMsgStream.SetPulsarClient(pulsarAddress)
	t.tickMsgStream.CreatePulsarProducers(Params.ProxyTimeTickChannelNames())
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

	if !tt.checkFunc(tt.currentTick) {
		return nil
	}
	msgPack := msgstream.MsgPack{}
	timeTickMsg := &msgstream.TimeTickMsg{
		TimeTickMsg: internalpb.TimeTickMsg{
			MsgType:   internalpb.MsgType_kTimeTick,
			PeerID:    tt.peerID,
			Timestamp: tt.currentTick,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
	tt.tickMsgStream.Produce(&msgPack)
	tt.lastTick = tt.currentTick
	return nil
}

func (tt *timeTick) tickLoop() {
	defer tt.wg.Done()
	tt.timer = time.NewTicker(tt.interval)
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
