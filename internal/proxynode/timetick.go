package proxynode

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type tickCheckFunc = func(Timestamp) bool

type timeTick struct {
	lastTick    Timestamp
	currentTick Timestamp
	interval    time.Duration

	pulsarProducer pulsar.Producer

	tsoAllocator  *allocator.TimestampAllocator
	tickMsgStream msgstream.MsgStream
	msFactory     msgstream.Factory

	peerID    UniqueID
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    func()
	timer     *time.Ticker
	tickLock  sync.RWMutex
	checkFunc tickCheckFunc
}

func newTimeTick(ctx context.Context,
	tsoAllocator *allocator.TimestampAllocator,
	interval time.Duration,
	checkFunc tickCheckFunc,
	factory msgstream.Factory) *timeTick {
	ctx1, cancel := context.WithCancel(ctx)
	t := &timeTick{
		ctx:          ctx1,
		cancel:       cancel,
		tsoAllocator: tsoAllocator,
		interval:     interval,
		peerID:       Params.ProxyID,
		checkFunc:    checkFunc,
		msFactory:    factory,
	}

	t.tickMsgStream, _ = t.msFactory.NewMsgStream(t.ctx)
	t.tickMsgStream.AsProducer(Params.ProxyTimeTickChannelNames)
	log.Debug("proxynode", zap.Strings("proxynode AsProducer", Params.ProxyTimeTickChannelNames))
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
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{uint32(Params.ProxyID)},
		},
		TimeTickMsg: internalpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     0,
				Timestamp: tt.currentTick,
				SourceID:  tt.peerID,
			},
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
	err := tt.tickMsgStream.Produce(&msgPack)
	if err != nil {
		log.Warn("proxynode", zap.String("error", err.Error()))
	}
	tt.tickLock.Lock()
	defer tt.tickLock.Unlock()
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
				log.Warn("timeTick error")
			}
		case <-tt.ctx.Done():
			return
		}
	}
}

func (tt *timeTick) LastTick() Timestamp {
	tt.tickLock.RLock()
	defer tt.tickLock.RUnlock()
	return tt.lastTick
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
