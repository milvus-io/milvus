package rmqms

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type RmqMsgStream struct {
	isServing        int64
	idAllocator      *masterservice.GlobalIDAllocator
	ctx              context.Context
	serverLoopWg     sync.WaitGroup
	serverLoopCtx    context.Context
	serverLoopCancel func()

	// tso ticker
	tsoTicker *time.Ticker
}

func NewRmqMsgStream() *RmqMsgStream {
	//idAllocator := master.NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{""}, "singleNode/rocksmq", "gid"))
	//if err := idAllocator.Initialize(); err != nil {
	//	return nil
	//}
	//
	//return &RmqMsgStream{
	//	idAllocator: idAllocator,
	//}

	return nil
}

func (ms *RmqMsgStream) startServerLoop(ctx context.Context) error {
	ms.serverLoopCtx, ms.serverLoopCancel = context.WithCancel(ctx)

	ms.serverLoopWg.Add(1)
	go ms.tsLoop()

	return nil
}

func (ms *RmqMsgStream) stopServerLoop() {
	ms.serverLoopCancel()
	ms.serverLoopWg.Wait()
}

func (ms *RmqMsgStream) tsLoop() {
	defer ms.serverLoopWg.Done()

	ms.tsoTicker = time.NewTicker(masterservice.UpdateTimestampStep)
	defer ms.tsoTicker.Stop()

	ctx, cancel := context.WithCancel(ms.serverLoopCtx)
	defer cancel()

	for {
		select {
		case <-ms.tsoTicker.C:
			if err := ms.idAllocator.UpdateID(); err != nil {
				log.Println("failed to update id", err)
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Println("tsLoop is closed")
			return
		}
	}
}

func (ms *RmqMsgStream) Start() {
	if err := ms.startServerLoop(ms.ctx); err != nil {
		return
	}

	atomic.StoreInt64(&ms.isServing, 1)
}

func (ms *RmqMsgStream) Close() {
	if !atomic.CompareAndSwapInt64(&ms.isServing, 1, 0) {
		// server is already closed
		return
	}

	log.Print("closing server")

	ms.stopServerLoop()
}

func (ms *RmqMsgStream) Produce(pack *msgstream.MsgPack) error {
	return nil
}

func (ms *RmqMsgStream) Consume() *msgstream.MsgPack {
	return nil
}

func (ms *RmqMsgStream) Chan() <-chan *msgstream.MsgPack {
	return nil
}
