package dataservice

import (
	"log"

	"golang.org/x/net/context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type (
	proxyTimeTickWatcher struct {
		allocator  segmentAllocator
		msgQueue   chan *msgstream.TimeTickMsg
		ctx        context.Context
		cancelFunc context.CancelFunc
	}
	dataNodeTimeTickWatcher struct {
		allocator  segmentAllocator
		msgQueue   chan *msgstream.TimeTickMsg
		ctx        context.Context
		cancelFunc context.CancelFunc
	}
)

func newProxyTimeTickWatcher(ctx context.Context, allocator segmentAllocator) *proxyTimeTickWatcher {
	cancel, cancelFunc := context.WithCancel(ctx)
	return &proxyTimeTickWatcher{
		allocator:  allocator,
		msgQueue:   make(chan *msgstream.TimeTickMsg, 1),
		ctx:        cancel,
		cancelFunc: cancelFunc,
	}
}

func (watcher *proxyTimeTickWatcher) Start() {
	go watcher.handleProxyTimeTickMsg()
}

func (watcher *proxyTimeTickWatcher) Close() {
	watcher.cancelFunc()
}

func (watcher *proxyTimeTickWatcher) Watch(msg *msgstream.TimeTickMsg) {
	watcher.msgQueue <- msg
}

func (watcher *proxyTimeTickWatcher) handleProxyTimeTickMsg() {
	for {
		select {
		case <-watcher.ctx.Done():
			return
		case msg := <-watcher.msgQueue:
			if err := watcher.allocator.ExpireAllocations(msg.Timestamp); err != nil {
				log.Printf("expire allocations error : %s", err.Error())
			}
		}
	}
}

func newDataNodeTimeTickWatcher(ctx context.Context, allocator segmentAllocator) *dataNodeTimeTickWatcher {
	cancel, cancelFunc := context.WithCancel(ctx)
	return &dataNodeTimeTickWatcher{
		allocator:  allocator,
		msgQueue:   make(chan *msgstream.TimeTickMsg, 1),
		ctx:        cancel,
		cancelFunc: cancelFunc,
	}
}

func (watcher *dataNodeTimeTickWatcher) Watch(msg *msgstream.TimeTickMsg) {
	watcher.msgQueue <- msg
}

func (watcher *dataNodeTimeTickWatcher) Start() {
	go watcher.handleDataNodeTimeTickMsg()
}

func (watcher *dataNodeTimeTickWatcher) Close() {
	watcher.cancelFunc()
}

func (watcher *dataNodeTimeTickWatcher) handleDataNodeTimeTickMsg() {
	for {
		select {
		case <-watcher.ctx.Done():
			return
		case msg := <-watcher.msgQueue:
			segments, err := watcher.allocator.GetSealedSegments()
			if err != nil {
				log.Printf("get sealed segments error %s", err.Error())
				continue
			}
			for _, id := range segments {
				expired, err := watcher.allocator.IsAllocationsExpired(id, msg.Timestamp)
				if err != nil {
					log.Printf("check allocations expired error %s", err.Error())
					continue
				}
				if expired {
					// TODO: flush segment
					watcher.allocator.DropSegment(id)
				}
			}
		}
	}
}
