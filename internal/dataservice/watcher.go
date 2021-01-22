package dataservice

import (
	"log"

	"golang.org/x/net/context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type (
	proxyTimeTickWatcher struct {
		allocator segmentAllocator
		msgQueue  chan *msgstream.TimeTickMsg
	}
	dataNodeTimeTickWatcher struct {
		allocator segmentAllocator
		msgQueue  chan *msgstream.TimeTickMsg
	}
)

func newProxyTimeTickWatcher(allocator segmentAllocator) *proxyTimeTickWatcher {
	return &proxyTimeTickWatcher{
		allocator: allocator,
		msgQueue:  make(chan *msgstream.TimeTickMsg, 1),
	}
}

func (watcher *proxyTimeTickWatcher) StartBackgroundLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("proxy time tick watcher clsoed")
			return
		case msg := <-watcher.msgQueue:
			if err := watcher.allocator.ExpireAllocations(msg.Base.Timestamp); err != nil {
				log.Printf("expire allocations error : %s", err.Error())
			}
		}
	}
}

func (watcher *proxyTimeTickWatcher) Watch(msg *msgstream.TimeTickMsg) {
	watcher.msgQueue <- msg
}

func newDataNodeTimeTickWatcher(allocator segmentAllocator) *dataNodeTimeTickWatcher {
	return &dataNodeTimeTickWatcher{
		allocator: allocator,
		msgQueue:  make(chan *msgstream.TimeTickMsg, 1),
	}
}

func (watcher *dataNodeTimeTickWatcher) Watch(msg *msgstream.TimeTickMsg) {
	watcher.msgQueue <- msg
}

func (watcher *dataNodeTimeTickWatcher) StartBackgroundLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("data node time tick watcher clsoed")
			return
		case msg := <-watcher.msgQueue:
			segments, err := watcher.allocator.GetSealedSegments()
			if err != nil {
				log.Printf("get sealed segments error %s", err.Error())
				continue
			}
			for _, id := range segments {
				expired, err := watcher.allocator.IsAllocationsExpired(id, msg.Base.Timestamp)
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
