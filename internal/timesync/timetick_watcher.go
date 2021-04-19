package timesync

import (
	"context"
	"log"

	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type TimeTickWatcher interface {
	Watch(msg *ms.TimeTickMsg)
	StartBackgroundLoop(ctx context.Context)
}

type MsgTimeTickWatcher struct {
	streams  []ms.MsgStream
	msgQueue chan *ms.TimeTickMsg
}

func NewMsgTimeTickWatcher(streams ...ms.MsgStream) *MsgTimeTickWatcher {
	watcher := &MsgTimeTickWatcher{
		streams:  streams,
		msgQueue: make(chan *ms.TimeTickMsg),
	}
	return watcher
}

func (watcher *MsgTimeTickWatcher) Watch(msg *ms.TimeTickMsg) {
	watcher.msgQueue <- msg
}

func (watcher *MsgTimeTickWatcher) StartBackgroundLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("msg time tick watcher closed")
			return
		case msg := <-watcher.msgQueue:
			msgPack := &ms.MsgPack{}
			msgPack.Msgs = append(msgPack.Msgs, msg)
			for _, stream := range watcher.streams {
				if err := stream.Broadcast(msgPack); err != nil {
					log.Printf("stream broadcast failed %s", err.Error())
				}
			}
		}
	}
}

func (watcher *MsgTimeTickWatcher) Close() {
}
