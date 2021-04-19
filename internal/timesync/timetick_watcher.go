package timesync

import (
	"log"

	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type TimeTickWatcher interface {
	Watch(msg *ms.TimeTickMsg)
	Start()
	Close()
}

type MsgTimeTickWatcher struct {
	streams []ms.MsgStream
}

func (watcher *MsgTimeTickWatcher) Watch(msg *ms.TimeTickMsg) {
	msgPack := &ms.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	for _, stream := range watcher.streams {
		if err := stream.Broadcast(msgPack); err != nil {
			log.Printf("stream broadcast failed %s", err.Error())
		}
	}

}

func (watcher *MsgTimeTickWatcher) Start() {
}

func (watcher *MsgTimeTickWatcher) Close() {
}
