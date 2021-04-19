package timesync

import (
	"context"
	"log"
	"sync"

	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type MsgProducer struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	ttBarrier TimeTickBarrier
	watchers  []TimeTickWatcher
}

func NewTimeSyncMsgProducer(ttBarrier TimeTickBarrier, watchers ...TimeTickWatcher) (*MsgProducer, error) {
	return &MsgProducer{
		ttBarrier: ttBarrier,
		watchers:  watchers,
	}, nil
}

func (producer *MsgProducer) broadcastMsg() {
	defer producer.wg.Done()
	for {
		select {
		case <-producer.ctx.Done():
			log.Printf("broadcast context done, exit")
			return
		default:
		}
		tt, err := producer.ttBarrier.GetTimeTick()
		if err != nil {
			log.Printf("broadcast get time tick error")
		}
		baseMsg := ms.BaseMsg{
			BeginTimestamp: tt,
			EndTimestamp:   tt,
			HashValues:     []uint32{0},
		}
		timeTickResult := internalpb2.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kTimeTick,
				MsgID:     0,
				Timestamp: tt,
				SourceID:  0,
			},
		}
		timeTickMsg := &ms.TimeTickMsg{
			BaseMsg:     baseMsg,
			TimeTickMsg: timeTickResult,
		}
		for _, watcher := range producer.watchers {
			watcher.Watch(timeTickMsg)
		}
	}
}

func (producer *MsgProducer) Start(ctx context.Context) {
	producer.ctx, producer.cancel = context.WithCancel(ctx)
	producer.wg.Add(1 + len(producer.watchers))
	for _, watcher := range producer.watchers {
		go producer.startWatcher(watcher)
	}
	go producer.broadcastMsg()
}

func (producer *MsgProducer) startWatcher(watcher TimeTickWatcher) {
	defer producer.wg.Done()
	watcher.StartBackgroundLoop(producer.ctx)
}

func (producer *MsgProducer) Close() {
	producer.cancel()
	producer.wg.Wait()
}
