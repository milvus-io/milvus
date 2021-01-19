package timesync

import (
	"context"
	"log"

	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type MsgProducer struct {
	ttBarrier TimeTickBarrier

	ctx    context.Context
	cancel context.CancelFunc

	watchers []TimeTickWatcher
}

func NewTimeSyncMsgProducer(ctx context.Context, ttBarrier TimeTickBarrier, watchers ...TimeTickWatcher) (*MsgProducer, error) {
	childCtx, cancelFunc := context.WithCancel(ctx)
	return &MsgProducer{
		ctx:       childCtx,
		cancel:    cancelFunc,
		ttBarrier: ttBarrier,
		watchers:  watchers,
	}, nil
}

func (producer *MsgProducer) broadcastMsg() {
	for {
		select {
		case <-producer.ctx.Done():
			log.Printf("broadcast context done, exit")
		default:
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
}

func (producer *MsgProducer) Start() error {
	err := producer.ttBarrier.Start()
	if err != nil {
		return err
	}

	for _, watcher := range producer.watchers {
		watcher.Start()
	}

	go producer.broadcastMsg()

	return nil
}

func (producer *MsgProducer) Close() {
	producer.cancel()
	producer.ttBarrier.Close()
	for _, watcher := range producer.watchers {
		watcher.Close()
	}
}
