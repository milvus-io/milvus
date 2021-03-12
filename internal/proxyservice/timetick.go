package proxyservice

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type TimeTick struct {
	ttBarrier TimeTickBarrier
	channels  []msgstream.MsgStream
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func (tt *TimeTick) Start() error {
	log.Debug("start time tick ...")
	tt.wg.Add(1)
	go func() {
		defer tt.wg.Done()
		for {
			select {
			case <-tt.ctx.Done():
				log.Debug("time tick loop was canceled by context!")
				return
			default:
				current, err := tt.ttBarrier.GetTimeTick()
				if err != nil {
					log.Error("GetTimeTick error", zap.Error(err))
					break
				}
				msgPack := msgstream.MsgPack{}
				timeTickMsg := &msgstream.TimeTickMsg{
					BaseMsg: msgstream.BaseMsg{
						HashValues: []uint32{0},
					},
					TimeTickMsg: internalpb.TimeTickMsg{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_TimeTick,
							MsgID:     0,
							Timestamp: current,
							SourceID:  0,
						},
					},
				}
				msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
				for _, msg := range msgPack.Msgs {
					log.Debug("proxyservice", zap.Stringer("msg type", msg.Type()))
				}
				for _, channel := range tt.channels {
					err = channel.Broadcast(tt.ctx, &msgPack)
					if err != nil {
						log.Error("proxyservice", zap.String("send time tick error", err.Error()))
					}
				}
			}
		}
	}()

	for _, channel := range tt.channels {
		channel.Start()
	}

	err := tt.ttBarrier.Start()
	if err != nil {
		return err
	}

	return nil
}

func (tt *TimeTick) Close() {
	for _, channel := range tt.channels {
		channel.Close()
	}
	tt.ttBarrier.Close()
	tt.cancel()
	tt.wg.Wait()
}

func newTimeTick(ctx context.Context, ttBarrier TimeTickBarrier, channels ...msgstream.MsgStream) *TimeTick {
	ctx1, cancel := context.WithCancel(ctx)
	return &TimeTick{ctx: ctx1, cancel: cancel, ttBarrier: ttBarrier, channels: channels}
}
