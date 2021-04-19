package proxyservice

import (
	"context"
	"log"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type (
	TimeTick interface {
		Start() error
		Close()
	}

	TimeTickImpl struct {
		ttBarrier TimeTickBarrier
		channels  []msgstream.MsgStream
		wg        sync.WaitGroup
		ctx       context.Context
		cancel    context.CancelFunc
	}
)

func (tt *TimeTickImpl) Start() error {
	log.Println("start time tick ...")
	tt.wg.Add(1)
	go func() {
		defer tt.wg.Done()
		for {
			select {
			case <-tt.ctx.Done():
				log.Println("time tick loop was canceled by context!")
				return
			default:
				current, err := tt.ttBarrier.GetTimeTick()
				if err != nil {
					log.Println("GetTimeTick error: ", err)
					break
				}
				msgPack := msgstream.MsgPack{}
				timeTickMsg := &msgstream.TimeTickMsg{
					BaseMsg: msgstream.BaseMsg{
						HashValues: []uint32{0},
					},
					TimeTickMsg: internalpb2.TimeTickMsg{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_kTimeTick,
							MsgID:     0,
							Timestamp: current,
							SourceID:  0,
						},
					},
				}
				msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
				for _, msg := range msgPack.Msgs {
					log.Println("msg type xxxxxxxxxxxxxxxxxxxxxxxx", msg.Type())
				}
				for _, channel := range tt.channels {
					err = channel.Broadcast(tt.ctx, &msgPack)
					if err != nil {
						log.Println("send time tick error: ", err)
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

func (tt *TimeTickImpl) Close() {
	for _, channel := range tt.channels {
		channel.Close()
	}
	tt.ttBarrier.Close()
	tt.cancel()
	tt.wg.Wait()
}

func newTimeTick(ctx context.Context, ttBarrier TimeTickBarrier, channels ...msgstream.MsgStream) TimeTick {
	ctx1, cancel := context.WithCancel(ctx)
	return &TimeTickImpl{ctx: ctx1, cancel: cancel, ttBarrier: ttBarrier, channels: channels}
}
