// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxyservice

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/timesync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type TimeTick struct {
	ttBarrier timesync.TimeTickBarrier
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
				//for _, msg := range msgPack.Msgs {
				//	log.Debug("proxyservice", zap.Stringer("msg type", msg.Type()))
				//}
				for _, channel := range tt.channels {
					err = channel.Broadcast(&msgPack)
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

	tt.ttBarrier.Start()

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

func newTimeTick(ctx context.Context, ttBarrier timesync.TimeTickBarrier, channels ...msgstream.MsgStream) *TimeTick {
	ctx1, cancel := context.WithCancel(ctx)
	return &TimeTick{ctx: ctx1, cancel: cancel, ttBarrier: ttBarrier, channels: channels}
}
