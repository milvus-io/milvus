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

package proxy

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type tickCheckFunc = func(Timestamp) bool

type timeTick struct {
	lastTick    Timestamp
	currentTick Timestamp
	interval    time.Duration

	pulsarProducer pulsar.Producer

	tsoAllocator  *TimestampAllocator
	tickMsgStream msgstream.MsgStream
	msFactory     msgstream.Factory

	peerID    UniqueID
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    func()
	timer     *time.Ticker
	tickLock  sync.RWMutex
	checkFunc tickCheckFunc
}

func newTimeTick(ctx context.Context,
	tsoAllocator *TimestampAllocator,
	interval time.Duration,
	checkFunc tickCheckFunc,
	factory msgstream.Factory) *timeTick {
	ctx1, cancel := context.WithCancel(ctx)
	t := &timeTick{
		ctx:          ctx1,
		cancel:       cancel,
		tsoAllocator: tsoAllocator,
		interval:     interval,
		peerID:       Params.ProxyID,
		checkFunc:    checkFunc,
		msFactory:    factory,
	}

	t.tickMsgStream, _ = t.msFactory.NewMsgStream(t.ctx)
	t.tickMsgStream.AsProducer(Params.ProxyTimeTickChannelNames)
	log.Debug("proxy", zap.Strings("proxy AsProducer", Params.ProxyTimeTickChannelNames))
	return t
}

func (tt *timeTick) tick() error {
	if tt.lastTick == tt.currentTick {
		ts, err := tt.tsoAllocator.AllocOne()
		if err != nil {
			return err
		}
		tt.currentTick = ts
	}

	if !tt.checkFunc(tt.currentTick) {
		return nil
	}
	msgPack := msgstream.MsgPack{}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{uint32(Params.ProxyID)},
		},
		TimeTickMsg: internalpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     0,
				Timestamp: tt.currentTick,
				SourceID:  tt.peerID,
			},
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
	err := tt.tickMsgStream.Produce(&msgPack)
	if err != nil {
		log.Warn("proxy", zap.String("error", err.Error()))
	}
	tt.tickLock.Lock()
	defer tt.tickLock.Unlock()
	tt.lastTick = tt.currentTick
	return nil
}

func (tt *timeTick) tickLoop() {
	defer tt.wg.Done()
	tt.timer = time.NewTicker(tt.interval)
	for {
		select {
		case <-tt.timer.C:
			if err := tt.tick(); err != nil {
				log.Warn("timeTick error")
			}
		case <-tt.ctx.Done():
			return
		}
	}
}

func (tt *timeTick) LastTick() Timestamp {
	tt.tickLock.RLock()
	defer tt.tickLock.RUnlock()
	return tt.lastTick
}

func (tt *timeTick) Start() error {
	tt.lastTick = 0
	ts, err := tt.tsoAllocator.AllocOne()
	if err != nil {
		return err
	}

	tt.currentTick = ts
	tt.tickMsgStream.Start()
	tt.wg.Add(1)
	go tt.tickLoop()
	return nil
}

func (tt *timeTick) Close() {
	if tt.timer != nil {
		tt.timer.Stop()
	}
	tt.cancel()
	tt.tickMsgStream.Close()
	tt.wg.Wait()
}
