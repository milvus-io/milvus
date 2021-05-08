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
	"math"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/timesync"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/stretchr/testify/assert"
)

func ttStreamProduceLoop(ctx context.Context, ttStream msgstream.MsgStream, durationInterval time.Duration, sourceID int64) {
	log.Debug("ttStreamProduceLoop", zap.Any("durationInterval", durationInterval))
	timer := time.NewTicker(durationInterval)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				ttMsgs := &msgstream.MsgPack{
					BeginTs:        0,
					EndTs:          0,
					Msgs:           nil,
					StartPositions: nil,
					EndPositions:   nil,
				}

				currentT := uint64(time.Now().Nanosecond())
				msg := &msgstream.TimeTickMsg{
					BaseMsg: msgstream.BaseMsg{
						Ctx:            ctx,
						BeginTimestamp: 0,
						EndTimestamp:   0,
						HashValues:     nil,
						MsgPosition:    nil,
					},
					TimeTickMsg: internalpb.TimeTickMsg{
						Base: &commonpb.MsgBase{
							MsgType:   0,
							MsgID:     0,
							Timestamp: currentT,
							SourceID:  sourceID,
						},
					},
				}

				ttMsgs.Msgs = append(ttMsgs.Msgs, msg)

				_ = ttStream.Produce(ttMsgs)
				//log.Debug("ttStreamProduceLoop", zap.Any("Send", currentT))
			}
		}
	}()
}

func TestTimeTick_Start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ttStream := msgstream.NewSimpleMsgStream()
	sourceID := 1
	peerIds := []UniqueID{UniqueID(sourceID)}
	interval := 100
	minTtInterval := Timestamp(interval)

	durationInterval := time.Duration(interval*int(math.Pow10(6))) >> 18

	ttBarrier := timesync.NewSoftTimeTickBarrier(ctx, ttStream, peerIds, minTtInterval)
	channels := msgstream.NewSimpleMsgStream()

	tick := newTimeTick(ctx, ttBarrier, channels)
	err := tick.Start()
	ttStreamProduceLoop(ctx, ttStream, durationInterval, int64(sourceID))
	assert.Equal(t, nil, err)
	defer tick.Close()
}

func TestTimeTick_Close(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ttStream := msgstream.NewSimpleMsgStream()
	sourceID := 1
	peerIds := []UniqueID{UniqueID(sourceID)}
	interval := 100
	minTtInterval := Timestamp(interval)

	durationInterval := time.Duration(interval*int(math.Pow10(6))) >> 18

	ttBarrier := timesync.NewSoftTimeTickBarrier(ctx, ttStream, peerIds, minTtInterval)
	channels := msgstream.NewSimpleMsgStream()

	tick := newTimeTick(ctx, ttBarrier, channels)
	err := tick.Start()
	ttStreamProduceLoop(ctx, ttStream, durationInterval, int64(sourceID))
	assert.Equal(t, nil, err)
	defer tick.Close()
}
