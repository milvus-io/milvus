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
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	ms "github.com/milvus-io/milvus/internal/msgstream"
)

type (
	TimeTickBarrier interface {
		GetTimeTick() (Timestamp, error)
		Start() error
		Close()
		AddPeer(peerID UniqueID) error
		TickChan() <-chan Timestamp
	}

	softTimeTickBarrier struct {
		peer2LastTt   map[UniqueID]Timestamp
		peerMtx       sync.RWMutex
		minTtInterval Timestamp
		lastTt        int64
		outTt         chan Timestamp
		ttStream      ms.MsgStream
		ctx           context.Context
		cancel        context.CancelFunc
	}
)

func (ttBarrier *softTimeTickBarrier) TickChan() <-chan Timestamp {
	return ttBarrier.outTt
}

func (ttBarrier *softTimeTickBarrier) AddPeer(peerID UniqueID) error {
	ttBarrier.peerMtx.Lock()
	defer ttBarrier.peerMtx.Unlock()

	_, ok := ttBarrier.peer2LastTt[peerID]
	if ok {
		log.Debug("proxyservice", zap.Int64("no need to add duplicated peer", peerID))
		return nil
	}

	ttBarrier.peer2LastTt[peerID] = 0

	return nil
}

func (ttBarrier *softTimeTickBarrier) GetTimeTick() (Timestamp, error) {
	select {
	case <-ttBarrier.ctx.Done():
		return 0, errors.New("getTimeTick closed")
	case ts, ok := <-ttBarrier.outTt:
		if !ok {
			return 0, errors.New("getTimeTick closed")
		}
		num := len(ttBarrier.outTt)
		for i := 0; i < num; i++ {
			ts, ok = <-ttBarrier.outTt
			if !ok {
				return 0, errors.New("getTimeTick closed")
			}
		}
		atomic.StoreInt64(&(ttBarrier.lastTt), int64(ts))
		return ts, ttBarrier.ctx.Err()
	}
}

func (ttBarrier *softTimeTickBarrier) Start() error {
	go func() {
		for {
			select {
			case <-ttBarrier.ctx.Done():
				log.Warn("TtBarrierStart", zap.Error(ttBarrier.ctx.Err()))
				return

			case ttmsgs := <-ttBarrier.ttStream.Chan():
				ttBarrier.peerMtx.RLock()
				if len(ttmsgs.Msgs) > 0 {
					for _, timetickmsg := range ttmsgs.Msgs {
						ttmsg := timetickmsg.(*ms.TimeTickMsg)
						oldT, ok := ttBarrier.peer2LastTt[ttmsg.Base.SourceID]

						if !ok {
							log.Warn("softTimeTickBarrier", zap.Int64("peerID %d not exist", ttmsg.Base.SourceID))
							continue
						}
						if ttmsg.Base.Timestamp > oldT {
							ttBarrier.peer2LastTt[ttmsg.Base.SourceID] = ttmsg.Base.Timestamp

							// get a legal Timestamp
							ts := ttBarrier.minTimestamp()
							lastTt := atomic.LoadInt64(&(ttBarrier.lastTt))
							if lastTt != 0 && ttBarrier.minTtInterval > ts-Timestamp(lastTt) {
								continue
							}
							ttBarrier.outTt <- ts
						}
					}
				}
				ttBarrier.peerMtx.RUnlock()
			}
		}
	}()

	ttBarrier.ttStream.Start()

	return nil
}

func newSoftTimeTickBarrier(ctx context.Context,
	ttStream ms.MsgStream,
	peerIds []UniqueID,
	minTtInterval Timestamp) TimeTickBarrier {

	if len(peerIds) <= 0 {
		log.Warn("[newSoftTimeTickBarrier] Warning: peerIds is empty!")
		//return nil
	}

	sttbarrier := softTimeTickBarrier{}
	sttbarrier.minTtInterval = minTtInterval
	sttbarrier.ttStream = ttStream
	sttbarrier.outTt = make(chan Timestamp, 1024)
	sttbarrier.ctx, sttbarrier.cancel = context.WithCancel(ctx)
	sttbarrier.peer2LastTt = make(map[UniqueID]Timestamp)
	for _, id := range peerIds {
		sttbarrier.peer2LastTt[id] = Timestamp(0)
	}
	if len(peerIds) != len(sttbarrier.peer2LastTt) {
		log.Warn("[newSoftTimeTickBarrier] Warning: there are duplicate peerIds!")
	}

	return &sttbarrier
}

func (ttBarrier *softTimeTickBarrier) Close() {
	ttBarrier.cancel()
}

func (ttBarrier *softTimeTickBarrier) minTimestamp() Timestamp {
	tempMin := Timestamp(math.MaxUint64)
	for _, tt := range ttBarrier.peer2LastTt {
		if tt < tempMin {
			tempMin = tt
		}
	}
	return tempMin
}
