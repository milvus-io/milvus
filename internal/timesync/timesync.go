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

package timesync

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/logutil"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/log"
	ms "github.com/milvus-io/milvus/internal/msgstream"
)

type (
	Timestamp = typeutil.Timestamp
	UniqueID  = typeutil.UniqueID

	TimeTickBarrier interface {
		GetTimeTick() (Timestamp, error)
		Start()
		Close()
		AddPeer(peerID UniqueID) error
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
		wg            sync.WaitGroup
	}

	hardTimeTickBarrier struct {
		peer2Tt   map[UniqueID]Timestamp
		peer2TtMu sync.Mutex
		outTt     chan Timestamp
		ttStream  ms.MsgStream
		ctx       context.Context
		cancel    context.CancelFunc
		wg        sync.WaitGroup
	}
)

func NewSoftTimeTickBarrier(ctx context.Context, ttStream ms.MsgStream, peerIds []UniqueID, minTtInterval Timestamp) *softTimeTickBarrier {
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

func (ttBarrier *softTimeTickBarrier) Start() {
	ttBarrier.wg.Add(1)
	go func() {
		defer ttBarrier.wg.Done()
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
}

func (ttBarrier *softTimeTickBarrier) Close() {
	ttBarrier.cancel()
	ttBarrier.wg.Wait()
	ttBarrier.ttStream.Close()
}

func (ttBarrier *softTimeTickBarrier) AddPeer(peerID UniqueID) error {
	ttBarrier.peerMtx.Lock()
	defer ttBarrier.peerMtx.Unlock()

	_, ok := ttBarrier.peer2LastTt[peerID]
	if ok {
		log.Debug("softTimeTickBarrier.AddPeer", zap.Int64("no need to add duplicated peer", peerID))
		return nil
	}

	ttBarrier.peer2LastTt[peerID] = 0

	return nil
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

func (ttBarrier *hardTimeTickBarrier) GetTimeTick() (Timestamp, error) {
	select {
	case <-ttBarrier.ctx.Done():
		return 0, errors.New("getTimeTick closed")
	case ts, ok := <-ttBarrier.outTt:
		if !ok {
			return 0, errors.New("getTimeTick closed")
		}
		return ts, ttBarrier.ctx.Err()
	}
}

func (ttBarrier *hardTimeTickBarrier) Start() {
	// Last timestamp synchronized
	ttBarrier.wg.Add(1)
	state := Timestamp(0)
	go func(ctx context.Context) {
		defer logutil.LogPanic()
		defer ttBarrier.wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Debug("[TtBarrierStart] shut down", zap.Error(ttBarrier.ctx.Err()))
				return
			default:
			}
			ttmsgs := ttBarrier.ttStream.Consume()

			if ttmsgs != nil && len(ttmsgs.Msgs) > 0 {
				log.Debug("receive tt msg")
				ttBarrier.peer2TtMu.Lock()
				for _, timetickmsg := range ttmsgs.Msgs {
					// Suppose ttmsg.Timestamp from stream is always larger than the previous one,
					// that `ttmsg.Timestamp > oldT`
					ttmsg := timetickmsg.(*ms.TimeTickMsg)

					oldT, ok := ttBarrier.peer2Tt[ttmsg.Base.SourceID]
					if !ok {
						log.Warn("[hardTimeTickBarrier] peerID not exist", zap.Int64("peerID", ttmsg.Base.SourceID))
						continue
					}

					if oldT > state {
						log.Warn("[hardTimeTickBarrier] peer's timestamp ahead",
							zap.Int64("peerID", ttmsg.Base.SourceID), zap.Uint64("timestamp", ttmsg.Base.Timestamp))
					}

					ttBarrier.peer2Tt[ttmsg.Base.SourceID] = ttmsg.Base.Timestamp
					newState := ttBarrier.minTimestamp()
					if newState > state {
						ttBarrier.outTt <- newState
						state = newState
					}
				}
				ttBarrier.peer2TtMu.Unlock()
			}
		}
	}(ttBarrier.ctx)
}

func (ttBarrier *hardTimeTickBarrier) Close() {
	ttBarrier.cancel()
	ttBarrier.wg.Wait()
}

func (ttBarrier *hardTimeTickBarrier) minTimestamp() Timestamp {
	tempMin := Timestamp(math.MaxUint64)
	for _, tt := range ttBarrier.peer2Tt {
		if tt < tempMin {
			tempMin = tt
		}
	}
	return tempMin
}

func (ttBarrier *hardTimeTickBarrier) AddPeer(peerID UniqueID) error {
	ttBarrier.peer2TtMu.Lock()
	defer ttBarrier.peer2TtMu.Unlock()
	if _, ok := ttBarrier.peer2Tt[peerID]; ok {
		return fmt.Errorf("peer %d already exist", peerID)
	}
	ttBarrier.peer2Tt[peerID] = Timestamp(0)
	return nil
}

func NewHardTimeTickBarrier(ctx context.Context, ttStream ms.MsgStream, peerIds []UniqueID) *hardTimeTickBarrier {
	ttbarrier := hardTimeTickBarrier{}
	ttbarrier.ttStream = ttStream
	ttbarrier.outTt = make(chan Timestamp, 1024)

	ttbarrier.peer2Tt = make(map[UniqueID]Timestamp)
	ttbarrier.ctx, ttbarrier.cancel = context.WithCancel(ctx)
	for _, id := range peerIds {
		ttbarrier.peer2Tt[id] = Timestamp(0)
	}
	if len(peerIds) != len(ttbarrier.peer2Tt) {
		log.Warn("[newHardTimeTickBarrier] there are duplicate peerIds!", zap.Int64s("peerIDs", peerIds))
	}

	return &ttbarrier
}
