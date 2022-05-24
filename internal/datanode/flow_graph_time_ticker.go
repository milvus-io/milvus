// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datanode

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

type sendTimeTick func(Timestamp, []int64) error

// mergedTimeTickerSender reduces time ticker sending rate when datanode is doing `fast-forwarding`
// it makes sure time ticker send at most 10 times a second (1tick/100millisecond)
// and the last time tick is always sent
type mergedTimeTickerSender struct {
	ts         uint64
	segmentIDs map[int64]struct{}
	lastSent   time.Time
	mu         sync.Mutex

	cond *sync.Cond   // condition to send timeticker
	send sendTimeTick // actual sender logic

	wg        sync.WaitGroup
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newMergedTimeTickerSender(send sendTimeTick) *mergedTimeTickerSender {
	mt := &mergedTimeTickerSender{
		ts:         0, // 0 for not tt send
		segmentIDs: make(map[int64]struct{}),
		cond:       sync.NewCond(&sync.Mutex{}),
		send:       send,
		closeCh:    make(chan struct{}),
	}
	mt.wg.Add(2)
	go mt.tick()
	go mt.work()

	return mt
}

func (mt *mergedTimeTickerSender) bufferTs(ts Timestamp, segmentIDs []int64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.ts = ts
	for _, sid := range segmentIDs {
		mt.segmentIDs[sid] = struct{}{}
	}

	if !mt.lastSent.IsZero() && time.Since(mt.lastSent) > time.Millisecond*100 {
		mt.cond.L.Lock()
		defer mt.cond.L.Unlock()
		mt.cond.Signal()
	}
}

func (mt *mergedTimeTickerSender) tick() {
	defer mt.wg.Done()
	// this duration might be configuable in the future
	t := time.NewTicker(time.Millisecond * 100) // 100 millisecond, 1/2 of rootcoord timetick duration
	defer t.Stop()
	for {
		select {
		case <-t.C:
			mt.cond.L.Lock()
			mt.cond.Signal() // allow worker to check every 0.1s
			mt.cond.L.Unlock()
		case <-mt.closeCh:
			return
		}
	}
}

func (mt *mergedTimeTickerSender) isClosed() bool {
	select {
	case <-mt.closeCh:
		return true
	default:
		return false
	}
}

func (mt *mergedTimeTickerSender) work() {
	defer mt.wg.Done()
	lastTs := uint64(0)
	for {
		mt.cond.L.Lock()
		if mt.isClosed() {
			mt.cond.L.Unlock()
			return
		}
		mt.cond.Wait()
		mt.cond.L.Unlock()

		mt.mu.Lock()
		if mt.ts != lastTs {
			var sids []int64
			for sid := range mt.segmentIDs {
				sids = append(sids, sid)
			}
			mt.segmentIDs = make(map[int64]struct{})
			lastTs = mt.ts
			mt.lastSent = time.Now()

			if err := mt.send(mt.ts, sids); err != nil {
				log.Error("send hard time tick failed", zap.Error(err))
			}
		}
		mt.mu.Unlock()
	}
}

func (mt *mergedTimeTickerSender) close() {
	mt.closeOnce.Do(func() {
		mt.cond.L.Lock()
		close(mt.closeCh)
		mt.cond.Broadcast()
		mt.cond.L.Unlock()
		mt.wg.Wait()
	})
}
