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

	"go.uber.org/atomic"
)

type sendTimeTick func(Timestamp) error

// mergedTimeTickerSender reduces time ticker sending rate when datanode is doing `fast-forwarding`
// it makes sure time ticker send at most 10 times a second (1tick/100millisecond)
// and the last time tick is always sent
type mergedTimeTickerSender struct {
	ts   atomic.Uint64 // current ts value
	cond *sync.Cond    // condition to send timeticker
	send sendTimeTick  // actual sender logic

	lastSent  time.Time
	lastMut   sync.RWMutex
	wg        sync.WaitGroup
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newMergedTimeTickerSender(send sendTimeTick) *mergedTimeTickerSender {
	mt := &mergedTimeTickerSender{
		cond:    sync.NewCond(&sync.Mutex{}),
		send:    send,
		closeCh: make(chan struct{}),
	}
	mt.ts.Store(0) // 0 for not tt send

	mt.wg.Add(2)
	go mt.tick()
	go mt.work()

	return mt
}

func (mt *mergedTimeTickerSender) bufferTs(ts Timestamp) {
	mt.ts.Store(ts)
	mt.lastMut.RLock()
	defer mt.lastMut.RUnlock()

	if !mt.lastSent.IsZero() && time.Since(mt.lastSent) > time.Millisecond*100 {
		mt.cond.Signal()
	}
}

func (mt *mergedTimeTickerSender) tick() {
	defer mt.wg.Done()
	// this duration might be configuable in the future
	t := time.Tick(time.Millisecond * 100) // 100 millisecond, 1/2 of rootcoord timetick duration
	for {
		select {
		case <-t:
			mt.cond.Signal() // allow worker to check every 0.1s
		case <-mt.closeCh:
			return
		}
	}
}

func (mt *mergedTimeTickerSender) work() {
	defer mt.wg.Done()
	ts, lastTs := uint64(0), uint64(0)
	for {
		select {
		case <-mt.closeCh:
			return
		default:
		}

		mt.cond.L.Lock()
		mt.cond.Wait()
		ts = mt.ts.Load()
		mt.cond.L.Unlock()
		if ts != lastTs {
			mt.send(ts)
			lastTs = ts
			mt.lastMut.Lock()
			mt.lastSent = time.Now()
			mt.lastMut.Unlock()
		}
	}
}

func (mt *mergedTimeTickerSender) close() {
	mt.closeOnce.Do(func() {
		close(mt.closeCh)
		mt.cond.Broadcast()
		mt.wg.Wait()
	})
}
