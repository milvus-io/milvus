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

package querynode

import (
	"context"
	"math"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

type tSafeWatcher struct {
	notifyChan chan bool
}

func newTSafeWatcher() *tSafeWatcher {
	return &tSafeWatcher{
		notifyChan: make(chan bool, 1),
	}
}

func (watcher *tSafeWatcher) notify() {
	if len(watcher.notifyChan) == 0 {
		watcher.notifyChan <- true
		//log.Debug("tSafe watcher notify done")
	}
}

// deprecated
func (watcher *tSafeWatcher) hasUpdate() {
	<-watcher.notifyChan
}

func (watcher *tSafeWatcher) watcherChan() <-chan bool {
	return watcher.notifyChan
}

type tSafer interface {
	get() Timestamp
	set(id UniqueID, t Timestamp)
	registerTSafeWatcher(t *tSafeWatcher)
	start()
	close()
	removeRecord(partitionID UniqueID)
}

type tSafeMsg struct {
	t  Timestamp
	id UniqueID // collectionID or partitionID
}

type tSafe struct {
	ctx         context.Context
	cancel      context.CancelFunc
	channel     Channel
	tSafeMu     sync.Mutex // guards all fields
	tSafe       Timestamp
	watcherList []*tSafeWatcher
	tSafeChan   chan tSafeMsg
	tSafeRecord map[UniqueID]Timestamp
}

func newTSafe(ctx context.Context, channel Channel) tSafer {
	ctx1, cancel := context.WithCancel(ctx)
	const channelSize = 4096

	var t tSafer = &tSafe{
		ctx:         ctx1,
		cancel:      cancel,
		channel:     channel,
		watcherList: make([]*tSafeWatcher, 0),
		tSafeChan:   make(chan tSafeMsg, channelSize),
		tSafeRecord: make(map[UniqueID]Timestamp),
	}
	return t
}

func (ts *tSafe) start() {
	go func() {
		for {
			select {
			case <-ts.ctx.Done():
				log.Debug("tSafe context done",
					zap.Any("channel", ts.channel),
				)
				return
			case m := <-ts.tSafeChan:
				ts.tSafeMu.Lock()
				ts.tSafeRecord[m.id] = m.t
				var tmpT Timestamp = math.MaxUint64
				for _, t := range ts.tSafeRecord {
					if t <= tmpT {
						tmpT = t
					}
				}
				ts.tSafe = tmpT
				for _, watcher := range ts.watcherList {
					watcher.notify()
				}

				//log.Debug("set tSafe done",
				//	zap.Any("id", m.id),
				//	zap.Any("channel", ts.channel),
				//	zap.Any("t", m.t),
				//	zap.Any("tSafe", ts.tSafe))
				ts.tSafeMu.Unlock()
			}
		}
	}()
}

// removeRecord for deleting the old partition which has been released,
// if we don't delete this, tSafe would always be the old partition's timestamp
// (because we set tSafe to the minimum timestamp) from old partition
// flow graph which has been closed and would not update tSafe any more.
// removeRecord should be called when flow graph is been removed.
func (ts *tSafe) removeRecord(partitionID UniqueID) {
	ts.tSafeMu.Lock()
	defer ts.tSafeMu.Unlock()

	log.Debug("remove tSafeRecord",
		zap.Any("partitionID", partitionID),
	)
	delete(ts.tSafeRecord, partitionID)
}

func (ts *tSafe) registerTSafeWatcher(t *tSafeWatcher) {
	ts.tSafeMu.Lock()
	defer ts.tSafeMu.Unlock()
	ts.watcherList = append(ts.watcherList, t)
}

func (ts *tSafe) get() Timestamp {
	ts.tSafeMu.Lock()
	defer ts.tSafeMu.Unlock()
	return ts.tSafe
}

func (ts *tSafe) set(id UniqueID, t Timestamp) {
	ts.tSafeMu.Lock()
	defer ts.tSafeMu.Unlock()

	msg := tSafeMsg{
		t:  t,
		id: id,
	}
	ts.tSafeChan <- msg
}

func (ts *tSafe) close() {
	ts.tSafeMu.Lock()
	defer ts.tSafeMu.Unlock()

	ts.cancel()
	for _, watcher := range ts.watcherList {
		close(watcher.notifyChan)
	}
}
