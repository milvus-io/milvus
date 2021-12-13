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
	"sync"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type tSafeWatcher struct {
	notifyChan chan bool
	closeCh    chan struct{}
}

func newTSafeWatcher() *tSafeWatcher {
	return &tSafeWatcher{
		notifyChan: make(chan bool, 1),
		closeCh:    make(chan struct{}, 1),
	}
}

func (watcher *tSafeWatcher) notify() {
	if len(watcher.notifyChan) == 0 {
		watcher.notifyChan <- true
	}
}

func (watcher *tSafeWatcher) watcherChan() <-chan bool {
	return watcher.notifyChan
}

func (watcher *tSafeWatcher) close() {
	watcher.closeCh <- struct{}{}
}

type tSafe struct {
	channel     Channel
	tSafeMu     sync.Mutex // guards all fields
	tSafe       Timestamp
	watcherList []*tSafeWatcher
}

func newTSafe(channel Channel) *tSafe {
	return &tSafe{
		channel:     channel,
		watcherList: make([]*tSafeWatcher, 0),
		tSafe:       typeutil.ZeroTimestamp,
	}
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

func (ts *tSafe) set(t Timestamp) {
	ts.tSafeMu.Lock()
	defer ts.tSafeMu.Unlock()
	ts.tSafe = t
	for _, watcher := range ts.watcherList {
		watcher.notify()
	}
	//log.Debug("set tSafe done",
	//	zap.Any("channel", ts.channel),
	//	zap.Any("t", m.t))
}
