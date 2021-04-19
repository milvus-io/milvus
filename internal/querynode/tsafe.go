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
	}
}

func (watcher *tSafeWatcher) hasUpdate() {
	<-watcher.notifyChan
}

type tSafer interface {
	get() Timestamp
	set(t Timestamp)
	registerTSafeWatcher(t *tSafeWatcher)
	close()
}

type tSafe struct {
	tSafeMu     sync.Mutex // guards all fields
	tSafe       Timestamp
	watcherList []*tSafeWatcher
}

func newTSafe() tSafer {
	var t tSafer = &tSafe{
		watcherList: make([]*tSafeWatcher, 0),
	}
	return t
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
}

func (ts *tSafe) close() {
	ts.tSafeMu.Lock()
	defer ts.tSafeMu.Unlock()

	for _, watcher := range ts.watcherList {
		close(watcher.notifyChan)
	}
}
