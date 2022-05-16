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

package querynode

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
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
	channel Channel
	tSafeMu sync.Mutex // guards all fields
	tSafe   Timestamp
	watcher *tSafeWatcher
}

func newTSafe(channel Channel) *tSafe {
	return &tSafe{
		channel: channel,
		tSafe:   typeutil.ZeroTimestamp,
	}
}

func (ts *tSafe) registerTSafeWatcher(t *tSafeWatcher) error {
	ts.tSafeMu.Lock()
	defer ts.tSafeMu.Unlock()
	if ts.watcher != nil {
		log.Warn("tSafeWatcher register more than once", zap.String("channel", ts.channel))
		return fmt.Errorf("tSafeWatcher has been existed, channel = %s", ts.channel)
	}
	ts.watcher = t
	return nil
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
	if ts.watcher != nil {
		ts.watcher.notify()
	}
	//log.Debug("set tSafe done",
	//	zap.Any("channel", ts.channel),
	//	zap.Any("t", m.t))
}

// close calls the close method of internal watcher if any
func (ts *tSafe) close() {
	if ts.watcher != nil {
		ts.watcher.close()
	}
}
