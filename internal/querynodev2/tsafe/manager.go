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

package tsafe

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Manager is the interface for tsafe manager.
type Manager interface {
	Get(vChannel string) (Timestamp, error)
	Set(vChannel string, timestamp Timestamp) error
	Add(ctx context.Context, vChannel string, timestamp Timestamp)
	Remove(ctx context.Context, vChannel string)
	Watch() Listener
	WatchChannel(channel string) Listener

	Min() (string, Timestamp)
}

// tSafeManager implements `Manager` interface.
type tSafeManager struct {
	mu        sync.Mutex             // guards tSafes
	tSafes    map[string]*tSafe      // map[DMLChannel]*tSafe
	listeners map[string][]*listener // map[DMLChannel][]*listener, key "" means all channels.
}

func (t *tSafeManager) Watch() Listener {
	return t.WatchChannel("")
}

func (t *tSafeManager) WatchChannel(channel string) Listener {
	t.mu.Lock()
	defer t.mu.Unlock()
	l := newListener(t, channel)
	t.listeners[channel] = append(t.listeners[channel], l)
	return l
}

func (t *tSafeManager) Add(ctx context.Context, vChannel string, timestamp uint64) {
	ts, _ := tsoutil.ParseTS(timestamp)
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.tSafes[vChannel]; !ok {
		t.tSafes[vChannel] = newTSafe(vChannel, timestamp)
	}
	log.Ctx(ctx).Info("add tSafe done",
		zap.String("channel", vChannel), zap.Time("timestamp", ts))
}

func (t *tSafeManager) Get(vChannel string) (Timestamp, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ts, err := t.get(vChannel)
	if err != nil {
		return 0, err
	}
	return ts.get(), nil
}

func (t *tSafeManager) Set(vChannel string, timestamp Timestamp) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	ts, err := t.get(vChannel)
	if err != nil {
		return fmt.Errorf("set tSafe failed, err = %w", err)
	}
	ts.set(timestamp)
	t.notifyAll(vChannel)
	return nil
}

func (t *tSafeManager) Remove(ctx context.Context, vChannel string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tsafe, ok := t.tSafes[vChannel]
	if ok {
		tsafe.close()
	}
	for _, l := range t.listeners[vChannel] {
		l.close()
	}
	delete(t.tSafes, vChannel)
	delete(t.listeners, vChannel)
	log.Ctx(ctx).Info("remove tSafe replica",
		zap.String("vChannel", vChannel))
}

func (t *tSafeManager) Min() (string, Timestamp) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var minChannel string
	minTt := MaxTimestamp
	for channel, tsafe := range t.tSafes {
		t := tsafe.get()
		if t < minTt && t != 0 {
			minChannel = channel
			minTt = t
		}
	}
	return minChannel, minTt
}

func (t *tSafeManager) get(vChannel string) (*tSafe, error) {
	if _, ok := t.tSafes[vChannel]; !ok {
		return nil, fmt.Errorf("cannot found tSafer, vChannel = %s", vChannel)
	}
	return t.tSafes[vChannel], nil
}

// since notifyAll called by setTSafe, no need to lock
func (t *tSafeManager) notifyAll(channel string) {
	for _, l := range t.listeners[""] {
		l.nonBlockingNotify()
	}
	for _, l := range t.listeners[channel] {
		l.nonBlockingNotify()
	}
}

func NewTSafeReplica() Manager {
	replica := &tSafeManager{
		tSafes:    make(map[string]*tSafe),
		listeners: make(map[string][]*listener),
	}
	return replica
}
