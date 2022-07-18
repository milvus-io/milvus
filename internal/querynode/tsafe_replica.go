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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// TSafeReplicaInterface is the interface wrapper of tSafeReplica
type TSafeReplicaInterface interface {
	getTSafe(vChannel Channel) (Timestamp, error)
	setTSafe(vChannel Channel, timestamp Timestamp) error
	addTSafe(vChannel Channel)
	removeTSafe(vChannel Channel)
	Watch() Listener
	WatchChannel(channel Channel) Listener
}

type tSafe struct {
	channel Channel
	mu      sync.RWMutex // guards all fields
	tSafe   Timestamp
	closed  bool
}

func (ts *tSafe) valid() bool {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return !ts.closed
}

func (ts *tSafe) close() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.closed = true
}

func (ts *tSafe) get() Timestamp {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.tSafe
}

func (ts *tSafe) set(t Timestamp) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.tSafe = t
}

func newTSafe(channel Channel) *tSafe {
	return &tSafe{
		channel: channel,
		tSafe:   typeutil.ZeroTimestamp,
	}
}

// tSafeReplica implements `TSafeReplicaInterface` interface.
type tSafeReplica struct {
	mu        sync.Mutex              // guards tSafes
	tSafes    map[Channel]*tSafe      // map[DMLChannel|deltaChannel]*tSafe
	listeners map[Channel][]*listener // map[DMLChannel|deltaChannel][]*listener, key "" means all channels.
}

// since notifyAll called by setTSafe, no need to lock
func (t *tSafeReplica) notifyAll(channel Channel) {
	for _, l := range t.listeners[""] {
		l.nonBlockingNotify()
	}
	for _, l := range t.listeners[channel] {
		l.nonBlockingNotify()
	}
}

func (t *tSafeReplica) Watch() Listener {
	return t.WatchChannel("")
}

func (t *tSafeReplica) WatchChannel(channel Channel) Listener {
	t.mu.Lock()
	defer t.mu.Unlock()
	l := newListener(t, channel)
	t.listeners[channel] = append(t.listeners[channel], l)
	return l
}

func (t *tSafeReplica) getTSafe(vChannel Channel) (Timestamp, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ts, err := t.getTSafePrivate(vChannel)
	if err != nil {
		return 0, err
	}
	return ts.get(), nil
}

func (t *tSafeReplica) setTSafe(vChannel Channel, timestamp Timestamp) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	ts, err := t.getTSafePrivate(vChannel)
	if err != nil {
		return fmt.Errorf("set tSafe failed, err = %w", err)
	}
	ts.set(timestamp)
	t.notifyAll(vChannel)
	return nil
}

func (t *tSafeReplica) getTSafePrivate(vChannel Channel) (*tSafe, error) {
	if _, ok := t.tSafes[vChannel]; !ok {
		return nil, fmt.Errorf("cannot found tSafer, vChannel = %s", vChannel)
	}
	return t.tSafes[vChannel], nil
}

func (t *tSafeReplica) addTSafe(vChannel Channel) {
	log.Info("add tSafe done",
		zap.String("channel", vChannel))
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.tSafes[vChannel]; !ok {
		t.tSafes[vChannel] = newTSafe(vChannel)
	}
}

func (t *tSafeReplica) removeTSafe(vChannel Channel) {
	log.Info("remove tSafe replica",
		zap.String("vChannel", vChannel))
	t.mu.Lock()
	defer t.mu.Unlock()
	tsafe, ok := t.tSafes[vChannel]
	if ok {
		tsafe.close()
	}
	for _, l := range t.listeners[vChannel] {
		l.unregister()
	}
	delete(t.tSafes, vChannel)
	delete(t.listeners, vChannel)
}

func newTSafeReplica() TSafeReplicaInterface {
	var replica TSafeReplicaInterface = &tSafeReplica{
		tSafes:    make(map[string]*tSafe),
		listeners: make(map[string][]*listener),
	}
	return replica
}

type Listener interface {
	On() <-chan struct{}
	Unregister()
}

type listener struct {
	tsafe   *tSafeReplica
	channel Channel
	ch      chan struct{}
}

func (l *listener) On() <-chan struct{} {
	return l.ch
}

func (l *listener) Unregister() {
	l.tsafe.mu.Lock()
	defer l.tsafe.mu.Unlock()
	l.unregister()
}

// unregister remove the listener from the tSafeReplica without lock
func (l *listener) unregister() {
	for i, listen := range l.tsafe.listeners[l.channel] {
		if l == listen {
			close(l.ch)
			l.tsafe.listeners[l.channel] = append(l.tsafe.listeners[l.channel][:i], l.tsafe.listeners[l.channel][i+1:]...)
			break
		}
	}
}

func (l *listener) nonBlockingNotify() {
	select {
	case l.ch <- struct{}{}:
	default:
	}
}

//func (l *listener) blockingNotify() {
//	l.ch <- struct{}{}
//}

func newListener(tsafe *tSafeReplica, channel Channel) *listener {
	return &listener{
		tsafe:   tsafe,
		channel: channel,
		ch:      make(chan struct{}, 1),
	}
}
