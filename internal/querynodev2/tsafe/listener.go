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

type Listener interface {
	On() <-chan struct{}
	Close()
}

type listener struct {
	tsafe   *tSafeManager
	channel string
	ch      chan struct{}
}

func (l *listener) On() <-chan struct{} {
	return l.ch
}

func (l *listener) Close() {
	l.tsafe.mu.Lock()
	defer l.tsafe.mu.Unlock()
	l.close()
}

// close remove the listener from the tSafeReplica without lock
func (l *listener) close() {
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

func newListener(tsafe *tSafeManager, channel string) *listener {
	return &listener{
		tsafe:   tsafe,
		channel: channel,
		ch:      make(chan struct{}, 1),
	}
}
