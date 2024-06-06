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

package msgdispatcher

import (
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type target struct {
	vchannel string
	ch       chan *MsgPack
	pos      *Pos

	closeMu   sync.Mutex
	closeOnce sync.Once
	closed    bool
}

func newTarget(vchannel string, pos *Pos) *target {
	t := &target{
		vchannel: vchannel,
		ch:       make(chan *MsgPack, paramtable.Get().MQCfg.TargetBufSize.GetAsInt()),
		pos:      pos,
	}
	t.closed = false
	return t
}

func (t *target) close() {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	t.closeOnce.Do(func() {
		t.closed = true
		close(t.ch)
	})
}

func (t *target) send(pack *MsgPack) error {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	if t.closed {
		return nil
	}
	maxTolerantLag := paramtable.Get().MQCfg.MaxTolerantLag.GetAsDuration(time.Second)
	select {
	case <-time.After(maxTolerantLag):
		return fmt.Errorf("send target timeout, vchannel=%s, timeout=%s", t.vchannel, maxTolerantLag)
	case t.ch <- pack:
		return nil
	}
}
