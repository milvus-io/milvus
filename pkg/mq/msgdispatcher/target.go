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
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type target struct {
	vchannel string
	ch       chan *MsgPack
	pos      *Pos

	closeOnce sync.Once
	closed    atomic.Bool
	sendCnt   int64
	cntLock   *sync.Mutex
	cntCond   *sync.Cond
}

func newTarget(vchannel string, pos *Pos) *target {
	ch := make(chan *MsgPack, paramtable.Get().MQCfg.TargetBufSize.GetAsInt())
	return newTargetWithChan(vchannel, pos, ch)
}

func newTargetWithChan(vchannel string, pos *Pos, packChan chan *MsgPack) *target {
	t := &target{
		vchannel: vchannel,
		ch:       packChan,
		pos:      pos,
	}
	t.cntLock = &sync.Mutex{}
	t.cntCond = sync.NewCond(t.cntLock)
	t.closed.Store(false)
	return t
}

func (t *target) close() {
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		// not block the close method
		go func() {
			t.cntLock.Lock()
			for t.sendCnt > 0 {
				t.cntCond.Wait()
			}
			t.cntLock.Unlock()
			close(t.ch)
		}()
	})
}

func (t *target) send(pack *MsgPack) error {
	if t.closed.Load() {
		return nil
	}
	t.cntLock.Lock()
	t.sendCnt++
	t.cntLock.Unlock()
	defer func() {
		t.cntLock.Lock()
		t.sendCnt--
		t.cntLock.Unlock()
		t.cntCond.Signal()
	}()
	maxTolerantLag := paramtable.Get().MQCfg.MaxTolerantLag.GetAsDuration(time.Second)
	select {
	case <-time.After(maxTolerantLag):
		return fmt.Errorf("send target timeout, vchannel=%s, timeout=%s", t.vchannel, maxTolerantLag)
	case t.ch <- pack:
		return nil
	}
}
