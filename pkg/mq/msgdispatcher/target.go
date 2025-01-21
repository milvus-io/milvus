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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type target struct {
	vchannel string
	ch       chan *MsgPack
	pos      *Pos

	closeMu         sync.Mutex
	closeOnce       sync.Once
	closed          bool
	maxLag          time.Duration
	timer           *time.Timer
	replicateConfig *msgstream.ReplicateConfig

	cancelCh lifetime.SafeChan
}

func newTarget(vchannel string, pos *Pos, replicateConfig *msgstream.ReplicateConfig) *target {
	maxTolerantLag := paramtable.Get().MQCfg.MaxTolerantLag.GetAsDuration(time.Second)
	t := &target{
		vchannel:        vchannel,
		ch:              make(chan *MsgPack, paramtable.Get().MQCfg.TargetBufSize.GetAsInt()),
		pos:             pos,
		cancelCh:        lifetime.NewSafeChan(),
		maxLag:          maxTolerantLag,
		timer:           time.NewTimer(maxTolerantLag),
		replicateConfig: replicateConfig,
	}
	t.closed = false
	if replicateConfig != nil {
		log.Info("have replicate config",
			zap.String("vchannel", vchannel),
			zap.String("replicateID", replicateConfig.ReplicateID))
	}
	return t
}

func (t *target) close() {
	t.cancelCh.Close()
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	t.closeOnce.Do(func() {
		t.closed = true
		t.timer.Stop()
		close(t.ch)
	})
}

func (t *target) send(pack *MsgPack) error {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	if t.closed {
		return nil
	}

	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
	t.timer.Reset(t.maxLag)
	select {
	case <-t.cancelCh.CloseCh():
		log.Info("target closed", zap.String("vchannel", t.vchannel))
		return nil
	case <-t.timer.C:
		return fmt.Errorf("send target timeout, vchannel=%s, timeout=%s", t.vchannel, t.maxLag)
	case t.ch <- pack:
		return nil
	}
}
