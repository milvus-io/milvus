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
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type target struct {
	vchannel           string
	ch                 chan *MsgPack
	subPos             SubPos
	pos                *Pos
	filterSameTimeTick bool
	latestTimeTick     uint64
	isLagged           bool

	closeMu   sync.Mutex
	closeOnce sync.Once
	closed    bool
	maxLag    time.Duration
	timer     *time.Timer

	cancelCh lifetime.SafeChan
}

func newTarget(streamConfig *StreamConfig, filterSameTimeTick bool) *target {
	maxTolerantLag := paramtable.Get().MQCfg.MaxTolerantLag.GetAsDuration(time.Second)
	t := &target{
		vchannel:           streamConfig.VChannel,
		ch:                 make(chan *MsgPack, paramtable.Get().MQCfg.TargetBufSize.GetAsInt()),
		subPos:             streamConfig.SubPos,
		pos:                streamConfig.Pos,
		filterSameTimeTick: filterSameTimeTick,
		latestTimeTick:     0,
		cancelCh:           lifetime.NewSafeChan(),
		maxLag:             maxTolerantLag,
		timer:              time.NewTimer(maxTolerantLag),
	}
	t.closed = false
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
		mlog.Info(context.TODO(), "close target chan", mlog.String("vchannel", t.vchannel))
	})
}

func (t *target) send(pack *MsgPack) error {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	if t.closed {
		return nil
	}

	if t.filterSameTimeTick {
		if pack.EndPositions[0].GetTimestamp() <= t.latestTimeTick {
			if len(pack.Msgs) > 0 {
				// only filter out the msg that is only timetick message,
				// So it's a unexpected behavior if the msgs is not empty
				mlog.Warn(context.TODO(), "some data lost when time tick filtering",
					mlog.String("vchannel", t.vchannel),
					mlog.Uint64("latestTimeTick", t.latestTimeTick),
					mlog.Uint64("packEndTs", pack.EndPositions[0].GetTimestamp()),
					mlog.Int("msgCount", len(pack.Msgs)),
				)
			}
			// filter out the msg that is already sent with the same timetick.
			return nil
		}
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
		mlog.Info(context.TODO(), "target closed", mlog.String("vchannel", t.vchannel))
		return nil
	case <-t.timer.C:
		t.isLagged = true
		return merr.WrapErrMqInternalMsg("send target timeout, vchannel=%s, timeout=%s, beginTs=%d, endTs=%d, latestTimeTick=%d", t.vchannel, t.maxLag, pack.BeginTs, pack.EndTs, t.latestTimeTick)
	case t.ch <- pack:
		if len(pack.EndPositions) > 0 {
			t.latestTimeTick = pack.EndPositions[0].GetTimestamp()
		}
		return nil
	}
}
