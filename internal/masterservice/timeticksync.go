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

package masterservice

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type timetickSync struct {
	core          *Core
	lock          sync.Mutex
	proxyTimeTick map[typeutil.UniqueID]*channelTimeTickMsg
	sendChan      chan map[typeutil.UniqueID]*channelTimeTickMsg
}

type channelTimeTickMsg struct {
	in       *internalpb.ChannelTimeTickMsg
	timeTick map[string]typeutil.Timestamp
}

func newChannelTimeTickMsg(in *internalpb.ChannelTimeTickMsg) *channelTimeTickMsg {
	msg := &channelTimeTickMsg{
		in:       in,
		timeTick: make(map[string]typeutil.Timestamp),
	}
	for idx := range in.ChannelNames {
		msg.timeTick[in.ChannelNames[idx]] = in.Timestamps[idx]
	}
	return msg
}

func (c *channelTimeTickMsg) getTimetick(channelName string) typeutil.Timestamp {
	tt, ok := c.timeTick[channelName]
	if ok {
		return tt
	}
	return c.in.DefaultTimestamp
}

func newTimeTickSync(core *Core) *timetickSync {
	return &timetickSync{
		lock:          sync.Mutex{},
		core:          core,
		proxyTimeTick: make(map[typeutil.UniqueID]*channelTimeTickMsg),
		sendChan:      make(chan map[typeutil.UniqueID]*channelTimeTickMsg, 16),
	}
}

// sendToChannel send all channels' timetick to sendChan
// lock is needed by the invoker
func (t *timetickSync) sendToChannel() {
	if len(t.proxyTimeTick) == 0 {
		return
	}
	for _, v := range t.proxyTimeTick {
		if v == nil {
			return
		}
	}
	// clear proxyTimeTick and send a clone
	ptt := make(map[typeutil.UniqueID]*channelTimeTickMsg)
	for k, v := range t.proxyTimeTick {
		ptt[k] = v
		t.proxyTimeTick[k] = nil
	}
	t.sendChan <- ptt
}

// UpdateTimeTick check msg validation and send it to local channel
func (t *timetickSync) UpdateTimeTick(in *internalpb.ChannelTimeTickMsg) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if len(in.ChannelNames) == 0 {
		return nil
	}
	if len(in.Timestamps) != len(in.ChannelNames) {
		return fmt.Errorf("Invalid TimeTickMsg")
	}

	prev, ok := t.proxyTimeTick[in.Base.SourceID]
	if !ok {
		return fmt.Errorf("Skip ChannelTimeTickMsg from un-recognized proxy node %d", in.Base.SourceID)
	}
	if in.Base.SourceID == t.core.session.ServerID {
		if prev != nil && prev.in.DefaultTimestamp >= in.DefaultTimestamp {
			log.Debug("timestamp go back", zap.Int64("source id", in.Base.SourceID), zap.Uint64("prev ts", prev.in.DefaultTimestamp), zap.Uint64("curr ts", in.DefaultTimestamp))
			return nil
		}
	}
	if in.DefaultTimestamp == 0 {
		mints := minTimeTick(in.Timestamps...)
		log.Debug("default time stamp is zero, set it to the min value of inputs", zap.Int64("proxy id", in.Base.SourceID), zap.Uint64("min ts", mints))
		in.DefaultTimestamp = mints
	}

	t.proxyTimeTick[in.Base.SourceID] = newChannelTimeTickMsg(in)
	t.sendToChannel()
	return nil
}

func (t *timetickSync) AddProxyNode(sess *sessionutil.Session) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.proxyTimeTick[sess.ServerID] = nil
}

func (t *timetickSync) DelProxyNode(sess *sessionutil.Session) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if _, ok := t.proxyTimeTick[sess.ServerID]; ok {
		delete(t.proxyTimeTick, sess.ServerID)
		t.sendToChannel()
	}
}

func (t *timetickSync) GetProxyNodes(sess []*sessionutil.Session) {
	t.lock.Lock()
	defer t.lock.Unlock()
	for _, s := range sess {
		t.proxyTimeTick[s.ServerID] = nil
	}
}

// StartWatch watch proxy node change and process all channels' timetick msg
func (t *timetickSync) StartWatch() {
	for {
		select {
		case <-t.core.ctx.Done():
			log.Debug("master service context done", zap.Error(t.core.ctx.Err()))
			return
		case ptt, ok := <-t.sendChan:
			if !ok {
				log.Debug("timetickSync sendChan closed")
				return
			}

			// reduce each channel to get min timestamp
			mtt := ptt[t.core.session.ServerID]
			for _, chanName := range mtt.in.ChannelNames {
				mints := mtt.getTimetick(chanName)
				for _, tt := range ptt {
					ts := tt.getTimetick(chanName)
					if ts < mints {
						mints = ts
					}
				}
				if err := t.SendChannelTimeTick(chanName, mints); err != nil {
					log.Debug("SendChannelTimeTick fail", zap.Error(err))
				}
			}

		}
	}
}

// SendChannelTimeTick send each channel's min timetick to msg stream
func (t *timetickSync) SendChannelTimeTick(chanName string, ts typeutil.Timestamp) error {
	msgPack := msgstream.MsgPack{}
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: ts,
		EndTimestamp:   ts,
		HashValues:     []uint32{0},
	}
	timeTickResult := internalpb.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
			MsgID:     0,
			Timestamp: ts,
			SourceID:  t.core.session.ServerID,
		},
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)

	err := t.core.dmlChannels.Broadcast(chanName, &msgPack)
	if err == nil {
		metrics.MasterInsertChannelTimeTick.WithLabelValues(chanName).Set(float64(tsoutil.Mod24H(ts)))
	}
	return err
}

// GetProxyNodeNum return the num of detected proxy node
func (t *timetickSync) GetProxyNodeNum() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return len(t.proxyTimeTick)
}

// GetChanNum return the num of channel
func (t *timetickSync) GetChanNum() int {
	return t.core.dmlChannels.GetNumChannles()
}

func minTimeTick(tt ...typeutil.Timestamp) typeutil.Timestamp {
	var ret typeutil.Timestamp
	for _, t := range tt {
		if ret == 0 {
			ret = t
		} else {
			if t < ret {
				ret = t
			}
		}
	}
	return ret
}
