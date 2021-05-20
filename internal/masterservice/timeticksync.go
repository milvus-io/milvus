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
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type timetickSync struct {
	lock          sync.Mutex
	ctx           context.Context
	msFactory     msgstream.Factory
	proxyTimeTick map[typeutil.UniqueID]*internalpb.ChannelTimeTickMsg
	chanStream    map[string]msgstream.MsgStream
	sendChan      chan map[typeutil.UniqueID]*internalpb.ChannelTimeTickMsg
	addChan       <-chan *sessionutil.Session
	delChan       <-chan *sessionutil.Session
}

func newTimeTickSync(ctx context.Context, factory msgstream.Factory) (*timetickSync, error) {
	const proxyNodePrefix = "proxynode"
	gm := sessionutil.GlobalSessionManager()
	addChan, delChan := gm.WatchServices(ctx, proxyNodePrefix)
	gm.UpdateSessions(proxyNodePrefix)

	tss := timetickSync{
		lock:          sync.Mutex{},
		ctx:           ctx,
		msFactory:     factory,
		proxyTimeTick: make(map[typeutil.UniqueID]*internalpb.ChannelTimeTickMsg),
		chanStream:    make(map[string]msgstream.MsgStream),
		sendChan:      make(chan map[typeutil.UniqueID]*internalpb.ChannelTimeTickMsg),
		addChan:       addChan,
		delChan:       delChan,
	}

	pnSessions := gm.GetSessions(proxyNodePrefix)
	for _, pns := range pnSessions {
		tss.proxyTimeTick[pns.ServerID] = nil
	}
	return &tss, nil
}

// sendToChannel send all channels' timetick to sendChan
// lock is needed by the invoker
func (t *timetickSync) sendToChannel() {
	for _, v := range t.proxyTimeTick {
		if v == nil {
			return
		}
	}
	// clear proxyTimeTick and send a clone
	ptt := make(map[typeutil.UniqueID]*internalpb.ChannelTimeTickMsg)
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
	_, ok := t.proxyTimeTick[in.Base.SourceID]
	if !ok {
		return fmt.Errorf("Skip ChannelTimeTickMsg from un-recognized proxy node %d", in.Base.SourceID)
	}
	t.proxyTimeTick[in.Base.SourceID] = in
	t.sendToChannel()
	return nil
}

// StartWatch watch proxy node change and process all channels' timetick msg
func (t *timetickSync) StartWatch() {
	for {
		select {
		case <-t.ctx.Done():
			log.Debug("timetickSync context done", zap.Error(t.ctx.Err()))
			return
		case pns := <-t.addChan:
			t.lock.Lock()
			t.proxyTimeTick[pns.ServerID] = nil
			t.lock.Unlock()
		case pns := <-t.delChan:
			t.lock.Lock()
			delete(t.proxyTimeTick, pns.ServerID)
			t.sendToChannel()
			t.lock.Unlock()
		case ptt, ok := <-t.sendChan:
			if !ok {
				log.Debug("timetickSync sendChan closed", zap.Error(t.ctx.Err()))
				return
			}
			// reduce each channel to get min timestamp
			chanName2TimeTickMap := make(map[string]typeutil.Timestamp)
			for _, cttMsg := range ptt {
				chanNum := len(cttMsg.ChannelNames)
				for i := 0; i < chanNum; i++ {
					name := cttMsg.ChannelNames[i]
					ts := cttMsg.Timestamps[i]
					cts, ok := chanName2TimeTickMap[name]
					if !ok || ts < cts {
						chanName2TimeTickMap[name] = ts
					}
				}
			}
			// send timetick msg to msg stream
			for chanName, chanTs := range chanName2TimeTickMap {
				if err := t.SendChannelTimeTick(chanName, chanTs); err != nil {
					log.Debug("SendChannelTimeTick fail", zap.Error(t.ctx.Err()))
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
			SourceID:  int64(Params.NodeID),
		},
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)

	t.lock.Lock()
	defer t.lock.Unlock()

	// send timetick msg to msg stream
	var err error
	var stream msgstream.MsgStream
	stream, ok := t.chanStream[chanName]
	if !ok {
		stream, err = t.msFactory.NewMsgStream(t.ctx)
		if err != nil {
			return err
		}
		stream.AsProducer([]string{chanName})
		t.chanStream[chanName] = stream
	}
	return stream.Broadcast(&msgPack)
}

// GetProxyNodeNum return the num of detected proxy node
func (t *timetickSync) GetProxyNodeNum() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return len(t.proxyTimeTick)
}

// GetChanNum return the num of channel
func (t *timetickSync) GetChanNum() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return len(t.chanStream)
}
