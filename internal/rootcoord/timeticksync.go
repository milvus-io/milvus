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

package rootcoord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	// TODO: better to be configurable
	enableTtChecker        = true
	timeTickSyncTtInterval = 2 * time.Minute
	ttCheckerName          = "rootTtChecker"
	ttCheckerWarnMsg       = fmt.Sprintf("RootCoord haven't synchronized the time tick for %f minutes", timeTickSyncTtInterval.Minutes())
	ddlSourceID            = UniqueID(-1)
)

type ttHistogram struct {
	sync.Map
}

func newTtHistogram() *ttHistogram {
	return &ttHistogram{}
}

func (h *ttHistogram) update(channel string, ts Timestamp) {
	h.Store(channel, ts)
}

func (h *ttHistogram) get(channel string) Timestamp {
	ts, ok := h.Load(channel)
	if !ok {
		return typeutil.ZeroTimestamp
	}
	return ts.(Timestamp)
}

func (h *ttHistogram) remove(channels ...string) {
	for _, channel := range channels {
		h.Delete(channel)
	}
}

type timetickSync struct {
	ctx      context.Context
	sourceID typeutil.UniqueID

	dmlChannels *dmlChannels // used for insert

	lock           sync.Mutex
	sess2ChanTsMap map[typeutil.UniqueID]*chanTsMsg
	sendChan       chan map[typeutil.UniqueID]*chanTsMsg

	syncedTtHistogram *ttHistogram
}

type chanTsMsg struct {
	chanTsMap map[string]typeutil.Timestamp
	defaultTs typeutil.Timestamp
	cnt       int64
}

func newChanTsMsg(in *internalpb.ChannelTimeTickMsg, cnt int64) *chanTsMsg {
	msg := &chanTsMsg{
		chanTsMap: make(map[string]typeutil.Timestamp),
		defaultTs: in.DefaultTimestamp,
		cnt:       cnt,
	}
	for idx := range in.ChannelNames {
		msg.chanTsMap[in.ChannelNames[idx]] = in.Timestamps[idx]
	}
	return msg
}

func (c *chanTsMsg) getTimetick(channelName string) typeutil.Timestamp {
	if ts, ok := c.chanTsMap[channelName]; ok {
		return ts
	}
	return c.defaultTs
}

func newTimeTickSync(ctx context.Context, sourceID int64, factory msgstream.Factory, chanMap map[typeutil.UniqueID][]string) *timetickSync {
	// if the old channels number used by the user is greater than the set default value currently
	// keep the old channels
	chanNum := getNeedChanNum(Params.RootCoordCfg.DmlChannelNum.GetAsInt(), chanMap)

	// initialize dml channels used for insert
	dmlChannels := newDmlChannels(ctx, factory, Params.CommonCfg.RootCoordDml.GetValue(), int64(chanNum))

	// recover physical channels for all collections
	for collID, chanNames := range chanMap {
		dmlChannels.addChannels(chanNames...)
		log.Info("recover physical channels", zap.Int64("collectionID", collID), zap.Strings("physical channels", chanNames))
	}

	return &timetickSync{
		ctx:      ctx,
		sourceID: sourceID,

		dmlChannels: dmlChannels,

		lock:           sync.Mutex{},
		sess2ChanTsMap: make(map[typeutil.UniqueID]*chanTsMsg),

		// 1 is the most reasonable capacity. In fact, Milvus can only focus on the latest time tick.
		sendChan: make(chan map[typeutil.UniqueID]*chanTsMsg, 1),

		syncedTtHistogram: newTtHistogram(),
	}
}

// sendToChannel send all channels' timetick to sendChan
// lock is needed by the invoker
func (t *timetickSync) sendToChannel() bool {
	if len(t.sess2ChanTsMap) == 0 {
		return false
	}

	// detect whether rootcoord receives ttMsg from all source sessions
	maxCnt := int64(0)
	idleSessionList := make([]typeutil.UniqueID, 0, len(t.sess2ChanTsMap))
	for id, v := range t.sess2ChanTsMap {
		if v == nil {
			idleSessionList = append(idleSessionList, id)
		} else {
			if maxCnt < v.cnt {
				maxCnt = v.cnt
			}
		}
	}

	if len(idleSessionList) > 0 {
		// give warning every 2 second if not get ttMsg from source sessions
		if maxCnt%10 == 0 {
			log.Warn("session idle for long time", zap.Any("idle list", idleSessionList),
				zap.Any("idle time", Params.ProxyCfg.TimeTickInterval.GetAsInt64()*time.Millisecond.Milliseconds()*maxCnt))
		}
		return false
	}

	// clear sess2ChanTsMap and send a clone
	ptt := make(map[typeutil.UniqueID]*chanTsMsg)
	for k, v := range t.sess2ChanTsMap {
		ptt[k] = v
		t.sess2ChanTsMap[k] = nil
	}

	select {
	case t.sendChan <- ptt:
	default:
		// The consumer of `sendChan` haven't completed its operation. If we send the `ptt` here, the consumer will
		// always get an older time tick. The older time tick in `sendChan` will block newer time tick in next window.
		// However, in fact the consumer can only focus on the newest.

		// TODO: maybe a metric should be here.
	}

	return true
}

// UpdateTimeTick check msg validation and send it to local channel
func (t *timetickSync) updateTimeTick(in *internalpb.ChannelTimeTickMsg, reason string) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if len(in.ChannelNames) == 0 && in.DefaultTimestamp == 0 {
		return nil
	}
	if len(in.Timestamps) != len(in.ChannelNames) {
		return fmt.Errorf("invalid TimeTickMsg, timestamp and channelname size mismatch")
	}

	prev, ok := t.sess2ChanTsMap[in.Base.SourceID]
	if !ok {
		return fmt.Errorf("skip ChannelTimeTickMsg from un-recognized session %d", in.Base.SourceID)
	}

	if in.Base.SourceID == t.sourceID {
		if prev != nil && in.DefaultTimestamp < prev.defaultTs {
			log.Warn("timestamp go back", zap.Int64("source id", in.Base.SourceID),
				zap.Uint64("curr ts", in.DefaultTimestamp),
				zap.Uint64("prev ts", prev.defaultTs),
				zap.String("reason", reason))
			return nil
		}
	}

	if prev == nil {
		t.sess2ChanTsMap[in.Base.SourceID] = newChanTsMsg(in, 1)
	} else {
		t.sess2ChanTsMap[in.Base.SourceID] = newChanTsMsg(in, prev.cnt+1)
	}
	t.sendToChannel()
	return nil
}

func (t *timetickSync) addSession(sess *sessionutil.Session) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.sess2ChanTsMap[sess.ServerID] = nil
	log.Info("Add session for timeticksync", zap.Int64("serverID", sess.ServerID))
}

func (t *timetickSync) delSession(sess *sessionutil.Session) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if _, ok := t.sess2ChanTsMap[sess.ServerID]; ok {
		delete(t.sess2ChanTsMap, sess.ServerID)
		log.Info("Remove session from timeticksync", zap.Int64("serverID", sess.ServerID))
		t.sendToChannel()
	}
}

func (t *timetickSync) initSessions(sess []*sessionutil.Session) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.sess2ChanTsMap = make(map[typeutil.UniqueID]*chanTsMsg)
	// Init DDL source
	t.sess2ChanTsMap[ddlSourceID] = nil
	for _, s := range sess {
		t.sess2ChanTsMap[s.ServerID] = nil
		log.Info("Init proxy sessions for timeticksync", zap.Int64("serverID", s.ServerID))
	}
}

// StartWatch watches on session changes and processes timeTick messages of all channels.
func (t *timetickSync) startWatch(wg *sync.WaitGroup) {
	defer wg.Done()

	var checker *timerecord.LongTermChecker
	if enableTtChecker {
		checker = timerecord.NewLongTermChecker(t.ctx, ttCheckerName, timeTickSyncTtInterval, ttCheckerWarnMsg)
		checker.Start()
		defer checker.Stop()
	}

	for {
		select {
		case <-t.ctx.Done():
			log.Info("rootcoord context done", zap.Error(t.ctx.Err()))
			return
		case sessTimetick, ok := <-t.sendChan:
			if !ok {
				log.Info("timetickSync sendChan closed")
				return
			}
			if enableTtChecker {
				checker.Check()
			}
			// reduce each channel to get min timestamp
			local := sessTimetick[ddlSourceID]
			if len(local.chanTsMap) == 0 {
				continue
			}
			hdr := fmt.Sprintf("send ts to %d channels", len(local.chanTsMap))
			tr := timerecord.NewTimeRecorder(hdr)
			wg := sync.WaitGroup{}
			for chanName, ts := range local.chanTsMap {
				wg.Add(1)
				go func(chanName string, ts typeutil.Timestamp) {
					mints := ts
					for _, tt := range sessTimetick {
						currTs := tt.getTimetick(chanName)
						if currTs < mints {
							mints = currTs
						}
					}
					if err := t.sendTimeTickToChannel([]string{chanName}, mints); err != nil {
						log.Warn("SendTimeTickToChannel fail", zap.Error(err))
					} else {
						t.syncedTtHistogram.update(chanName, mints)
					}
					wg.Done()
				}(chanName, ts)
			}
			wg.Wait()
			span := tr.ElapseSpan()
			metrics.RootCoordSyncTimeTickLatency.Observe(float64(span.Milliseconds()))
			// rootcoord send tt msg to all channels every 200ms by default
			if span > Params.ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond) {
				log.Warn("rootcoord send tt to all channels too slowly",
					zap.Int("chanNum", len(local.chanTsMap)), zap.Int64("span", span.Milliseconds()))
			}
		}
	}
}

// SendTimeTickToChannel send each channel's min timetick to msg stream
func (t *timetickSync) sendTimeTickToChannel(chanNames []string, ts typeutil.Timestamp) error {
	func() {
		sub := tsoutil.SubByNow(ts)
		for _, chanName := range chanNames {
			metrics.RootCoordInsertChannelTimeTick.WithLabelValues(chanName).Set(float64(sub))
		}
	}()

	msgPack := msgstream.MsgPack{}
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: ts,
		EndTimestamp:   ts,
		HashValues:     []uint32{0},
	}
	timeTickResult := msgpb.TimeTickMsg{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_TimeTick),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithTimeStamp(ts),
			commonpbutil.WithSourceID(t.sourceID),
		),
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
	if err := t.dmlChannels.broadcast(chanNames, &msgPack); err != nil {
		return err
	}

	return nil
}

// GetSessionNum return the num of detected sessions
func (t *timetickSync) getSessionNum() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return len(t.sess2ChanTsMap)
}

// /////////////////////////////////////////////////////////////////////////////
// getDmlChannelNames returns list of channel names.
func (t *timetickSync) getDmlChannelNames(count int) []string {
	return t.dmlChannels.getChannelNames(count)
}

// GetDmlChannelNum return the num of dml channels
func (t *timetickSync) getDmlChannelNum() int {
	return t.dmlChannels.getChannelNum()
}

// ListDmlChannels return all in-use dml channel names
func (t *timetickSync) listDmlChannels() []string {
	return t.dmlChannels.listChannels()
}

// AddDmlChannels add dml channels
func (t *timetickSync) addDmlChannels(names ...string) {
	t.dmlChannels.addChannels(names...)
	log.Info("add dml channels", zap.Strings("channels", names))
}

// RemoveDmlChannels remove dml channels
func (t *timetickSync) removeDmlChannels(names ...string) {
	t.dmlChannels.removeChannels(names...)
	// t.syncedTtHistogram.remove(names...) // channel ts shouldn't go back.
	log.Info("remove dml channels", zap.Strings("channels", names))
}

// BroadcastDmlChannels broadcasts msg pack into dml channels
func (t *timetickSync) broadcastDmlChannels(chanNames []string, pack *msgstream.MsgPack) error {
	return t.dmlChannels.broadcast(chanNames, pack)
}

// BroadcastMarkDmlChannels broadcasts msg pack into dml channels
func (t *timetickSync) broadcastMarkDmlChannels(chanNames []string, pack *msgstream.MsgPack) (map[string][]byte, error) {
	return t.dmlChannels.broadcastMark(chanNames, pack)
}

func (t *timetickSync) getSyncedTimeTick(channel string) Timestamp {
	return t.syncedTtHistogram.get(channel)
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
