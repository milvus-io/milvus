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
	"math"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

var (
	// TODO: better to be configurable
	enableTtChecker        = true
	timeTickSyncTtInterval = 2 * time.Minute
	ttCheckerName          = "rootTtChecker"
	ttCheckerWarnMsg       = fmt.Sprintf("RootCoord haven't synchronized the time tick for %f minutes", timeTickSyncTtInterval.Minutes())
)

type timetickSync struct {
	ctx      context.Context
	sourceID typeutil.UniqueID

	dmlChannels   *dmlChannels // used for insert
	deltaChannels *dmlChannels // used for delete

	lock           sync.Mutex
	sess2ChanTsMap map[typeutil.UniqueID]*chanTsMsg
	sendChan       chan map[typeutil.UniqueID]*chanTsMsg

	// record ddl timetick info
	ddlLock  sync.RWMutex
	ddlMinTs typeutil.Timestamp
	ddlTsSet map[typeutil.Timestamp]struct{}
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
	// initialize dml channels used for insert
	dmlChannels := newDmlChannels(ctx, factory, Params.MsgChannelCfg.RootCoordDml, Params.RootCoordCfg.DmlChannelNum)
	// initialize delta channels used for delete, share Params.DmlChannelNum with dmlChannels
	deltaChannels := newDmlChannels(ctx, factory, Params.MsgChannelCfg.RootCoordDelta, Params.RootCoordCfg.DmlChannelNum)

	// recover physical channels for all collections
	for collID, chanNames := range chanMap {
		dmlChannels.addChannels(chanNames...)
		log.Debug("recover physical channels", zap.Int64("collID", collID), zap.Any("physical channels", chanNames))

		var err error
		deltaChanNames := make([]string, len(chanNames))
		for i, chanName := range chanNames {
			deltaChanNames[i], err = funcutil.ConvertChannelName(chanName, Params.MsgChannelCfg.RootCoordDml, Params.MsgChannelCfg.RootCoordDelta)
			if err != nil {
				log.Error("failed to convert dml channel name to delta channel name", zap.String("chanName", chanName))
				panic("invalid dml channel name " + chanName)
			}
		}
		deltaChannels.addChannels(deltaChanNames...)
		log.Debug("recover delta channels", zap.Int64("collID", collID), zap.Any("delta channels", deltaChanNames))
	}

	return &timetickSync{
		ctx:      ctx,
		sourceID: sourceID,

		dmlChannels:   dmlChannels,
		deltaChannels: deltaChannels,

		lock:           sync.Mutex{},
		sess2ChanTsMap: make(map[typeutil.UniqueID]*chanTsMsg),
		sendChan:       make(chan map[typeutil.UniqueID]*chanTsMsg, 16),

		ddlLock:  sync.RWMutex{},
		ddlMinTs: typeutil.Timestamp(math.MaxUint64),
		ddlTsSet: make(map[typeutil.Timestamp]struct{}),
	}
}

// sendToChannel send all channels' timetick to sendChan
// lock is needed by the invoker
func (t *timetickSync) sendToChannel() {
	if len(t.sess2ChanTsMap) == 0 {
		return
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
				zap.Any("idle time", Params.ProxyCfg.TimeTickInterval.Milliseconds()*maxCnt))
		}
		return
	}

	// clear sess2ChanTsMap and send a clone
	ptt := make(map[typeutil.UniqueID]*chanTsMsg)
	for k, v := range t.sess2ChanTsMap {
		ptt[k] = v
		t.sess2ChanTsMap[k] = nil
	}
	t.sendChan <- ptt
}

// AddDmlTimeTick add ts into ddlTimetickInfos[sourceID],
// can be used to tell if DDL operation is in process.
func (t *timetickSync) addDdlTimeTick(ts typeutil.Timestamp, reason string) {
	t.ddlLock.Lock()
	defer t.ddlLock.Unlock()

	if ts < t.ddlMinTs {
		t.ddlMinTs = ts
	}
	t.ddlTsSet[ts] = struct{}{}

	log.Debug("add ddl timetick", zap.Uint64("minTs", t.ddlMinTs), zap.Uint64("ts", ts),
		zap.Int("len(ddlTsSet)", len(t.ddlTsSet)), zap.String("reason", reason))
}

// RemoveDdlTimeTick is invoked in UpdateTimeTick.
// It clears the ts generated by AddDdlTimeTick, indicates DDL operation finished.
func (t *timetickSync) removeDdlTimeTick(ts typeutil.Timestamp, reason string) {
	t.ddlLock.Lock()
	defer t.ddlLock.Unlock()

	delete(t.ddlTsSet, ts)
	log.Debug("remove ddl timetick", zap.Uint64("ts", ts), zap.Int("len(ddlTsSet)", len(t.ddlTsSet)),
		zap.String("reason", reason))
	if len(t.ddlTsSet) == 0 {
		t.ddlMinTs = typeutil.Timestamp(math.MaxUint64)
	} else if t.ddlMinTs == ts {
		// re-calculate minTs
		minTs := typeutil.Timestamp(math.MaxUint64)
		for tt := range t.ddlTsSet {
			if tt < minTs {
				minTs = tt
			}
		}
		t.ddlMinTs = minTs
		log.Debug("update ddl minTs", zap.Any("minTs", minTs))
	}
}

func (t *timetickSync) getDdlMinTimeTick() typeutil.Timestamp {
	t.ddlLock.Lock()
	defer t.ddlLock.Unlock()

	return t.ddlMinTs
}

// UpdateTimeTick check msg validation and send it to local channel
func (t *timetickSync) updateTimeTick(in *internalpb.ChannelTimeTickMsg, reason string) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if len(in.ChannelNames) == 0 && in.DefaultTimestamp == 0 {
		return nil
	}
	if len(in.Timestamps) != len(in.ChannelNames) {
		return fmt.Errorf("invalid TimeTickMsg")
	}

	prev, ok := t.sess2ChanTsMap[in.Base.SourceID]
	if !ok {
		return fmt.Errorf("skip ChannelTimeTickMsg from un-recognized session %d", in.Base.SourceID)
	}

	// if ddl operation not finished, skip current ts update
	ddlMinTs := t.getDdlMinTimeTick()
	if in.DefaultTimestamp > ddlMinTs {
		log.Debug("ddl not finished", zap.Int64("source id", in.Base.SourceID),
			zap.Uint64("curr ts", in.DefaultTimestamp),
			zap.Uint64("ddlMinTs", ddlMinTs),
			zap.String("reason", reason))
		return nil
	}

	if in.Base.SourceID == t.sourceID {
		if prev != nil && in.DefaultTimestamp <= prev.defaultTs {
			log.Debug("timestamp go back", zap.Int64("source id", in.Base.SourceID),
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
	log.Debug("Add session for timeticksync", zap.Int64("serverID", sess.ServerID))
}

func (t *timetickSync) delSession(sess *sessionutil.Session) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if _, ok := t.sess2ChanTsMap[sess.ServerID]; ok {
		delete(t.sess2ChanTsMap, sess.ServerID)
		log.Debug("Remove session from timeticksync", zap.Int64("serverID", sess.ServerID))
		t.sendToChannel()
	}
}

func (t *timetickSync) initSessions(sess []*sessionutil.Session) {
	t.lock.Lock()
	defer t.lock.Unlock()
	for _, s := range sess {
		t.sess2ChanTsMap[s.ServerID] = nil
		log.Debug("Init proxy sessions for timeticksync", zap.Int64("serverID", s.ServerID))
	}
}

// StartWatch watch session change and process all channels' timetick msg
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
			log.Debug("rootcoord context done", zap.Error(t.ctx.Err()))
			return
		case sessTimetick, ok := <-t.sendChan:
			if !ok {
				log.Debug("timetickSync sendChan closed")
				return
			}
			if enableTtChecker {
				checker.Check()
			}
			// reduce each channel to get min timestamp
			local := sessTimetick[t.sourceID]
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
						log.Debug("SendTimeTickToChannel fail", zap.Error(err))
					}
					wg.Done()
				}(chanName, ts)
			}
			wg.Wait()
			span := tr.ElapseSpan()
			metrics.RootCoordSyncTimeTickLatency.Observe(float64(span.Milliseconds()))
			// rootcoord send tt msg to all channels every 200ms by default
			if span > Params.ProxyCfg.TimeTickInterval {
				log.Warn("rootcoord send tt to all channels too slowly",
					zap.Int("chanNum", len(local.chanTsMap)), zap.Int64("span", span.Milliseconds()))
			}
		}
	}
}

// SendTimeTickToChannel send each channel's min timetick to msg stream
func (t *timetickSync) sendTimeTickToChannel(chanNames []string, ts typeutil.Timestamp) error {
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
			SourceID:  t.sourceID,
		},
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)

	if err := t.dmlChannels.broadcast(chanNames, &msgPack); err != nil {
		return err
	}

	for _, chanName := range chanNames {
		metrics.RootCoordInsertChannelTimeTick.WithLabelValues(chanName).Set(float64(tsoutil.Mod24H(ts)))
	}
	return nil
}

// GetSessionNum return the num of detected sessions
func (t *timetickSync) getSessionNum() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return len(t.sess2ChanTsMap)
}

///////////////////////////////////////////////////////////////////////////////
// GetDmlChannelName return a valid dml channel name
func (t *timetickSync) getDmlChannelName() string {
	return t.dmlChannels.getChannelName()
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
}

// RemoveDmlChannels remove dml channels
func (t *timetickSync) removeDmlChannels(names ...string) {
	t.dmlChannels.removeChannels(names...)
}

// BroadcastDmlChannels broadcasts msg pack into dml channels
func (t *timetickSync) broadcastDmlChannels(chanNames []string, pack *msgstream.MsgPack) error {
	return t.dmlChannels.broadcast(chanNames, pack)
}

// BroadcastMarkDmlChannels broadcasts msg pack into dml channels
func (t *timetickSync) broadcastMarkDmlChannels(chanNames []string, pack *msgstream.MsgPack) (map[string][]byte, error) {
	return t.dmlChannels.broadcastMark(chanNames, pack)
}

///////////////////////////////////////////////////////////////////////////////
// GetDeltaChannelName return a valid delta channel name
func (t *timetickSync) getDeltaChannelName() string {
	return t.deltaChannels.getChannelName()
}

// AddDeltaChannels add delta channels
func (t *timetickSync) addDeltaChannels(names ...string) {
	t.deltaChannels.addChannels(names...)
}

// RemoveDeltaChannels remove delta channels
func (t *timetickSync) removeDeltaChannels(names ...string) {
	t.deltaChannels.removeChannels(names...)
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
