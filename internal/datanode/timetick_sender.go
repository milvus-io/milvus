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

package datanode

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

// timeTickSender is to merge channel states updated by flow graph node and send to datacoord periodically
// timeTickSender hold a SegmentStats time sequence cache for each channel,
// after send succeeds will clean the cache earlier than the sended timestamp
type timeTickSender struct {
	nodeID int64
	broker broker.Broker

	wg         sync.WaitGroup
	cancelFunc context.CancelFunc

	options []retry.Option

	mu                sync.RWMutex
	channelStatsCache map[string]*segmentStats // vchannel -> segmentStats
}

// data struct only used in timeTickSender
type segmentStats struct {
	ts    uint64
	stats map[int64]*commonpb.SegmentStats
}

func newTimeTickSender(broker broker.Broker, nodeID int64, opts ...retry.Option) *timeTickSender {
	return &timeTickSender{
		nodeID:            nodeID,
		broker:            broker,
		channelStatsCache: make(map[string]*segmentStats, 0),
		options:           opts,
	}
}

func (m *timeTickSender) start() {
	m.wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	m.cancelFunc = cancel
	go func() {
		defer m.wg.Done()
		m.work(ctx)
	}()
}

func (m *timeTickSender) Stop() {
	if m.cancelFunc != nil {
		m.cancelFunc()
		m.wg.Wait()
	}
}

func (m *timeTickSender) work(ctx context.Context) {
	ticker := time.NewTicker(Params.DataNodeCfg.DataNodeTimeTickInterval.GetAsDuration(time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("timeTickSender context done")
			return
		case <-ticker.C:
			m.sendReport(ctx)
		}
	}
}

func (m *timeTickSender) update(channelName string, timestamp uint64, stats []*commonpb.SegmentStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.channelStatsCache[channelName]
	if !ok {
		m.channelStatsCache[channelName] = &segmentStats{
			ts:    timestamp,
			stats: make(map[int64]*commonpb.SegmentStats),
		}
	}
	for _, stat := range stats {
		m.channelStatsCache[channelName].stats[stat.GetSegmentID()] = stat
	}
	m.channelStatsCache[channelName].ts = timestamp
}

func (m *timeTickSender) assembleDatanodeTtMsg() ([]*msgpb.DataNodeTtMsg, map[string]uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var msgs []*msgpb.DataNodeTtMsg
	sendedLastTss := make(map[string]uint64, 0)

	for channelName, segStats := range m.channelStatsCache {
		toSendSegmentStats := make([]*commonpb.SegmentStats, 0)
		for segmentID, stat := range segStats.stats {
			toSendSegmentStats = append(toSendSegmentStats, &commonpb.SegmentStats{
				SegmentID: segmentID,
				NumRows:   stat.GetNumRows(),
			})
		}
		msgs = append(msgs, &msgpb.DataNodeTtMsg{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_DataNodeTt),
				commonpbutil.WithSourceID(m.nodeID),
			),
			ChannelName:   channelName,
			Timestamp:     segStats.ts,
			SegmentsStats: toSendSegmentStats,
		})
		sendedLastTss[channelName] = segStats.ts
	}

	return msgs, sendedLastTss
}

func (m *timeTickSender) cleanStatesCache(sendedLastTss map[string]uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sizeBeforeClean := len(m.channelStatsCache)
	log := log.With(zap.Any("sendedLastTss", sendedLastTss), zap.Int("sizeBeforeClean", sizeBeforeClean))
	for channelName, sendedLastTs := range sendedLastTss {
		segStats, ok := m.channelStatsCache[channelName]
		if ok && segStats.ts <= sendedLastTs {
			delete(m.channelStatsCache, channelName)
		}
	}
	log.RatedDebug(30, "timeTickSender channelStatsCache", zap.Int("sizeAfterClean", len(m.channelStatsCache)))
}

func (m *timeTickSender) sendReport(ctx context.Context) error {
	toSendMsgs, sendLastTss := m.assembleDatanodeTtMsg()
	log.RatedDebug(30, "timeTickSender send datanode timetick message", zap.Any("toSendMsgs", toSendMsgs), zap.Any("sendLastTss", sendLastTss))
	err := retry.Do(ctx, func() error {
		return m.broker.ReportTimeTick(ctx, toSendMsgs)
	}, m.options...)
	if err != nil {
		log.Error("ReportDataNodeTtMsgs fail after retry", zap.Error(err))
		return err
	}
	m.cleanStatesCache(sendLastTss)
	return nil
}
