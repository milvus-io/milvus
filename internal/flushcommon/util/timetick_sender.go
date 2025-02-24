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

package util

import (
	"context"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type StatsUpdater interface {
	Update(channel string, ts typeutil.Timestamp, stats []*commonpb.SegmentStats)
	GetLatestTimestamp(channel string) typeutil.Timestamp
}

// TimeTickSender is to merge channel states updated by flow graph node and send to datacoord periodically
// TimeTickSender hold segmentStats cache for each channel,
// after send succeeds will clean the cache earlier than last sent timestamp
type TimeTickSender struct {
	nodeID int64
	broker broker.Broker

	wg         sync.WaitGroup
	cancelFunc context.CancelFunc

	options []retry.Option

	mu         sync.RWMutex
	statsCache map[string]*channelStats // channel -> channelStats
}

type channelStats struct {
	segStats map[int64]*segmentStats // segmentID -> segmentStats
	lastTs   uint64
}

// data struct only used in TimeTickSender
type segmentStats struct {
	*commonpb.SegmentStats
	ts uint64
}

func NewTimeTickSender(broker broker.Broker, nodeID int64, opts ...retry.Option) *TimeTickSender {
	return &TimeTickSender{
		nodeID:     nodeID,
		broker:     broker,
		statsCache: make(map[string]*channelStats),
		options:    opts,
		mu:         sync.RWMutex{},
	}
}

func (m *TimeTickSender) Start() {
	m.wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	m.cancelFunc = cancel
	go func() {
		defer m.wg.Done()
		m.work(ctx)
	}()
	log.Info("timeTick sender started")
}

func (m *TimeTickSender) Stop() {
	if m.cancelFunc != nil {
		m.cancelFunc()
		m.wg.Wait()
	}
}

func (m *TimeTickSender) work(ctx context.Context) {
	ticker := time.NewTicker(paramtable.Get().DataNodeCfg.DataNodeTimeTickInterval.GetAsDuration(time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("TimeTickSender context done")
			return
		case <-ticker.C:
			m.sendReport(ctx)
		}
	}
}

func (m *TimeTickSender) Update(channelName string, timestamp uint64, segStats []*commonpb.SegmentStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.statsCache[channelName]
	if !ok {
		m.statsCache[channelName] = &channelStats{
			segStats: make(map[int64]*segmentStats),
		}
	}
	for _, stats := range segStats {
		segmentID := stats.GetSegmentID()
		m.statsCache[channelName].segStats[segmentID] = &segmentStats{
			SegmentStats: stats,
			ts:           timestamp,
		}
	}
	m.statsCache[channelName].lastTs = timestamp
}

func (m *TimeTickSender) GetLatestTimestamp(channel string) typeutil.Timestamp {
	m.mu.RLock()
	defer m.mu.RUnlock()
	chStats, ok := m.statsCache[channel]
	if !ok {
		log.Warn("channel not found in TimeTickSender", zap.String("channel", channel))
		return 0
	}
	return chStats.lastTs
}

func (m *TimeTickSender) assembleDatanodeTtMsg() ([]*msgpb.DataNodeTtMsg, map[string]uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var msgs []*msgpb.DataNodeTtMsg
	lastSentTss := make(map[string]uint64, 0)

	for channelName, chanStats := range m.statsCache {
		toSendSegmentStats := lo.Map(lo.Values(chanStats.segStats), func(stats *segmentStats, _ int) *commonpb.SegmentStats {
			return stats.SegmentStats
		})
		msgs = append(msgs, &msgpb.DataNodeTtMsg{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_DataNodeTt),
				commonpbutil.WithSourceID(m.nodeID),
			),
			ChannelName:   channelName,
			Timestamp:     chanStats.lastTs,
			SegmentsStats: toSendSegmentStats,
		})
		lastSentTss[channelName] = chanStats.lastTs
	}

	return msgs, lastSentTss
}

func (m *TimeTickSender) cleanStatesCache(lastSentTss map[string]uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sizeBeforeClean := len(m.statsCache)
	for channelName, lastSentTs := range lastSentTss {
		_, ok := m.statsCache[channelName]
		if ok {
			for segmentID, stats := range m.statsCache[channelName].segStats {
				if stats.ts <= lastSentTs {
					delete(m.statsCache[channelName].segStats, segmentID)
				}
			}

			if len(m.statsCache[channelName].segStats) == 0 {
				delete(m.statsCache, channelName)
			}
		}
	}
	log.RatedDebug(30, "TimeTickSender stats", zap.Any("lastSentTss", lastSentTss), zap.Int("sizeBeforeClean", sizeBeforeClean), zap.Int("sizeAfterClean", len(m.statsCache)))
}

func (m *TimeTickSender) sendReport(ctx context.Context) error {
	toSendMsgs, sendLastTss := m.assembleDatanodeTtMsg()
	log.RatedDebug(30, "TimeTickSender send datanode timetick message", zap.Any("toSendMsgs", toSendMsgs), zap.Any("sendLastTss", sendLastTss))
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
