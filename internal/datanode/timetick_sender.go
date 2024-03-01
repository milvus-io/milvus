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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

// timeTickSender periodically sends segment states to DataCoord.
// It caches the segmentStats, and after a successful send,
// it cleans the segment states cache before the last send timestamp.
type timeTickSender struct {
	nodeID int64
	broker broker.Broker

	wg         sync.WaitGroup
	cancelFunc context.CancelFunc

	options []retry.Option

	mu         sync.RWMutex
	statsCache map[int64]*segmentStats // segmentID -> segmentStats
}

// data struct only used in timeTickSender
type segmentStats struct {
	*commonpb.SegmentStats
	ts      uint64
	channel string
}

func newTimeTickSender(broker broker.Broker, nodeID int64, opts ...retry.Option) *timeTickSender {
	return &timeTickSender{
		nodeID:     nodeID,
		broker:     broker,
		statsCache: make(map[int64]*segmentStats),
		options:    opts,
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

func (m *timeTickSender) update(channelName string, timestamp uint64, segStats []*commonpb.SegmentStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, stats := range segStats {
		_, ok := m.statsCache[stats.GetSegmentID()]
		if !ok {
			m.statsCache[stats.GetSegmentID()] = &segmentStats{
				SegmentStats: stats,
				ts:           timestamp,
				channel:      channelName,
			}
		} else {
			m.statsCache[stats.GetSegmentID()].ts = timestamp
			m.statsCache[stats.GetSegmentID()].SegmentStats = stats
		}
	}
}

func (m *timeTickSender) assembleDatanodeTtMsg() ([]*msgpb.DataNodeTtMsg, map[string]uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var msgs []*msgpb.DataNodeTtMsg
	sendedLastTss := make(map[string]uint64, 0)

	statsByChannel := lo.GroupBy(lo.Values(m.statsCache), func(stats *segmentStats) string {
		return stats.channel
	})
	for channelName, segStats := range statsByChannel {
		var lastTs uint64
		toSendSegmentStats := make([]*commonpb.SegmentStats, 0)
		for _, stats := range segStats {
			if stats.ts > lastTs {
				lastTs = stats.ts
			}
			toSendSegmentStats = append(toSendSegmentStats, stats.SegmentStats)
		}
		msgs = append(msgs, &msgpb.DataNodeTtMsg{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_DataNodeTt),
				commonpbutil.WithSourceID(m.nodeID),
			),
			ChannelName:   channelName,
			Timestamp:     lastTs,
			SegmentsStats: toSendSegmentStats,
		})
		sendedLastTss[channelName] = lastTs
	}

	return msgs, sendedLastTss
}

func (m *timeTickSender) cleanStatesCache(sendedLastTss map[string]uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sizeBeforeClean := len(m.statsCache)
	log := log.With(zap.Any("sendedLastTss", sendedLastTss), zap.Int("sizeBeforeClean", sizeBeforeClean))
	for segmentID, segStats := range m.statsCache {
		if segStats.ts <= sendedLastTss[segStats.channel] {
			delete(m.statsCache, segmentID)
		}
	}
	log.RatedDebug(30, "timeTickSender statsCache", zap.Int("sizeAfterClean", len(m.statsCache)))
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
