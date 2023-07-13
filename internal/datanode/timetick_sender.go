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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/retry"
)

// timeTickSender is to merge channel states updated by flow graph node and send to datacoord periodically
// timeTickSender hold a SegmentStats time sequence cache for each channel,
// after send succeeds will clean the cache earlier than the sended timestamp
type timeTickSender struct {
	nodeID    int64
	dataCoord types.DataCoord

	mu                  sync.Mutex
	channelStatesCaches map[string]*segmentStatesSequence // string -> *segmentStatesSequence
}

// data struct only used in timeTickSender
type segmentStatesSequence struct {
	data map[uint64][]*datapb.SegmentStats // ts -> segmentStats
}

func newTimeTickSender(dataCoord types.DataCoord, nodeID int64) *timeTickSender {
	return &timeTickSender{
		nodeID:              nodeID,
		dataCoord:           dataCoord,
		channelStatesCaches: make(map[string]*segmentStatesSequence, 0),
	}
}

func (m *timeTickSender) start(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(Params.DataNodeCfg.DataNodeTimeTickInterval) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("timeTickSender context done")
			return
		case t := <-ticker.C:
			m.sendReport(ctx, uint64(t.Unix()))
		}
	}
}

func (m *timeTickSender) update(channelName string, timestamp uint64, segmentStats []*datapb.SegmentStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	channelStates, ok := m.channelStatesCaches[channelName]
	if !ok {
		channelStates = &segmentStatesSequence{
			data: make(map[uint64][]*datapb.SegmentStats, 0),
		}
	}
	// suppose timestamp should keep growing, the latter can override the former.
	// Use append to protect even when timestamp comes in the wrong order. mergeDatanodeTtMsg will merge them
	channelStates.data[timestamp] = append(channelStates.data[timestamp], segmentStats...)
	// channelStates.data[timestamp] = segmentStats

	m.channelStatesCaches[channelName] = channelStates
}

func (m *timeTickSender) mergeDatanodeTtMsg() ([]*datapb.DataNodeTtMsg, map[string]uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var msgs []*datapb.DataNodeTtMsg
	sendedLastTss := make(map[string]uint64, 0)

	for channelName, channelSegmentStates := range m.channelStatesCaches {
		var lastTs uint64
		segNumRows := make(map[int64]int64, 0)
		for ts, segmentStates := range channelSegmentStates.data {
			if ts > lastTs {
				lastTs = ts
			}
			// merge the same segments into one
			for _, segmentStat := range segmentStates {
				if v, ok := segNumRows[segmentStat.GetSegmentID()]; ok {
					// numRows is supposed to keep growing
					if segmentStat.GetNumRows() > v {
						segNumRows[segmentStat.GetSegmentID()] = segmentStat.GetNumRows()
					}
				} else {
					segNumRows[segmentStat.GetSegmentID()] = segmentStat.GetNumRows()
				}
			}
		}
		toSendSegmentStats := make([]*datapb.SegmentStats, 0)
		for id, numRows := range segNumRows {
			toSendSegmentStats = append(toSendSegmentStats, &datapb.SegmentStats{
				SegmentID: id,
				NumRows:   numRows,
			})
		}
		msgs = append(msgs, &datapb.DataNodeTtMsg{
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
	sizeBeforeClean := len(m.channelStatesCaches)
	log := log.With(zap.Any("sendedLastTss", sendedLastTss), zap.Int("sizeBeforeClean", sizeBeforeClean))
	for channelName, sendedLastTs := range sendedLastTss {
		channelCache, ok := m.channelStatesCaches[channelName]
		if ok {
			for ts := range channelCache.data {
				if ts <= sendedLastTs {
					delete(channelCache.data, ts)
				}
			}
			m.channelStatesCaches[channelName] = channelCache
		}
		if len(channelCache.data) == 0 {
			delete(m.channelStatesCaches, channelName)
		}
	}
	log.RatedDebug(30, "timeTickSender channelStatesCaches", zap.Int("sizeAfterClean", len(m.channelStatesCaches)))
}

func (m *timeTickSender) sendReport(ctx context.Context, submitTs Timestamp) error {
	toSendMsgs, sendLastTss := m.mergeDatanodeTtMsg()
	log.RatedDebug(30, "timeTickSender send datanode timetick message", zap.Any("toSendMsgs", toSendMsgs), zap.Any("sendLastTss", sendLastTss))
	err := retry.Do(ctx, func() error {
		statusResp, err := m.dataCoord.ReportDataNodeTtMsgs(ctx, &datapb.ReportDataNodeTtMsgsRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_DataNodeTt),
				commonpbutil.WithTimeStamp(submitTs),
				commonpbutil.WithSourceID(m.nodeID),
			),
			Msgs: toSendMsgs,
		})
		if err != nil {
			log.Warn("error happen when ReportDataNodeTtMsgs", zap.Error(err))
			return err
		}
		if statusResp.GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("ReportDataNodeTtMsgs resp status not succeed",
				zap.String("error_code", statusResp.GetErrorCode().String()),
				zap.String("reason", statusResp.GetReason()))
			return errors.New(statusResp.GetReason())
		}
		return nil
	}, retry.Attempts(20), retry.Sleep(time.Millisecond*100))
	if err != nil {
		log.Error("ReportDataNodeTtMsgs fail after retry", zap.Error(err))
		return err
	}
	m.cleanStatesCache(sendLastTss)
	return nil
}
