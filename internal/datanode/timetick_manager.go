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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"go.uber.org/zap"
)

// timeTickManager is to manage metadata related to channel state and timetick
type timeTickManager struct {
	dataNode *DataNode // datanode Reference

	ticker            *time.Ticker
	channel2Ts        sync.Map // string -> uint64
	channel2SegStates sync.Map // string -> sync.Map(segmentID)*datapb.SegmentStats
}

func newTimeTickManager(datanode *DataNode) *timeTickManager {
	return &timeTickManager{
		dataNode: datanode,
	}
}

func (m *timeTickManager) start(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(Params.DataNodeCfg.DataNodeTimeTickInterval) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("timeTickManager context done")
			return
		case t := <-ticker.C:
			m.submitReport(uint64(t.Unix()))
		}
	}
}

func (m *timeTickManager) submitReport(submitTs Timestamp) {
	var msgs []*datapb.DataNodeTtMsg
	m.channel2Ts.Range(func(key, value any) bool {
		channelName := key.(string)
		ts := value.(uint64)
		segmentStats := make([]*datapb.SegmentStats, 0)
		channelSegStates, _ := m.channel2SegStates.Load(channelName)
		channelSegStatesMap := channelSegStates.(*sync.Map)
		channelSegStatesMap.Range(func(segmentID, segmentStat any) bool {
			// clone
			segmentStats = append(segmentStats, &datapb.SegmentStats{
				SegmentID: segmentStat.(*datapb.SegmentStats).GetSegmentID(),
				NumRows:   segmentStat.(*datapb.SegmentStats).GetNumRows(),
			})
			return true
		})
		msgs = append(msgs, &datapb.DataNodeTtMsg{
			ChannelName:   channelName,
			Timestamp:     ts,
			SegmentsStats: segmentStats,
		})
		// delete cache
		m.channel2SegStates.Delete(channelName)
		m.channel2Ts.Delete(channelName)
		return true
	})
	statusResp, err := m.dataNode.dataCoord.ReportDataNodeTtMsgs(m.dataNode.ctx, &datapb.ReportDataNodeTtMsgsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DataNodeTt),
			commonpbutil.WithTimeStamp(submitTs),
			commonpbutil.WithSourceID(m.dataNode.session.ServerID),
		),
		Msgs: msgs,
	})
	if err != nil {
		log.Warn("error happen when ReportDataNodeTtMsgs", zap.Error(err))
	}
	if statusResp.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("ReportDataNodeTtMsgs resp status not succeed",
			zap.String("error_code", statusResp.GetErrorCode().String()),
			zap.String("reason", statusResp.GetReason()))
	}
}

func (m *timeTickManager) update(channelName string, timestamp uint64, segmentStats []*datapb.SegmentStats) {
	m.channel2Ts.Store(channelName, timestamp)

	m.channel2SegStates.LoadOrStore(channelName, &sync.Map{})
	channelSegStates, _ := m.channel2SegStates.Load(channelName)
	channelSegStatesMap := channelSegStates.(*sync.Map)
	for _, segmentStat := range segmentStats {
		channelSegStatesMap.LoadOrStore(segmentStat.GetSegmentID(), segmentStat)
	}
}
