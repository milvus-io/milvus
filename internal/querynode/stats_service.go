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

package querynode

import (
	"context"
	"strings"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type statsService struct {
	ctx context.Context

	replica ReplicaInterface

	fieldStatsChan chan []*internalpb.FieldStats
	statsStream    msgstream.MsgStream
	msFactory      msgstream.Factory
}

func newStatsService(ctx context.Context, replica ReplicaInterface, fieldStatsChan chan []*internalpb.FieldStats, factory msgstream.Factory) *statsService {

	return &statsService{
		ctx: ctx,

		replica: replica,

		fieldStatsChan: fieldStatsChan,
		statsStream:    nil,

		msFactory: factory,
	}
}

func (sService *statsService) start() {
	sleepTimeInterval := Params.StatsPublishInterval

	// start pulsar
	producerChannels := []string{Params.StatsChannelName}

	statsStream, _ := sService.msFactory.NewMsgStream(sService.ctx)
	statsStream.AsProducer(producerChannels)
	log.Debug("querynode AsProducer: " + strings.Join(producerChannels, ", "))

	var statsMsgStream msgstream.MsgStream = statsStream

	sService.statsStream = statsMsgStream
	sService.statsStream.Start()

	// start service
	for {
		select {
		case <-sService.ctx.Done():
			log.Debug("stats service closed")
			return
		case <-time.After(time.Duration(sleepTimeInterval) * time.Millisecond):
			sService.publicStatistic(nil)
		case fieldStats := <-sService.fieldStatsChan:
			sService.publicStatistic(fieldStats)
		}
	}
}

func (sService *statsService) close() {
	if sService.statsStream != nil {
		sService.statsStream.Close()
	}
}

func (sService *statsService) publicStatistic(fieldStats []*internalpb.FieldStats) {
	segStats := sService.replica.getSegmentStatistics()

	queryNodeStats := internalpb.QueryNodeStats{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_QueryNodeStats,
			SourceID: Params.QueryNodeID,
		},
		SegStats:   segStats,
		FieldStats: fieldStats,
	}

	var msg msgstream.TsMsg = &msgstream.QueryNodeStatsMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		QueryNodeStats: queryNodeStats,
	}

	var msgPack = msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{msg},
	}
	err := sService.statsStream.Produce(&msgPack)
	if err != nil {
		log.Error(err.Error())
	}
}
