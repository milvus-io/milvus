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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

// NOTE: start pulsar before test
func TestStatsService_start(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	msFactory.SetParams(m)
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, nil, msFactory)
	node.statsService.start()
	node.Stop()
}

//NOTE: start pulsar before test
func TestSegmentManagement_sendSegmentStatistic(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)

	const receiveBufSize = 1024
	// start pulsar
	producerChannels := []string{Params.StatsChannelName}

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": receiveBufSize,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err := msFactory.SetParams(m)
	assert.Nil(t, err)

	statsStream, err := msFactory.NewMsgStream(node.queryNodeLoopCtx)
	assert.Nil(t, err)
	statsStream.AsProducer(producerChannels)

	var statsMsgStream msgstream.MsgStream = statsStream

	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, nil, msFactory)
	node.statsService.statsStream = statsMsgStream
	node.statsService.statsStream.Start()

	// send stats
	node.statsService.publicStatistic(nil)
	node.Stop()
}
