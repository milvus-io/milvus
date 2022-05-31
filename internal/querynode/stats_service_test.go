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

package querynode

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/dependency"

	"github.com/stretchr/testify/assert"
)

// NOTE: start pulsar before test
func TestStatsService_start(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)

	factory := dependency.NewDefaultFactory(true)
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.metaReplica, factory)
	node.statsService.start()
	node.Stop()
}

//NOTE: start pulsar before test
func TestSegmentManagement_sendSegmentStatistic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	const receiveBufSize = 1024
	// start pulsar
	producerChannels := []string{Params.CommonCfg.QueryNodeStats}

	factory := dependency.NewDefaultFactory(true)

	statsStream, err := factory.NewMsgStream(node.queryNodeLoopCtx)
	assert.Nil(t, err)
	statsStream.AsProducer(producerChannels)

	var statsMsgStream msgstream.MsgStream = statsStream

	node.statsService = newStatsService(node.queryNodeLoopCtx, node.metaReplica, factory)
	node.statsService.statsStream = statsMsgStream
	node.statsService.statsStream.Start()

	// send stats
	node.statsService.publicStatistic(nil)
	node.Stop()
}
