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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
)

type testNode struct {
	*BaseNode
	outChannel chan msgstream.Timestamp
}

func (t *testNode) Operate(in Msg) Msg {
	msg := in.(*msgstream.MsgPack)
	if t.outChannel != nil {
		t.outChannel <- msg.BeginTs
	}
	return msg
}

type PipelineSuite struct {
	suite.Suite
	pipeline   *pipeline
	outChannel chan msgstream.Timestamp
}

func (suite *PipelineSuite) SetupTest() {
	suite.outChannel = make(chan msgstream.Timestamp, 1)
	suite.pipeline = &pipeline{
		nodes:           []*nodeCtx{},
		nodeTtInterval:  0,
		enableTtChecker: false,
	}

	suite.pipeline.addNode(&testNode{
		BaseNode: &BaseNode{
			name:           "test-node1",
			maxQueueLength: 8,
		},
	})

	suite.pipeline.addNode(&testNode{
		BaseNode: &BaseNode{
			name:           "test-node2",
			maxQueueLength: 8,
		},
	})

	suite.pipeline.addNode(&testNode{
		BaseNode: &BaseNode{
			name:           "test-node3",
			maxQueueLength: 8,
		},
		outChannel: suite.outChannel,
	})
}

func (suite *PipelineSuite) TestBasic() {
	suite.pipeline.Start()
	defer suite.pipeline.Close()

	for i := 0; i < 100; i++ {
		suite.pipeline.inputChannel <- &msgstream.MsgPack{BeginTs: msgstream.Timestamp(i)}
		suite.pipeline.process()
		output := <-suite.outChannel
		suite.Equal(i, int(output))
	}
}

func TestPipeline(t *testing.T) {
	suite.Run(t, new(PipelineSuite))
}
