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
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/stretchr/testify/suite"
)

type StreamPipelineSuite struct {
	suite.Suite
	pipeline   *streamPipeline
	inChannel  chan *msgstream.MsgPack
	outChannel chan msgstream.Timestamp
	//data
	length int
	//mock
	msgstream *msgstream.MockMsgStream
}

func (suite *StreamPipelineSuite) SetupTest() {
	suite.inChannel = make(chan *msgstream.MsgPack, 1)
	suite.outChannel = make(chan msgstream.Timestamp)
	suite.msgstream = msgstream.NewMockMsgStream(suite.T())
	suite.msgstream.EXPECT().Chan().Return(suite.inChannel)
	suite.msgstream.EXPECT().Close().Return()
	suite.pipeline = NewPipelineWithStream(suite.msgstream, 0, false, "nil").(*streamPipeline)
	suite.length = 4
}

func (suite *StreamPipelineSuite) TestBasic() {
	for i := 1; i <= suite.length; i++ {
		suite.pipeline.addNode(&testNode{
			BaseNode: &BaseNode{
				name:           fmt.Sprintf("test-node-%d", i),
				maxQueueLength: 8,
			},
			outChannel: suite.outChannel,
		})
	}

	suite.pipeline.Start()
	defer suite.pipeline.Close()
	suite.pipeline.inputChannel <- &msgstream.MsgPack{}

	for i := 1; i <= suite.length; i++ {
		output := <-suite.outChannel
		suite.Equal(msgstream.Timestamp(i), output)
	}
}

func TestStreamPipeline(t *testing.T) {
	suite.Run(t, new(StreamPipelineSuite))
}
