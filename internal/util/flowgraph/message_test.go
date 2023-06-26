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

package flowgraph

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/stretchr/testify/assert"
)

type MockMsg struct {
	Ctx context.Context
}

func (bm *MockMsg) TraceCtx() context.Context {
	return bm.Ctx
}

func (bm *MockMsg) SetTraceCtx(ctx context.Context) {
	bm.Ctx = ctx
}

func (bm *MockMsg) ID() msgstream.UniqueID {
	return 0
}

func (bm *MockMsg) SetID(id msgstream.UniqueID) {
	// do nothing
}

func (bm *MockMsg) BeginTs() Timestamp {
	return 0
}

func (bm *MockMsg) EndTs() Timestamp {
	return 0
}

func (bm *MockMsg) Type() msgstream.MsgType {
	return 0
}

func (bm *MockMsg) SourceID() int64 {
	return 0
}

func (bm *MockMsg) HashKeys() []uint32 {
	return []uint32{0}
}

func (bm *MockMsg) Marshal(msgstream.TsMsg) (msgstream.MarshalType, error) {
	return nil, nil
}

func (bm *MockMsg) Unmarshal(msgstream.MarshalType) (msgstream.TsMsg, error) {
	return nil, nil
}

func (bm *MockMsg) Position() *MsgPosition {
	return nil
}

func (bm *MockMsg) SetPosition(position *MsgPosition) {

}

func (bm *MockMsg) Size() int {
	return 0
}

func Test_GenerateMsgStreamMsg(t *testing.T) {
	messages := make([]msgstream.TsMsg, 1)
	messages[0] = &MockMsg{
		Ctx: context.TODO(),
	}

	timestampMin := uint64(0)
	timestampMax := uint64(1000)
	streamMsg := GenerateMsgStreamMsg(messages, timestampMin, timestampMax, nil, nil)
	assert.NotNil(t, streamMsg)
	assert.Equal(t, len(streamMsg.tsMessages), len(messages))
	assert.Equal(t, streamMsg.timestampMin, timestampMin)
	assert.Equal(t, streamMsg.timestampMax, timestampMax)
	assert.Nil(t, streamMsg.startPositions)
	assert.Nil(t, streamMsg.endPositions)
}

func TestMsgStreamMsg(t *testing.T) {
	messages := make([]msgstream.TsMsg, 1)
	messages[0] = &MockMsg{
		Ctx: context.TODO(),
	}

	var timestampMin Timestamp
	var timestampMax Timestamp = 100
	streamMsg := &MsgStreamMsg{
		tsMessages:     messages,
		timestampMin:   timestampMin,
		timestampMax:   timestampMax,
		startPositions: nil,
		endPositions:   nil,
	}

	tt := streamMsg.TimeTick()
	assert.Equal(t, tt, timestampMax)

	nodeID := streamMsg.DownStreamNodeIdx()
	assert.Equal(t, nodeID, 0)

	msgs := streamMsg.TsMessages()
	assert.Equal(t, len(msgs), len(messages))

	min := streamMsg.TimestampMin()
	assert.Equal(t, min, timestampMin)

	max := streamMsg.TimestampMax()
	assert.Equal(t, max, timestampMax)

	startPos := streamMsg.StartPositions()
	assert.Nil(t, startPos)

	endPos := streamMsg.EndPositions()
	assert.Nil(t, endPos)
}
