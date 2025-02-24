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
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
)

// Msg is an abstract class that contains a method to get the time tick of this message
type Msg interface {
	TimeTick() Timestamp
	IsClose() bool
}

type BaseMsg struct {
	isCloseMsg bool
}

func (msg BaseMsg) IsCloseMsg() bool {
	return msg.isCloseMsg
}

func NewBaseMsg(isCloseMsg bool) BaseMsg {
	return BaseMsg{
		isCloseMsg: isCloseMsg,
	}
}

// MsgStreamMsg is a wrapper of TsMsg in flowgraph
type MsgStreamMsg struct {
	BaseMsg
	tsMessages     []msgstream.TsMsg
	timestampMin   Timestamp
	timestampMax   Timestamp
	startPositions []*MsgPosition
	endPositions   []*MsgPosition
}

// GenerateMsgStreamMsg is used to create a new MsgStreamMsg object
func GenerateMsgStreamMsg(tsMessages []msgstream.TsMsg, timestampMin, timestampMax Timestamp, startPos []*MsgPosition, endPos []*MsgPosition) *MsgStreamMsg {
	return &MsgStreamMsg{
		tsMessages:     tsMessages,
		timestampMin:   timestampMin,
		timestampMax:   timestampMax,
		startPositions: startPos,
		endPositions:   endPos,
	}
}

// TimeTick returns the timetick of this message
func (msMsg *MsgStreamMsg) TimeTick() Timestamp {
	return msMsg.timestampMax
}

func (msMsg *MsgStreamMsg) IsClose() bool {
	return msMsg.isCloseMsg
}

// DownStreamNodeIdx returns 0
func (msMsg *MsgStreamMsg) DownStreamNodeIdx() int {
	return 0
}

// TsMessages returns the origin TsMsg object list
func (msMsg *MsgStreamMsg) TsMessages() []msgstream.TsMsg {
	return msMsg.tsMessages
}

// TimestampMin returns the minimal timestamp in the TsMsg list
func (msMsg *MsgStreamMsg) TimestampMin() Timestamp {
	return msMsg.timestampMin
}

// TimestampMax returns the maximal timestamp in the TsMsg list
func (msMsg *MsgStreamMsg) TimestampMax() Timestamp {
	return msMsg.timestampMax
}

// StartPositions returns the start position of TsMsgs
func (msMsg *MsgStreamMsg) StartPositions() []*MsgPosition {
	return msMsg.startPositions
}

// EndPositions returns the end position of TsMsgs
func (msMsg *MsgStreamMsg) EndPositions() []*MsgPosition {
	return msMsg.endPositions
}
