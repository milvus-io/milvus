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

package flowgraph

import (
	"github.com/milvus-io/milvus/internal/msgstream"
)

type Msg interface {
	TimeTick() Timestamp
}

type MsgStreamMsg struct {
	tsMessages     []msgstream.TsMsg
	timestampMin   Timestamp
	timestampMax   Timestamp
	startPositions []*MsgPosition
	endPositions   []*MsgPosition
}

func GenerateMsgStreamMsg(tsMessages []msgstream.TsMsg, timestampMin, timestampMax Timestamp, startPos []*MsgPosition, endPos []*MsgPosition) *MsgStreamMsg {
	return &MsgStreamMsg{
		tsMessages:     tsMessages,
		timestampMin:   timestampMin,
		timestampMax:   timestampMax,
		startPositions: startPos,
		endPositions:   endPos,
	}
}

func (msMsg *MsgStreamMsg) TimeTick() Timestamp {
	return msMsg.timestampMax
}

func (msMsg *MsgStreamMsg) DownStreamNodeIdx() int {
	return 0
}

func (msMsg *MsgStreamMsg) TsMessages() []msgstream.TsMsg {
	return msMsg.tsMessages
}

func (msMsg *MsgStreamMsg) TimestampMin() Timestamp {
	return msMsg.timestampMin
}

func (msMsg *MsgStreamMsg) TimestampMax() Timestamp {
	return msMsg.timestampMax
}

func (msMsg *MsgStreamMsg) StartPositions() []*MsgPosition {
	return msMsg.startPositions
}

func (msMsg *MsgStreamMsg) EndPositions() []*MsgPosition {
	return msMsg.endPositions
}
