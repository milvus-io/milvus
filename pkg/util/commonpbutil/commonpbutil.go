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

package commonpbutil

import (
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

const MsgIDNeedFill int64 = 0

type MsgBaseOptions func(*commonpb.MsgBase)

func WithMsgType(msgType commonpb.MsgType) MsgBaseOptions {
	return func(msgBase *commonpb.MsgBase) {
		msgBase.MsgType = msgType
	}
}

func WithMsgID(msgID int64) MsgBaseOptions {
	return func(msgBase *commonpb.MsgBase) {
		msgBase.MsgID = msgID
	}
}

func WithTimeStamp(ts uint64) MsgBaseOptions {
	return func(msgBase *commonpb.MsgBase) {
		msgBase.Timestamp = ts
	}
}

func WithSourceID(sourceID int64) MsgBaseOptions {
	return func(msgBase *commonpb.MsgBase) {
		msgBase.SourceID = sourceID
	}
}

func WithTargetID(targetID int64) MsgBaseOptions {
	return func(msgBase *commonpb.MsgBase) {
		msgBase.TargetID = targetID
	}
}

func GetNowTimestamp() uint64 {
	return uint64(time.Now().Unix())
}

func FillMsgBaseFromClient(sourceID int64, options ...MsgBaseOptions) MsgBaseOptions {
	return func(msgBase *commonpb.MsgBase) {
		if msgBase.Timestamp == 0 {
			msgBase.Timestamp = GetNowTimestamp()
		}
		if msgBase.SourceID == 0 {
			msgBase.SourceID = sourceID
		}
		for _, op := range options {
			op(msgBase)
		}
	}
}

func newMsgBaseDefault() *commonpb.MsgBase {
	return &commonpb.MsgBase{
		MsgType: commonpb.MsgType_Undefined,
		MsgID:   MsgIDNeedFill,
	}
}

func NewMsgBase(options ...MsgBaseOptions) *commonpb.MsgBase {
	msgBase := newMsgBaseDefault()
	for _, op := range options {
		op(msgBase)
	}
	return msgBase
}

func UpdateMsgBase(msgBase *commonpb.MsgBase, options ...MsgBaseOptions) *commonpb.MsgBase {
	if msgBase == nil {
		return nil
	}
	msgBaseRt := msgBase
	for _, op := range options {
		op(msgBaseRt)
	}
	return msgBaseRt
}
