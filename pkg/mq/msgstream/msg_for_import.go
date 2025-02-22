/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package msgstream

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
)

type ImportMsg struct {
	BaseMsg
	*msgpb.ImportMsg
}

var _ TsMsg = (*ImportMsg)(nil)

func (i *ImportMsg) ID() UniqueID {
	return i.Base.MsgID
}

func (i *ImportMsg) SetID(id UniqueID) {
	i.Base.MsgID = id
}

func (i *ImportMsg) Type() MsgType {
	return i.Base.MsgType
}

func (i *ImportMsg) SourceID() int64 {
	return i.Base.SourceID
}

func (i *ImportMsg) Marshal(input TsMsg) (MarshalType, error) {
	importMsg := input.(*ImportMsg)
	mb, err := proto.Marshal(importMsg.ImportMsg)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (i *ImportMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	importMsg := &msgpb.ImportMsg{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, importMsg)
	if err != nil {
		return nil, err
	}
	rr := &ImportMsg{ImportMsg: importMsg}
	rr.BeginTimestamp = importMsg.GetBase().GetTimestamp()
	rr.EndTimestamp = importMsg.GetBase().GetTimestamp()

	return rr, nil
}

func (i *ImportMsg) Size() int {
	return proto.Size(i.ImportMsg)
}
