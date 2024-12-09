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

type ReplicateMsg struct {
	BaseMsg
	*msgpb.ReplicateMsg
}

var _ TsMsg = (*ReplicateMsg)(nil)

func (r *ReplicateMsg) ID() UniqueID {
	return r.Base.MsgID
}

func (r *ReplicateMsg) SetID(id UniqueID) {
	r.Base.MsgID = id
}

func (r *ReplicateMsg) Type() MsgType {
	return r.Base.MsgType
}

func (r *ReplicateMsg) SourceID() int64 {
	return r.Base.SourceID
}

func (r *ReplicateMsg) Marshal(input TsMsg) (MarshalType, error) {
	replicateMsg := input.(*ReplicateMsg)
	mb, err := proto.Marshal(replicateMsg.ReplicateMsg)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (r *ReplicateMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	replicateMsg := &msgpb.ReplicateMsg{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, replicateMsg)
	if err != nil {
		return nil, err
	}
	rr := &ReplicateMsg{ReplicateMsg: replicateMsg}
	rr.BeginTimestamp = replicateMsg.GetBase().GetTimestamp()
	rr.EndTimestamp = replicateMsg.GetBase().GetTimestamp()

	return rr, nil
}

func (r *ReplicateMsg) Size() int {
	return proto.Size(r.ReplicateMsg)
}
