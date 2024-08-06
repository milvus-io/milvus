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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

type LoadPartitionsMsg struct {
	BaseMsg
	*milvuspb.LoadPartitionsRequest
}

var _ TsMsg = &LoadPartitionsMsg{}

func (l *LoadPartitionsMsg) ID() UniqueID {
	return l.Base.MsgID
}

func (l *LoadPartitionsMsg) SetID(id UniqueID) {
	l.Base.MsgID = id
}

func (l *LoadPartitionsMsg) Type() MsgType {
	return l.Base.MsgType
}

func (l *LoadPartitionsMsg) SourceID() int64 {
	return l.Base.SourceID
}

func (l *LoadPartitionsMsg) Marshal(input TsMsg) (MarshalType, error) {
	loadPartitionsMsg := input.(*LoadPartitionsMsg)
	loadPartitionsRequest := loadPartitionsMsg.LoadPartitionsRequest
	mb, err := proto.Marshal(loadPartitionsRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (l *LoadPartitionsMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	loadPartitionsRequest := &milvuspb.LoadPartitionsRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, loadPartitionsRequest)
	if err != nil {
		return nil, err
	}
	loadPartitionsMsg := &LoadPartitionsMsg{LoadPartitionsRequest: loadPartitionsRequest}
	loadPartitionsMsg.BeginTimestamp = loadPartitionsMsg.GetBase().GetTimestamp()
	loadPartitionsMsg.EndTimestamp = loadPartitionsMsg.GetBase().GetTimestamp()

	return loadPartitionsMsg, nil
}

func (l *LoadPartitionsMsg) Size() int {
	return proto.Size(l.LoadPartitionsRequest)
}

type ReleasePartitionsMsg struct {
	BaseMsg
	*milvuspb.ReleasePartitionsRequest
}

var _ TsMsg = &ReleasePartitionsMsg{}

func (r *ReleasePartitionsMsg) ID() UniqueID {
	return r.Base.MsgID
}

func (r *ReleasePartitionsMsg) SetID(id UniqueID) {
	r.Base.MsgID = id
}

func (r *ReleasePartitionsMsg) Type() MsgType {
	return r.Base.MsgType
}

func (r *ReleasePartitionsMsg) SourceID() int64 {
	return r.Base.SourceID
}

func (r *ReleasePartitionsMsg) Marshal(input TsMsg) (MarshalType, error) {
	releasePartitionsMsg := input.(*ReleasePartitionsMsg)
	releasePartitionsRequest := releasePartitionsMsg.ReleasePartitionsRequest
	mb, err := proto.Marshal(releasePartitionsRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (r *ReleasePartitionsMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	releasePartitionsRequest := &milvuspb.ReleasePartitionsRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, releasePartitionsRequest)
	if err != nil {
		return nil, err
	}
	releasePartitionsMsg := &ReleasePartitionsMsg{ReleasePartitionsRequest: releasePartitionsRequest}
	releasePartitionsMsg.BeginTimestamp = releasePartitionsMsg.GetBase().GetTimestamp()
	releasePartitionsMsg.EndTimestamp = releasePartitionsMsg.GetBase().GetTimestamp()
	return releasePartitionsMsg, nil
}

func (r *ReleasePartitionsMsg) Size() int {
	return proto.Size(r.ReleasePartitionsRequest)
}
