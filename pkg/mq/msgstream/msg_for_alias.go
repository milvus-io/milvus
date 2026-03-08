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

type CreateAliasMsg struct {
	BaseMsg
	*milvuspb.CreateAliasRequest
}

var _ TsMsg = &CreateAliasMsg{}

func (c *CreateAliasMsg) ID() UniqueID {
	return c.Base.MsgID
}

func (c *CreateAliasMsg) SetID(id UniqueID) {
	c.Base.MsgID = id
}

func (c *CreateAliasMsg) Type() MsgType {
	return c.Base.MsgType
}

func (c *CreateAliasMsg) SourceID() int64 {
	return c.Base.SourceID
}

func (c *CreateAliasMsg) Marshal(input TsMsg) (MarshalType, error) {
	createAliasMsg := input.(*CreateAliasMsg)
	createAliasRequest := createAliasMsg.CreateAliasRequest
	mb, err := proto.Marshal(createAliasRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (c *CreateAliasMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createAliasRequest := &milvuspb.CreateAliasRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, createAliasRequest)
	if err != nil {
		return nil, err
	}
	createAliasMsg := &CreateAliasMsg{CreateAliasRequest: createAliasRequest}
	createAliasMsg.BeginTimestamp = createAliasRequest.GetBase().GetTimestamp()
	createAliasMsg.EndTimestamp = createAliasRequest.GetBase().GetTimestamp()
	return createAliasMsg, nil
}

func (c *CreateAliasMsg) Size() int {
	return proto.Size(c.CreateAliasRequest)
}

type DropAliasMsg struct {
	BaseMsg
	*milvuspb.DropAliasRequest
}

var _ TsMsg = &DropAliasMsg{}

func (d *DropAliasMsg) ID() UniqueID {
	return d.Base.MsgID
}

func (d *DropAliasMsg) SetID(id UniqueID) {
	d.Base.MsgID = id
}

func (d *DropAliasMsg) Type() MsgType {
	return d.Base.MsgType
}

func (d *DropAliasMsg) SourceID() int64 {
	return d.Base.SourceID
}

func (d *DropAliasMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropAliasMsg := input.(*DropAliasMsg)
	dropAliasRequest := dropAliasMsg.DropAliasRequest
	mb, err := proto.Marshal(dropAliasRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (d *DropAliasMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropAliasRequest := &milvuspb.DropAliasRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, dropAliasRequest)
	if err != nil {
		return nil, err
	}
	dropAliasMsg := &DropAliasMsg{DropAliasRequest: dropAliasRequest}
	dropAliasMsg.BeginTimestamp = dropAliasRequest.GetBase().GetTimestamp()
	dropAliasMsg.EndTimestamp = dropAliasRequest.GetBase().GetTimestamp()
	return dropAliasMsg, nil
}

func (d *DropAliasMsg) Size() int {
	return proto.Size(d.DropAliasRequest)
}

type AlterAliasMsg struct {
	BaseMsg
	*milvuspb.AlterAliasRequest
}

var _ TsMsg = &AlterAliasMsg{}

func (a *AlterAliasMsg) ID() UniqueID {
	return a.Base.MsgID
}

func (a *AlterAliasMsg) SetID(id UniqueID) {
	a.Base.MsgID = id
}

func (a *AlterAliasMsg) Type() MsgType {
	return a.Base.MsgType
}

func (a *AlterAliasMsg) SourceID() int64 {
	return a.Base.SourceID
}

func (a *AlterAliasMsg) Marshal(input TsMsg) (MarshalType, error) {
	alterAliasMsg := input.(*AlterAliasMsg)
	alterAliasRequest := alterAliasMsg.AlterAliasRequest
	mb, err := proto.Marshal(alterAliasRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (a *AlterAliasMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	alterAliasRequest := &milvuspb.AlterAliasRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, alterAliasRequest)
	if err != nil {
		return nil, err
	}
	alterAliasMsg := &AlterAliasMsg{AlterAliasRequest: alterAliasRequest}
	alterAliasMsg.BeginTimestamp = alterAliasRequest.GetBase().GetTimestamp()
	alterAliasMsg.EndTimestamp = alterAliasRequest.GetBase().GetTimestamp()
	return alterAliasMsg, nil
}

func (a *AlterAliasMsg) Size() int {
	return proto.Size(a.AlterAliasRequest)
}
