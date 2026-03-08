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

type CreateDatabaseMsg struct {
	BaseMsg
	*milvuspb.CreateDatabaseRequest
}

var _ TsMsg = &CreateDatabaseMsg{}

func (c *CreateDatabaseMsg) ID() UniqueID {
	return c.Base.MsgID
}

func (c *CreateDatabaseMsg) SetID(id UniqueID) {
	c.Base.MsgID = id
}

func (c *CreateDatabaseMsg) Type() MsgType {
	return c.Base.MsgType
}

func (c *CreateDatabaseMsg) SourceID() int64 {
	return c.Base.SourceID
}

func (c *CreateDatabaseMsg) Marshal(input TsMsg) (MarshalType, error) {
	createDataBaseMsg := input.(*CreateDatabaseMsg)
	createDatabaseRequest := createDataBaseMsg.CreateDatabaseRequest
	mb, err := proto.Marshal(createDatabaseRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (c *CreateDatabaseMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createDatabaseRequest := &milvuspb.CreateDatabaseRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, createDatabaseRequest)
	if err != nil {
		return nil, err
	}
	createDatabaseMsg := &CreateDatabaseMsg{CreateDatabaseRequest: createDatabaseRequest}
	createDatabaseMsg.BeginTimestamp = createDatabaseMsg.GetBase().GetTimestamp()
	createDatabaseMsg.EndTimestamp = createDatabaseMsg.GetBase().GetTimestamp()

	return createDatabaseMsg, nil
}

func (c *CreateDatabaseMsg) Size() int {
	return proto.Size(c.CreateDatabaseRequest)
}

type DropDatabaseMsg struct {
	BaseMsg
	*milvuspb.DropDatabaseRequest
}

var _ TsMsg = &DropDatabaseMsg{}

func (d *DropDatabaseMsg) ID() UniqueID {
	return d.Base.MsgID
}

func (d *DropDatabaseMsg) SetID(id UniqueID) {
	d.Base.MsgID = id
}

func (d *DropDatabaseMsg) Type() MsgType {
	return d.Base.MsgType
}

func (d *DropDatabaseMsg) SourceID() int64 {
	return d.Base.SourceID
}

func (d *DropDatabaseMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropDataBaseMsg := input.(*DropDatabaseMsg)
	dropDatabaseRequest := dropDataBaseMsg.DropDatabaseRequest
	mb, err := proto.Marshal(dropDatabaseRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (d *DropDatabaseMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropDatabaseRequest := &milvuspb.DropDatabaseRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, dropDatabaseRequest)
	if err != nil {
		return nil, err
	}
	dropDatabaseMsg := &DropDatabaseMsg{DropDatabaseRequest: dropDatabaseRequest}
	dropDatabaseMsg.BeginTimestamp = dropDatabaseMsg.GetBase().GetTimestamp()
	dropDatabaseMsg.EndTimestamp = dropDatabaseMsg.GetBase().GetTimestamp()

	return dropDatabaseMsg, nil
}

func (d *DropDatabaseMsg) Size() int {
	return proto.Size(d.DropDatabaseRequest)
}

type AlterDatabaseMsg struct {
	BaseMsg
	*milvuspb.AlterDatabaseRequest
}

var _ TsMsg = &AlterDatabaseMsg{}

func (a *AlterDatabaseMsg) ID() UniqueID {
	return a.Base.MsgID
}

func (a *AlterDatabaseMsg) SetID(id UniqueID) {
	a.Base.MsgID = id
}

func (a *AlterDatabaseMsg) Type() MsgType {
	return a.Base.MsgType
}

func (a *AlterDatabaseMsg) SourceID() int64 {
	return a.Base.SourceID
}

func (a *AlterDatabaseMsg) Marshal(input TsMsg) (MarshalType, error) {
	alterDataBaseMsg := input.(*AlterDatabaseMsg)
	alterDatabaseRequest := alterDataBaseMsg.AlterDatabaseRequest
	mb, err := proto.Marshal(alterDatabaseRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (a *AlterDatabaseMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	alterDatabaseRequest := &milvuspb.AlterDatabaseRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, alterDatabaseRequest)
	if err != nil {
		return nil, err
	}
	alterDatabaseMsg := &AlterDatabaseMsg{AlterDatabaseRequest: alterDatabaseRequest}
	alterDatabaseMsg.BeginTimestamp = alterDatabaseMsg.GetBase().GetTimestamp()
	alterDatabaseMsg.EndTimestamp = alterDatabaseMsg.GetBase().GetTimestamp()

	return alterDatabaseMsg, nil
}

func (a *AlterDatabaseMsg) Size() int {
	return proto.Size(a.AlterDatabaseRequest)
}
