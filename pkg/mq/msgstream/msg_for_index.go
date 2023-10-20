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
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// CreateIndexMsg is a message pack that contains create index request
type CreateIndexMsg struct {
	BaseMsg
	milvuspb.CreateIndexRequest
}

// interface implementation validation
var _ TsMsg = &CreateIndexMsg{}

// ID returns the ID of this message pack
func (it *CreateIndexMsg) ID() UniqueID {
	return it.Base.MsgID
}

// SetID set the ID of this message pack
func (it *CreateIndexMsg) SetID(id UniqueID) {
	it.Base.MsgID = id
}

// Type returns the type of this message pack
func (it *CreateIndexMsg) Type() MsgType {
	return it.Base.MsgType
}

// SourceID indicates which component generated this message
func (it *CreateIndexMsg) SourceID() int64 {
	return it.Base.SourceID
}

// Marshal is used to serialize a message pack to byte array
func (it *CreateIndexMsg) Marshal(input TsMsg) (MarshalType, error) {
	createIndexMsg := input.(*CreateIndexMsg)
	createIndexRequest := &createIndexMsg.CreateIndexRequest
	mb, err := proto.Marshal(createIndexRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserialize a message pack from byte array
func (it *CreateIndexMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createIndexRequest := milvuspb.CreateIndexRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &createIndexRequest)
	if err != nil {
		return nil, err
	}
	createIndexMsg := &CreateIndexMsg{CreateIndexRequest: createIndexRequest}
	createIndexMsg.BeginTimestamp = createIndexMsg.GetBase().GetTimestamp()
	createIndexMsg.EndTimestamp = createIndexMsg.GetBase().GetTimestamp()

	return createIndexMsg, nil
}

func (it *CreateIndexMsg) Size() int {
	return proto.Size(&it.CreateIndexRequest)
}

// DropIndexMsg is a message pack that contains drop index request
type DropIndexMsg struct {
	BaseMsg
	milvuspb.DropIndexRequest
}

var _ TsMsg = &DropIndexMsg{}

func (d *DropIndexMsg) ID() UniqueID {
	return d.Base.MsgID
}

func (d *DropIndexMsg) SetID(id UniqueID) {
	d.Base.MsgID = id
}

func (d *DropIndexMsg) Type() MsgType {
	return d.Base.MsgType
}

func (d *DropIndexMsg) SourceID() int64 {
	return d.Base.SourceID
}

func (d *DropIndexMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropIndexMsg := input.(*DropIndexMsg)
	dropIndexRequest := &dropIndexMsg.DropIndexRequest
	mb, err := proto.Marshal(dropIndexRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (d *DropIndexMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropIndexRequest := milvuspb.DropIndexRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &dropIndexRequest)
	if err != nil {
		return nil, err
	}
	dropIndexMsg := &DropIndexMsg{DropIndexRequest: dropIndexRequest}
	dropIndexMsg.BeginTimestamp = dropIndexMsg.GetBase().GetTimestamp()
	dropIndexMsg.EndTimestamp = dropIndexMsg.GetBase().GetTimestamp()

	return dropIndexMsg, nil
}

func (d *DropIndexMsg) Size() int {
	return proto.Size(&d.DropIndexRequest)
}
