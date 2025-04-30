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

type CreateUserMsg struct {
	BaseMsg
	*milvuspb.CreateCredentialRequest
}

var _ TsMsg = &CreateUserMsg{}

func (c *CreateUserMsg) ID() UniqueID {
	return c.Base.MsgID
}

func (c *CreateUserMsg) SetID(id UniqueID) {
	c.Base.MsgID = id
}

func (c *CreateUserMsg) Type() MsgType {
	return c.Base.MsgType
}

func (c *CreateUserMsg) SourceID() int64 {
	return c.Base.SourceID
}

func (c *CreateUserMsg) Marshal(input TsMsg) (MarshalType, error) {
	createUserMsg := input.(*CreateUserMsg)
	createUserRequest := createUserMsg.CreateCredentialRequest
	mb, err := proto.Marshal(createUserRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (c *CreateUserMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createUserRequest := &milvuspb.CreateCredentialRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, createUserRequest)
	if err != nil {
		return nil, err
	}
	createUserMsg := &CreateUserMsg{CreateCredentialRequest: createUserRequest}
	createUserMsg.BeginTimestamp = createUserMsg.GetBase().GetTimestamp()
	createUserMsg.EndTimestamp = createUserMsg.GetBase().GetTimestamp()
	return createUserMsg, nil
}

func (c *CreateUserMsg) Size() int {
	return proto.Size(c.CreateCredentialRequest)
}

type UpdateUserMsg struct {
	BaseMsg
	*milvuspb.UpdateCredentialRequest
}

var _ TsMsg = &UpdateUserMsg{}

func (c *UpdateUserMsg) ID() UniqueID {
	return c.Base.MsgID
}

func (c *UpdateUserMsg) SetID(id UniqueID) {
	c.Base.MsgID = id
}

func (c *UpdateUserMsg) Type() MsgType {
	return c.Base.MsgType
}

func (c *UpdateUserMsg) SourceID() int64 {
	return c.Base.SourceID
}

func (c *UpdateUserMsg) Marshal(input TsMsg) (MarshalType, error) {
	updateUserMsg := input.(*UpdateUserMsg)
	updateUserRequest := updateUserMsg.UpdateCredentialRequest
	mb, err := proto.Marshal(updateUserRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (c *UpdateUserMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	updateUserRequest := &milvuspb.UpdateCredentialRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, updateUserRequest)
	if err != nil {
		return nil, err
	}
	updateUserMsg := &UpdateUserMsg{UpdateCredentialRequest: updateUserRequest}
	updateUserMsg.BeginTimestamp = updateUserMsg.GetBase().GetTimestamp()
	updateUserMsg.EndTimestamp = updateUserMsg.GetBase().GetTimestamp()
	return updateUserMsg, nil
}

func (c *UpdateUserMsg) Size() int {
	return proto.Size(c.UpdateCredentialRequest)
}

type DeleteUserMsg struct {
	BaseMsg
	*milvuspb.DeleteCredentialRequest
}

var _ TsMsg = &DeleteUserMsg{}

func (c *DeleteUserMsg) ID() UniqueID {
	return c.Base.MsgID
}

func (c *DeleteUserMsg) SetID(id UniqueID) {
	c.Base.MsgID = id
}

func (c *DeleteUserMsg) Type() MsgType {
	return c.Base.MsgType
}

func (c *DeleteUserMsg) SourceID() int64 {
	return c.Base.SourceID
}

func (c *DeleteUserMsg) Marshal(input TsMsg) (MarshalType, error) {
	deleteUserMsg := input.(*DeleteUserMsg)
	deleteUserRequest := deleteUserMsg.DeleteCredentialRequest
	mb, err := proto.Marshal(deleteUserRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (c *DeleteUserMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	deleteUserRequest := &milvuspb.DeleteCredentialRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, deleteUserRequest)
	if err != nil {
		return nil, err
	}
	deleteUserMsg := &DeleteUserMsg{DeleteCredentialRequest: deleteUserRequest}
	deleteUserMsg.BeginTimestamp = deleteUserMsg.GetBase().GetTimestamp()
	deleteUserMsg.EndTimestamp = deleteUserMsg.GetBase().GetTimestamp()
	return deleteUserMsg, nil
}

func (c *DeleteUserMsg) Size() int {
	return proto.Size(c.DeleteCredentialRequest)
}

type CreateRoleMsg struct {
	BaseMsg
	*milvuspb.CreateRoleRequest
}

var _ TsMsg = &CreateRoleMsg{}

func (c *CreateRoleMsg) ID() UniqueID {
	return c.Base.MsgID
}

func (c *CreateRoleMsg) SetID(id UniqueID) {
	c.Base.MsgID = id
}

func (c *CreateRoleMsg) Type() MsgType {
	return c.Base.MsgType
}

func (c *CreateRoleMsg) SourceID() int64 {
	return c.Base.SourceID
}

func (c *CreateRoleMsg) Marshal(input TsMsg) (MarshalType, error) {
	createRoleMsg := input.(*CreateRoleMsg)
	createRoleRequest := createRoleMsg.CreateRoleRequest
	mb, err := proto.Marshal(createRoleRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (c *CreateRoleMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createRoleRequest := &milvuspb.CreateRoleRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, createRoleRequest)
	if err != nil {
		return nil, err
	}
	createRoleMsg := &CreateRoleMsg{CreateRoleRequest: createRoleRequest}
	createRoleMsg.BeginTimestamp = createRoleMsg.GetBase().GetTimestamp()
	createRoleMsg.EndTimestamp = createRoleMsg.GetBase().GetTimestamp()
	return createRoleMsg, nil
}

func (c *CreateRoleMsg) Size() int {
	return proto.Size(c.CreateRoleRequest)
}

type DropRoleMsg struct {
	BaseMsg
	*milvuspb.DropRoleRequest
}

var _ TsMsg = &DropRoleMsg{}

func (c *DropRoleMsg) ID() UniqueID {
	return c.Base.MsgID
}

func (c *DropRoleMsg) SetID(id UniqueID) {
	c.Base.MsgID = id
}

func (c *DropRoleMsg) Type() MsgType {
	return c.Base.MsgType
}

func (c *DropRoleMsg) SourceID() int64 {
	return c.Base.SourceID
}

func (c *DropRoleMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropRoleMsg := input.(*DropRoleMsg)
	dropRoleRequest := dropRoleMsg.DropRoleRequest
	mb, err := proto.Marshal(dropRoleRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (c *DropRoleMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropRoleRequest := &milvuspb.DropRoleRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, dropRoleRequest)
	if err != nil {
		return nil, err
	}
	dropRoleMsg := &DropRoleMsg{DropRoleRequest: dropRoleRequest}
	dropRoleMsg.BeginTimestamp = dropRoleMsg.GetBase().GetTimestamp()
	dropRoleMsg.EndTimestamp = dropRoleMsg.GetBase().GetTimestamp()
	return dropRoleMsg, nil
}

func (c *DropRoleMsg) Size() int {
	return proto.Size(c.DropRoleRequest)
}

type OperateUserRoleMsg struct {
	BaseMsg
	*milvuspb.OperateUserRoleRequest
}

var _ TsMsg = &OperateUserRoleMsg{}

func (c *OperateUserRoleMsg) ID() UniqueID {
	return c.Base.MsgID
}

func (c *OperateUserRoleMsg) SetID(id UniqueID) {
	c.Base.MsgID = id
}

func (c *OperateUserRoleMsg) Type() MsgType {
	return c.Base.MsgType
}

func (c *OperateUserRoleMsg) SourceID() int64 {
	return c.Base.SourceID
}

func (c *OperateUserRoleMsg) Marshal(input TsMsg) (MarshalType, error) {
	operateUserRoleMsg := input.(*OperateUserRoleMsg)
	operateUserRoleRequest := operateUserRoleMsg.OperateUserRoleRequest
	mb, err := proto.Marshal(operateUserRoleRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (c *OperateUserRoleMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	operateUserRoleRequest := &milvuspb.OperateUserRoleRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, operateUserRoleRequest)
	if err != nil {
		return nil, err
	}
	operateUserRoleMsg := &OperateUserRoleMsg{OperateUserRoleRequest: operateUserRoleRequest}
	operateUserRoleMsg.BeginTimestamp = operateUserRoleMsg.GetBase().GetTimestamp()
	operateUserRoleMsg.EndTimestamp = operateUserRoleMsg.GetBase().GetTimestamp()
	return operateUserRoleMsg, nil
}

func (c *OperateUserRoleMsg) Size() int {
	return proto.Size(c.OperateUserRoleRequest)
}

type OperatePrivilegeMsg struct {
	BaseMsg
	*milvuspb.OperatePrivilegeRequest
}

var _ TsMsg = &OperatePrivilegeMsg{}

func (c *OperatePrivilegeMsg) ID() UniqueID {
	return c.Base.MsgID
}

func (c *OperatePrivilegeMsg) SetID(id UniqueID) {
	c.Base.MsgID = id
}

func (c *OperatePrivilegeMsg) Type() MsgType {
	return c.Base.MsgType
}

func (c *OperatePrivilegeMsg) SourceID() int64 {
	return c.Base.SourceID
}

func (c *OperatePrivilegeMsg) Marshal(input TsMsg) (MarshalType, error) {
	operatePrivilegeMsg := input.(*OperatePrivilegeMsg)
	operatePrivilegeRequest := operatePrivilegeMsg.OperatePrivilegeRequest
	mb, err := proto.Marshal(operatePrivilegeRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (c *OperatePrivilegeMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	operatePrivilegeRequest := &milvuspb.OperatePrivilegeRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, operatePrivilegeRequest)
	if err != nil {
		return nil, err
	}
	operatePrivilegeMsg := &OperatePrivilegeMsg{OperatePrivilegeRequest: operatePrivilegeRequest}
	operatePrivilegeMsg.BeginTimestamp = operatePrivilegeMsg.GetBase().GetTimestamp()
	operatePrivilegeMsg.EndTimestamp = operatePrivilegeMsg.GetBase().GetTimestamp()
	return operatePrivilegeMsg, nil
}

func (c *OperatePrivilegeMsg) Size() int {
	return proto.Size(c.OperatePrivilegeRequest)
}

type OperatePrivilegeV2Msg struct {
	BaseMsg
	*milvuspb.OperatePrivilegeV2Request
}

var _ TsMsg = &OperatePrivilegeV2Msg{}

func (c *OperatePrivilegeV2Msg) ID() UniqueID {
	return c.Base.MsgID
}

func (c *OperatePrivilegeV2Msg) SetID(id UniqueID) {
	c.Base.MsgID = id
}

func (c *OperatePrivilegeV2Msg) Type() MsgType {
	return c.Base.MsgType
}

func (c *OperatePrivilegeV2Msg) SourceID() int64 {
	return c.Base.SourceID
}

func (c *OperatePrivilegeV2Msg) Marshal(input TsMsg) (MarshalType, error) {
	operatePrivilegeV2Msg := input.(*OperatePrivilegeV2Msg)
	operatePrivilegeV2Request := operatePrivilegeV2Msg.OperatePrivilegeV2Request
	mb, err := proto.Marshal(operatePrivilegeV2Request)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (c *OperatePrivilegeV2Msg) Unmarshal(input MarshalType) (TsMsg, error) {
	operatePrivilegeV2Request := &milvuspb.OperatePrivilegeV2Request{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, operatePrivilegeV2Request)
	if err != nil {
		return nil, err
	}
	operatePrivilegeV2Msg := &OperatePrivilegeV2Msg{OperatePrivilegeV2Request: operatePrivilegeV2Request}
	operatePrivilegeV2Msg.BeginTimestamp = operatePrivilegeV2Msg.GetBase().GetTimestamp()
	operatePrivilegeV2Msg.EndTimestamp = operatePrivilegeV2Msg.GetBase().GetTimestamp()
	return operatePrivilegeV2Msg, nil
}

func (c *OperatePrivilegeV2Msg) Size() int {
	return proto.Size(c.OperatePrivilegeV2Request)
}
