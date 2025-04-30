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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

func TestCreateUser(t *testing.T) {
	var msg TsMsg = &CreateUserMsg{
		CreateCredentialRequest: &milvuspb.CreateCredentialRequest{
			Base: &commonpb.MsgBase{
				MsgType:       commonpb.MsgType_CreateCredential,
				MsgID:         100,
				Timestamp:     1000,
				SourceID:      10000,
				TargetID:      100000,
				ReplicateInfo: nil,
			},
			Username: "unit_user",
			Password: "unit_password",
		},
	}
	assert.EqualValues(t, 100, msg.ID())
	msg.SetID(200)
	assert.EqualValues(t, 200, msg.ID())
	assert.Equal(t, commonpb.MsgType_CreateCredential, msg.Type())
	assert.EqualValues(t, 10000, msg.SourceID())

	msgBytes, err := msg.Marshal(msg)
	assert.NoError(t, err)

	var newMsg TsMsg = &CreateUserMsg{}
	_, err = newMsg.Unmarshal("1")
	assert.Error(t, err)

	newMsg, err = newMsg.Unmarshal(msgBytes)
	assert.NoError(t, err)
	assert.EqualValues(t, 200, newMsg.ID())
	assert.EqualValues(t, 1000, newMsg.BeginTs())
	assert.EqualValues(t, 1000, newMsg.EndTs())
	assert.EqualValues(t, "unit_user", newMsg.(*CreateUserMsg).Username)
	assert.EqualValues(t, "unit_password", newMsg.(*CreateUserMsg).Password)

	assert.True(t, msg.Size() > 0)
}

func TestUpdateUser(t *testing.T) {
	var msg TsMsg = &UpdateUserMsg{
		UpdateCredentialRequest: &milvuspb.UpdateCredentialRequest{
			Base: &commonpb.MsgBase{
				MsgType:       commonpb.MsgType_UpdateCredential,
				MsgID:         100,
				Timestamp:     1000,
				SourceID:      10000,
				TargetID:      100000,
				ReplicateInfo: nil,
			},
			Username:    "unit_user",
			OldPassword: "unit_old_password",
			NewPassword: "unit_new_password",
		},
	}
	assert.EqualValues(t, 100, msg.ID())
	msg.SetID(200)
	assert.EqualValues(t, 200, msg.ID())
	assert.Equal(t, commonpb.MsgType_UpdateCredential, msg.Type())
	assert.EqualValues(t, 10000, msg.SourceID())

	msgBytes, err := msg.Marshal(msg)
	assert.NoError(t, err)

	var newMsg TsMsg = &UpdateUserMsg{}
	_, err = newMsg.Unmarshal("1")
	assert.Error(t, err)

	newMsg, err = newMsg.Unmarshal(msgBytes)
	assert.NoError(t, err)
	assert.EqualValues(t, 200, newMsg.ID())
	assert.EqualValues(t, 1000, newMsg.BeginTs())
	assert.EqualValues(t, 1000, newMsg.EndTs())
	assert.EqualValues(t, "unit_user", newMsg.(*UpdateUserMsg).Username)
	assert.EqualValues(t, "unit_old_password", newMsg.(*UpdateUserMsg).OldPassword)
	assert.EqualValues(t, "unit_new_password", newMsg.(*UpdateUserMsg).NewPassword)

	assert.True(t, msg.Size() > 0)
}

func TestDeleteUser(t *testing.T) {
	var msg TsMsg = &DeleteUserMsg{
		DeleteCredentialRequest: &milvuspb.DeleteCredentialRequest{
			Base: &commonpb.MsgBase{
				MsgType:       commonpb.MsgType_DeleteCredential,
				MsgID:         100,
				Timestamp:     1000,
				SourceID:      10000,
				TargetID:      100000,
				ReplicateInfo: nil,
			},
			Username: "unit_user",
		},
	}
	assert.EqualValues(t, 100, msg.ID())
	msg.SetID(200)
	assert.EqualValues(t, 200, msg.ID())
	assert.Equal(t, commonpb.MsgType_DeleteCredential, msg.Type())
	assert.EqualValues(t, 10000, msg.SourceID())

	msgBytes, err := msg.Marshal(msg)
	assert.NoError(t, err)

	var newMsg TsMsg = &DeleteUserMsg{}
	_, err = newMsg.Unmarshal("1")
	assert.Error(t, err)

	newMsg, err = newMsg.Unmarshal(msgBytes)
	assert.NoError(t, err)
	assert.EqualValues(t, 200, newMsg.ID())
	assert.EqualValues(t, 1000, newMsg.BeginTs())
	assert.EqualValues(t, 1000, newMsg.EndTs())
	assert.EqualValues(t, "unit_user", newMsg.(*DeleteUserMsg).Username)

	assert.True(t, msg.Size() > 0)
}

func TestCreateRole(t *testing.T) {
	var msg TsMsg = &CreateRoleMsg{
		CreateRoleRequest: &milvuspb.CreateRoleRequest{
			Base: &commonpb.MsgBase{
				MsgType:       commonpb.MsgType_CreateRole,
				MsgID:         100,
				Timestamp:     1000,
				SourceID:      10000,
				TargetID:      100000,
				ReplicateInfo: nil,
			},
			Entity: &milvuspb.RoleEntity{
				Name: "unit_role",
			},
		},
	}
	assert.EqualValues(t, 100, msg.ID())
	msg.SetID(200)
	assert.EqualValues(t, 200, msg.ID())
	assert.Equal(t, commonpb.MsgType_CreateRole, msg.Type())
	assert.EqualValues(t, 10000, msg.SourceID())

	msgBytes, err := msg.Marshal(msg)
	assert.NoError(t, err)

	var newMsg TsMsg = &CreateRoleMsg{}
	_, err = newMsg.Unmarshal("1")
	assert.Error(t, err)

	newMsg, err = newMsg.Unmarshal(msgBytes)
	assert.NoError(t, err)
	assert.EqualValues(t, 200, newMsg.ID())
	assert.EqualValues(t, 1000, newMsg.BeginTs())
	assert.EqualValues(t, 1000, newMsg.EndTs())
	assert.EqualValues(t, "unit_role", newMsg.(*CreateRoleMsg).GetEntity().GetName())

	assert.True(t, msg.Size() > 0)
}

func TestDropRole(t *testing.T) {
	var msg TsMsg = &DropRoleMsg{
		DropRoleRequest: &milvuspb.DropRoleRequest{
			Base: &commonpb.MsgBase{
				MsgType:       commonpb.MsgType_DropRole,
				MsgID:         100,
				Timestamp:     1000,
				SourceID:      10000,
				TargetID:      100000,
				ReplicateInfo: nil,
			},
			RoleName: "unit_role",
		},
	}
	assert.EqualValues(t, 100, msg.ID())
	msg.SetID(200)
	assert.EqualValues(t, 200, msg.ID())
	assert.Equal(t, commonpb.MsgType_DropRole, msg.Type())
	assert.EqualValues(t, 10000, msg.SourceID())

	msgBytes, err := msg.Marshal(msg)
	assert.NoError(t, err)

	var newMsg TsMsg = &DropRoleMsg{}
	_, err = newMsg.Unmarshal("1")
	assert.Error(t, err)

	newMsg, err = newMsg.Unmarshal(msgBytes)
	assert.NoError(t, err)
	assert.EqualValues(t, 200, newMsg.ID())
	assert.EqualValues(t, 1000, newMsg.BeginTs())
	assert.EqualValues(t, 1000, newMsg.EndTs())
	assert.EqualValues(t, "unit_role", newMsg.(*DropRoleMsg).GetRoleName())

	assert.True(t, msg.Size() > 0)
}

func TestOperateUserRole(t *testing.T) {
	var msg TsMsg = &OperateUserRoleMsg{
		OperateUserRoleRequest: &milvuspb.OperateUserRoleRequest{
			Base: &commonpb.MsgBase{
				MsgType:       commonpb.MsgType_OperateUserRole,
				MsgID:         100,
				Timestamp:     1000,
				SourceID:      10000,
				TargetID:      100000,
				ReplicateInfo: nil,
			},
			RoleName: "unit_role",
			Username: "unit_user",
			Type:     milvuspb.OperateUserRoleType_AddUserToRole,
		},
	}
	assert.EqualValues(t, 100, msg.ID())
	msg.SetID(200)
	assert.EqualValues(t, 200, msg.ID())
	assert.Equal(t, commonpb.MsgType_OperateUserRole, msg.Type())
	assert.EqualValues(t, 10000, msg.SourceID())

	msgBytes, err := msg.Marshal(msg)
	assert.NoError(t, err)

	var newMsg TsMsg = &OperateUserRoleMsg{}
	_, err = newMsg.Unmarshal("1")
	assert.Error(t, err)

	newMsg, err = newMsg.Unmarshal(msgBytes)
	assert.NoError(t, err)
	assert.EqualValues(t, 200, newMsg.ID())
	assert.EqualValues(t, 1000, newMsg.BeginTs())
	assert.EqualValues(t, 1000, newMsg.EndTs())
	assert.EqualValues(t, "unit_role", newMsg.(*OperateUserRoleMsg).GetRoleName())
	assert.EqualValues(t, "unit_user", newMsg.(*OperateUserRoleMsg).GetUsername())
	assert.EqualValues(t, milvuspb.OperateUserRoleType_AddUserToRole, newMsg.(*OperateUserRoleMsg).GetType())

	assert.True(t, msg.Size() > 0)
}

func TestOperatePrivilege(t *testing.T) {
	var msg TsMsg = &OperatePrivilegeMsg{
		OperatePrivilegeRequest: &milvuspb.OperatePrivilegeRequest{
			Base: &commonpb.MsgBase{
				MsgType:       commonpb.MsgType_OperatePrivilege,
				MsgID:         100,
				Timestamp:     1000,
				SourceID:      10000,
				TargetID:      100000,
				ReplicateInfo: nil,
			},
			Entity: &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: "unit_role"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				ObjectName: "col1",
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: "unit_user"},
					Privilege: &milvuspb.PrivilegeEntity{Name: "unit_privilege"},
				},
				DbName: "unit_db",
			},
			Type: milvuspb.OperatePrivilegeType_Grant,
		},
	}
	assert.EqualValues(t, 100, msg.ID())
	msg.SetID(200)
	assert.EqualValues(t, 200, msg.ID())
	assert.Equal(t, commonpb.MsgType_OperatePrivilege, msg.Type())
	assert.EqualValues(t, 10000, msg.SourceID())

	msgBytes, err := msg.Marshal(msg)
	assert.NoError(t, err)

	var newMsg TsMsg = &OperatePrivilegeMsg{}
	_, err = newMsg.Unmarshal("1")
	assert.Error(t, err)

	newMsg, err = newMsg.Unmarshal(msgBytes)
	assert.NoError(t, err)
	assert.EqualValues(t, 200, newMsg.ID())
	assert.EqualValues(t, 1000, newMsg.BeginTs())
	assert.EqualValues(t, 1000, newMsg.EndTs())
	assert.EqualValues(t, "unit_role", newMsg.(*OperatePrivilegeMsg).GetEntity().GetRole().GetName())
	assert.EqualValues(t, "Collection", newMsg.(*OperatePrivilegeMsg).GetEntity().GetObject().GetName())
	assert.EqualValues(t, "col1", newMsg.(*OperatePrivilegeMsg).GetEntity().GetObjectName())
	assert.EqualValues(t, "unit_user", newMsg.(*OperatePrivilegeMsg).GetEntity().GetGrantor().GetUser().GetName())
	assert.EqualValues(t, "unit_privilege", newMsg.(*OperatePrivilegeMsg).GetEntity().GetGrantor().GetPrivilege().GetName())
	assert.EqualValues(t, milvuspb.OperatePrivilegeType_Grant, newMsg.(*OperatePrivilegeMsg).GetType())

	assert.True(t, msg.Size() > 0)
}

func TestOperatePrivilegeV2(t *testing.T) {
	var msg TsMsg = &OperatePrivilegeV2Msg{
		OperatePrivilegeV2Request: &milvuspb.OperatePrivilegeV2Request{
			Base: &commonpb.MsgBase{
				MsgType:       commonpb.MsgType_OperatePrivilegeV2,
				MsgID:         100,
				Timestamp:     1000,
				SourceID:      10000,
				TargetID:      100000,
				ReplicateInfo: nil,
			},
			Grantor: &milvuspb.GrantorEntity{
				User: &milvuspb.UserEntity{Name: "unit_user"},
				Privilege: &milvuspb.PrivilegeEntity{
					Name: "unit_privilege",
				},
			},
			Type: milvuspb.OperatePrivilegeType_Grant,
		},
	}
	assert.EqualValues(t, 100, msg.ID())
	msg.SetID(200)
	assert.EqualValues(t, 200, msg.ID())
	assert.Equal(t, commonpb.MsgType_OperatePrivilegeV2, msg.Type())
	assert.EqualValues(t, 10000, msg.SourceID())

	msgBytes, err := msg.Marshal(msg)
	assert.NoError(t, err)

	var newMsg TsMsg = &OperatePrivilegeV2Msg{}
	_, err = newMsg.Unmarshal("1")
	assert.Error(t, err)

	newMsg, err = newMsg.Unmarshal(msgBytes)
	assert.NoError(t, err)
	assert.EqualValues(t, 200, newMsg.ID())
	assert.EqualValues(t, 1000, newMsg.BeginTs())
	assert.EqualValues(t, 1000, newMsg.EndTs())
	assert.EqualValues(t, "unit_user", newMsg.(*OperatePrivilegeV2Msg).GetGrantor().GetUser().GetName())
	assert.EqualValues(t, "unit_privilege", newMsg.(*OperatePrivilegeV2Msg).GetGrantor().GetPrivilege().GetName())
}
