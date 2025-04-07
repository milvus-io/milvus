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

package milvusclient

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
)

type ListUserOption interface {
	Request() *milvuspb.ListCredUsersRequest
}

// listUserOption is the struct to build ListCredUsersRequest
// left empty for not attribute needed right now
type listUserOption struct{}

func (opt *listUserOption) Request() *milvuspb.ListCredUsersRequest {
	return &milvuspb.ListCredUsersRequest{}
}

func NewListUserOption() *listUserOption {
	return &listUserOption{}
}

type DescribeUserOption interface {
	Request() *milvuspb.SelectUserRequest
}

type describeUserOption struct {
	userName string
}

func (opt *describeUserOption) Request() *milvuspb.SelectUserRequest {
	return &milvuspb.SelectUserRequest{
		User: &milvuspb.UserEntity{
			Name: opt.userName,
		},
		IncludeRoleInfo: true,
	}
}

func NewDescribeUserOption(userName string) *describeUserOption {
	return &describeUserOption{
		userName: userName,
	}
}

type CreateUserOption interface {
	Request() *milvuspb.CreateCredentialRequest
}

type createUserOption struct {
	userName string
	password string
}

func (opt *createUserOption) Request() *milvuspb.CreateCredentialRequest {
	return &milvuspb.CreateCredentialRequest{
		Username: opt.userName,
		Password: crypto.Base64Encode(opt.password),
	}
}

func NewCreateUserOption(userName, password string) *createUserOption {
	return &createUserOption{
		userName: userName,
		password: password,
	}
}

type UpdatePasswordOption interface {
	Request() *milvuspb.UpdateCredentialRequest
}

type updatePasswordOption struct {
	userName    string
	oldPassword string
	newPassword string
}

func (opt *updatePasswordOption) Request() *milvuspb.UpdateCredentialRequest {
	return &milvuspb.UpdateCredentialRequest{
		Username:    opt.userName,
		OldPassword: crypto.Base64Encode(opt.oldPassword),
		NewPassword: crypto.Base64Encode(opt.newPassword),
	}
}

func NewUpdatePasswordOption(userName, oldPassword, newPassword string) *updatePasswordOption {
	return &updatePasswordOption{
		userName:    userName,
		oldPassword: oldPassword,
		newPassword: newPassword,
	}
}

type DropUserOption interface {
	Request() *milvuspb.DeleteCredentialRequest
}

type dropUserOption struct {
	userName string
}

func (opt *dropUserOption) Request() *milvuspb.DeleteCredentialRequest {
	return &milvuspb.DeleteCredentialRequest{
		Username: opt.userName,
	}
}

func NewDropUserOption(userName string) *dropUserOption {
	return &dropUserOption{
		userName: userName,
	}
}

type ListRoleOption interface {
	Request() *milvuspb.SelectRoleRequest
}

type listRoleOption struct{}

func (opt *listRoleOption) Request() *milvuspb.SelectRoleRequest {
	return &milvuspb.SelectRoleRequest{
		IncludeUserInfo: false,
	}
}

func NewListRoleOption() *listRoleOption {
	return &listRoleOption{}
}

type CreateRoleOption interface {
	Request() *milvuspb.CreateRoleRequest
}

type createRoleOption struct {
	roleName string
}

func (opt *createRoleOption) Request() *milvuspb.CreateRoleRequest {
	return &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: opt.roleName},
	}
}

func NewCreateRoleOption(roleName string) *createRoleOption {
	return &createRoleOption{
		roleName: roleName,
	}
}

type GrantRoleOption interface {
	Request() *milvuspb.OperateUserRoleRequest
}

type grantRoleOption struct {
	roleName string
	userName string
}

func (opt *grantRoleOption) Request() *milvuspb.OperateUserRoleRequest {
	return &milvuspb.OperateUserRoleRequest{
		Username: opt.userName,
		RoleName: opt.roleName,
		Type:     milvuspb.OperateUserRoleType_AddUserToRole,
	}
}

func NewGrantRoleOption(userName, roleName string) *grantRoleOption {
	return &grantRoleOption{
		roleName: roleName,
		userName: userName,
	}
}

type RevokeRoleOption interface {
	Request() *milvuspb.OperateUserRoleRequest
}

type revokeRoleOption struct {
	roleName string
	userName string
}

func (opt *revokeRoleOption) Request() *milvuspb.OperateUserRoleRequest {
	return &milvuspb.OperateUserRoleRequest{
		Username: opt.userName,
		RoleName: opt.roleName,
		Type:     milvuspb.OperateUserRoleType_RemoveUserFromRole,
	}
}

func NewRevokeRoleOption(userName, roleName string) *revokeRoleOption {
	return &revokeRoleOption{
		roleName: roleName,
		userName: userName,
	}
}

type DropRoleOption interface {
	Request() *milvuspb.DropRoleRequest
}

type dropDropRoleOption struct {
	roleName string
}

func (opt *dropDropRoleOption) Request() *milvuspb.DropRoleRequest {
	return &milvuspb.DropRoleRequest{
		RoleName: opt.roleName,
	}
}

func NewDropRoleOption(roleName string) *dropDropRoleOption {
	return &dropDropRoleOption{
		roleName: roleName,
	}
}

type DescribeRoleOption interface {
	SelectRoleRequest() *milvuspb.SelectRoleRequest
	Request() *milvuspb.SelectGrantRequest
}

type describeRoleOption struct {
	roleName string
	dbName   string
}

func (opt *describeRoleOption) SelectRoleRequest() *milvuspb.SelectRoleRequest {
	return &milvuspb.SelectRoleRequest{
		Role: &milvuspb.RoleEntity{
			Name: opt.roleName,
		},
	}
}

func (opt *describeRoleOption) Request() *milvuspb.SelectGrantRequest {
	return &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{
			Role:   &milvuspb.RoleEntity{Name: opt.roleName},
			DbName: opt.dbName,
		},
	}
}

func NewDescribeRoleOption(roleName string) *describeRoleOption {
	return &describeRoleOption{
		roleName: roleName,
	}
}

func (opt *describeRoleOption) WithDbName(dbName string) *describeRoleOption {
	opt.dbName = dbName
	return opt
}

type GrantPrivilegeOption interface {
	Request() *milvuspb.OperatePrivilegeRequest
}

type grantPrivilegeOption struct {
	roleName      string
	privilegeName string
	objectName    string
	objectType    string
	dbName        string
}

func (opt *grantPrivilegeOption) Request() *milvuspb.OperatePrivilegeRequest {
	return &milvuspb.OperatePrivilegeRequest{
		Entity: &milvuspb.GrantEntity{
			Role: &milvuspb.RoleEntity{Name: opt.roleName},
			Grantor: &milvuspb.GrantorEntity{
				Privilege: &milvuspb.PrivilegeEntity{Name: opt.privilegeName},
			},
			Object: &milvuspb.ObjectEntity{
				Name: opt.objectType,
			},
			ObjectName: opt.objectName,
			DbName:     opt.dbName,
		},

		Type: milvuspb.OperatePrivilegeType_Grant,
	}
}

func NewGrantPrivilegeOption(roleName, objectType, privilegeName, objectName string) *grantPrivilegeOption {
	return &grantPrivilegeOption{
		roleName:      roleName,
		privilegeName: privilegeName,
		objectName:    objectName,
		objectType:    objectType,
	}
}

func (opt *grantPrivilegeOption) WithDbName(dbName string) *grantPrivilegeOption {
	opt.dbName = dbName
	return opt
}

type RevokePrivilegeOption interface {
	Request() *milvuspb.OperatePrivilegeRequest
}

type revokePrivilegeOption struct {
	roleName      string
	privilegeName string
	objectName    string
	objectType    string
	dbName        string
}

func (opt *revokePrivilegeOption) Request() *milvuspb.OperatePrivilegeRequest {
	return &milvuspb.OperatePrivilegeRequest{
		Entity: &milvuspb.GrantEntity{
			Role: &milvuspb.RoleEntity{Name: opt.roleName},
			Grantor: &milvuspb.GrantorEntity{
				Privilege: &milvuspb.PrivilegeEntity{Name: opt.privilegeName},
			},
			Object: &milvuspb.ObjectEntity{
				Name: opt.objectType,
			},
			ObjectName: opt.objectName,
			DbName:     opt.dbName,
		},

		Type: milvuspb.OperatePrivilegeType_Revoke,
	}
}

func NewRevokePrivilegeOption(roleName, objectType, privilegeName, objectName string) *revokePrivilegeOption {
	return &revokePrivilegeOption{
		roleName:      roleName,
		privilegeName: privilegeName,
		objectName:    objectName,
		objectType:    objectType,
	}
}

func (opt *revokePrivilegeOption) WithDbName(dbName string) *revokePrivilegeOption {
	opt.dbName = dbName
	return opt
}

type GrantV2Option GrantPrivilegeV2Option

// GrantPrivilegeV2Option is the interface builds OperatePrivilegeV2Request
type GrantPrivilegeV2Option interface {
	Request() *milvuspb.OperatePrivilegeV2Request
}

type grantPrivilegeV2Option struct {
	roleName       string
	privilegeName  string
	dbName         string
	collectionName string
}

func (opt *grantPrivilegeV2Option) Request() *milvuspb.OperatePrivilegeV2Request {
	return &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: opt.roleName},
		Grantor: &milvuspb.GrantorEntity{
			Privilege: &milvuspb.PrivilegeEntity{Name: opt.privilegeName},
		},
		Type:           milvuspb.OperatePrivilegeType_Grant,
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
	}
}

// Deprecated, use `NewGrantPrivilegeV2Option` instead
func NewGrantV2Option(roleName, privilegeName, dbName, collectionName string) *grantPrivilegeV2Option {
	return &grantPrivilegeV2Option{
		roleName:       roleName,
		privilegeName:  privilegeName,
		dbName:         dbName,
		collectionName: collectionName,
	}
}

func NewGrantPrivilegeV2Option(roleName, privilegeName, collectionName string) *grantPrivilegeV2Option {
	return &grantPrivilegeV2Option{
		roleName:       roleName,
		privilegeName:  privilegeName,
		collectionName: collectionName,
	}
}

func (opt *grantPrivilegeV2Option) WithDbName(dbName string) *grantPrivilegeV2Option {
	opt.dbName = dbName
	return opt
}

type RevokeV2Option RevokePrivilegeV2Option

// RevokePrivilegeV2Option is the interface builds OperatePrivilegeV2Request
type RevokePrivilegeV2Option interface {
	Request() *milvuspb.OperatePrivilegeV2Request
}

type revokePrivilegeV2Option struct {
	roleName       string
	privilegeName  string
	dbName         string
	collectionName string
}

func (opt *revokePrivilegeV2Option) Request() *milvuspb.OperatePrivilegeV2Request {
	return &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: opt.roleName},
		Grantor: &milvuspb.GrantorEntity{
			Privilege: &milvuspb.PrivilegeEntity{Name: opt.privilegeName},
		},
		Type:           milvuspb.OperatePrivilegeType_Revoke,
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
	}
}

// Deprecated, use `NewRevokePrivilegeV2Option` instead
func NewRevokeV2Option(roleName, privilegeName, dbName, collectionName string) *revokePrivilegeV2Option {
	return &revokePrivilegeV2Option{
		roleName:       roleName,
		privilegeName:  privilegeName,
		dbName:         dbName,
		collectionName: collectionName,
	}
}

func NewRevokePrivilegeV2Option(roleName, privilegeName, collectionName string) *revokePrivilegeV2Option {
	return &revokePrivilegeV2Option{
		roleName:       roleName,
		privilegeName:  privilegeName,
		collectionName: collectionName,
	}
}

func (opt *revokePrivilegeV2Option) WithDbName(dbName string) *revokePrivilegeV2Option {
	opt.dbName = dbName
	return opt
}

// CreatePrivilegeGroupOption is the interface builds CreatePrivilegeGroupRequest
type CreatePrivilegeGroupOption interface {
	Request() *milvuspb.CreatePrivilegeGroupRequest
}

type createPrivilegeGroupOption struct {
	groupName string
}

func (opt *createPrivilegeGroupOption) Request() *milvuspb.CreatePrivilegeGroupRequest {
	return &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: opt.groupName,
	}
}

func NewCreatePrivilegeGroupOption(groupName string) *createPrivilegeGroupOption {
	return &createPrivilegeGroupOption{
		groupName: groupName,
	}
}

// DropPrivilegeGroupOption is the interface builds DropPrivilegeGroupRequest
type DropPrivilegeGroupOption interface {
	Request() *milvuspb.DropPrivilegeGroupRequest
}

type dropPrivilegeGroupOption struct {
	groupName string
}

func (opt *dropPrivilegeGroupOption) Request() *milvuspb.DropPrivilegeGroupRequest {
	return &milvuspb.DropPrivilegeGroupRequest{
		GroupName: opt.groupName,
	}
}

func NewDropPrivilegeGroupOption(groupName string) *dropPrivilegeGroupOption {
	return &dropPrivilegeGroupOption{
		groupName: groupName,
	}
}

// ListPrivilegeGroupsOption is the interface builds ListPrivilegeGroupsRequest
type ListPrivilegeGroupsOption interface {
	Request() *milvuspb.ListPrivilegeGroupsRequest
}

type listPrivilegeGroupsOption struct{}

func (opt *listPrivilegeGroupsOption) Request() *milvuspb.ListPrivilegeGroupsRequest {
	return &milvuspb.ListPrivilegeGroupsRequest{}
}

func NewListPrivilegeGroupsOption() *listPrivilegeGroupsOption {
	return &listPrivilegeGroupsOption{}
}

// OperatePrivilegeGroupOption is the interface builds OperatePrivilegeGroupRequest
type OperatePrivilegeGroupOption interface {
	Request() *milvuspb.OperatePrivilegeGroupRequest
}

type operatePrivilegeGroupOption struct {
	groupName   string
	privileges  []*milvuspb.PrivilegeEntity
	operateType milvuspb.OperatePrivilegeGroupType
}

func (opt *operatePrivilegeGroupOption) Request() *milvuspb.OperatePrivilegeGroupRequest {
	return &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  opt.groupName,
		Privileges: opt.privileges,
		Type:       opt.operateType,
	}
}

// Deprecated, use AddPrivilegeToGroupOption/ RemovePrivilegeFromGroupOption instead
func NewOperatePrivilegeGroupOption(groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) *operatePrivilegeGroupOption {
	return &operatePrivilegeGroupOption{
		groupName:   groupName,
		privileges:  privileges,
		operateType: operateType,
	}
}

type AddPrivilegeToGroupOption interface {
	Request() *milvuspb.OperatePrivilegeGroupRequest
}

type addPrivilegeToGroupOption struct {
	privileges []string
	groupName  string
}

func (opt *addPrivilegeToGroupOption) Request() *milvuspb.OperatePrivilegeGroupRequest {
	return &milvuspb.OperatePrivilegeGroupRequest{
		GroupName: opt.groupName,
		Privileges: lo.Map(opt.privileges, func(privilege string, _ int) *milvuspb.PrivilegeEntity {
			return &milvuspb.PrivilegeEntity{
				Name: privilege,
			}
		}),
		Type: milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup,
	}
}

func NewAddPrivilegesToGroupOption(groupName string, privileges ...string) *addPrivilegeToGroupOption {
	return &addPrivilegeToGroupOption{
		groupName:  groupName,
		privileges: privileges,
	}
}

type RemovePrivilegeFromGroupOption interface {
	Request() *milvuspb.OperatePrivilegeGroupRequest
}

type removePrivilegeFromGroupOption struct {
	privileges []string
	groupName  string
}

func (opt *removePrivilegeFromGroupOption) Request() *milvuspb.OperatePrivilegeGroupRequest {
	return &milvuspb.OperatePrivilegeGroupRequest{
		GroupName: opt.groupName,
		Privileges: lo.Map(opt.privileges, func(privilege string, _ int) *milvuspb.PrivilegeEntity {
			return &milvuspb.PrivilegeEntity{
				Name: privilege,
			}
		}),
		Type: milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup,
	}
}

func NewRemovePrivilegesFromGroupOption(groupName string, privileges ...string) *removePrivilegeFromGroupOption {
	return &removePrivilegeFromGroupOption{
		groupName:  groupName,
		privileges: privileges,
	}
}
