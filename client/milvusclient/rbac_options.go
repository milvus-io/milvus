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
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// GrantV2Option is the interface builds OperatePrivilegeV2Request
type GrantV2Option interface {
	Request() *milvuspb.OperatePrivilegeV2Request
}

type grantV2Option struct {
	roleName       string
	privilegeName  string
	dbName         string
	collectionName string
}

func (opt *grantV2Option) Request() *milvuspb.OperatePrivilegeV2Request {
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

func NewGrantV2Option(roleName, privilegeName, dbName, collectionName string) *grantV2Option {
	return &grantV2Option{
		roleName:       roleName,
		privilegeName:  privilegeName,
		dbName:         dbName,
		collectionName: collectionName,
	}
}

// RevokeV2Option is the interface builds OperatePrivilegeV2Request
type RevokeV2Option interface {
	Request() *milvuspb.OperatePrivilegeV2Request
}

type revokeV2Option struct {
	roleName       string
	privilegeName  string
	dbName         string
	collectionName string
}

func (opt *revokeV2Option) Request() *milvuspb.OperatePrivilegeV2Request {
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

func NewRevokeV2Option(roleName, privilegeName, dbName, collectionName string) *revokeV2Option {
	return &revokeV2Option{
		roleName:       roleName,
		privilegeName:  privilegeName,
		dbName:         dbName,
		collectionName: collectionName,
	}
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

func NewOperatePrivilegeGroupOption(groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) *operatePrivilegeGroupOption {
	return &operatePrivilegeGroupOption{
		groupName:   groupName,
		privileges:  privileges,
		operateType: operateType,
	}
}
