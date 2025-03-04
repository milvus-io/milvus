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

package entity

type User struct {
	UserName string
	Roles    []string
}

type Role struct {
	RoleName   string
	Privileges []GrantItem
}

type GrantItem struct {
	Object     string
	ObjectName string
	RoleName   string
	Grantor    string
	Privilege  string
	DbName     string
}

type UserInfo struct {
	UserDescription
	Password string
}

// UserDescription is the model for RBAC user description object.
type UserDescription struct {
	Name  string
	Roles []string
}

type RBACMeta struct {
	Users           []*UserInfo
	Roles           []*Role
	RoleGrants      []*RoleGrants
	PrivilegeGroups []*PrivilegeGroup
}

// RoleGrants is the model for RBAC role description object.
type RoleGrants struct {
	Object        string
	ObjectName    string
	RoleName      string
	GrantorName   string
	PrivilegeName string
	DbName        string
}
