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

package util

import "github.com/milvus-io/milvus/internal/proto/commonpb"

// Meta Prefix consts
const (
	SegmentMetaPrefix    = "queryCoord-segmentMeta"
	ChangeInfoMetaPrefix = "queryCoord-sealedSegmentChangeInfo"
	HeaderAuthorize      = "authorization"
	// HeaderSourceID identify requests from Milvus members and client requests
	HeaderSourceID = "sourceId"
	// MemberCredID id for Milvus members (data/index/query node/coord component)
	MemberCredID        = "@@milvus-member@@"
	CredentialSeperator = ":"
	UserRoot            = "root"
	DefaultRootPassword = "Milvus"
	DefaultTenant       = ""
	RoleAdmin           = "admin"
	RolePublic          = "public"
	UserPrincipalType   = "User"
	RolePrincipalType   = "Role"

	All     = "All"
	Create  = "Create"
	Drop    = "Drop"
	Alter   = "Alter"
	Read    = "Read"
	Load    = "Load"
	Release = "Release"
	Compact = "Compact"
	Insert  = "Insert"
	Delete  = "Delete"
	// privilege name for using api
)

var (
	ResourcePrivilegeAPIName = map[int32]string{
		0: All,
		1: Create,
		2: Drop,
		3: Alter,
		4: Read,
		5: Load,
		6: Release,
		7: Compact,
		8: Insert,
		9: Delete,
	}

	ResourcePrivilegeAPIValue = map[string]int32{
		All:     0,
		Create:  1,
		Drop:    2,
		Alter:   3,
		Read:    4,
		Load:    5,
		Release: 6,
		Compact: 7,
		Insert:  8,
		Delete:  9,
	}
)

// StringSet convert array to map for conveniently check if the array contains an element
func StringSet(strings []string) map[string]interface{} {
	stringsMap := make(map[string]interface{})
	for _, str := range strings {
		stringsMap[str] = nil
	}
	return stringsMap
}

func StringList(stringMap map[string]interface{}) []string {
	strs := make([]string, 0, len(stringMap))
	for k := range stringMap {
		strs = append(strs, k)
	}
	return strs
}

// GetResourceTypes get all resource types. This kind of data is constant and doesn't support to CRUD
func GetResourceTypes() []string {
	resourceTypes := make([]string, 0, len(commonpb.ResourceType_value))
	for k := range commonpb.ResourceType_value {
		resourceTypes = append(resourceTypes, k)
	}
	return resourceTypes
}

// GetResourcePrivileges get the mapping between resource types and privileges. This kind of data is constant and doesn't support to CRUD
func GetResourcePrivileges() map[string][]string {
	return map[string][]string{
		commonpb.ResourceType_Collection.String(): {
			All,
			Create,
			Drop,
			Alter,
			Read,
			Load,
			Release,
			Compact,
			Insert,
			Delete,
		},
	}
}

func PrivilegeNameForAPI(name string) string {
	index, ok := commonpb.ResourcePrivilege_value[name]
	if !ok {
		return ""
	}
	return ResourcePrivilegeAPIName[index]
}

func PrivilegeNameForDb(name string) string {
	index, ok := ResourcePrivilegeAPIValue[name]
	if !ok {
		return ""
	}
	return commonpb.ResourcePrivilege_name[index]
}

// GetPrincipalTypes get the valid principal types. This kind of data is constant and doesn't support to CRUD
func GetPrincipalTypes() []string {
	return []string{UserPrincipalType, RolePrincipalType}
}
