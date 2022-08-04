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
	HandoffSegmentPrefix = "querycoord-handoff"
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
)

// ObjectPrivilegeAPI privilege name for using api
type ObjectPrivilegeAPI int32

func (o ObjectPrivilegeAPI) String() string {
	switch o {
	case All:
		return "All"
	case CreateCollection:
		return "CreateCollection"
	case DropCollection:
		return "DropCollection"
	case DescribeCollection:
		return "DescribeCollection"
	case ShowCollections:
		return "ShowCollections"
	case Load:
		return "Load"
	case Release:
		return "Release"
	case Compact:
		return "Compact"
	case Insert:
		return "Insert"
	case Delete:
		return "Delete"
	default:
		return ""
	}
}

const (
	All ObjectPrivilegeAPI = iota
	CreateCollection
	DropCollection
	DescribeCollection
	ShowCollections
	Load
	Release
	Compact
	Insert
	Delete

	None = 999
)

func GetObjectPrivilegeFromName(name string) ObjectPrivilegeAPI {
	switch name {
	case All.String():
		return All
	case CreateCollection.String():
		return CreateCollection
	case DropCollection.String():
		return DropCollection
	case DescribeCollection.String():
		return DescribeCollection
	case ShowCollections.String():
		return ShowCollections
	case Load.String():
		return Load
	case Release.String():
		return Release
	case Compact.String():
		return Compact
	case Insert.String():
		return Insert
	case Delete.String():
		return Delete
	default:
		return None
	}
}

// StringSet convert array to map for conveniently check if the array contains an element
func StringSet(strings []string) map[string]struct{} {
	stringsMap := make(map[string]struct{})
	for _, str := range strings {
		stringsMap[str] = struct{}{}
	}
	return stringsMap
}

func StringList(stringMap map[string]struct{}) []string {
	strs := make([]string, 0, len(stringMap))
	for k := range stringMap {
		strs = append(strs, k)
	}
	return strs
}

// GetObjectPrivileges get the mapping between object types and privileges. This kind of data is constant and doesn't support to CRUD
func GetObjectPrivileges() map[string][]string {
	return map[string][]string{
		commonpb.ObjectType_Collection.String(): {
			Load.String(),
			Release.String(),
			Compact.String(),
			Insert.String(),
			Delete.String(),
		},
		commonpb.ObjectType_Global.String(): {
			All.String(),
			CreateCollection.String(),
			DropCollection.String(),
			DescribeCollection.String(),
			ShowCollections.String(),
		},
	}
}

func PrivilegeNameForAPI(name string) string {
	index, ok := commonpb.ObjectPrivilege_value[name]
	if !ok {
		return ""
	}
	return ObjectPrivilegeAPI(index).String()
}

func PrivilegeNameForDb(name string) string {
	o := GetObjectPrivilegeFromName(name)
	if o == None {
		return ""
	}
	return commonpb.ObjectPrivilege_name[int32(o)]
}
