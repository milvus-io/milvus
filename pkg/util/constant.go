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

import (
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Meta Prefix consts
const (
	MetaStoreTypeEtcd = "etcd"
	MetaStoreTypeTiKV = "tikv"

	SegmentMetaPrefix    = "queryCoord-segmentMeta"
	ChangeInfoMetaPrefix = "queryCoord-sealedSegmentChangeInfo"

	// FlushedSegmentPrefix TODO @cai.zhang: remove this
	FlushedSegmentPrefix = "flushed-segment"
	// HandoffSegmentPrefix TODO @cai.zhang: remove this
	HandoffSegmentPrefix = "querycoord-handoff"
	// SegmentReferPrefix TODO @cai.zhang: remove this
	SegmentReferPrefix = "segmentRefer"

	SegmentIndexPrefix = "segment-index"
	FieldIndexPrefix   = "field-index"

	HeaderAuthorize = "authorization"
	HeaderToken     = "token"
	// HeaderSourceID identify requests from Milvus members and client requests
	HeaderSourceID = "sourceId"
	// MemberCredID id for Milvus members (data/index/query node/coord component)
	MemberCredID        = "@@milvus-member@@"
	CredentialSeperator = ":"
	UserRoot            = "root"
	PasswordHolder      = "___"
	DefaultTenant       = ""
	RoleAdmin           = "admin"
	RolePublic          = "public"
	DefaultDBName       = "default"
	DefaultDBID         = int64(1)
	NonDBID             = int64(0)
	InvalidDBID         = int64(-1)

	PrivilegeWord      = "Privilege"
	PrivilegeGroupWord = "PrivilegeGroup"
	AnyWord            = "*"

	IdentifierKey = "identifier"

	HeaderUserAgent = "user-agent"
	HeaderDBName    = "dbName"

	RoleConfigPrivileges = "privileges"
	RoleConfigObjectType = "object_type"
	RoleConfigObjectName = "object_name"
	RoleConfigDBName     = "db_name"
	RoleConfigPrivilege  = "privilege"

	MaxEtcdTxnNum = 128
	GB            = 1024 * 1024 * 1024
)

const (
	// ParamsKeyToParse is the key of the param to build index.
	ParamsKeyToParse = common.IndexParamsKey
)

var (
	DefaultRoles = []string{RoleAdmin, RolePublic}
	BuiltinRoles = []string{}

	ObjectPrivileges = map[string][]string{
		commonpb.ObjectType_Collection.String(): {
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeLoad.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeRelease.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCompaction.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeInsert.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDelete.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeUpsert.String()),

			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGetStatistics.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateIndex.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeIndexDetail.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropIndex.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeSearch.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeFlush.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeQuery.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeLoadBalance.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeImport.String()),

			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGetLoadingProgress.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGetLoadState.String()),

			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreatePartition.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropPartition.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeShowPartitions.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeHasPartition.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGetFlushState.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupReadOnly.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupReadWrite.String()),
		},
		commonpb.ObjectType_Global.String(): {
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeAll.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateCollection.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropCollection.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDescribeCollection.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeShowCollections.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeRenameCollection.String()),

			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateOwnership.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropOwnership.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeSelectOwnership.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeManageOwnership.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeBackupRBAC.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeRestoreRBAC.String()),

			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateResourceGroup.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeUpdateResourceGroups.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropResourceGroup.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDescribeResourceGroup.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeListResourceGroups.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeTransferReplica.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeTransferNode.String()),

			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeFlushAll.String()),

			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateDatabase.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropDatabase.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeListDatabases.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeAlterDatabase.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDescribeDatabase.String()),

			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateAlias.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropAlias.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDescribeAlias.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeListAliases.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupAdmin.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreatePrivilegeGroup.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropPrivilegeGroup.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeListPrivilegeGroups.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeOperatePrivilegeGroup.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupClusterReadOnly.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupClusterReadWrite.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupClusterAdmin.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupDatabaseReadOnly.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupDatabaseReadWrite.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupDatabaseAdmin.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupCollectionReadOnly.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupCollectionReadWrite.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGroupCollectionAdmin.String()),
		},
		commonpb.ObjectType_User.String(): {
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeUpdateUser.String()),
			MetaStore2API(commonpb.ObjectPrivilege_PrivilegeSelectUser.String()),
		},
	}

	RelatedPrivileges = map[string][]string{
		commonpb.ObjectPrivilege_PrivilegeLoad.String(): {
			commonpb.ObjectPrivilege_PrivilegeGetLoadState.String(),
			commonpb.ObjectPrivilege_PrivilegeGetLoadingProgress.String(),
		},
		commonpb.ObjectPrivilege_PrivilegeFlush.String(): {
			commonpb.ObjectPrivilege_PrivilegeGetFlushState.String(),
		},
	}

	ReadOnlyPrivilegeGroup = []string{
		commonpb.ObjectPrivilege_PrivilegeQuery.String(),
		commonpb.ObjectPrivilege_PrivilegeSearch.String(),
		commonpb.ObjectPrivilege_PrivilegeIndexDetail.String(),
		commonpb.ObjectPrivilege_PrivilegeGetFlushState.String(),
		commonpb.ObjectPrivilege_PrivilegeGetLoadState.String(),
		commonpb.ObjectPrivilege_PrivilegeGetLoadingProgress.String(),
		commonpb.ObjectPrivilege_PrivilegeHasPartition.String(),
		commonpb.ObjectPrivilege_PrivilegeShowPartitions.String(),
		commonpb.ObjectPrivilege_PrivilegeShowCollections.String(),
		commonpb.ObjectPrivilege_PrivilegeListAliases.String(),
		commonpb.ObjectPrivilege_PrivilegeListDatabases.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeDatabase.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeAlias.String(),
		commonpb.ObjectPrivilege_PrivilegeGetStatistics.String(),
	}
	ReadWritePrivilegeGroup = []string{
		commonpb.ObjectPrivilege_PrivilegeQuery.String(),
		commonpb.ObjectPrivilege_PrivilegeSearch.String(),
		commonpb.ObjectPrivilege_PrivilegeIndexDetail.String(),
		commonpb.ObjectPrivilege_PrivilegeGetFlushState.String(),
		commonpb.ObjectPrivilege_PrivilegeGetLoadState.String(),
		commonpb.ObjectPrivilege_PrivilegeGetLoadingProgress.String(),
		commonpb.ObjectPrivilege_PrivilegeHasPartition.String(),
		commonpb.ObjectPrivilege_PrivilegeShowPartitions.String(),
		commonpb.ObjectPrivilege_PrivilegeShowCollections.String(),
		commonpb.ObjectPrivilege_PrivilegeListAliases.String(),
		commonpb.ObjectPrivilege_PrivilegeListDatabases.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeDatabase.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeAlias.String(),
		commonpb.ObjectPrivilege_PrivilegeGetStatistics.String(),
		commonpb.ObjectPrivilege_PrivilegeCreateIndex.String(),
		commonpb.ObjectPrivilege_PrivilegeDropIndex.String(),
		commonpb.ObjectPrivilege_PrivilegeCreatePartition.String(),
		commonpb.ObjectPrivilege_PrivilegeDropPartition.String(),
		commonpb.ObjectPrivilege_PrivilegeLoad.String(),
		commonpb.ObjectPrivilege_PrivilegeRelease.String(),
		commonpb.ObjectPrivilege_PrivilegeInsert.String(),
		commonpb.ObjectPrivilege_PrivilegeDelete.String(),
		commonpb.ObjectPrivilege_PrivilegeUpsert.String(),
		commonpb.ObjectPrivilege_PrivilegeImport.String(),
		commonpb.ObjectPrivilege_PrivilegeFlush.String(),
		commonpb.ObjectPrivilege_PrivilegeCompaction.String(),
		commonpb.ObjectPrivilege_PrivilegeLoadBalance.String(),
		commonpb.ObjectPrivilege_PrivilegeRenameCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeCreateAlias.String(),
		commonpb.ObjectPrivilege_PrivilegeDropAlias.String(),
	}
	AdminPrivilegeGroup = []string{
		commonpb.ObjectPrivilege_PrivilegeCreateCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeDropCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeQuery.String(),
		commonpb.ObjectPrivilege_PrivilegeSearch.String(),
		commonpb.ObjectPrivilege_PrivilegeIndexDetail.String(),
		commonpb.ObjectPrivilege_PrivilegeGetFlushState.String(),
		commonpb.ObjectPrivilege_PrivilegeGetLoadState.String(),
		commonpb.ObjectPrivilege_PrivilegeGetLoadingProgress.String(),
		commonpb.ObjectPrivilege_PrivilegeHasPartition.String(),
		commonpb.ObjectPrivilege_PrivilegeShowPartitions.String(),
		commonpb.ObjectPrivilege_PrivilegeShowCollections.String(),
		commonpb.ObjectPrivilege_PrivilegeListAliases.String(),
		commonpb.ObjectPrivilege_PrivilegeListDatabases.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeDatabase.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeAlias.String(),
		commonpb.ObjectPrivilege_PrivilegeGetStatistics.String(),
		commonpb.ObjectPrivilege_PrivilegeCreateIndex.String(),
		commonpb.ObjectPrivilege_PrivilegeDropIndex.String(),
		commonpb.ObjectPrivilege_PrivilegeCreateCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeDropCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeCreatePartition.String(),
		commonpb.ObjectPrivilege_PrivilegeDropPartition.String(),
		commonpb.ObjectPrivilege_PrivilegeLoad.String(),
		commonpb.ObjectPrivilege_PrivilegeRelease.String(),
		commonpb.ObjectPrivilege_PrivilegeInsert.String(),
		commonpb.ObjectPrivilege_PrivilegeDelete.String(),
		commonpb.ObjectPrivilege_PrivilegeUpsert.String(),
		commonpb.ObjectPrivilege_PrivilegeImport.String(),
		commonpb.ObjectPrivilege_PrivilegeFlush.String(),
		commonpb.ObjectPrivilege_PrivilegeCompaction.String(),
		commonpb.ObjectPrivilege_PrivilegeLoadBalance.String(),
		commonpb.ObjectPrivilege_PrivilegeRenameCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeCreateAlias.String(),
		commonpb.ObjectPrivilege_PrivilegeDropAlias.String(),
		commonpb.ObjectPrivilege_PrivilegeCreateOwnership.String(),
		commonpb.ObjectPrivilege_PrivilegeDropOwnership.String(),
		commonpb.ObjectPrivilege_PrivilegeSelectOwnership.String(),
		commonpb.ObjectPrivilege_PrivilegeManageOwnership.String(),
		commonpb.ObjectPrivilege_PrivilegeSelectUser.String(),
		commonpb.ObjectPrivilege_PrivilegeUpdateUser.String(),
		commonpb.ObjectPrivilege_PrivilegeBackupRBAC.String(),
		commonpb.ObjectPrivilege_PrivilegeRestoreRBAC.String(),
		commonpb.ObjectPrivilege_PrivilegeCreateResourceGroup.String(),
		commonpb.ObjectPrivilege_PrivilegeUpdateResourceGroups.String(),
		commonpb.ObjectPrivilege_PrivilegeDropResourceGroup.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeResourceGroup.String(),
		commonpb.ObjectPrivilege_PrivilegeListResourceGroups.String(),
		commonpb.ObjectPrivilege_PrivilegeTransferReplica.String(),
		commonpb.ObjectPrivilege_PrivilegeTransferNode.String(),
		commonpb.ObjectPrivilege_PrivilegeCreateDatabase.String(),
		commonpb.ObjectPrivilege_PrivilegeDropDatabase.String(),
		commonpb.ObjectPrivilege_PrivilegeAlterDatabase.String(),
		commonpb.ObjectPrivilege_PrivilegeFlush.String(),
	}
)

// rbac v2 uses privilege level to group privileges rather than object type
var (
	CollectionReadOnlyPrivileges = ConvertPrivileges([]string{
		commonpb.ObjectPrivilege_PrivilegeQuery.String(),
		commonpb.ObjectPrivilege_PrivilegeSearch.String(),
		commonpb.ObjectPrivilege_PrivilegeIndexDetail.String(),
		commonpb.ObjectPrivilege_PrivilegeGetFlushState.String(),
		commonpb.ObjectPrivilege_PrivilegeGetLoadState.String(),
		commonpb.ObjectPrivilege_PrivilegeGetLoadingProgress.String(),
		commonpb.ObjectPrivilege_PrivilegeHasPartition.String(),
		commonpb.ObjectPrivilege_PrivilegeShowPartitions.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeCollection.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeAlias.String(),
		commonpb.ObjectPrivilege_PrivilegeGetStatistics.String(),
		commonpb.ObjectPrivilege_PrivilegeListAliases.String(),
	})

	CollectionReadWritePrivileges = append(CollectionReadOnlyPrivileges,
		ConvertPrivileges([]string{
			commonpb.ObjectPrivilege_PrivilegeLoad.String(),
			commonpb.ObjectPrivilege_PrivilegeRelease.String(),
			commonpb.ObjectPrivilege_PrivilegeInsert.String(),
			commonpb.ObjectPrivilege_PrivilegeDelete.String(),
			commonpb.ObjectPrivilege_PrivilegeUpsert.String(),
			commonpb.ObjectPrivilege_PrivilegeImport.String(),
			commonpb.ObjectPrivilege_PrivilegeFlush.String(),
			commonpb.ObjectPrivilege_PrivilegeCompaction.String(),
			commonpb.ObjectPrivilege_PrivilegeLoadBalance.String(),
			commonpb.ObjectPrivilege_PrivilegeCreateIndex.String(),
			commonpb.ObjectPrivilege_PrivilegeDropIndex.String(),
			commonpb.ObjectPrivilege_PrivilegeCreatePartition.String(),
			commonpb.ObjectPrivilege_PrivilegeDropPartition.String(),
		})...,
	)

	CollectionAdminPrivileges = append(CollectionReadWritePrivileges,
		ConvertPrivileges([]string{
			commonpb.ObjectPrivilege_PrivilegeCreateAlias.String(),
			commonpb.ObjectPrivilege_PrivilegeDropAlias.String(),
		})...,
	)

	DatabaseReadOnlyPrivileges = ConvertPrivileges([]string{
		commonpb.ObjectPrivilege_PrivilegeShowCollections.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeDatabase.String(),
	})

	DatabaseReadWritePrivileges = append(DatabaseReadOnlyPrivileges,
		ConvertPrivileges([]string{
			commonpb.ObjectPrivilege_PrivilegeAlterDatabase.String(),
		})...,
	)

	DatabaseAdminPrivileges = append(DatabaseReadWritePrivileges,
		ConvertPrivileges([]string{
			commonpb.ObjectPrivilege_PrivilegeCreateCollection.String(),
			commonpb.ObjectPrivilege_PrivilegeDropCollection.String(),
		})...,
	)

	ClusterReadOnlyPrivileges = ConvertPrivileges([]string{
		commonpb.ObjectPrivilege_PrivilegeListDatabases.String(),
		commonpb.ObjectPrivilege_PrivilegeSelectOwnership.String(),
		commonpb.ObjectPrivilege_PrivilegeSelectUser.String(),
		commonpb.ObjectPrivilege_PrivilegeDescribeResourceGroup.String(),
		commonpb.ObjectPrivilege_PrivilegeListResourceGroups.String(),
		commonpb.ObjectPrivilege_PrivilegeListPrivilegeGroups.String(),
	})

	ClusterReadWritePrivileges = append(ClusterReadOnlyPrivileges,
		ConvertPrivileges([]string{
			commonpb.ObjectPrivilege_PrivilegeFlushAll.String(),
			commonpb.ObjectPrivilege_PrivilegeTransferNode.String(),
			commonpb.ObjectPrivilege_PrivilegeTransferReplica.String(),
			commonpb.ObjectPrivilege_PrivilegeUpdateResourceGroups.String(),
		})...,
	)

	ClusterAdminPrivileges = append(ClusterReadWritePrivileges,
		ConvertPrivileges([]string{
			commonpb.ObjectPrivilege_PrivilegeBackupRBAC.String(),
			commonpb.ObjectPrivilege_PrivilegeRestoreRBAC.String(),
			commonpb.ObjectPrivilege_PrivilegeCreateDatabase.String(),
			commonpb.ObjectPrivilege_PrivilegeDropDatabase.String(),
			commonpb.ObjectPrivilege_PrivilegeCreateOwnership.String(),
			commonpb.ObjectPrivilege_PrivilegeDropOwnership.String(),
			commonpb.ObjectPrivilege_PrivilegeManageOwnership.String(),
			commonpb.ObjectPrivilege_PrivilegeCreateResourceGroup.String(),
			commonpb.ObjectPrivilege_PrivilegeDropResourceGroup.String(),
			commonpb.ObjectPrivilege_PrivilegeUpdateUser.String(),
			commonpb.ObjectPrivilege_PrivilegeRenameCollection.String(),
			commonpb.ObjectPrivilege_PrivilegeCreatePrivilegeGroup.String(),
			commonpb.ObjectPrivilege_PrivilegeDropPrivilegeGroup.String(),
			commonpb.ObjectPrivilege_PrivilegeOperatePrivilegeGroup.String(),
		})...,
	)
)

// ConvertPrivileges converts each privilege from metastore format to API format.
func ConvertPrivileges(privileges []string) []string {
	return lo.Map(privileges, func(name string, _ int) string {
		return MetaStore2API(name)
	})
}

func GetPrivilegeLevel(privilege string) string {
	if lo.Contains(ClusterAdminPrivileges, privilege) {
		return milvuspb.PrivilegeLevel_Cluster.String()
	}
	if lo.Contains(DatabaseAdminPrivileges, privilege) {
		return milvuspb.PrivilegeLevel_Database.String()
	}
	if lo.Contains(CollectionAdminPrivileges, privilege) {
		return milvuspb.PrivilegeLevel_Collection.String()
	}
	return ""
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

// MetaStore2API convert meta-store's privilege name to api's
// example: PrivilegeAll -> All
func MetaStore2API(name string) string {
	if strings.HasPrefix(name, PrivilegeGroupWord) {
		return name[len(PrivilegeGroupWord):]
	}
	if strings.HasPrefix(name, PrivilegeWord) {
		return name[len(PrivilegeWord):]
	}
	return name
}

func PrivilegeNameForAPI(name string) string {
	_, ok := commonpb.ObjectPrivilege_value[name]
	if !ok {
		if strings.HasPrefix(name, PrivilegeGroupWord) {
			return typeutil.After(name, PrivilegeGroupWord)
		}
		return ""
	}
	return MetaStore2API(name)
}

func PrivilegeNameForMetastore(name string) string {
	// check if name is single privilege
	dbPrivilege := PrivilegeWord + name
	_, ok := commonpb.ObjectPrivilege_value[dbPrivilege]
	if !ok {
		// check if name is privilege group
		dbPrivilege := PrivilegeGroupWord + name
		_, ok := commonpb.ObjectPrivilege_value[dbPrivilege]
		if !ok {
			return ""
		}
		return dbPrivilege
	}
	return dbPrivilege
}

// check if the name is defined by built in privileges or privilege groups in system
func IsPrivilegeNameDefined(name string) bool {
	return PrivilegeNameForMetastore(name) != ""
}

func IsBuiltinPrivilegeGroup(name string) bool {
	dbPrivilege := PrivilegeGroupWord + name
	_, ok := commonpb.ObjectPrivilege_value[dbPrivilege]
	return ok
}

func PrivilegeGroupNameForMetastore(name string) string {
	return PrivilegeGroupWord + name
}

func IsAnyWord(word string) bool {
	return word == AnyWord
}

func IsBuiltinRole(roleName string) bool {
	for _, builtinRole := range BuiltinRoles {
		if builtinRole == roleName {
			return true
		}
	}
	return false
}

func GetObjectType(privName string) string {
	for objectType, privs := range ObjectPrivileges {
		if lo.Contains(privs, privName) {
			return objectType
		}
	}
	return commonpb.ObjectType_Global.String()
}
