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

package httpserver

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

// v2
const (
	// --- category ---
	DataBaseCategory        = "/databases/"
	CollectionCategory      = "/collections/"
	EntityCategory          = "/entities/"
	PartitionCategory       = "/partitions/"
	UserCategory            = "/users/"
	RoleCategory            = "/roles/"
	IndexCategory           = "/indexes/"
	AliasCategory           = "/aliases/"
	ImportJobCategory       = "/jobs/import/"
	PrivilegeGroupCategory  = "/privilege_groups/"
	CollectionFieldCategory = "/collections/fields/"
	ResourceGroupCategory   = "/resource_groups/"

	ListAction           = "list"
	HasAction            = "has"
	DescribeAction       = "describe"
	CreateAction         = "create"
	DropAction           = "drop"
	StatsAction          = "get_stats"
	LoadStateAction      = "get_load_state"
	RenameAction         = "rename"
	LoadAction           = "load"
	RefreshLoadAction    = "refresh_load"
	ReleaseAction        = "release"
	QueryAction          = "query"
	GetAction            = "get"
	DeleteAction         = "delete"
	InsertAction         = "insert"
	UpsertAction         = "upsert"
	SearchAction         = "search"
	AdvancedSearchAction = "advanced_search"
	HybridSearchAction   = "hybrid_search"

	UpdatePasswordAction            = "update_password"
	GrantRoleAction                 = "grant_role"
	RevokeRoleAction                = "revoke_role"
	GrantPrivilegeAction            = "grant_privilege"
	RevokePrivilegeAction           = "revoke_privilege"
	GrantPrivilegeActionV2          = "grant_privilege_v2"
	RevokePrivilegeActionV2         = "revoke_privilege_v2"
	AlterAction                     = "alter"
	AlterPropertiesAction           = "alter_properties"
	DropPropertiesAction            = "drop_properties"
	CompactAction                   = "compact"
	CompactionStateAction           = "get_compaction_state"
	FlushAction                     = "flush"
	GetProgressAction               = "get_progress" // deprecated, keep it for compatibility, use `/v2/vectordb/jobs/import/describe` instead
	AddPrivilegesToGroupAction      = "add_privileges_to_group"
	RemovePrivilegesFromGroupAction = "remove_privileges_from_group"
	TransferReplicaAction           = "transfer_replica"
)

const (
	ContextRequest                = "request"
	ContextUsername               = "username"
	VectorCollectionsPath         = "/vector/collections"
	VectorCollectionsCreatePath   = "/vector/collections/create"
	VectorCollectionsDescribePath = "/vector/collections/describe"
	VectorCollectionsDropPath     = "/vector/collections/drop"
	VectorInsertPath              = "/vector/insert"
	VectorUpsertPath              = "/vector/upsert"
	VectorSearchPath              = "/vector/search"
	VectorGetPath                 = "/vector/get"
	VectorQueryPath               = "/vector/query"
	VectorDeletePath              = "/vector/delete"

	ShardNumDefault = 1

	EnableDynamic = true
	EnableAutoID  = true
	DisableAutoID = false

	HTTPCollectionName       = "collectionName"
	HTTPCollectionID         = "collectionID"
	HTTPDbName               = "dbName"
	HTTPDbID                 = "dbID"
	HTTPProperties           = "properties"
	HTTPPartitionName        = "partitionName"
	HTTPPartitionNames       = "partitionNames"
	HTTPUserName             = "userName"
	HTTPRoleName             = "roleName"
	HTTPIndexName            = "indexName"
	HTTPIndexField           = "fieldName"
	HTTPAliasName            = "aliasName"
	HTTPRequestData          = "data"
	HTTPRequestDefaultValue  = "defaultValue"
	DefaultDbName            = "default"
	DefaultIndexName         = "vector_idx"
	DefaultAliasName         = "the_alias"
	DefaultOutputFields      = "*"
	HTTPHeaderAllowInt64     = "Accept-Type-Allow-Int64"
	HTTPHeaderDBName         = "DB-Name"
	HTTPHeaderRequestTimeout = "Request-Timeout"
	HTTPDefaultTimeout       = 30 * time.Second
	HTTPReturnCode           = "code"
	HTTPReturnMessage        = "message"
	HTTPReturnData           = "data"
	HTTPReturnCost           = "cost"
	HTTPReturnRecalls        = "recalls"
	HTTPReturnLoadState      = "loadState"
	HTTPReturnLoadProgress   = "loadProgress"
	HTTPReturnTopks          = "topks"

	HTTPReturnHas = "has"

	HTTPReturnFieldName             = "name"
	HTTPReturnFieldID               = "id"
	HTTPReturnFieldType             = "type"
	HTTPReturnFieldPrimaryKey       = "primaryKey"
	HTTPReturnFieldPartitionKey     = "partitionKey"
	HTTPReturnFieldClusteringKey    = "clusteringKey"
	HTTPReturnFieldNullable         = "nullable"
	HTTPReturnFieldDefaultValue     = "defaultValue"
	HTTPReturnFieldAutoID           = "autoId"
	HTTPReturnFieldElementType      = "elementType"
	HTTPReturnDescription           = "description"
	HTTPReturnFieldIsFunctionOutput = "isFunctionOutput"

	HTTPReturnFunctionName             = "name"
	HTTPReturnFunctionID               = "id"
	HTTPReturnFunctionType             = "type"
	HTTPReturnFunctionInputFieldNames  = "inputFieldNames"
	HTTPReturnFunctionOutputFieldNames = "outputFieldNames"
	HTTPReturnFunctionParams           = "params"

	HTTPReturnIndexMetricType      = "metricType"
	HTTPReturnIndexType            = "indexType"
	HTTPIndexOffsetCacheEnabledKey = "indexoffsetcache.enabled"
	HTTPMmapEnabledKey             = "mmap.enabled"
	HTTPReturnIndexTotalRows       = "totalRows"
	HTTPReturnIndexPendingRows     = "pendingRows"
	HTTPReturnIndexIndexedRows     = "indexedRows"
	HTTPReturnIndexState           = "indexState"
	HTTPReturnIndexFailReason      = "failReason"

	HTTPReturnDistance = "distance"

	HTTPReturnRowCount = "rowCount"

	HTTPReturnObjectType         = "objectType"
	HTTPReturnObjectName         = "objectName"
	HTTPReturnPrivilege          = "privilege"
	HTTPReturnGrantor            = "grantor"
	HTTPReturnDbName             = "dbName"
	HTTPReturnPrivilegeGroupName = "privilegeGroupName"
	HTTPReturnPrivileges         = "privileges"
	HTTPReturnPrivilegeGroups    = "privilegeGroups"

	DefaultMetricType       = metric.COSINE
	DefaultPrimaryFieldName = "id"
	DefaultVectorFieldName  = "vector"

	Dim = "dim"
)

const (
	ParamAnnsField       = "anns_field"
	Params               = "params"
	ParamRoundDecimal    = "round_decimal"
	ParamOffset          = "offset"
	ParamLimit           = "limit"
	ParamRadius          = "radius"
	ParamRangeFilter     = "range_filter"
	ParamGroupByField    = "group_by_field"
	ParamGroupSize       = "group_size"
	ParamStrictGroupSize = "strict_group_size"
	BoundedTimestamp     = 2
)
