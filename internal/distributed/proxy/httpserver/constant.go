package httpserver

import "time"

// v2
const (
	// --- category ---
	CollectionCategory = "/collections/"
	EntityCategory     = "/entities/"
	PartitionCategory  = "/partitions/"
	UserCategory       = "/users/"
	RoleCategory       = "/roles/"
	IndexCategory      = "/indexes/"
	AliasCategory      = "/aliases/"
	ImportJobCategory  = "/jobs/import/"

	ListAction         = "list"
	HasAction          = "has"
	DescribeAction     = "describe"
	CreateAction       = "create"
	DropAction         = "drop"
	StatsAction        = "get_stats"
	LoadStateAction    = "get_load_state"
	RenameAction       = "rename"
	LoadAction         = "load"
	ReleaseAction      = "release"
	QueryAction        = "query"
	GetAction          = "get"
	DeleteAction       = "delete"
	InsertAction       = "insert"
	UpsertAction       = "upsert"
	SearchAction       = "search"
	HybridSearchAction = "hybrid_search"

	UpdatePasswordAction  = "update_password"
	GrantRoleAction       = "grant_role"
	RevokeRoleAction      = "revoke_role"
	GrantPrivilegeAction  = "grant_privilege"
	RevokePrivilegeAction = "revoke_privilege"
	AlterAction           = "alter"
	GetProgressAction     = "get_progress"
)

const (
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

	PrimaryFieldName = "id"
	VectorFieldName  = "vector"

	EnableDynamic = true
	EnableAutoID  = true
	DisableAutoID = false

	HTTPCollectionName       = "collectionName"
	HTTPDbName               = "dbName"
	HTTPPartitionName        = "partitionName"
	HTTPPartitionNames       = "partitionNames"
	HTTPUserName             = "userName"
	HTTPRoleName             = "roleName"
	HTTPIndexName            = "indexName"
	HTTPIndexField           = "fieldName"
	HTTPAliasName            = "aliasName"
	DefaultDbName            = "default"
	DefaultIndexName         = "vector_idx"
	DefaultAliasName         = "the_alias"
	DefaultOutputFields      = "*"
	HTTPHeaderAllowInt64     = "Accept-Type-Allow-Int64"
	HTTPHeaderRequestTimeout = "Request-Timeout"
	HTTPDefaultTimeout       = 30 * time.Second
	HTTPReturnCode           = "code"
	HTTPReturnMessage        = "message"
	HTTPReturnData           = "data"
	HTTPReturnLoadState      = "loadState"
	HTTPReturnLoadProgress   = "loadProgress"

	HTTPReturnHas = "has"

	HTTPReturnFieldName       = "name"
	HTTPReturnFieldType       = "type"
	HTTPReturnFieldPrimaryKey = "primaryKey"
	HTTPReturnFieldAutoID     = "autoId"
	HTTPReturnDescription     = "description"

	HTTPReturnIndexMetricsType = "metricType"
	HTTPReturnIndexType        = "indexType"
	HTTPReturnIndexTotalRows   = "totalRows"
	HTTPReturnIndexPendingRows = "pendingRows"
	HTTPReturnIndexIndexedRows = "indexedRows"
	HTTPReturnIndexState       = "indexState"
	HTTPReturnIndexFailReason  = "failReason"

	HTTPReturnDistance = "distance"

	HTTPReturnRowCount = "rowCount"

	HTTPReturnObjectType = "objectType"
	HTTPReturnObjectName = "objectName"
	HTTPReturnPrivilege  = "privilege"
	HTTPReturnGrantor    = "grantor"
	HTTPReturnDbName     = "dbName"

	DefaultMetricType       = "L2"
	DefaultPrimaryFieldName = "id"
	DefaultVectorFieldName  = "vector"

	Dim = "dim"
)

const (
	ParamAnnsField    = "anns_field"
	Params            = "params"
	ParamRoundDecimal = "round_decimal"
	ParamOffset       = "offset"
	ParamLimit        = "limit"
	ParamRadius       = "radius"
	ParamRangeFilter  = "range_filter"
	ParamGroupByField = "group_by_field"
	BoundedTimestamp  = 2
)
