package httpserver

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

	EnableDynamic = true
	EnableAutoID  = true
	DisableAutoID = false

	HTTPCollectionName   = "collectionName"
	HTTPDbName           = "dbName"
	DefaultDbName        = "default"
	DefaultIndexName     = "vector_idx"
	DefaultOutputFields  = "*"
	HTTPHeaderAllowInt64 = "Accept-Type-Allow-Int64"
	HTTPReturnCode       = "code"
	HTTPReturnMessage    = "message"
	HTTPReturnData       = "data"

	HTTPReturnFieldName       = "name"
	HTTPReturnFieldType       = "type"
	HTTPReturnFieldPrimaryKey = "primaryKey"
	HTTPReturnFieldAutoID     = "autoId"
	HTTPReturnDescription     = "description"

	HTTPReturnIndexName        = "indexName"
	HTTPReturnIndexField       = "fieldName"
	HTTPReturnIndexMetricsType = "metricType"

	HTTPReturnDistance = "distance"

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
	BoundedTimestamp  = 2
)
