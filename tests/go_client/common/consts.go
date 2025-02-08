package common

import "github.com/milvus-io/milvus/client/v2/index"

// cost default field name
const (
	DefaultInt8FieldName          = "int8"
	DefaultInt16FieldName         = "int16"
	DefaultInt32FieldName         = "int32"
	DefaultInt64FieldName         = "int64"
	DefaultBoolFieldName          = "bool"
	DefaultFloatFieldName         = "float"
	DefaultDoubleFieldName        = "double"
	DefaultTextFieldName          = "text"
	DefaultVarcharFieldName       = "varchar"
	DefaultJSONFieldName          = "json"
	DefaultArrayFieldName         = "array"
	DefaultFloatVecFieldName      = "floatVec"
	DefaultBinaryVecFieldName     = "binaryVec"
	DefaultFloat16VecFieldName    = "fp16Vec"
	DefaultBFloat16VecFieldName   = "bf16Vec"
	DefaultTextSparseVecFieldName = "textSparseVec"
	DefaultSparseVecFieldName     = "sparseVec"
	DefaultDynamicNumberField     = "dynamicNumber"
	DefaultDynamicStringField     = "dynamicString"
	DefaultDynamicBoolField       = "dynamicBool"
	DefaultDynamicListField       = "dynamicList"
	DefaultBoolArrayField         = "boolArray"
	DefaultInt8ArrayField         = "int8Array"
	DefaultInt16ArrayField        = "int16Array"
	DefaultInt32ArrayField        = "int32Array"
	DefaultInt64ArrayField        = "int64Array"
	DefaultFloatArrayField        = "floatArray"
	DefaultDoubleArrayField       = "doubleArray"
	DefaultVarcharArrayField      = "varcharArray"
)

// cost for test cases
const (
	RowCount       = "row_count"
	DefaultTimeout = 120
	DefaultDim     = 128
	DefaultShards  = int32(2)
	DefaultNb      = 3000
	DefaultNq      = 5
	DefaultLimit   = 10
	TestCapacity   = 100 // default array field capacity
	TestMaxLen     = 100 // default varchar field max length
)

// const default value from milvus config
const (
	MaxPartitionNum         = 1024
	DefaultDynamicFieldName = "$meta"
	QueryCountFieldName     = "count(*)"
	DefaultPartition        = "_default"
	DefaultIndexName        = "_default_idx_102"
	DefaultIndexNameBinary  = "_default_idx_100"
	DefaultRgName           = "__default_resource_group"
	DefaultDb               = "default"
	MaxDim                  = 32768
	MaxLength               = int64(65535)
	MaxCollectionNameLen    = 255
	DefaultRgCapacity       = 1000000
	RetentionDuration       = 40   // common.retentionDuration
	MaxCapacity             = 4096 // max array capacity
	DefaultPartitionNum     = 16   // default num_partitions
	MaxTopK                 = 16384
	MaxVectorFieldNum       = 4
	MaxShardNum             = 16
)

const (
	IndexStateIndexStateNone index.IndexState = 0
	IndexStateUnissued       index.IndexState = 1
	IndexStateInProgress     index.IndexState = 2
	IndexStateFinished       index.IndexState = 3
	IndexStateFailed         index.IndexState = 4
	IndexStateRetry          index.IndexState = 5
)

// part database properties
const (
	DatabaseMaxCollections   = "database.max.collections"
	DatabaseResourceGroups   = "database.resource_groups"
	DatabaseReplicaNumber    = "database.replica.number"
	DatabaseForceDenyWriting = "database.force.deny.writing"
	DatabaseForceDenyReading = "database.force.deny.reading"
	DatabaseDiskQuotaMb      = "database.diskQuota.mb"
)

// const for full text search
const (
	DefaultTextLang = "en"
)
