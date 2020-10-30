package utils

import (
	"github.com/milvus-io/milvus-sdk-go/milvus"
)

var DefaultNlist int = 32
var DefaultNprobe = 68
var DefaultMetricType = milvus.L2
var DefaultBinaryMetricType = milvus.JACCARD
var DefaultTopk = 5
var DefaultNq = 2
var DefaultFieldFloatName string = "float"
var DefaultFieldIntName string = "int64"
var DefaultFieldFloatVectorName string = "float_vector"
var DefaultFieldBinaryVectorName string = "binary_vector"
var DefaultDimension int = 128
var DefaultSegmentRowLimit int = 5000
var DefaultNb = 6000
var DefaultIndexType = milvus.IVFSQ8
var DefaulltBinaryIndexType = milvus.BINIVFFLAT
var DefaultIntValues = GenDefaultIntValues(DefaultNb)
var DefaultFloatValues = GenDefaultFloatValues(DefaultNb)
var DefaultFloatVector = GenFloatVectors(DefaultDimension, 1, false)
var DefaultFloatVectors = GenFloatVectors(DefaultDimension, DefaultNb, false)

var DefaultBinaryVector = GenBinaryVectors(DefaultDimension, 1)
var DefaultBinaryVectors = GenBinaryVectors(DefaultDimension, DefaultNb)

var L2Indexes = GenIndexes(milvus.L2)
var IpIndexes = GenIndexes(milvus.IP)
var DefaultL2Index = Struct2Map(GenIndexes(milvus.L2)[2])

var DefaultQueries = GenVectorQuery(DefaultMetricType, DefaultIndexType)
var DefaultBinaryQueries = GenVectorQuery(DefaultBinaryMetricType, DefaulltBinaryIndexType)
var DefaultDsl = GenDSL(DefaultFieldFloatVectorName, DefaultMetricType, DefaultIndexType)
var DefaultBinaryDsl = GenDSL(DefaultFieldBinaryVectorName, DefaultBinaryMetricType, DefaulltBinaryIndexType)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type Index struct {
	IndexType  milvus.IndexType
	MetricType milvus.MetricType
	Params     map[string]interface{}
}

type FloatQuery struct {
	Topk       int
	MetricType milvus.MetricType
	Query      [][]float32
	Params     map[string]interface{}
}

type BinaryQuery struct {
	Topk       int
	MetricType milvus.MetricType
	Query      [][]uint8
	Params     map[string]interface{}
}

var allIndexTypes = []milvus.IndexType{
	milvus.FLAT,
	milvus.IVFFLAT,
	milvus.IVFSQ8,
	milvus.IVFSQ8H,
	milvus.IVFPQ,
	milvus.HNSW,
	// milvus.RNSG,
	milvus.ANNOY,
}

var floatIndexParams = []map[string]interface{}{
	{"nlist": DefaultNlist},
	{"nlist": DefaultNlist},
	{"nlist": DefaultNlist},
	{"nlist": DefaultNlist},
	{"nlist": DefaultNlist, "m": 16},
	{"M": 48, "efConstruction": 500},
	// {"search_length": 50, "out_degree": 40, "candidate_pool_size": 100, "knng": 50},
	{"n_trees": 50},
}

var binaryIndexParams = []map[string]interface{}{
	{"nlist": DefaultNlist},
	{"nlist": DefaultNlist},
}

var floatQueryParams = []map[string]interface{}{
	{"nprobe": DefaultNprobe},
	{"nprobe": DefaultNprobe},
	{"nprobe": DefaultNprobe},
	{"nprobe": DefaultNprobe},
	{"nprobe": DefaultNprobe},
	{"ef": 64},
	// {"search_length": 100},
	{"search_k": 1000},
}

var binaryQueryParams = []map[string]interface{}{
	{"nprobe": DefaultNprobe},
	{"nprobe": DefaultNprobe},
}

var floatIndexTypes = []milvus.IndexType{
	milvus.FLAT,
	milvus.IVFFLAT,
	milvus.IVFSQ8,
	milvus.IVFSQ8H,
	milvus.IVFPQ,
	milvus.HNSW,
	// milvus.RNSG,
	milvus.ANNOY,
}

var binaryIndexTypes = []milvus.IndexType{
	milvus.BINFLAT,
	milvus.BINIVFFLAT,
}

var floatMetricTypes = []milvus.MetricType{
	milvus.L2,
	milvus.IP,
}

var binaryMetricTypes = []milvus.MetricType{
	milvus.HAMMING,
	milvus.JACCARD,
}
