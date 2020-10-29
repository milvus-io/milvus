package utils

import (
	"bytes"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/milvus-io/milvus-sdk-go/milvus"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var defaultNlist = 32

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type Index struct {
	IndexType  milvus.IndexType
	MetricType milvus.MetricType
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
	{"nlist": defaultNlist},
	{"nlist": defaultNlist},
	{"nlist": defaultNlist},
	{"nlist": defaultNlist},
	{"nlist": defaultNlist, "m": 16},
	{"M": 48, "efConstruction": 500},
	// {"search_length": 50, "out_degree": 40, "candidate_pool_size": 100, "knng": 50},
	{"n_trees": 50},
}

var binaryIndexParams = []map[string]interface{}{
	{"nlist": defaultNlist},
	{"nlist": defaultNlist},
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

func RandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func In(target milvus.MetricType, all []milvus.MetricType) bool {
	for _, element := range all {
		if target == element {
			return true
		}
	}
	return false
}

func Camel2Case(name string) string {
	buffer := bytes.NewBuffer([]byte{})
	for i, r := range name {
		if unicode.IsUpper(r) {
			if i != 0 {
				buffer.WriteRune('_')
			}
			buffer.WriteRune(unicode.ToLower(r))
		} else {
			buffer.WriteRune(r)
		}
	}
	return buffer.String()
}

func Struct2Map(obj interface{}) map[string]interface{} {
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	var data = make(map[string]interface{})
	for i := 0; i < t.NumField(); i++ {
		data[strings.ToLower(t.Field(i).Name)] = v.Field(i).Interface()
	}
	return data
}

func Index2Map(index Index) map[string]interface{} {
	t := reflect.TypeOf(index)
	v := reflect.ValueOf(index)
	var data = make(map[string]interface{})
	for i := 0; i < t.NumField(); i++ {
		data[Camel2Case(t.Field(i).Name)] = v.Field(i).Interface()
	}
	return data
}

func Normalize(d int, v []float32) {
	var norm float32
	for i := 0; i < d; i++ {
		norm += v[i] * v[i]
	}
	norm = float32(math.Sqrt(float64(norm)))
	for i := 0; i < d; i++ {
		v[i] /= norm
	}
}

func GenDefaultIntValues(nb int) []int64 {
	values := make([]int64, nb)
	for i := 0; i < nb; i++ {
		values[i] = int64(i)
	}
	return values
}

func GenDefaultFloatValues(nb int) []float32 {
	values := make([]float32, nb)
	for i := 0; i < nb; i++ {
		values[i] = float32(i)
	}
	return values
}

func GenFloatVectors(dim int, nb int, normal bool) [][]float32 {
	rand.Seed(time.Now().UnixNano())
	vectors := make([][]float32, nb)
	for i := 0; i < nb; i++ {
		vector := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vector[j] = rand.Float32()
		}
		if normal {
			Normalize(dim, vector)
		}
		vectors[i] = vector
	}
	return vectors
}

func GenBinaryVectors(dim int, nb int) [][]byte {
	rand.Seed(time.Now().UnixNano())
	vectors := make([][]byte, nb)
	for i := 0; i < nb; i++ {
		vector := make([]uint8, dim)
		for j := 0; j < dim; j++ {
			vector[j] = uint8(rand.Intn(2))
		}
		vectors[i] = vector
	}
	return vectors
}

func GenIndexes(metricType milvus.MetricType) []Index {
	var indexes []Index
	if In(metricType, floatMetricTypes) {
		for i, indexType := range floatIndexTypes {
			var index Index
			index.IndexType = indexType
			index.MetricType = metricType
			index.Params = floatIndexParams[i]
			indexes = append(indexes, index)
		}
	} else if In(metricType, binaryMetricTypes) {
		for i, indexType := range binaryIndexTypes {
			var index Index
			index.IndexType = indexType
			index.MetricType = metricType
			index.Params = binaryIndexParams[i]
			indexes = append(indexes, index)
		}
	}
	return indexes
}

func GenInvalidStrs() []string {
	strs := []string{
		" name ",
		" ",
		"测试",
	}
	return strs
}

func GenDefaultMapping()  {

}
