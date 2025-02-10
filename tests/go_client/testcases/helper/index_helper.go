package helper

import (
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
)

func GetDefaultVectorIndex(fieldType entity.FieldType) index.Index {
	switch fieldType {
	case entity.FieldTypeFloatVector, entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector:
		return index.NewHNSWIndex(entity.COSINE, 8, 200)
	case entity.FieldTypeBinaryVector:
		return index.NewBinIvfFlatIndex(entity.JACCARD, 64)
	case entity.FieldTypeSparseVector:
		return index.NewSparseInvertedIndex(entity.IP, 0.1)
	default:
		return index.NewAutoIndex(entity.COSINE)
	}
}

type IndexParams struct {
	Schema        *entity.Schema
	FieldIndexMap map[string]index.Index
}

func TNewIndexParams(schema *entity.Schema) *IndexParams {
	return &IndexParams{
		Schema: schema,
	}
}

func (opt *IndexParams) TWithFieldIndex(mFieldIndex map[string]index.Index) *IndexParams {
	opt.FieldIndexMap = mFieldIndex
	return opt
}

/*
utils func
*/
var SupportFloatMetricType = []entity.MetricType{
	entity.L2,
	entity.IP,
	entity.COSINE,
}

var SupportBinFlatMetricType = []entity.MetricType{
	entity.JACCARD,
	entity.HAMMING,
	entity.SUBSTRUCTURE,
	entity.SUPERSTRUCTURE,
}

var SupportBinIvfFlatMetricType = []entity.MetricType{
	entity.JACCARD,
	entity.HAMMING,
}

var SupportFullTextSearchMetricsType = []entity.MetricType{
	entity.BM25,
}

var UnsupportedSparseVecMetricsType = []entity.MetricType{
	entity.L2,
	entity.COSINE,
	entity.JACCARD,
	entity.HAMMING,
	entity.SUBSTRUCTURE,
	entity.SUPERSTRUCTURE,
}

// GenAllFloatIndex gen all float vector index
func GenAllFloatIndex(metricType entity.MetricType) []index.Index {
	nlist := 128
	var allFloatIndex []index.Index

	idxFlat := index.NewFlatIndex(metricType)
	idxIvfFlat := index.NewIvfFlatIndex(metricType, nlist)
	idxIvfSq8 := index.NewIvfSQ8Index(metricType, nlist)
	idxIvfPq := index.NewIvfPQIndex(metricType, nlist, 16, 8)
	idxHnsw := index.NewHNSWIndex(metricType, 8, 96)
	idxScann := index.NewSCANNIndex(metricType, 16, true)
	idxDiskAnn := index.NewDiskANNIndex(metricType)
	allFloatIndex = append(allFloatIndex, idxFlat, idxIvfFlat, idxIvfSq8, idxIvfPq, idxHnsw, idxScann, idxDiskAnn)

	return allFloatIndex
}

func SupportScalarIndexFieldType(field entity.FieldType) bool {
	vectorFieldTypes := []entity.FieldType{
		entity.FieldTypeBinaryVector, entity.FieldTypeFloatVector,
		entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector,
		entity.FieldTypeSparseVector, entity.FieldTypeJSON,
	}
	for _, vectorFieldType := range vectorFieldTypes {
		if field == vectorFieldType {
			return false
		}
	}
	return true
}
