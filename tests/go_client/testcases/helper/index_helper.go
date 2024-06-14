package helper

import (
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

func GetDefaultVectorIndex(fieldType entity.FieldType) index.Index {
	switch fieldType {
	case entity.FieldTypeFloatVector, entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector:
		return index.NewHNSWIndex(entity.COSINE, 8, 200)
	case entity.FieldTypeBinaryVector:
		return index.NewGenericIndex(common.DefaultBinaryVecFieldName, map[string]string{"nlist": "64", index.MetricTypeKey: "JACCARD", index.IndexTypeKey: "BIN_IVF_FLAT"})
	//	return binary index
	case entity.FieldTypeSparseVector:
			return index.NewGenericIndex(common.DefaultSparseVecFieldName, map[string]string{"drop_ratio_build": "0.1", index.MetricTypeKey: "IP", index.IndexTypeKey: "SPARSE_INVERTED_INDEX"})
	default:
		return nil
		//	return auto index
	}
}

type IndexParams struct {
	Schema        *entity.Schema
	FieldIndexMap map[string]index.Index
}

func NewIndexParams(schema *entity.Schema) *IndexParams {
	return &IndexParams{
		Schema: schema,
	}
}

func (opt *IndexParams) TWithFieldIndex(mFieldIndex map[string]index.Index) *IndexParams {
	opt.FieldIndexMap = mFieldIndex
	return opt
}
