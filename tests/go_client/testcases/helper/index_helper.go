package helper

import (
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
)

func GetDefaultVectorIndex(fieldType entity.FieldType) index.Index {
	switch fieldType {
	case entity.FieldTypeFloatVector, entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector, entity.FieldTypeSparseVector:
		return index.NewHNSWIndex(entity.COSINE, 8, 200)
	case entity.FieldTypeBinaryVector:
		return nil
	//	return binary index
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
