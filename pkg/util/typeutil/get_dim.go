package typeutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// GetDim get dimension of field. Maybe also helpful outside.
func GetDim(field *schemapb.FieldSchema) (int64, error) {
	return CreateFieldSchemaHelper(field).GetDim()
}
