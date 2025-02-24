package typeutil

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// HashKey2Partitions hash partition keys to partitions
func HashKey2Partitions(fieldSchema *schemapb.FieldSchema, keys []*planpb.GenericValue, partitionNames []string) ([]string, error) {
	selectedPartitions := make(map[string]struct{})
	numPartitions := uint32(len(partitionNames))
	switch fieldSchema.GetDataType() {
	case schemapb.DataType_Int64:
		for _, key := range keys {
			if int64Val, ok := key.GetVal().(*planpb.GenericValue_Int64Val); ok {
				value, _ := typeutil.Hash32Int64(int64Val.Int64Val)
				partitionName := partitionNames[value%numPartitions]
				selectedPartitions[partitionName] = struct{}{}
			} else {
				return nil, errors.New("the data type of the data and the schema do not match")
			}
		}
	case schemapb.DataType_VarChar:
		for _, key := range keys {
			if stringVal, ok := key.GetVal().(*planpb.GenericValue_StringVal); ok {
				value := typeutil.HashString2Uint32(stringVal.StringVal)
				partitionName := partitionNames[value%numPartitions]
				selectedPartitions[partitionName] = struct{}{}
			} else {
				return nil, errors.New("the data type of the data and the schema do not match")
			}
		}
	default:
		return nil, errors.New("currently only support DataType Int64 or VarChar as partition keys")
	}

	result := make([]string, 0)
	for partitionName := range selectedPartitions {
		result = append(result, partitionName)
	}

	return result, nil
}
