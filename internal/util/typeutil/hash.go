package typeutil

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

// hashMix computes a hash by mixing the upper and lower 64-bit values.
// This is inspired by MurmurHash.
func HashMix(upper, lower uint64) uint64 {
	const kMul uint64 = 0x9ddfea08eb382d69

	// Step 1: Mix lower and upper with kMul
	a := (lower ^ upper) * kMul
	a ^= a >> 47

	// Step 2: Mix the result with upper and kMul
	b := (upper ^ a) * kMul
	b ^= b >> 47

	// Step 3: Final mix
	b *= kMul

	return b
}

// NextPowerOfTwo computes the next power of two greater than or equal to n
func NextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}

	n-- // Decrement n to handle cases where n is already a power of two
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n>>32 != 0 { // Handle larger integers on 64-bit systems
		n |= n >> 32
	}
	n++ // Increment to get the next power of two
	return n
}
