package typeutil

import (
	"strconv"

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

// HashMix computes a hash by mixing the upper and lower 64-bit values.
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

// NextPowerOfTwo computes the next power of two greater than or equal to n.
// Assumptions: n is expected to be non-negative.
// Behavior:
//   - Returns 0 if n < 0 (negative input sentinel)
//   - Returns 0 if n >= 1<<(bits-1) where bits is the int bit width (overflow sentinel)
//     because computing the next power of two would overflow the int type
//   - Returns 1 if n <= 1
//   - Otherwise returns the smallest power of two >= n
func NextPowerOfTwo(n int) int {
	// Handle negative inputs
	if n < 0 {
		return 0
	}

	// Handle overflow case: if n >= 2^(bits-1), the next power of two would be 2^bits
	// which would overflow. For 32-bit ints: max is 2^31-1, so if n >= 2^30, overflow.
	// For 64-bit ints: max is 2^63-1, so if n >= 2^62, overflow.
	bits := strconv.IntSize
	maxPowerOfTwo := 1 << (bits - 1)
	if n >= maxPowerOfTwo {
		return 0
	}

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
