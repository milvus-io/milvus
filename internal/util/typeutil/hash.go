// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package typeutil

import (
	"errors"
	"hash/crc32"
	"unsafe"

	"github.com/spaolacci/murmur3"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/planpb"
)

const substringLengthForCRC = 100

// Hash32Bytes hashing a byte array to uint32
func Hash32Bytes(b []byte) (uint32, error) {
	h := murmur3.New32()
	if _, err := h.Write(b); err != nil {
		return 0, err
	}
	return h.Sum32() & 0x7fffffff, nil
}

// Hash32Uint64 hashing an uint64 nubmer to uint32
func Hash32Uint64(v uint64) (uint32, error) {
	// need unsafe package to get element byte size
	/* #nosec G103 */
	b := make([]byte, unsafe.Sizeof(v))
	common.Endian.PutUint64(b, v)
	return Hash32Bytes(b)
}

// Hash32Int64 hashing an int64 number to uint32
func Hash32Int64(v int64) (uint32, error) {
	return Hash32Uint64(uint64(v))
}

// Hash32String hashing a string to int64
func Hash32String(s string) (int64, error) {
	b := []byte(s)
	v, err := Hash32Bytes(b)
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

// HashString2Uint32 hashing a string to uint32
func HashString2Uint32(v string) uint32 {
	subString := v
	if len(v) > substringLengthForCRC {
		subString = v[:substringLengthForCRC]
	}

	return crc32.ChecksumIEEE([]byte(subString))
}

// HashPK2Channels hash primary keys to channels
func HashPK2Channels(primaryKeys *schemapb.IDs, shardNames []string) []uint32 {
	numShard := uint32(len(shardNames))
	var hashValues []uint32
	switch primaryKeys.IdField.(type) {
	case *schemapb.IDs_IntId:
		pks := primaryKeys.GetIntId().Data
		for _, pk := range pks {
			value, _ := Hash32Int64(pk)
			hashValues = append(hashValues, value%numShard)
		}
	case *schemapb.IDs_StrId:
		pks := primaryKeys.GetStrId().Data
		for _, pk := range pks {
			hash := HashString2Uint32(pk)
			hashValues = append(hashValues, hash%numShard)
		}
	default:
		//TODO::
	}

	return hashValues
}

// HashKey2PartitionsWithFilter hash partition keys to partitions
func HashKey2PartitionsWithFilter(rowOffsets []int, keys *schemapb.FieldData, partitionNames []string) ([]uint32, error) {
	var hashValues []uint32
	numPartitions := uint32(len(partitionNames))
	switch keys.Field.(type) {
	case *schemapb.FieldData_Scalars:
		scalarField := keys.GetScalars()
		switch scalarField.Data.(type) {
		case *schemapb.ScalarField_LongData:
			longKeys := scalarField.GetLongData().Data
			for _, offset := range rowOffsets {
				value, _ := Hash32Int64(longKeys[offset])
				hashValues = append(hashValues, value%numPartitions)
			}
		case *schemapb.ScalarField_StringData:
			stringKeys := scalarField.GetStringData().Data
			for _, offset := range rowOffsets {
				value := HashString2Uint32(stringKeys[offset])
				hashValues = append(hashValues, value%numPartitions)
			}
		default:
			return nil, errors.New("currently only support DataType Int64 or VarChar as partition key Field")
		}
	default:
		return nil, errors.New("currently not support vector field as partition keys")
	}

	return hashValues, nil
}

// HashKey2Partitions hash partition keys to partitions
func HashKey2Partitions(fieldSchema *schemapb.FieldSchema, keys []*planpb.GenericValue, partitionNames []string) ([]string, error) {
	selectedPartitions := make(map[string]struct{})
	numPartitions := uint32(len(partitionNames))
	switch fieldSchema.GetDataType() {
	case schemapb.DataType_Int64:
		for _, key := range keys {
			if int64Val, ok := key.GetVal().(*planpb.GenericValue_Int64Val); ok {
				value, _ := Hash32Int64(int64Val.Int64Val)
				partitionName := partitionNames[value%numPartitions]
				selectedPartitions[partitionName] = struct{}{}
			} else {
				return nil, errors.New("the data type of the data and the schema do not match")
			}
		}
	case schemapb.DataType_VarChar:
		for _, key := range keys {
			if stringVal, ok := key.GetVal().(*planpb.GenericValue_StringVal); ok {
				value := HashString2Uint32(stringVal.StringVal)
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
