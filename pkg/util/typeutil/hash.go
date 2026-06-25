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
	"hash/crc32"
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/spaolacci/murmur3"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const substringLengthForCRC = 100

type routingIndex uint32

func (idx routingIndex) String() string {
	return strconv.FormatUint(uint64(idx), 10)
}

type int64RoutingHasher struct{}

func (int64RoutingHasher) Hash(key int64) (uint64, error) {
	value, err := Hash32Int64(key)
	return uint64(value), err
}

type stringRoutingHasher struct{}

func (stringRoutingHasher) Hash(key string) (uint64, error) {
	return uint64(HashString2Uint32(key)), nil
}

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

// HashString2LessUint32 hashing a string to uint32 but less than uint32 max
func HashString2LessUint32(v string) uint32 {
	subString := v
	if len(v) > substringLengthForCRC {
		subString = v[:substringLengthForCRC]
	}

	return crc32.ChecksumIEEE([]byte(subString)) % math.MaxUint32
}

func newRoutingIndexValues(targetCount int) []routingIndex {
	values := make([]routingIndex, 0, targetCount)
	for idx := 0; idx < targetCount; idx++ {
		values = append(values, routingIndex(idx))
	}
	return values
}

func locateRoutingIndexes[K comparable](keys []K, targetCount int, hasher common.Hasher[K]) ([]uint32, error) {
	table := common.NewHashRoutingTable[K](newRoutingIndexValues(targetCount), hasher)
	var hashValues []uint32
	for _, key := range keys {
		idx, err := table.LocateKey(key)
		if err != nil {
			return nil, err
		}
		hashValues = append(hashValues, uint32(idx))
	}
	return hashValues, nil
}

// HashPK2Channels hash primary keys to channels
func HashPK2Channels(primaryKeys *schemapb.IDs, shardNames []string) ([]uint32, error) {
	var hashValues []uint32
	var err error
	switch primaryKeys.IdField.(type) {
	case *schemapb.IDs_IntId:
		pks := primaryKeys.GetIntId().Data
		hashValues, err = locateRoutingIndexes(pks, len(shardNames), int64RoutingHasher{})
	case *schemapb.IDs_StrId:
		pks := primaryKeys.GetStrId().Data
		hashValues, err = locateRoutingIndexes(pks, len(shardNames), stringRoutingHasher{})
	default:
		// TODO::
	}

	return hashValues, err
}

// HashKey2Partitions hash partition keys to partitions
func HashKey2Partitions(keys *schemapb.FieldData, partitionNames []string) ([]uint32, error) {
	switch keys.Field.(type) {
	case *schemapb.FieldData_Scalars:
		scalarField := keys.GetScalars()
		switch scalarField.Data.(type) {
		case *schemapb.ScalarField_LongData:
			longKeys := scalarField.GetLongData().Data
			return locateRoutingIndexes(longKeys, len(partitionNames), int64RoutingHasher{})
		case *schemapb.ScalarField_StringData:
			stringKeys := scalarField.GetStringData().Data
			return locateRoutingIndexes(stringKeys, len(partitionNames), stringRoutingHasher{})
		default:
			return nil, merr.WrapErrParameterInvalidMsg("currently only support DataType Int64 or VarChar as partition key Field")
		}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("currently not support vector field as partition keys")
	}
}

// this method returns a static sequence for partitions for partiton key mode
func RearrangePartitionsForPartitionKey(partitions map[string]int64) ([]string, []int64, error) {
	// Make sure the order of the partition names got every time is the same
	partitionNames := make([]string, len(partitions))
	partitionIDs := make([]int64, len(partitions))
	for partitionName, partitionID := range partitions {
		splits := strings.Split(partitionName, "_")
		if len(splits) < 2 {
			return nil, nil, merr.WrapErrParameterInvalidMsg("bad default partion name in partition key mode: %s", partitionName)
		}
		index, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		if (index >= int64(len(partitions))) || (index < 0) {
			return nil, nil, merr.WrapErrParameterInvalidMsg("illegal partition index in partition key mode: %s", partitionName)
		}

		partitionNames[index] = partitionName
		partitionIDs[index] = partitionID
	}

	return partitionNames, partitionIDs, nil
}
