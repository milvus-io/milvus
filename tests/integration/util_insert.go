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

package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/spaolacci/murmur3"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
)

func (s *MiniClusterSuite) WaitForFlush(ctx context.Context, segIDs []int64, flushTs uint64, dbName, collectionName string) {
	flushed := func() bool {
		resp, err := s.Cluster.MilvusClient.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
			SegmentIDs:     segIDs,
			FlushTs:        flushTs,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		if err != nil {
			return false
		}
		return resp.GetFlushed()
	}
	for !flushed() {
		select {
		case <-ctx.Done():
			s.FailNow("failed to wait for flush until ctx done")
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func NewInt64FieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: GenerateInt64Array(numRows, 0),
					},
				},
			},
		},
	}
}

func NewInt64FieldDataWithStart(fieldName string, numRows int, start int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: GenerateInt64Array(numRows, start),
					},
				},
			},
		},
	}
}

func NewInt64FieldDataNullableWithStart(fieldName string, numRows, start int) *schemapb.FieldData {
	validData, num := GenerateBoolArray(numRows)
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: GenerateInt64Array(num, int64(start)),
					},
				},
			},
		},
		ValidData: validData,
	}
}

func NewInt64SameFieldData(fieldName string, numRows int, value int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: GenerateSameInt64Array(numRows, value),
					},
				},
			},
		},
	}
}

func NewVarCharSameFieldData(fieldName string, numRows int, value string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_String,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: GenerateSameStringArray(numRows, value),
					},
				},
			},
		},
	}
}

func NewVarCharFieldData(fieldName string, numRows int, nullable bool) *schemapb.FieldData {
	numValid := numRows
	if nullable {
		numValid = numRows / 2
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_String,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: testutils.GenerateStringArray(numValid),
						// Data: testutils.GenerateStringArray(numRows),
					},
				},
			},
		},
		ValidData: testutils.GenerateBoolArray(numRows),
	}
}

func NewStringFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return testutils.NewStringFieldData(fieldName, numRows)
}

// note: unlike testutils's NewGeometryFieldData ,integration's NewGeometryFieldData generate wkt string bytes
func NewGeometryFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return testutils.NewGeometryFieldDataWktFormat(fieldName, numRows)
}

func NewFloatVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewFloatVectorFieldData(fieldName, numRows, dim)
}

func NewFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewFloat16VectorFieldData(fieldName, numRows, dim)
}

func NewBFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewBFloat16VectorFieldData(fieldName, numRows, dim)
}

func NewBinaryVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewBinaryVectorFieldData(fieldName, numRows, dim)
}

func NewSparseFloatVectorFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return testutils.NewSparseFloatVectorFieldData(fieldName, numRows)
}

func NewInt8VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewInt8VectorFieldData(fieldName, numRows, dim)
}

func NewStructArrayFieldData(schema *schemapb.StructArrayFieldSchema, fieldName string, numRow int, dim int) *schemapb.FieldData {
	fieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfStruct,
		FieldName: fieldName,
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{
				Fields: testutils.GenerateArrayOfStructArray(schema, numRow, dim),
			},
		},
	}
	return fieldData
}

func GenerateInt64Array(numRows int, start int64) []int64 {
	ret := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = int64(i) + start
	}
	return ret
}

func GenerateSameInt64Array(numRows int, value int64) []int64 {
	ret := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = value
	}
	return ret
}

func GenerateBoolArray(numRows int) ([]bool, int) {
	var num int
	ret := make([]bool, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = i%2 == 0
		if ret[i] {
			num++
		}
	}
	return ret, num
}

func GenerateSameStringArray(numRows int, value string) []string {
	ret := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = value
	}
	return ret
}

func GenerateSparseFloatArray(numRows int) *schemapb.SparseFloatArray {
	return testutils.GenerateSparseFloatVectors(numRows)
}

func GenerateHashKeys(numRows int) []uint32 {
	return testutils.GenerateHashKeys(numRows)
}

// GenerateChannelBalancedPrimaryKeys generates primary keys that are evenly distributed across channels.
// It supports both Int64 and VarChar primary key types.
// For Int64: uses murmur3 hash (same as typeutil.Hash32Int64)
// For VarChar: uses crc32 hash (same as typeutil.HashString2Uint32)
func GenerateChannelBalancedPrimaryKeys(fieldName string, fieldType schemapb.DataType, numRows int, numChannels int) *schemapb.FieldData {
	switch fieldType {
	case schemapb.DataType_Int64:
		return &schemapb.FieldData{
			Type:      schemapb.DataType_Int64,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: GenerateBalancedInt64PKs(numRows, numChannels),
						},
					},
				},
			},
		}
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		return &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: GenerateBalancedVarCharPKs(numRows, numChannels),
						},
					},
				},
			},
		}
	default:
		panic(fmt.Sprintf("not supported primary key type: %s", fieldType))
	}
}

// GenerateBalancedInt64PKs generates int64 primary keys that are evenly distributed across channels.
// This ensures each channel receives exactly numRows/numChannels items based on PK hash values.
// The function searches for PKs that hash to each channel to achieve exact distribution.
func GenerateBalancedInt64PKs(numRows int, numChannels int) []int64 {
	if numChannels <= 0 {
		numChannels = 1
	}

	// Calculate how many items each channel should receive
	baseCount := numRows / numChannels
	remainder := numRows % numChannels

	// Collect PKs for each channel
	channelPKs := make([][]int64, numChannels)
	targetCounts := make([]int, numChannels)
	for ch := 0; ch < numChannels; ch++ {
		targetCounts[ch] = baseCount
		if ch < remainder {
			targetCounts[ch]++
		}
		channelPKs[ch] = make([]int64, 0, targetCounts[ch])
	}

	// Search for PKs that hash to each channel
	for pk := int64(1); ; pk++ {
		// Calculate which channel this PK would go to
		hash := hashInt64ForChannel(pk)
		ch := int(hash % uint32(numChannels))

		if len(channelPKs[ch]) < targetCounts[ch] {
			channelPKs[ch] = append(channelPKs[ch], pk)
		}

		// Check if all channels have enough PKs
		done := true
		for ch := 0; ch < numChannels; ch++ {
			if len(channelPKs[ch]) < targetCounts[ch] {
				done = false
				break
			}
		}
		if done {
			break
		}
	}

	// Combine all PKs
	result := make([]int64, 0, numRows)
	for ch := 0; ch < numChannels; ch++ {
		result = append(result, channelPKs[ch]...)
	}

	return result
}

// hashInt64ForChannel computes the hash value for channel assignment.
// This mirrors the logic in typeutil.Hash32Int64 and HashPK2Channels.
func hashInt64ForChannel(v int64) uint32 {
	// Must match the behavior of typeutil.Hash32Int64
	// which uses common.Endian (binary.LittleEndian)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))

	// Use murmur3 hash (same as typeutil.Hash32Bytes)
	h := murmur3.New32()
	h.Write(b)
	return h.Sum32() & 0x7fffffff
}

// GenerateBalancedVarCharPKs generates varchar primary keys that are evenly distributed across channels.
// This ensures each channel receives exactly numRows/numChannels items based on PK hash values.
// The function searches for PKs that hash to each channel to achieve exact distribution.
func GenerateBalancedVarCharPKs(numRows int, numChannels int) []string {
	if numChannels <= 0 {
		numChannels = 1
	}

	// Calculate how many items each channel should receive
	baseCount := numRows / numChannels
	remainder := numRows % numChannels

	// Collect PKs for each channel
	channelPKs := make([][]string, numChannels)
	targetCounts := make([]int, numChannels)
	for ch := 0; ch < numChannels; ch++ {
		targetCounts[ch] = baseCount
		if ch < remainder {
			targetCounts[ch]++
		}
		channelPKs[ch] = make([]string, 0, targetCounts[ch])
	}

	// Search for PKs that hash to each channel
	for i := 1; ; i++ {
		// Generate a unique string PK
		pk := fmt.Sprintf("pk_%d", i)

		// Calculate which channel this PK would go to
		hash := hashVarCharForChannel(pk)
		ch := int(hash % uint32(numChannels))

		if len(channelPKs[ch]) < targetCounts[ch] {
			channelPKs[ch] = append(channelPKs[ch], pk)
		}

		// Check if all channels have enough PKs
		done := true
		for ch := 0; ch < numChannels; ch++ {
			if len(channelPKs[ch]) < targetCounts[ch] {
				done = false
				break
			}
		}
		if done {
			break
		}
	}

	// Combine all PKs
	result := make([]string, 0, numRows)
	for ch := 0; ch < numChannels; ch++ {
		result = append(result, channelPKs[ch]...)
	}

	return result
}

// hashVarCharForChannel computes the hash value for channel assignment of varchar PKs.
// This mirrors the logic in typeutil.HashString2Uint32 and HashPK2Channels.
func hashVarCharForChannel(v string) uint32 {
	// Must match the behavior of typeutil.HashString2Uint32
	// which uses crc32.ChecksumIEEE with substring limit of 100 chars
	subString := v
	if len(v) > 100 {
		subString = v[:100]
	}
	return crc32.ChecksumIEEE([]byte(subString))
}
