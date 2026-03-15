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

package storage

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func TestIsBinaryStatsFormat(t *testing.T) {
	assert.False(t, IsBinaryStatsFormat(nil))
	assert.False(t, IsBinaryStatsFormat([]byte{}))
	assert.False(t, IsBinaryStatsFormat([]byte(`[{"fieldID":100}]`)))
	assert.True(t, IsBinaryStatsFormat([]byte("milvupks"+"xxxxxxxx")))
}

func TestSerializeDeserializeBinaryStats_Int64(t *testing.T) {
	// Build a bloom filter with known data
	bf := bloomfilter.NewBloomFilterWithType(10000, 0.001, "BlockedBloomFilter")

	// Add some int64 PKs
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := make([]byte, 8)
		common.Endian.PutUint64(key, uint64(i))
		bf.Add(key)
	}

	stats := []*PrimaryKeyStats{
		{
			FieldID: 100,
			PkType:  int64(schemapb.DataType_Int64),
			BFType:  bloomfilter.BlockedBF,
			BF:      bf,
			MinPk:   NewInt64PrimaryKey(0),
			MaxPk:   NewInt64PrimaryKey(int64(numKeys - 1)),
		},
	}

	// Serialize to binary
	data, err := SerializeBinaryStats(stats)
	require.NoError(t, err)
	require.NotNil(t, data)

	// Verify magic
	assert.True(t, IsBinaryStatsFormat(data))

	// Deserialize
	result, err := DeserializeBinaryStats(data)
	require.NoError(t, err)
	require.Len(t, result, 1)

	r := result[0]
	assert.Equal(t, int64(100), r.FieldID)
	assert.Equal(t, int64(schemapb.DataType_Int64), r.PkType)
	assert.Equal(t, bloomfilter.BlockedBF, r.BFType)

	// Verify min/max PK
	assert.Equal(t, int64(0), r.MinPk.(*Int64PrimaryKey).Value)
	assert.Equal(t, int64(numKeys-1), r.MaxPk.(*Int64PrimaryKey).Value)

	// Verify bloom filter correctness
	for i := 0; i < numKeys; i++ {
		key := make([]byte, 8)
		common.Endian.PutUint64(key, uint64(i))
		assert.True(t, r.BF.Test(key), "key %d should be found", i)
	}

	// Verify false positive rate
	falsePositives := 0
	for i := numKeys; i < numKeys+10000; i++ {
		key := make([]byte, 8)
		common.Endian.PutUint64(key, uint64(i))
		if r.BF.Test(key) {
			falsePositives++
		}
	}
	assert.Less(t, falsePositives, 20, "too many false positives: %d/10000", falsePositives)
}

func TestSerializeDeserializeBinaryStats_VarChar(t *testing.T) {
	bf := bloomfilter.NewBloomFilterWithType(10000, 0.001, "BlockedBloomFilter")

	strings := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, s := range strings {
		bf.AddString(s)
	}

	stats := []*PrimaryKeyStats{
		{
			FieldID: 200,
			PkType:  int64(schemapb.DataType_VarChar),
			BFType:  bloomfilter.BlockedBF,
			BF:      bf,
			MinPk:   NewVarCharPrimaryKey("apple"),
			MaxPk:   NewVarCharPrimaryKey("elderberry"),
		},
	}

	data, err := SerializeBinaryStats(stats)
	require.NoError(t, err)

	result, err := DeserializeBinaryStats(data)
	require.NoError(t, err)
	require.Len(t, result, 1)

	r := result[0]
	assert.Equal(t, int64(200), r.FieldID)
	assert.Equal(t, int64(schemapb.DataType_VarChar), r.PkType)
	assert.Equal(t, "apple", r.MinPk.(*VarCharPrimaryKey).Value)
	assert.Equal(t, "elderberry", r.MaxPk.(*VarCharPrimaryKey).Value)

	// Verify bloom filter correctness
	for _, s := range strings {
		assert.True(t, r.BF.TestString(s), "string %q should be found", s)
	}
}

func TestSerializeDeserializeBinaryStats_VarChar_LongKeys(t *testing.T) {
	bf := bloomfilter.NewBloomFilterWithType(10000, 0.001, "BlockedBloomFilter")

	// Use PKs that exceed the old 48-byte limit (56 bytes minus two 4-byte length headers)
	longMin := "this-is-a-very-long-primary-key-value-that-exceeds-the-old-48-byte-limit-for-varchar-pks"
	longMax := "ZZZZ-another-very-long-primary-key-value-that-would-be-truncated-in-the-old-fixed-buffer"
	bf.AddString(longMin)
	bf.AddString(longMax)

	stats := []*PrimaryKeyStats{
		{
			FieldID: 200,
			PkType:  int64(schemapb.DataType_VarChar),
			BFType:  bloomfilter.BlockedBF,
			BF:      bf,
			MinPk:   NewVarCharPrimaryKey(longMin),
			MaxPk:   NewVarCharPrimaryKey(longMax),
		},
	}

	data, err := SerializeBinaryStats(stats)
	require.NoError(t, err)

	result, err := DeserializeBinaryStats(data)
	require.NoError(t, err)
	require.Len(t, result, 1)

	r := result[0]
	assert.Equal(t, longMin, r.MinPk.(*VarCharPrimaryKey).Value)
	assert.Equal(t, longMax, r.MaxPk.(*VarCharPrimaryKey).Value)

	// Verify bloom filter correctness
	assert.True(t, r.BF.TestString(longMin))
	assert.True(t, r.BF.TestString(longMax))
}

func TestSerializeDeserializeBinaryStats_MultiEntry(t *testing.T) {
	// Create multiple entries
	stats := make([]*PrimaryKeyStats, 3)
	for idx := 0; idx < 3; idx++ {
		bf := bloomfilter.NewBloomFilterWithType(5000, 0.001, "BlockedBloomFilter")
		base := idx * 1000
		for i := 0; i < 1000; i++ {
			key := make([]byte, 8)
			binary.LittleEndian.PutUint64(key, uint64(base+i))
			bf.Add(key)
		}
		stats[idx] = &PrimaryKeyStats{
			FieldID: 100,
			PkType:  int64(schemapb.DataType_Int64),
			BFType:  bloomfilter.BlockedBF,
			BF:      bf,
			MinPk:   NewInt64PrimaryKey(int64(base)),
			MaxPk:   NewInt64PrimaryKey(int64(base + 999)),
		}
	}

	data, err := SerializeBinaryStats(stats)
	require.NoError(t, err)

	result, err := DeserializeBinaryStats(data)
	require.NoError(t, err)
	require.Len(t, result, 3)

	// Verify each entry
	for idx, r := range result {
		base := idx * 1000
		assert.Equal(t, int64(base), r.MinPk.(*Int64PrimaryKey).Value)
		assert.Equal(t, int64(base+999), r.MaxPk.(*Int64PrimaryKey).Value)

		// Check bloom filter
		for i := 0; i < 1000; i++ {
			key := make([]byte, 8)
			binary.LittleEndian.PutUint64(key, uint64(base+i))
			assert.True(t, r.BF.Test(key), "entry %d, key %d should be found", idx, base+i)
		}
	}
}

func TestDeserializeStatsList_AutoDetectBinary(t *testing.T) {
	bf := bloomfilter.NewBloomFilterWithType(10000, 0.001, "BlockedBloomFilter")

	key := make([]byte, 8)
	common.Endian.PutUint64(key, uint64(42))
	bf.Add(key)

	stats := []*PrimaryKeyStats{
		{
			FieldID: 100,
			PkType:  int64(schemapb.DataType_Int64),
			BFType:  bloomfilter.BlockedBF,
			BF:      bf,
			MinPk:   NewInt64PrimaryKey(42),
			MaxPk:   NewInt64PrimaryKey(42),
		},
	}

	// Serialize to binary
	data, err := SerializeBinaryStats(stats)
	require.NoError(t, err)

	// Use DeserializeStatsList which should auto-detect binary format
	result, err := DeserializeStatsList(&Blob{Value: data})
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.True(t, result[0].BF.Test(key))
}

func TestSerializeBinaryStats_Empty(t *testing.T) {
	_, err := SerializeBinaryStats(nil)
	assert.Error(t, err)

	_, err = SerializeBinaryStats([]*PrimaryKeyStats{})
	assert.Error(t, err)
}

func TestDeserializeBinaryStats_InvalidData(t *testing.T) {
	// Too short
	_, err := DeserializeBinaryStats([]byte("short"))
	assert.Error(t, err)

	// Wrong magic
	data := make([]byte, BinaryFileHeaderSize)
	copy(data[:8], "wrongmgc")
	_, err = DeserializeBinaryStats(data)
	assert.Error(t, err)

	// Wrong version
	data2 := make([]byte, BinaryFileHeaderSize)
	copy(data2[:8], BinaryStatsMagic)
	binary.LittleEndian.PutUint32(data2[8:12], 99) // bad version
	_, err = DeserializeBinaryStats(data2)
	assert.Error(t, err)
}
