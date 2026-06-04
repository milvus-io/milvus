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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestIsBinaryStatsFormat(t *testing.T) {
	assert.False(t, IsBinaryStatsFormat(nil))
	assert.False(t, IsBinaryStatsFormat([]byte{}))
	assert.False(t, IsBinaryStatsFormat([]byte(`[{"fieldID":100}]`)))
	assert.True(t, IsBinaryStatsFormat([]byte("mpkstats"+"xxxxxxxx")))
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

func TestSerializeDeserializeBinaryStats_NilPrimaryKeys(t *testing.T) {
	t.Run("int64 nil min max encodes as zero", func(t *testing.T) {
		bf := bloomfilter.NewBloomFilterWithType(100, 0.01, "BlockedBloomFilter")
		stats := []*PrimaryKeyStats{
			{
				FieldID: 100,
				PkType:  int64(schemapb.DataType_Int64),
				BFType:  bloomfilter.BlockedBF,
				BF:      bf,
			},
		}

		data, err := SerializeBinaryStats(stats)
		require.NoError(t, err)

		result, err := DeserializeBinaryStats(data)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, int64(0), result[0].MinPk.(*Int64PrimaryKey).Value)
		assert.Equal(t, int64(0), result[0].MaxPk.(*Int64PrimaryKey).Value)
	})

	t.Run("varchar nil min max encodes as empty strings", func(t *testing.T) {
		bf := bloomfilter.NewBloomFilterWithType(100, 0.01, "BlockedBloomFilter")
		stats := []*PrimaryKeyStats{
			{
				FieldID: 200,
				PkType:  int64(schemapb.DataType_VarChar),
				BFType:  bloomfilter.BlockedBF,
				BF:      bf,
			},
		}

		data, err := SerializeBinaryStats(stats)
		require.NoError(t, err)

		result, err := DeserializeBinaryStats(data)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, "", result[0].MinPk.(*VarCharPrimaryKey).Value)
		assert.Equal(t, "", result[0].MaxPk.(*VarCharPrimaryKey).Value)
	})
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

func TestDeserializeBloomFilterStats_AutoDetectBinaryDefaultPath(t *testing.T) {
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

	data, err := SerializeBinaryStats(stats)
	require.NoError(t, err)

	result, err := DeserializeBloomFilterStats([]string{"root/stats/0"}, []*Blob{{Value: data}})
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

func TestSerializeBinaryStats_InvalidStats(t *testing.T) {
	bf := bloomfilter.NewBloomFilterWithType(10000, 0.001, "BlockedBloomFilter")
	stat := &PrimaryKeyStats{
		FieldID: 100,
		PkType:  int64(schemapb.DataType_Int64),
		BFType:  bloomfilter.BlockedBF,
		BF:      bf,
		MinPk:   NewInt64PrimaryKey(0),
		MaxPk:   NewInt64PrimaryKey(1),
	}

	_, err := SerializeBinaryStats([]*PrimaryKeyStats{nil})
	assert.Error(t, err)

	nilBF := *stat
	nilBF.BF = nil
	_, err = SerializeBinaryStats([]*PrimaryKeyStats{&nilBF})
	assert.Error(t, err)

	mixedField := *stat
	mixedField.FieldID = 101
	_, err = SerializeBinaryStats([]*PrimaryKeyStats{stat, &mixedField})
	assert.Error(t, err)

	mixedType := *stat
	mixedType.PkType = int64(schemapb.DataType_VarChar)
	_, err = SerializeBinaryStats([]*PrimaryKeyStats{stat, &mixedType})
	assert.Error(t, err)

	unsupportedType := *stat
	unsupportedType.PkType = int64(schemapb.DataType_Bool)
	_, err = SerializeBinaryStats([]*PrimaryKeyStats{&unsupportedType})
	assert.Error(t, err)

	wrongMinPkType := *stat
	wrongMinPkType.MinPk = NewVarCharPrimaryKey("wrong")
	_, err = SerializeBinaryStats([]*PrimaryKeyStats{&wrongMinPkType})
	assert.Error(t, err)

	wrongMaxPkType := *stat
	wrongMaxPkType.MaxPk = NewVarCharPrimaryKey("wrong")
	_, err = SerializeBinaryStats([]*PrimaryKeyStats{&wrongMaxPkType})
	assert.Error(t, err)

	wrongMinPkImpl := *stat
	wrongMinPkImpl.MinPk = fakeBinaryStatsPrimaryKey{typ: schemapb.DataType_Int64}
	_, err = SerializeBinaryStats([]*PrimaryKeyStats{&wrongMinPkImpl})
	assert.Error(t, err)

	vcStat := &PrimaryKeyStats{
		FieldID: 200,
		PkType:  int64(schemapb.DataType_VarChar),
		BFType:  bloomfilter.BlockedBF,
		BF:      bf,
		MinPk:   NewVarCharPrimaryKey("a"),
		MaxPk:   NewVarCharPrimaryKey("z"),
	}
	wrongVarcharPkImpl := *vcStat
	wrongVarcharPkImpl.MaxPk = fakeBinaryStatsPrimaryKey{typ: schemapb.DataType_VarChar}
	_, err = SerializeBinaryStats([]*PrimaryKeyStats{&wrongVarcharPkImpl})
	assert.Error(t, err)

	basicBF := *stat
	basicBF.BF = bloomfilter.NewBloomFilterWithType(100, 0.01, "BasicBloomFilter")
	_, err = SerializeBinaryStats([]*PrimaryKeyStats{&basicBF})
	assert.Error(t, err)

	zeroCapacity := *stat
	zeroCapacity.BF = fakeBinaryStatsBloomFilter{capacity: 0, k: 1}
	_, err = SerializeBinaryStats([]*PrimaryKeyStats{&zeroCapacity})
	assert.Error(t, err)

	unsupportedDump := *stat
	unsupportedDump.BF = fakeBinaryStatsBloomFilter{capacity: 512, k: 1}
	_, err = SerializeBinaryStats([]*PrimaryKeyStats{&unsupportedDump})
	assert.Error(t, err)

	_, err = SerializeBinaryStats([]*PrimaryKeyStats{stat, nil})
	assert.Error(t, err)

	if ^uint(0) > uint(^uint32(0)) {
		overflowBlocks := uint64(^uint32(0)) + 1
		overflowBlockCount := *stat
		overflowBlockCount.BF = fakeBinaryStatsBloomFilter{capacity: uint(overflowBlocks * BinaryBlockSize * 8), k: 1}
		_, err = SerializeBinaryStats([]*PrimaryKeyStats{&overflowBlockCount})
		assert.Error(t, err)
	}
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

	// Zero blocks
	data3 := makeBinaryStatsHeaderForTest()
	binary.LittleEndian.PutUint32(data3[BinaryFileHeaderSize:BinaryFileHeaderSize+4], 0)
	binary.LittleEndian.PutUint32(data3[BinaryFileHeaderSize+4:BinaryFileHeaderSize+8], 3)
	_, err = DeserializeBinaryStats(data3)
	assert.Error(t, err)

	// Zero hash function count
	data4 := makeBinaryStatsHeaderForTest()
	binary.LittleEndian.PutUint32(data4[BinaryFileHeaderSize:BinaryFileHeaderSize+4], 1)
	binary.LittleEndian.PutUint32(data4[BinaryFileHeaderSize+4:BinaryFileHeaderSize+8], 0)
	_, err = DeserializeBinaryStats(data4)
	assert.Error(t, err)

	// Truncated block data
	data5 := makeBinaryStatsHeaderForTest()
	binary.LittleEndian.PutUint32(data5[BinaryFileHeaderSize:BinaryFileHeaderSize+4], 1)
	binary.LittleEndian.PutUint32(data5[BinaryFileHeaderSize+4:BinaryFileHeaderSize+8], 3)
	_, err = DeserializeBinaryStats(data5)
	assert.Error(t, err)

	// Unsupported PK type
	data6 := makeBinaryStatsHeaderForTest()
	binary.LittleEndian.PutUint32(data6[16:20], uint32(schemapb.DataType_Bool))
	binary.LittleEndian.PutUint32(data6[BinaryFileHeaderSize:BinaryFileHeaderSize+4], 1)
	binary.LittleEndian.PutUint32(data6[BinaryFileHeaderSize+4:BinaryFileHeaderSize+8], 3)
	_, err = DeserializeBinaryStats(data6)
	assert.Error(t, err)

	// Header declares an entry, but no entry header bytes follow.
	data7 := make([]byte, BinaryFileHeaderSize)
	copy(data7[:8], BinaryStatsMagic)
	binary.LittleEndian.PutUint32(data7[8:12], BinaryStatsVersion)
	binary.LittleEndian.PutUint32(data7[12:16], 1)
	binary.LittleEndian.PutUint32(data7[16:20], uint32(schemapb.DataType_Int64))
	_, err = DeserializeBinaryStats(data7)
	assert.Error(t, err)

	// VarChar entry declares PK bytes that are not present.
	data8 := make([]byte, BinaryFileHeaderSize+BinaryEntryHeaderSize)
	copy(data8[:8], BinaryStatsMagic)
	binary.LittleEndian.PutUint32(data8[8:12], BinaryStatsVersion)
	binary.LittleEndian.PutUint32(data8[12:16], 1)
	binary.LittleEndian.PutUint32(data8[16:20], uint32(schemapb.DataType_VarChar))
	binary.LittleEndian.PutUint32(data8[BinaryFileHeaderSize:BinaryFileHeaderSize+4], 1)
	binary.LittleEndian.PutUint32(data8[BinaryFileHeaderSize+4:BinaryFileHeaderSize+8], 3)
	binary.LittleEndian.PutUint32(data8[BinaryFileHeaderSize+8:BinaryFileHeaderSize+12], 8)
	_, err = DeserializeBinaryStats(data8)
	assert.Error(t, err)

	// VarChar PK data exists but is malformed.
	data9 := make([]byte, BinaryFileHeaderSize+BinaryEntryHeaderSize+4+BinaryBlockSize)
	copy(data9[:8], BinaryStatsMagic)
	binary.LittleEndian.PutUint32(data9[8:12], BinaryStatsVersion)
	binary.LittleEndian.PutUint32(data9[12:16], 1)
	binary.LittleEndian.PutUint32(data9[16:20], uint32(schemapb.DataType_VarChar))
	binary.LittleEndian.PutUint32(data9[BinaryFileHeaderSize:BinaryFileHeaderSize+4], 1)
	binary.LittleEndian.PutUint32(data9[BinaryFileHeaderSize+4:BinaryFileHeaderSize+8], 3)
	binary.LittleEndian.PutUint32(data9[BinaryFileHeaderSize+8:BinaryFileHeaderSize+12], 4)
	_, err = DeserializeBinaryStats(data9)
	assert.Error(t, err)
}

func TestReadVarCharPKs_InvalidBuffers(t *testing.T) {
	_, _, err := readVarCharPKs([]byte{1, 2, 3})
	assert.Error(t, err)

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, 1)
	_, _, err = readVarCharPKs(buf)
	assert.Error(t, err)

	buf = []byte{0, 0, 0, 0}
	_, _, err = readVarCharPKs(buf)
	assert.Error(t, err)

	buf = make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[4:], 1)
	_, _, err = readVarCharPKs(buf)
	assert.Error(t, err)
}

func TestDeserializeStats_AutoDetectBinaryAndJSON(t *testing.T) {
	bf := bloomfilter.NewBloomFilterWithType(100, 0.01, "BlockedBloomFilter")
	key := make([]byte, 8)
	common.Endian.PutUint64(key, uint64(7))
	bf.Add(key)

	stat := &PrimaryKeyStats{
		FieldID: 100,
		PkType:  int64(schemapb.DataType_Int64),
		BFType:  bloomfilter.BlockedBF,
		BF:      bf,
		MinPk:   NewInt64PrimaryKey(7),
		MaxPk:   NewInt64PrimaryKey(7),
	}
	binaryData, err := SerializeBinaryStats([]*PrimaryKeyStats{stat})
	require.NoError(t, err)

	jsonWriter := &StatsWriter{}
	require.NoError(t, jsonWriter.Generate(stat))

	stats, err := DeserializeStats([]*Blob{
		{Value: nil},
		{Value: binaryData},
		{Value: jsonWriter.GetBuffer()},
	})
	require.NoError(t, err)
	require.Len(t, stats, 2)
	assert.True(t, stats[0].BF.Test(key))
	assert.True(t, stats[1].BF.Test(key))

	_, err = DeserializeStats([]*Blob{{Value: []byte("{bad-json")}})
	assert.Error(t, err)

	_, err = DeserializeStats([]*Blob{{Value: []byte(BinaryStatsMagic + "short")}})
	assert.Error(t, err)
}

func TestDeserializeStatsList_EmptyAndJSON(t *testing.T) {
	stats, err := DeserializeStatsList(&Blob{})
	require.NoError(t, err)
	assert.Empty(t, stats)

	stat, err := NewPrimaryKeyStats(100, int64(schemapb.DataType_Int64), 10)
	require.NoError(t, err)
	stat.Update(NewInt64PrimaryKey(1))

	writer := &StatsWriter{}
	require.NoError(t, writer.GenerateList([]*PrimaryKeyStats{stat}))

	stats, err = DeserializeStatsList(&Blob{Value: writer.GetBuffer()})
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.True(t, stats[0].MinPk.EQ(NewInt64PrimaryKey(1)))
}

func TestMarshalBFBlocks_InvalidInputs(t *testing.T) {
	err := marshalBFBlocks(make([]byte, BinaryBlockSize), bloomfilter.NewBloomFilterWithType(100, 0.01, "BasicBloomFilter"))
	assert.Error(t, err)

	err = marshalBFBlocks(make([]byte, 1), bloomfilter.NewBloomFilterWithType(100, 0.01, "BlockedBloomFilter"))
	assert.Error(t, err)
}

func makeBinaryStatsHeaderForTest() []byte {
	data := make([]byte, BinaryFileHeaderSize+BinaryEntryHeaderSize)
	copy(data[:8], BinaryStatsMagic)
	binary.LittleEndian.PutUint32(data[8:12], BinaryStatsVersion)
	binary.LittleEndian.PutUint32(data[12:16], 1)
	binary.LittleEndian.PutUint32(data[16:20], uint32(schemapb.DataType_Int64))
	binary.LittleEndian.PutUint32(data[20:24], 100)
	return data
}

type fakeBinaryStatsBloomFilter struct {
	capacity uint
	k        uint
}

func (f fakeBinaryStatsBloomFilter) Type() bloomfilter.BFType {
	return bloomfilter.BlockedBF
}

func (f fakeBinaryStatsBloomFilter) Cap() uint {
	return f.capacity
}

func (f fakeBinaryStatsBloomFilter) K() uint {
	return f.k
}

func (f fakeBinaryStatsBloomFilter) Add([]byte) {
}

func (f fakeBinaryStatsBloomFilter) AddString(string) {
}

func (f fakeBinaryStatsBloomFilter) Test([]byte) bool {
	return false
}

func (f fakeBinaryStatsBloomFilter) TestString(string) bool {
	return false
}

func (f fakeBinaryStatsBloomFilter) TestLocations([]uint64) bool {
	return false
}

func (f fakeBinaryStatsBloomFilter) BatchTestLocations(locs [][]uint64, _ []bool) []bool {
	return make([]bool, len(locs))
}

func (f fakeBinaryStatsBloomFilter) MarshalJSON() ([]byte, error) {
	return nil, nil
}

func (f fakeBinaryStatsBloomFilter) UnmarshalJSON([]byte) error {
	return nil
}

type fakeBinaryStatsPrimaryKey struct {
	typ schemapb.DataType
}

func (f fakeBinaryStatsPrimaryKey) GT(PrimaryKey) bool {
	return false
}

func (f fakeBinaryStatsPrimaryKey) GE(PrimaryKey) bool {
	return false
}

func (f fakeBinaryStatsPrimaryKey) LT(PrimaryKey) bool {
	return false
}

func (f fakeBinaryStatsPrimaryKey) LE(PrimaryKey) bool {
	return false
}

func (f fakeBinaryStatsPrimaryKey) EQ(PrimaryKey) bool {
	return false
}

func (f fakeBinaryStatsPrimaryKey) MarshalJSON() ([]byte, error) {
	return nil, nil
}

func (f fakeBinaryStatsPrimaryKey) UnmarshalJSON([]byte) error {
	return nil
}

func (f fakeBinaryStatsPrimaryKey) SetValue(interface{}) error {
	return nil
}

func (f fakeBinaryStatsPrimaryKey) GetValue() interface{} {
	return nil
}

func (f fakeBinaryStatsPrimaryKey) Type() schemapb.DataType {
	return f.typ
}

func (f fakeBinaryStatsPrimaryKey) Size() int64 {
	return 0
}
