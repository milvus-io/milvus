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
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestStatsWriter_Int64PrimaryKey(t *testing.T) {
	data := &Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &StatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &StatsReader{}
	sr.SetBuffer(b)
	stats, err := sr.GetPrimaryKeyStats()
	assert.NoError(t, err)
	maxPk := &Int64PrimaryKey{
		Value: 9,
	}
	minPk := &Int64PrimaryKey{
		Value: 1,
	}
	assert.Equal(t, true, stats.MaxPk.EQ(maxPk))
	assert.Equal(t, true, stats.MinPk.EQ(minPk))
	buffer := make([]byte, 8)
	for _, id := range data.Data {
		common.Endian.PutUint64(buffer, uint64(id))
		assert.True(t, stats.BF.Test(buffer))
	}

	msgs := &Int64FieldData{
		Data: []int64{},
	}
	err = sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, msgs)
	assert.NoError(t, err)
}

func TestStatsWriter_BF(t *testing.T) {
	value := make([]int64, 1000000)
	for i := 0; i < 1000000; i++ {
		value[i] = int64(i)
	}
	data := &Int64FieldData{
		Data: value,
	}
	t.Log(data.RowNum())
	sw := &StatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
	assert.NoError(t, err)

	stats := &PrimaryKeyStats{}
	stats.UnmarshalJSON(sw.buffer)
	buf := make([]byte, 8)

	for i := 0; i < 1000000; i++ {
		common.Endian.PutUint64(buf, uint64(i))
		assert.True(t, stats.BF.Test(buf))
	}

	common.Endian.PutUint64(buf, uint64(1000001))
	assert.False(t, stats.BF.Test(buf))

	assert.True(t, stats.MinPk.EQ(NewInt64PrimaryKey(0)))
	assert.True(t, stats.MaxPk.EQ(NewInt64PrimaryKey(999999)))
}

func TestStatsWriter_VarCharPrimaryKey(t *testing.T) {
	data := &StringFieldData{
		Data: []string{"bc", "ac", "abd", "cd", "milvus"},
	}
	sw := &StatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_VarChar, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &StatsReader{}
	sr.SetBuffer(b)
	stats, err := sr.GetPrimaryKeyStats()
	assert.NoError(t, err)
	maxPk := NewVarCharPrimaryKey("milvus")
	minPk := NewVarCharPrimaryKey("abd")
	assert.Equal(t, true, stats.MaxPk.EQ(maxPk))
	assert.Equal(t, true, stats.MinPk.EQ(minPk))
	for _, id := range data.Data {
		assert.True(t, stats.BF.TestString(id))
	}

	msgs := &Int64FieldData{
		Data: []int64{},
	}
	err = sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, msgs)
	assert.NoError(t, err)
}

func TestStatsWriter_UpgradePrimaryKey(t *testing.T) {
	data := &Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}

	bfType := paramtable.Get().CommonCfg.BloomFilterType.GetValue()
	stats := &PrimaryKeyStats{
		FieldID: common.RowIDField,
		Min:     1,
		Max:     9,
		BFType:  bloomfilter.BFTypeFromString(bfType),
		BF:      bloomfilter.NewBloomFilterWithType(100000, 0.05, bfType),
	}

	b := make([]byte, 8)
	for _, int64Value := range data.Data {
		common.Endian.PutUint64(b, uint64(int64Value))
		stats.BF.Add(b)
	}
	blob, err := json.Marshal(stats)
	assert.NoError(t, err)
	sr := &StatsReader{}
	sr.SetBuffer(blob)
	unmarshaledStats, err := sr.GetPrimaryKeyStats()
	assert.NoError(t, err)
	maxPk := &Int64PrimaryKey{
		Value: 9,
	}
	minPk := &Int64PrimaryKey{
		Value: 1,
	}
	assert.Equal(t, true, unmarshaledStats.MaxPk.EQ(maxPk))
	assert.Equal(t, true, unmarshaledStats.MinPk.EQ(minPk))
	buffer := make([]byte, 8)
	for _, id := range data.Data {
		common.Endian.PutUint64(buffer, uint64(id))
		assert.True(t, unmarshaledStats.BF.Test(buffer))
	}
}

func TestDeserializeBloomFilterStats(t *testing.T) {
	// Build a compound stats blob using GenerateList.
	stat, err := NewPrimaryKeyStats(1, int64(schemapb.DataType_Int64), 100)
	assert.NoError(t, err)
	stat.Update(NewInt64PrimaryKey(1))
	stat.Update(NewInt64PrimaryKey(2))

	sw := &StatsWriter{}
	sw.GenerateList([]*PrimaryKeyStats{stat})
	compoundBlob := &Blob{Value: sw.GetBuffer()}

	// Build a default (per-row) stats blob.
	sw2 := &StatsWriter{}
	err = sw2.GenerateByData(common.RowIDField, schemapb.DataType_Int64, &Int64FieldData{
		Data: []int64{10, 20},
	})
	assert.NoError(t, err)
	defaultBlob := &Blob{Value: sw2.GetBuffer()}

	t.Run("compound path at non-zero index", func(t *testing.T) {
		// The compound blob is at index 1 (not 0). Before the fix this
		// would incorrectly use blobs[0] (the default blob) and fail.
		paths := []string{
			"root/stats/0",
			"root/stats/" + CompoundStatsType.LogIdx(),
		}
		blobs := []*Blob{defaultBlob, compoundBlob}

		stats, err := DeserializeBloomFilterStats(paths, blobs)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(stats))
		assert.True(t, stats[0].MinPk.EQ(NewInt64PrimaryKey(1)))
		assert.True(t, stats[0].MaxPk.EQ(NewInt64PrimaryKey(2)))
	})

	t.Run("compound path at index zero", func(t *testing.T) {
		paths := []string{
			"root/stats/" + CompoundStatsType.LogIdx(),
		}
		blobs := []*Blob{compoundBlob}

		stats, err := DeserializeBloomFilterStats(paths, blobs)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(stats))
		assert.True(t, stats[0].MinPk.EQ(NewInt64PrimaryKey(1)))
	})

	t.Run("default stats fallback", func(t *testing.T) {
		paths := []string{"root/stats/0"}
		blobs := []*Blob{defaultBlob}

		stats, err := DeserializeBloomFilterStats(paths, blobs)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(stats))
		assert.True(t, stats[0].MinPk.EQ(NewInt64PrimaryKey(10)))
		assert.True(t, stats[0].MaxPk.EQ(NewInt64PrimaryKey(20)))
	})
}

func TestDeserializeStatsFailed(t *testing.T) {
	blob := &Blob{
		Value: []byte("abc"),
	}

	_, err := DeserializeStatsList(blob)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestDeserializeEmptyStats(t *testing.T) {
	blob := &Blob{
		Value: []byte{},
	}

	_, err := DeserializeStats([]*Blob{blob})
	assert.NoError(t, err)
}

func TestMarshalStats(t *testing.T) {
	stat, err := NewPrimaryKeyStats(1, int64(schemapb.DataType_Int64), 100000)
	assert.NoError(t, err)

	for i := 0; i < 10000; i++ {
		stat.Update(NewInt64PrimaryKey(int64(i)))
	}

	sw := &StatsWriter{}
	sw.GenerateList([]*PrimaryKeyStats{stat})
	bytes := sw.GetBuffer()

	sr := &StatsReader{}
	sr.SetBuffer(bytes)
	stat1, err := sr.GetPrimaryKeyStatsList()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(stat1))
	assert.Equal(t, stat.Min, stat1[0].Min)
	assert.Equal(t, stat.Max, stat1[0].Max)

	for i := 0; i < 10000; i++ {
		b := make([]byte, 8)
		common.Endian.PutUint64(b, uint64(i))
		assert.True(t, stat1[0].BF.Test(b))
	}
}

func TestBM25Stats_MemSize(t *testing.T) {
	stats := NewBM25Stats()
	baseSize := stats.MemSize()
	assert.Equal(t, int64(120), baseSize)

	// Add tokens and verify size grows
	for i := uint32(0); i < 100; i++ {
		stats.Append(map[uint32]float32{i: 1})
	}
	bytesPerEntry := paramtable.Get().QueryNodeCfg.BM25StatsBytesPerEntry.GetAsInt64()
	assert.Equal(t, int64(120)+100*bytesPerEntry, stats.MemSize())
}

func TestBM25Stats_DeserializeFromReader(t *testing.T) {
	t.Run("roundtrip", func(t *testing.T) {
		original := NewBM25Stats()
		for i := uint32(0); i < 50; i++ {
			original.Append(map[uint32]float32{i: 1})
		}

		data, err := original.Serialize()
		assert.NoError(t, err)

		restored := NewBM25Stats()
		err = restored.DeserializeFromReader(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, original.NumRow(), restored.NumRow())
		assert.Equal(t, original.GetAvgdl(), restored.GetAvgdl())
	})

	t.Run("accumulate_multiple", func(t *testing.T) {
		s1 := NewBM25Stats()
		s1.Append(map[uint32]float32{1: 1, 2: 1})
		d1, _ := s1.Serialize()

		s2 := NewBM25Stats()
		s2.Append(map[uint32]float32{2: 1, 3: 1})
		d2, _ := s2.Serialize()

		merged := NewBM25Stats()
		assert.NoError(t, merged.DeserializeFromReader(bytes.NewReader(d1)))
		assert.NoError(t, merged.DeserializeFromReader(bytes.NewReader(d2)))
		assert.Equal(t, int64(2), merged.NumRow())
	})

	t.Run("truncated_header", func(t *testing.T) {
		// Only 10 bytes, header needs 20 (version + numRow + tokenNum)
		data := make([]byte, 10)
		restored := NewBM25Stats()
		err := restored.DeserializeFromReader(bytes.NewReader(data))
		assert.Error(t, err)
	})

	t.Run("truncated_value", func(t *testing.T) {
		// Valid header + key but truncated value
		buf := new(bytes.Buffer)
		binary.Write(buf, common.Endian, int32(0))   // version
		binary.Write(buf, common.Endian, int64(1))   // numRow
		binary.Write(buf, common.Endian, int64(1))   // numToken
		binary.Write(buf, common.Endian, uint32(42)) // key
		binary.Write(buf, common.Endian, int16(1))   // truncated value (2 bytes instead of 4)

		restored := NewBM25Stats()
		err := restored.DeserializeFromReader(buf)
		assert.Error(t, err)
	})

	t.Run("empty_tokens", func(t *testing.T) {
		// Valid header, zero tokens
		buf := new(bytes.Buffer)
		binary.Write(buf, common.Endian, int32(0)) // version
		binary.Write(buf, common.Endian, int64(5)) // numRow
		binary.Write(buf, common.Endian, int64(0)) // numToken

		restored := NewBM25Stats()
		err := restored.DeserializeFromReader(buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), restored.NumRow())
	})
}
