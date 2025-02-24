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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestFieldStatsUpdate(t *testing.T) {
	fieldStat1, err := NewFieldStats(1, schemapb.DataType_Int8, 2)
	assert.NoError(t, err)
	fieldStat1.Update(NewInt8FieldValue(1))
	fieldStat1.Update(NewInt8FieldValue(3))
	assert.Equal(t, int8(3), fieldStat1.Max.GetValue())
	assert.Equal(t, int8(1), fieldStat1.Min.GetValue())

	fieldStat2, err := NewFieldStats(1, schemapb.DataType_Int16, 2)
	assert.NoError(t, err)
	fieldStat2.Update(NewInt16FieldValue(99))
	fieldStat2.Update(NewInt16FieldValue(201))
	assert.Equal(t, int16(201), fieldStat2.Max.GetValue())
	assert.Equal(t, int16(99), fieldStat2.Min.GetValue())

	fieldStat3, err := NewFieldStats(1, schemapb.DataType_Int32, 2)
	assert.NoError(t, err)
	fieldStat3.Update(NewInt32FieldValue(99))
	fieldStat3.Update(NewInt32FieldValue(201))
	assert.Equal(t, int32(201), fieldStat3.Max.GetValue())
	assert.Equal(t, int32(99), fieldStat3.Min.GetValue())

	fieldStat4, err := NewFieldStats(1, schemapb.DataType_Int64, 2)
	assert.NoError(t, err)
	fieldStat4.Update(NewInt64FieldValue(99))
	fieldStat4.Update(NewInt64FieldValue(201))
	assert.Equal(t, int64(201), fieldStat4.Max.GetValue())
	assert.Equal(t, int64(99), fieldStat4.Min.GetValue())

	fieldStat5, err := NewFieldStats(1, schemapb.DataType_Float, 2)
	assert.NoError(t, err)
	fieldStat5.Update(NewFloatFieldValue(99.0))
	fieldStat5.Update(NewFloatFieldValue(201.0))
	assert.Equal(t, float32(201.0), fieldStat5.Max.GetValue())
	assert.Equal(t, float32(99.0), fieldStat5.Min.GetValue())

	fieldStat6, err := NewFieldStats(1, schemapb.DataType_Double, 2)
	assert.NoError(t, err)
	fieldStat6.Update(NewDoubleFieldValue(9.9))
	fieldStat6.Update(NewDoubleFieldValue(20.1))
	assert.Equal(t, float64(20.1), fieldStat6.Max.GetValue())
	assert.Equal(t, float64(9.9), fieldStat6.Min.GetValue())

	fieldStat7, err := NewFieldStats(2, schemapb.DataType_String, 2)
	assert.NoError(t, err)
	fieldStat7.Update(NewStringFieldValue("a"))
	fieldStat7.Update(NewStringFieldValue("z"))
	assert.Equal(t, "z", fieldStat7.Max.GetValue())
	assert.Equal(t, "a", fieldStat7.Min.GetValue())

	fieldStat8, err := NewFieldStats(2, schemapb.DataType_VarChar, 2)
	assert.NoError(t, err)
	fieldStat8.Update(NewVarCharFieldValue("a"))
	fieldStat8.Update(NewVarCharFieldValue("z"))
	assert.Equal(t, "z", fieldStat8.Max.GetValue())
	assert.Equal(t, "a", fieldStat8.Min.GetValue())
}

func TestFieldStatsWriter_Int8FieldValue(t *testing.T) {
	data := &Int8FieldData{
		Data: []int8{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &FieldStatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int8, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &FieldStatsReader{}
	sr.SetBuffer(b)
	statsList, err := sr.GetFieldStatsList()
	assert.NoError(t, err)
	stats := statsList[0]
	maxPk := NewInt8FieldValue(9)
	minPk := NewInt8FieldValue(1)
	assert.Equal(t, true, stats.Max.EQ(maxPk))
	assert.Equal(t, true, stats.Min.EQ(minPk))
	buffer := make([]byte, 8)
	for _, id := range data.Data {
		common.Endian.PutUint64(buffer, uint64(id))
		assert.True(t, stats.BF.Test(buffer))
	}

	msgs := &Int8FieldData{
		Data: []int8{},
	}
	err = sw.GenerateByData(common.RowIDField, schemapb.DataType_Int8, msgs)
	assert.NoError(t, err)
}

func TestFieldStatsWriter_Int16FieldValue(t *testing.T) {
	data := &Int16FieldData{
		Data: []int16{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &FieldStatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int16, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &FieldStatsReader{}
	sr.SetBuffer(b)
	statsList, err := sr.GetFieldStatsList()
	assert.NoError(t, err)
	stats := statsList[0]
	maxPk := NewInt16FieldValue(9)
	minPk := NewInt16FieldValue(1)
	assert.Equal(t, true, stats.Max.EQ(maxPk))
	assert.Equal(t, true, stats.Min.EQ(minPk))
	buffer := make([]byte, 8)
	for _, id := range data.Data {
		common.Endian.PutUint64(buffer, uint64(id))
		assert.True(t, stats.BF.Test(buffer))
	}

	msgs := &Int16FieldData{
		Data: []int16{},
	}
	err = sw.GenerateByData(common.RowIDField, schemapb.DataType_Int16, msgs)
	assert.NoError(t, err)
}

func TestFieldStatsWriter_Int32FieldValue(t *testing.T) {
	data := &Int32FieldData{
		Data: []int32{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &FieldStatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int32, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &FieldStatsReader{}
	sr.SetBuffer(b)
	statsList, err := sr.GetFieldStatsList()
	assert.NoError(t, err)
	stats := statsList[0]
	maxPk := NewInt32FieldValue(9)
	minPk := NewInt32FieldValue(1)
	assert.Equal(t, true, stats.Max.EQ(maxPk))
	assert.Equal(t, true, stats.Min.EQ(minPk))
	buffer := make([]byte, 8)
	for _, id := range data.Data {
		common.Endian.PutUint64(buffer, uint64(id))
		assert.True(t, stats.BF.Test(buffer))
	}

	msgs := &Int32FieldData{
		Data: []int32{},
	}
	err = sw.GenerateByData(common.RowIDField, schemapb.DataType_Int32, msgs)
	assert.NoError(t, err)
}

func TestFieldStatsWriter_Int64FieldValue(t *testing.T) {
	data := &Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &FieldStatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &FieldStatsReader{}
	sr.SetBuffer(b)
	statsList, err := sr.GetFieldStatsList()
	assert.NoError(t, err)
	stats := statsList[0]
	maxPk := NewInt64FieldValue(9)
	minPk := NewInt64FieldValue(1)
	assert.Equal(t, true, stats.Max.EQ(maxPk))
	assert.Equal(t, true, stats.Min.EQ(minPk))
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

func TestFieldStatsWriter_FloatFieldValue(t *testing.T) {
	data := &FloatFieldData{
		Data: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &FieldStatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Float, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &FieldStatsReader{}
	sr.SetBuffer(b)
	statsList, err := sr.GetFieldStatsList()
	assert.NoError(t, err)
	stats := statsList[0]
	maxPk := NewFloatFieldValue(9)
	minPk := NewFloatFieldValue(1)
	assert.Equal(t, true, stats.Max.EQ(maxPk))
	assert.Equal(t, true, stats.Min.EQ(minPk))
	buffer := make([]byte, 8)
	for _, id := range data.Data {
		common.Endian.PutUint64(buffer, uint64(id))
		assert.True(t, stats.BF.Test(buffer))
	}

	msgs := &FloatFieldData{
		Data: []float32{},
	}
	err = sw.GenerateByData(common.RowIDField, schemapb.DataType_Float, msgs)
	assert.NoError(t, err)
}

func TestFieldStatsWriter_DoubleFieldValue(t *testing.T) {
	data := &DoubleFieldData{
		Data: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &FieldStatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Double, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &FieldStatsReader{}
	sr.SetBuffer(b)
	statsList, err := sr.GetFieldStatsList()
	assert.NoError(t, err)
	stats := statsList[0]
	maxPk := NewDoubleFieldValue(9)
	minPk := NewDoubleFieldValue(1)
	assert.Equal(t, true, stats.Max.EQ(maxPk))
	assert.Equal(t, true, stats.Min.EQ(minPk))
	buffer := make([]byte, 8)
	for _, id := range data.Data {
		common.Endian.PutUint64(buffer, uint64(id))
		assert.True(t, stats.BF.Test(buffer))
	}

	msgs := &DoubleFieldData{
		Data: []float64{},
	}
	err = sw.GenerateByData(common.RowIDField, schemapb.DataType_Double, msgs)
	assert.NoError(t, err)
}

func TestFieldStatsWriter_StringFieldValue(t *testing.T) {
	data := &StringFieldData{
		Data: []string{"bc", "ac", "abd", "cd", "milvus"},
	}
	sw := &FieldStatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_String, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()
	t.Log(string(b))

	sr := &FieldStatsReader{}
	sr.SetBuffer(b)
	statsList, err := sr.GetFieldStatsList()
	assert.NoError(t, err)
	stats := statsList[0]
	maxPk := NewStringFieldValue("milvus")
	minPk := NewStringFieldValue("abd")
	assert.Equal(t, true, stats.Max.EQ(maxPk))
	assert.Equal(t, true, stats.Min.EQ(minPk))
	for _, id := range data.Data {
		assert.True(t, stats.BF.TestString(id))
	}

	msgs := &Int64FieldData{
		Data: []int64{},
	}
	err = sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, msgs)
	assert.NoError(t, err)
}

func TestFieldStatsWriter_VarCharFieldValue(t *testing.T) {
	data := &StringFieldData{
		Data: []string{"bc", "ac", "abd", "cd", "milvus"},
	}
	sw := &FieldStatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_VarChar, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()
	t.Log(string(b))

	sr := &FieldStatsReader{}
	sr.SetBuffer(b)
	statsList, err := sr.GetFieldStatsList()
	assert.NoError(t, err)
	stats := statsList[0]
	maxPk := NewVarCharFieldValue("milvus")
	minPk := NewVarCharFieldValue("abd")
	assert.Equal(t, true, stats.Max.EQ(maxPk))
	assert.Equal(t, true, stats.Min.EQ(minPk))
	for _, id := range data.Data {
		assert.True(t, stats.BF.TestString(id))
	}

	msgs := &Int64FieldData{
		Data: []int64{},
	}
	err = sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, msgs)
	assert.NoError(t, err)
}

func TestFieldStatsWriter_BF(t *testing.T) {
	value := make([]int64, 1000000)
	for i := 0; i < 1000000; i++ {
		value[i] = int64(i)
	}
	data := &Int64FieldData{
		Data: value,
	}
	t.Log(data.RowNum())
	sw := &FieldStatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
	assert.NoError(t, err)

	sr := &FieldStatsReader{}
	sr.SetBuffer(sw.GetBuffer())
	statsList, err := sr.GetFieldStatsList()
	assert.NoError(t, err)
	stats := statsList[0]
	buf := make([]byte, 8)

	for i := 0; i < 1000000; i++ {
		common.Endian.PutUint64(buf, uint64(i))
		assert.True(t, stats.BF.Test(buf))
	}

	common.Endian.PutUint64(buf, uint64(1000001))
	assert.False(t, stats.BF.Test(buf))

	assert.True(t, stats.Min.EQ(NewInt64FieldValue(0)))
	assert.True(t, stats.Max.EQ(NewInt64FieldValue(999999)))
}

func TestFieldStatsWriter_UpgradePrimaryKey(t *testing.T) {
	data := &Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}

	stats := &PrimaryKeyStats{
		FieldID: common.RowIDField,
		Min:     1,
		Max:     9,
		BF:      bloomfilter.NewBloomFilterWithType(100000, 0.05, paramtable.Get().CommonCfg.BloomFilterType.GetValue()),
	}

	b := make([]byte, 8)
	for _, int64Value := range data.Data {
		common.Endian.PutUint64(b, uint64(int64Value))
		stats.BF.Add(b)
	}
	blob, err := json.Marshal(stats)
	assert.NoError(t, err)
	sr := &FieldStatsReader{}
	sr.SetBuffer(blob)
	unmarshalledStats, err := sr.GetFieldStatsList()
	assert.NoError(t, err)
	maxPk := &Int64FieldValue{
		Value: 9,
	}
	minPk := &Int64FieldValue{
		Value: 1,
	}
	assert.Equal(t, true, unmarshalledStats[0].Max.EQ(maxPk))
	assert.Equal(t, true, unmarshalledStats[0].Min.EQ(minPk))
	buffer := make([]byte, 8)
	for _, id := range data.Data {
		common.Endian.PutUint64(buffer, uint64(id))
		assert.True(t, unmarshalledStats[0].BF.Test(buffer))
	}
}

func TestDeserializeFieldStatsFailed(t *testing.T) {
	t.Run("empty field stats", func(t *testing.T) {
		blob := &Blob{
			Value: []byte{},
		}

		_, err := DeserializeFieldStats(blob)
		assert.NoError(t, err)
	})

	t.Run("invalid field stats", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("abc"),
		}

		_, err := DeserializeFieldStats(blob)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("valid field stats", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("[{\"fieldID\":1,\"max\":10, \"min\":1}]"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.NoError(t, err)
	})
}

func TestDeserializeFieldStats(t *testing.T) {
	t.Run("empty field stats", func(t *testing.T) {
		blob := &Blob{
			Value: []byte{},
		}

		_, err := DeserializeFieldStats(blob)
		assert.NoError(t, err)
	})

	t.Run("invalid field stats, not valid json", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("abc"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.Error(t, err)
	})

	t.Run("invalid field stats, no fieldID", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("{\"field\":\"a\"}"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.Error(t, err)
	})

	t.Run("invalid field stats, invalid fieldID", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("{\"fieldID\":\"a\"}"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.Error(t, err)
	})

	t.Run("invalid field stats, invalid type", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("{\"fieldID\":1,\"type\":\"a\"}"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.Error(t, err)
	})

	t.Run("invalid field stats, invalid type", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("{\"fieldID\":1,\"type\":\"a\"}"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.Error(t, err)
	})

	t.Run("invalid field stats, invalid max int64", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("{\"fieldID\":1,\"max\":\"a\"}"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.Error(t, err)
	})

	t.Run("invalid field stats, invalid min int64", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("{\"fieldID\":1,\"min\":\"a\"}"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.Error(t, err)
	})

	t.Run("invalid field stats, invalid max varchar", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("{\"fieldID\":1,\"type\":21,\"max\":2}"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.Error(t, err)
	})

	t.Run("invalid field stats, invalid min varchar", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("{\"fieldID\":1,\"type\":21,\"min\":1}"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.Error(t, err)
	})

	t.Run("valid int64 field stats", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("{\"fieldID\":1,\"max\":10, \"min\":1}"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.NoError(t, err)
	})

	t.Run("valid varchar field stats", func(t *testing.T) {
		blob := &Blob{
			Value: []byte("{\"fieldID\":1,\"type\":21,\"max\":\"z\", \"min\":\"a\"}"),
		}
		_, err := DeserializeFieldStats(blob)
		assert.NoError(t, err)
	})
}

func TestCompatible_ReadPrimaryKeyStatsWithFieldStatsReader(t *testing.T) {
	data := &Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &StatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &FieldStatsReader{}
	sr.SetBuffer(b)
	stats, err := sr.GetFieldStatsList()
	assert.NoError(t, err)
	maxPk := &Int64FieldValue{
		Value: 9,
	}
	minPk := &Int64FieldValue{
		Value: 1,
	}
	assert.Equal(t, true, stats[0].Max.EQ(maxPk))
	assert.Equal(t, true, stats[0].Min.EQ(minPk))
	assert.Equal(t, schemapb.DataType_Int64.String(), stats[0].Type.String())
	buffer := make([]byte, 8)
	for _, id := range data.Data {
		common.Endian.PutUint64(buffer, uint64(id))
		assert.True(t, stats[0].BF.Test(buffer))
	}

	msgs := &Int64FieldData{
		Data: []int64{},
	}
	err = sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, msgs)
	assert.NoError(t, err)
}

func TestFieldStatsUnMarshal(t *testing.T) {
	t.Run("fail", func(t *testing.T) {
		stats, err := NewFieldStats(1, schemapb.DataType_Int64, 1)
		assert.NoError(t, err)
		err = stats.UnmarshalJSON([]byte("{\"fieldID\":1,\"max\":10, }"))
		assert.Error(t, err)
		err = stats.UnmarshalJSON([]byte("{\"fieldID\":1,\"max\":10, \"maxPk\":\"A\"}"))
		assert.Error(t, err)
		err = stats.UnmarshalJSON([]byte("{\"fieldID\":1,\"max\":10, \"maxPk\":10, \"minPk\": \"b\"}"))
		assert.Error(t, err)
		// return AlwaysTrueBloomFilter when deserialize bloom filter failed.
		err = stats.UnmarshalJSON([]byte("{\"fieldID\":1,\"max\":10, \"maxPk\":10, \"minPk\": 1, \"bf\": \"2\"}"))
		assert.NoError(t, err)
	})

	t.Run("succeed", func(t *testing.T) {
		int8stats, err := NewFieldStats(1, schemapb.DataType_Int8, 1)
		assert.NoError(t, err)
		err = int8stats.UnmarshalJSON([]byte("{\"type\":2, \"fieldID\":1,\"max\":10, \"min\": 1}"))
		assert.NoError(t, err)

		int16stats, err := NewFieldStats(1, schemapb.DataType_Int16, 1)
		assert.NoError(t, err)
		err = int16stats.UnmarshalJSON([]byte("{\"type\":3, \"fieldID\":1,\"max\":10, \"min\": 1}"))
		assert.NoError(t, err)

		int32stats, err := NewFieldStats(1, schemapb.DataType_Int32, 1)
		assert.NoError(t, err)
		err = int32stats.UnmarshalJSON([]byte("{\"type\":4, \"fieldID\":1,\"max\":10, \"min\": 1}"))
		assert.NoError(t, err)

		int64stats, err := NewFieldStats(1, schemapb.DataType_Int64, 1)
		assert.NoError(t, err)
		err = int64stats.UnmarshalJSON([]byte("{\"type\":5, \"fieldID\":1,\"max\":10, \"min\": 1}"))
		assert.NoError(t, err)

		floatstats, err := NewFieldStats(1, schemapb.DataType_Float, 1)
		assert.NoError(t, err)
		err = floatstats.UnmarshalJSON([]byte("{\"type\":10, \"fieldID\":1,\"max\":10.0, \"min\": 1.2}"))
		assert.NoError(t, err)

		doublestats, err := NewFieldStats(1, schemapb.DataType_Double, 1)
		assert.NoError(t, err)
		err = doublestats.UnmarshalJSON([]byte("{\"type\":11, \"fieldID\":1,\"max\":10.0, \"min\": 1.2}"))
		assert.NoError(t, err)
	})
}

func TestCompatible_ReadFieldStatsWithPrimaryKeyStatsReader(t *testing.T) {
	data := &Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &FieldStatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &StatsReader{}
	sr.SetBuffer(b)
	statsList, err := sr.GetPrimaryKeyStatsList()
	assert.NoError(t, err)
	stats := statsList[0]
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

func TestMultiFieldStats(t *testing.T) {
	pkData := &Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	partitionKeyData := &Int64FieldData{
		Data: []int64{1, 10, 21, 31, 41, 51, 61, 71, 81},
	}

	sw := &FieldStatsWriter{}
	err := sw.GenerateByData(common.RowIDField, schemapb.DataType_Int64, pkData, partitionKeyData)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &FieldStatsReader{}
	sr.SetBuffer(b)
	statsList, err := sr.GetFieldStatsList()
	assert.Equal(t, 2, len(statsList))
	assert.NoError(t, err)

	pkStats := statsList[0]
	maxPk := NewInt64FieldValue(9)
	minPk := NewInt64FieldValue(1)
	assert.Equal(t, true, pkStats.Max.EQ(maxPk))
	assert.Equal(t, true, pkStats.Min.EQ(minPk))

	partitionKeyStats := statsList[1]
	maxPk2 := NewInt64FieldValue(81)
	minPk2 := NewInt64FieldValue(1)
	assert.Equal(t, true, partitionKeyStats.Max.EQ(maxPk2))
	assert.Equal(t, true, partitionKeyStats.Min.EQ(minPk2))
}

func TestVectorFieldStatsMarshal(t *testing.T) {
	stats, err := NewFieldStats(1, schemapb.DataType_FloatVector, 1)
	assert.NoError(t, err)
	centroid := NewFloatVectorFieldValue([]float32{1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0})
	stats.SetVectorCentroids(centroid)

	bytes, err := json.Marshal(stats)
	assert.NoError(t, err)

	stats2, err := NewFieldStats(1, schemapb.DataType_FloatVector, 1)
	assert.NoError(t, err)
	stats2.UnmarshalJSON(bytes)
	assert.Equal(t, 1, len(stats2.Centroids))
	assert.ElementsMatch(t, []VectorFieldValue{centroid}, stats2.Centroids)

	stats3, err := NewFieldStats(1, schemapb.DataType_FloatVector, 2)
	assert.NoError(t, err)
	centroid2 := NewFloatVectorFieldValue([]float32{9.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0})
	stats3.SetVectorCentroids(centroid, centroid2)

	bytes2, err := json.Marshal(stats3)
	assert.NoError(t, err)

	stats4, err := NewFieldStats(1, schemapb.DataType_FloatVector, 2)
	assert.NoError(t, err)
	stats4.UnmarshalJSON(bytes2)
	assert.Equal(t, 2, len(stats4.Centroids))
	assert.ElementsMatch(t, []VectorFieldValue{centroid, centroid2}, stats4.Centroids)
}

func TestFindMaxVersion(t *testing.T) {
	files := []string{"path/1", "path/2", "path/3"}
	version, path := FindPartitionStatsMaxVersion(files)
	assert.Equal(t, int64(3), version)
	assert.Equal(t, "path/3", path)

	files2 := []string{}
	version2, path2 := FindPartitionStatsMaxVersion(files2)
	assert.Equal(t, int64(-1), version2)
	assert.Equal(t, "", path2)
}
