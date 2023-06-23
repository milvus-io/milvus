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
	"encoding/json"
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
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

	stats := &PrimaryKeyStats{
		FieldID: common.RowIDField,
		Min:     1,
		Max:     9,
		BF:      bloom.NewWithEstimates(100000, 0.05),
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
