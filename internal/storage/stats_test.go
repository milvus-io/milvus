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

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/rootcoord"
)

func TestStatsWriter_Int64PrimaryKey(t *testing.T) {
	data := &Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	sw := &StatsWriter{}
	err := sw.generatePrimaryKeyStats(common.RowIDField, schemapb.DataType_Int64, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &StatsReader{}
	sr.SetBuffer(b)
	stats, err := sr.GetPrimaryKeyStats()
	assert.Nil(t, err)
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
	err = sw.generatePrimaryKeyStats(rootcoord.RowIDField, schemapb.DataType_Int64, msgs)
	assert.Nil(t, err)
}

func TestStatsWriter_StringPrimaryKey(t *testing.T) {
	data := &StringFieldData{
		Data: []string{"bc", "ac", "abd", "cd", "milvus"},
	}
	sw := &StatsWriter{}
	err := sw.generatePrimaryKeyStats(common.RowIDField, schemapb.DataType_VarChar, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &StatsReader{}
	sr.SetBuffer(b)
	stats, err := sr.GetPrimaryKeyStats()
	assert.Nil(t, err)
	maxPk := &StringPrimaryKey{
		Value: "milvus",
	}
	minPk := &StringPrimaryKey{
		Value: "abd",
	}
	assert.Equal(t, true, stats.MaxPk.EQ(maxPk))
	assert.Equal(t, true, stats.MinPk.EQ(minPk))
	for _, id := range data.Data {
		assert.True(t, stats.BF.TestString(id))
	}

	msgs := &Int64FieldData{
		Data: []int64{},
	}
	err = sw.generatePrimaryKeyStats(rootcoord.RowIDField, schemapb.DataType_Int64, msgs)
	assert.Nil(t, err)
}

func TestStatsWriter_UpgradePrimaryKey(t *testing.T) {
	data := &Int64FieldData{
		Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}

	stats := &PrimaryKeyStats{
		FieldID: common.RowIDField,
		Min:     1,
		Max:     9,
		BF:      bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive),
	}

	b := make([]byte, 8)
	for _, int64Value := range data.Data {
		common.Endian.PutUint64(b, uint64(int64Value))
		stats.BF.Add(b)
	}
	blob, err := json.Marshal(stats)
	assert.Nil(t, err)
	sr := &StatsReader{}
	sr.SetBuffer(blob)
	unmarshaledStats, err := sr.GetPrimaryKeyStats()
	assert.Nil(t, err)
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
