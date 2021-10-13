// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import (
	"encoding/binary"
	"testing"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/stretchr/testify/assert"
)

func TestStatsWriter_StatsInt64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	sw := &StatsWriter{}
	err := sw.StatsInt64(common.RowIDField, data)
	assert.NoError(t, err)
	b := sw.GetBuffer()

	sr := &StatsReader{}
	sr.SetBuffer(b)
	stats, err := sr.GetInt64Stats()
	assert.Nil(t, err)
	assert.Equal(t, stats.Max, int64(9))
	assert.Equal(t, stats.Min, int64(1))
	buffer := make([]byte, 8)
	for _, id := range data {
		binary.LittleEndian.PutUint64(buffer, uint64(id))
		assert.True(t, stats.BF.Test(buffer))
	}

	msgs := []int64{}
	err = sw.StatsInt64(rootcoord.RowIDField, msgs)
	assert.Nil(t, err)
}
