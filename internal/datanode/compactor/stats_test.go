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

package compactor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Compaction writers stamp FieldNullCounts (zero included, one entry per
// present field) on the insert binlogs they return;
// buildCompactionOutputStats must surface them on Statistics.NullCounts so
// DataCoord's verbatim copy satisfies the NullCounts presence contract.
func TestBuildCompactionOutputStats_NullCounts(t *testing.T) {
	t.Run("packed writer shape", func(t *testing.T) {
		insertLogs := []*datapb.FieldBinlog{
			{FieldID: 0, ChildFields: []int64{100, 101}, Binlogs: []*datapb.Binlog{
				{EntriesNum: 10, MemorySize: 64, FieldNullCounts: map[int64]int64{100: 3, 101: 0}},
				{EntriesNum: 10, MemorySize: 64, FieldNullCounts: map[int64]int64{100: 1, 101: 0}},
			}},
		}
		s := buildCompactionOutputStats(insertLogs, nil, 0)
		assert.Equal(t, map[int64]int64{100: 4, 101: 0}, s.GetNullCounts())
	})
	t.Run("l0 delta-only output has no entries", func(t *testing.T) {
		deltalogs := []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{EntriesNum: 5, MemorySize: 32}}},
		}
		s := buildCompactionOutputStats(nil, deltalogs, 0)
		assert.Nil(t, s.GetNullCounts())
	})
}
