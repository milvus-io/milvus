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

package metacache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// insertBinlog builds a single-field insert FieldBinlog map for the Digest tests.
func insertBinlog(fieldID, memSize, rows int64, tsTo uint64, nullCounts map[int64]int64) map[int64]*datapb.FieldBinlog {
	return map[int64]*datapb.FieldBinlog{
		fieldID: {
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{
				{MemorySize: memSize, EntriesNum: rows, TimestampTo: tsTo, FieldNullCounts: nullCounts},
			},
		},
	}
}

func TestSegmentStats_DigestPublish(t *testing.T) {
	s := NewEmptySegmentStats()
	s.Digest(insertBinlog(1, 100, 10, 5, map[int64]int64{1: 2}), nil, 0, 10, 1, 5)
	s.Digest(insertBinlog(1, 100, 10, 9, map[int64]int64{1: 1}), nil, 0, 10, 6, 9)
	got := s.Publish()
	assert.Equal(t, int64(200), got.GetInsertBinlogSize())
	assert.Equal(t, map[int64]int64{1: 3}, got.GetNullCounts())
}

func TestSegmentStats_CloneSharesNothingMutable(t *testing.T) {
	s := NewEmptySegmentStats()
	s.Digest(insertBinlog(1, 100, 10, 5, nil), nil, 0, 10, 1, 5)
	c := s.Clone()
	c.Digest(insertBinlog(1, 100, 10, 9, nil), nil, 0, 10, 6, 9)
	// original unchanged after cloning then mutating the clone
	assert.Equal(t, int64(100), s.Publish().GetInsertBinlogSize())
	assert.Equal(t, int64(200), c.Publish().GetInsertBinlogSize())
}
