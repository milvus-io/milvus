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

package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// RecordedFieldSize answers 0 for a field meta knows nothing about, and only
// GetFieldBinlogSize turns that into the whole-segment fallback.
func TestSegmentInfo_RecordedFieldSize(t *testing.T) {
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 4096}}},
		},
		Stats: &datapb.Statistics{
			InsertBinlogSize: 10000,
			LoadResource: &datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
				{GroupId: 0, FieldIds: []int64{100, 102}, MemorySize: 512},
			}},
		},
	}}

	assert.Equal(t, int64(4096), seg.RecordedFieldSize(101))
	assert.Equal(t, int64(4096), seg.GetFieldBinlogSize(101))
	// No FieldBinlog for the field: the LoadResource column group answers.
	assert.Equal(t, int64(512), seg.RecordedFieldSize(102))

	// Unknown field: nothing recorded, the whole segment only through the
	// fallback.
	assert.Equal(t, int64(0), seg.RecordedFieldSize(999))
	assert.Equal(t, seg.GetSegmentSize(), seg.GetFieldBinlogSize(999))
}
