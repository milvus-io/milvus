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

package storagecommon

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func TestSplitByFieldSize(t *testing.T) {
	tests := []struct {
		name           string
		fieldBinlogs   []*datapb.FieldBinlog
		splitThresHold int64
		expected       []ColumnGroup
	}{
		{
			name:           "Empty input",
			fieldBinlogs:   []*datapb.FieldBinlog{},
			splitThresHold: 100,
			expected:       []ColumnGroup{},
		},
		{
			name: "above threshold",
			fieldBinlogs: []*datapb.FieldBinlog{
				{
					FieldID: 0,
					Binlogs: []*datapb.Binlog{
						{LogSize: 1000, EntriesNum: 10},
					},
				},
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{LogSize: 2000, EntriesNum: 10},
					},
				},
			},
			splitThresHold: 50,
			expected: []ColumnGroup{
				{Columns: []int{0}},
				{Columns: []int{1}},
			},
		},
		{
			name: "one field",
			fieldBinlogs: []*datapb.FieldBinlog{
				{
					FieldID: 0,
					Binlogs: []*datapb.Binlog{
						{LogSize: 100, EntriesNum: 10},
					},
				},
			},
			splitThresHold: 50,
			expected: []ColumnGroup{
				{Columns: []int{0}},
			},
		},
		{
			name: "Multiple fields, mixed sizes",
			fieldBinlogs: []*datapb.FieldBinlog{
				{
					FieldID: 0,
					Binlogs: []*datapb.Binlog{ // (above)
						{LogSize: 500, EntriesNum: 5},
						{LogSize: 500, EntriesNum: 5},
					},
				},
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{LogSize: 200, EntriesNum: 20}, // (below)
					},
				},
				{
					FieldID: 2,
					Binlogs: []*datapb.Binlog{
						{LogSize: 500, EntriesNum: 10}, // (threshold)
					},
				},
				{
					FieldID: 3,
					Binlogs: []*datapb.Binlog{
						{LogSize: 400, EntriesNum: 10}, // (below)
					},
				},
			},
			splitThresHold: 50,
			expected: []ColumnGroup{
				{Columns: []int{0}},
				{Columns: []int{2}},
				{Columns: []int{1, 3}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SplitByFieldSize(tt.fieldBinlogs, tt.splitThresHold)
			assert.Equal(t, tt.expected, result)
		})
	}
}
