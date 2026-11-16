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
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestPruneNullCounts(t *testing.T) {
	mixed := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_FloatVector},
		{FieldID: 102, DataType: schemapb.DataType_VarChar, Nullable: true},
	}}
	allNullable := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, DataType: schemapb.DataType_FloatVector, Nullable: true},
		{FieldID: 102, DataType: schemapb.DataType_VarChar, Nullable: true},
	}}
	withStruct := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 102, DataType: schemapb.DataType_VarChar, Nullable: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 1000,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1001, DataType: schemapb.DataType_ArrayOfVector},
			},
		}},
	}
	withVectorArray := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 104, DataType: schemapb.DataType_ArrayOfVector},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 1000,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1001, DataType: schemapb.DataType_ArrayOfVector},
				{FieldID: 1002, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
			},
		}},
	}
	withFunctionOutput := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 102, DataType: schemapb.DataType_VarChar, Nullable: true},
		{FieldID: 103, DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
	}}

	tests := []struct {
		name     string
		stats    *datapb.Statistics
		schema   *schemapb.CollectionSchema
		want     map[int64]int64
		samePtr  bool
		wantNil  bool
		inputLen int
	}{
		{
			name:     "drops not-nullable keys only",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{100: 0, 101: 0, 102: 3}, DeleteNumRows: 7},
			schema:   mixed,
			want:     map[int64]int64{102: 3},
			inputLen: 3,
		},
		{
			name:     "all nullable keeps the same object",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{101: 0, 102: 3}},
			schema:   allNullable,
			want:     map[int64]int64{101: 0, 102: 3},
			samePtr:  true,
			inputLen: 2,
		},
		{
			name:     "all not-nullable empties the map to nil",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{100: 0, 101: 0}},
			schema:   mixed,
			want:     nil,
			inputLen: 2,
		},
		{
			name:     "not-nullable VectorArray struct sub-field is kept",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{102: 1, 1001: 0}},
			schema:   withStruct,
			want:     map[int64]int64{102: 1, 1001: 0},
			samePtr:  true,
			inputLen: 2,
		},
		{
			name:     "not-nullable VectorArray keys are kept, other not-nullable keys dropped",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{100: 0, 104: 0, 1001: 0, 1002: 0}},
			schema:   withVectorArray,
			want:     map[int64]int64{104: 0, 1001: 0},
			inputLen: 4,
		},
		{
			name:     "only not-nullable VectorArray keys keeps the same object",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{104: 0, 1001: 0}},
			schema:   withVectorArray,
			want:     map[int64]int64{104: 0, 1001: 0},
			samePtr:  true,
			inputLen: 2,
		},
		{
			name:     "key not in schema is kept",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{102: 1, 999: 0}},
			schema:   mixed,
			want:     map[int64]int64{102: 1, 999: 0},
			samePtr:  true,
			inputLen: 2,
		},
		{
			name:     "function output field is dropped",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{102: 0, 103: 0}},
			schema:   withFunctionOutput,
			want:     map[int64]int64{102: 0},
			inputLen: 2,
		},
		{
			name:    "nil stats",
			stats:   nil,
			schema:  mixed,
			wantNil: true,
		},
		{
			name:     "nil schema keeps the same object",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{100: 0, 102: 3}},
			schema:   nil,
			want:     map[int64]int64{100: 0, 102: 3},
			samePtr:  true,
			inputLen: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := pruneNullCounts(test.stats, prunableFields(test.schema))
			if test.wantNil {
				assert.Nil(t, got)
				return
			}
			if test.samePtr {
				assert.Same(t, test.stats, got)
			} else {
				assert.NotSame(t, test.stats, got)
				assert.Equal(t, test.stats.GetDeleteNumRows(), got.GetDeleteNumRows(),
					"the clone keeps every other field")
			}
			if test.want == nil {
				assert.Nil(t, got.NullCounts)
			} else {
				assert.Equal(t, test.want, got.GetNullCounts())
			}
			assert.Len(t, test.stats.GetNullCounts(), test.inputLen, "the input map must not be modified")
		})
	}
}

func TestKeptNullCounts(t *testing.T) {
	prunable := map[int64]struct{}{100: {}, 101: {}}
	tests := []struct {
		name        string
		in          map[int64]int64
		want        map[int64]int64
		wantChanged bool
	}{
		{"some kept", map[int64]int64{100: 0, 101: 0, 102: 3}, map[int64]int64{102: 3}, true},
		{"all dropped", map[int64]int64{100: 0, 101: 0}, nil, true},
		{"nothing dropped", map[int64]int64{102: 3, 999: 0}, map[int64]int64{102: 3, 999: 0}, false},
		{"nil map", nil, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := maps.Clone(tt.in)
			got, changed := keptNullCounts(tt.in, prunable)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantChanged, changed)
			assert.Equal(t, before, tt.in, "the input map is never written")
		})
	}
}
