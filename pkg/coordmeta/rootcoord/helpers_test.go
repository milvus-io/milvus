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

package rootcoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func Test_isMaxTs(t *testing.T) {
	type args struct {
		ts Timestamp
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{args: args{ts: typeutil.MaxTimestamp}, want: true},
		{args: args{ts: typeutil.ZeroTimestamp}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, isMaxTs(tt.args.ts), "isMaxTs(%v)", tt.args.ts)
		})
	}
}

func Test_maxAssignedFieldIDFromSchema(t *testing.T) {
	type args struct {
		schema *schemapb.CollectionSchema
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "collection with max field ID in struct array sub-field",
			args: args{
				schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: common.StartOfUserFieldID},
					},
					StructArrayFields: []*schemapb.StructArrayFieldSchema{
						{
							FieldID: common.StartOfUserFieldID + 1,
							Fields: []*schemapb.FieldSchema{
								{FieldID: common.StartOfUserFieldID + 2},
								{FieldID: common.StartOfUserFieldID + 10},
							},
						},
					},
				},
			},
			want: common.StartOfUserFieldID + 10,
		},
		{
			name: "collection with multiple struct array fields",
			args: args{
				schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: common.StartOfUserFieldID},
					},
					StructArrayFields: []*schemapb.StructArrayFieldSchema{
						{
							FieldID: common.StartOfUserFieldID + 1,
							Fields: []*schemapb.FieldSchema{
								{FieldID: common.StartOfUserFieldID + 2},
							},
						},
						{
							FieldID: common.StartOfUserFieldID + 5,
							Fields: []*schemapb.FieldSchema{
								{FieldID: common.StartOfUserFieldID + 6},
								{FieldID: common.StartOfUserFieldID + 7},
							},
						},
					},
				},
			},
			want: common.StartOfUserFieldID + 7,
		},
		{
			name: "collection with max_field_id property after field drop",
			args: args{
				schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: common.StartOfUserFieldID},     // 100
						{FieldID: common.StartOfUserFieldID + 1}, // 101
					},
					Properties: []*commonpb.KeyValuePair{
						{Key: common.MaxFieldIDKey, Value: "102"},
					},
				},
			},
			want: common.StartOfUserFieldID + 2, // 102, not 101
		},
		{
			name: "max_field_id property smaller than current fields",
			args: args{
				schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: common.StartOfUserFieldID},     // 100
						{FieldID: common.StartOfUserFieldID + 5}, // 105
					},
					Properties: []*commonpb.KeyValuePair{
						{Key: common.MaxFieldIDKey, Value: "102"},
					},
				},
			},
			want: common.StartOfUserFieldID + 5, // 105, current fields dominate
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MaxAssignedFieldIDFromSchema(tt.args.schema)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_updateMaxFieldIDProperty(t *testing.T) {
	t.Run("add to empty properties", func(t *testing.T) {
		props := UpdateMaxFieldIDProperty(nil, 105)
		assert.Len(t, props, 1)
		assert.Equal(t, common.MaxFieldIDKey, props[0].Key)
		assert.Equal(t, "105", props[0].Value)
	})

	t.Run("update existing property", func(t *testing.T) {
		props := []*commonpb.KeyValuePair{
			{Key: "other_key", Value: "other_value"},
			{Key: common.MaxFieldIDKey, Value: "100"},
		}
		result := UpdateMaxFieldIDProperty(props, 105)
		assert.Len(t, result, 2)
		assert.Equal(t, "105", result[1].Value)
	})

	t.Run("update existing property does not mutate original", func(t *testing.T) {
		original := &commonpb.KeyValuePair{Key: common.MaxFieldIDKey, Value: "100"}
		props := []*commonpb.KeyValuePair{
			{Key: "other_key", Value: "other_value"},
			original,
		}
		result := UpdateMaxFieldIDProperty(props, 105)
		assert.Len(t, result, 2)
		assert.Equal(t, "105", result[1].Value)
		// verify original is NOT modified
		assert.Equal(t, "100", original.Value)
	})

	t.Run("append to non-empty properties", func(t *testing.T) {
		props := []*commonpb.KeyValuePair{
			{Key: "other_key", Value: "other_value"},
		}
		result := UpdateMaxFieldIDProperty(props, 103)
		assert.Len(t, result, 2)
		assert.Equal(t, common.MaxFieldIDKey, result[1].Key)
		assert.Equal(t, "103", result[1].Value)
	})
}
