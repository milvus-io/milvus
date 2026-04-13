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
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func Test_EncodeMsgPositions(t *testing.T) {
	mp := &msgstream.MsgPosition{
		ChannelName: "test",
		MsgID:       []byte{1, 2, 3},
	}

	str, err := EncodeMsgPositions([]*msgstream.MsgPosition{})
	assert.Empty(t, str)
	assert.NoError(t, err)

	mps := []*msgstream.MsgPosition{mp}
	str, err = EncodeMsgPositions(mps)
	assert.NotEmpty(t, str)
	assert.NoError(t, err)
}

func Test_DecodeMsgPositions(t *testing.T) {
	mp := &msgstream.MsgPosition{
		ChannelName: "test",
		MsgID:       []byte{1, 2, 3},
	}

	str, err := EncodeMsgPositions([]*msgstream.MsgPosition{mp})
	assert.NoError(t, err)

	mpOut := make([]*msgstream.MsgPosition, 1)
	err = DecodeMsgPositions(str, &mpOut)
	assert.NoError(t, err)

	err = DecodeMsgPositions("", &mpOut)
	assert.NoError(t, err)

	err = DecodeMsgPositions("null", &mpOut)
	assert.NoError(t, err)
}

func Test_getTravelTs(t *testing.T) {
	type args struct {
		req TimeTravelRequest
	}
	tests := []struct {
		name string
		args args
		want Timestamp
	}{
		{args: args{req: &milvuspb.HasCollectionRequest{}}, want: typeutil.MaxTimestamp},
		{args: args{req: &milvuspb.DescribeCollectionRequest{TimeStamp: 100}}, want: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getTravelTs(tt.args.req), "getTravelTs(%v)", tt.args.req)
		})
	}
}

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

func Test_getCollectionRateLimitConfig(t *testing.T) {
	type args struct {
		properties map[string]string
		configKey  string
	}

	configMap := map[string]string{
		common.CollectionInsertRateMaxKey:   "5",
		common.CollectionInsertRateMinKey:   "5",
		common.CollectionDeleteRateMaxKey:   "5",
		common.CollectionDeleteRateMinKey:   "5",
		common.CollectionBulkLoadRateMaxKey: "5",
		common.CollectionBulkLoadRateMinKey: "5",
		common.CollectionQueryRateMaxKey:    "5",
		common.CollectionQueryRateMinKey:    "5",
		common.CollectionSearchRateMaxKey:   "5",
		common.CollectionSearchRateMinKey:   "5",
		common.CollectionDiskQuotaKey:       "5",
	}

	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			name: "test CollectionInsertRateMaxKey",
			args: args{
				properties: configMap,
				configKey:  common.CollectionInsertRateMaxKey,
			},
			want: float64(5 * 1024 * 1024),
		},
		{
			name: "test CollectionInsertRateMinKey",
			args: args{
				properties: configMap,
				configKey:  common.CollectionInsertRateMinKey,
			},
			want: float64(5 * 1024 * 1024),
		},
		{
			name: "test CollectionDeleteRateMaxKey",
			args: args{
				properties: configMap,
				configKey:  common.CollectionDeleteRateMaxKey,
			},
			want: float64(5 * 1024 * 1024),
		},

		{
			name: "test CollectionDeleteRateMinKey",
			args: args{
				properties: configMap,
				configKey:  common.CollectionDeleteRateMinKey,
			},
			want: float64(5 * 1024 * 1024),
		},
		{
			name: "test CollectionBulkLoadRateMaxKey",
			args: args{
				properties: configMap,
				configKey:  common.CollectionBulkLoadRateMaxKey,
			},
			want: float64(5 * 1024 * 1024),
		},

		{
			name: "test CollectionBulkLoadRateMinKey",
			args: args{
				properties: configMap,
				configKey:  common.CollectionBulkLoadRateMinKey,
			},
			want: float64(5 * 1024 * 1024),
		},

		{
			name: "test CollectionQueryRateMaxKey",
			args: args{
				properties: configMap,
				configKey:  common.CollectionQueryRateMaxKey,
			},
			want: float64(5),
		},

		{
			name: "test CollectionQueryRateMinKey",
			args: args{
				properties: configMap,
				configKey:  common.CollectionQueryRateMinKey,
			},
			want: float64(5),
		},

		{
			name: "test CollectionSearchRateMaxKey",
			args: args{
				properties: configMap,
				configKey:  common.CollectionSearchRateMaxKey,
			},
			want: float64(5),
		},

		{
			name: "test CollectionSearchRateMinKey",
			args: args{
				properties: configMap,
				configKey:  common.CollectionSearchRateMinKey,
			},
			want: float64(5),
		},

		{
			name: "test CollectionDiskQuotaKey",
			args: args{
				properties: configMap,
				configKey:  common.CollectionDiskQuotaKey,
			},
			want: float64(5 * 1024 * 1024),
		},

		{
			name: "test invalid config value",
			args: args{
				properties: map[string]string{common.CollectionDiskQuotaKey: "invalid value"},
				configKey:  common.CollectionDiskQuotaKey,
			},
			want: Params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat(),
		},
		{
			name: "test empty config item",
			args: args{
				properties: map[string]string{},
				configKey:  common.CollectionDiskQuotaKey,
			},
			want: Params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat(),
		},

		{
			name: "test unknown config item",
			args: args{
				properties: configMap,
				configKey:  "",
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getCollectionRateLimitConfig(tt.args.properties, tt.args.configKey)

			if got != tt.want {
				t.Errorf("getCollectionRateLimitConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetRateLimitConfigErr(t *testing.T) {
	key := common.CollectionQueryRateMaxKey
	t.Run("negative value", func(t *testing.T) {
		v := getRateLimitConfig(map[string]string{
			key: "-1",
		}, key, 1)
		assert.EqualValues(t, 1, v)
	})

	t.Run("valid value", func(t *testing.T) {
		v := getRateLimitConfig(map[string]string{
			key: "1",
		}, key, 100)
		assert.EqualValues(t, 1, v)
	})

	t.Run("not exist value", func(t *testing.T) {
		v := getRateLimitConfig(map[string]string{
			key: "1",
		}, "b", 100)
		assert.EqualValues(t, 100, v)
	})
}

func TestIsSubsetOfProperties(t *testing.T) {
	type args struct {
		src    []*commonpb.KeyValuePair
		target []*commonpb.KeyValuePair
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "empty src and empty target",
			args: args{
				src:    []*commonpb.KeyValuePair{},
				target: []*commonpb.KeyValuePair{},
			},
			want: true,
		},
		{
			name: "empty src with non-empty target",
			args: args{
				src: []*commonpb.KeyValuePair{},
				target: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
				},
			},
			want: true,
		},
		{
			name: "non-empty src with empty target",
			args: args{
				src: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
				},
				target: []*commonpb.KeyValuePair{},
			},
			want: false,
		},
		{
			name: "src is subset of target - single pair",
			args: args{
				src: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
				},
				target: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key2", Value: "value2"},
				},
			},
			want: true,
		},
		{
			name: "src is subset of target - multiple pairs",
			args: args{
				src: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key3", Value: "value3"},
				},
				target: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key2", Value: "value2"},
					{Key: "key3", Value: "value3"},
				},
			},
			want: true,
		},
		{
			name: "src equals target",
			args: args{
				src: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key2", Value: "value2"},
				},
				target: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key2", Value: "value2"},
				},
			},
			want: true,
		},
		{
			name: "src key not in target",
			args: args{
				src: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key_missing", Value: "value_missing"},
				},
				target: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key2", Value: "value2"},
				},
			},
			want: false,
		},
		{
			name: "src key exists but value differs",
			args: args{
				src: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key2", Value: "different_value"},
				},
				target: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key2", Value: "value2"},
				},
			},
			want: false,
		},
		{
			name: "duplicate keys in src - all match target",
			args: args{
				src: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key1", Value: "value1"},
				},
				target: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key2", Value: "value2"},
				},
			},
			want: true,
		},
		{
			name: "duplicate keys in target - src subset",
			args: args{
				src: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
				},
				target: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
					{Key: "key1", Value: "value1"},
					{Key: "key2", Value: "value2"},
				},
			},
			want: true,
		},
		{
			name: "empty string values",
			args: args{
				src: []*commonpb.KeyValuePair{
					{Key: "key1", Value: ""},
				},
				target: []*commonpb.KeyValuePair{
					{Key: "key1", Value: ""},
					{Key: "key2", Value: "value2"},
				},
			},
			want: true,
		},
		{
			name: "empty string value mismatch",
			args: args{
				src: []*commonpb.KeyValuePair{
					{Key: "key1", Value: ""},
				},
				target: []*commonpb.KeyValuePair{
					{Key: "key1", Value: "value1"},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsSubsetOfProperties(tt.args.src, tt.args.target)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_nextFieldID(t *testing.T) {
	type args struct {
		coll *model.Collection
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "collection with max field ID in struct array sub-field",
			args: args{
				coll: &model.Collection{
					Fields: []*model.Field{
						{FieldID: common.StartOfUserFieldID},
					},
					StructArrayFields: []*model.StructArrayField{
						{
							FieldID: common.StartOfUserFieldID + 1,
							Fields: []*model.Field{
								{FieldID: common.StartOfUserFieldID + 2},
								{FieldID: common.StartOfUserFieldID + 10},
							},
						},
					},
				},
			},
			want: common.StartOfUserFieldID + 11,
		},
		{
			name: "collection with multiple struct array fields",
			args: args{
				coll: &model.Collection{
					Fields: []*model.Field{
						{FieldID: common.StartOfUserFieldID},
					},
					StructArrayFields: []*model.StructArrayField{
						{
							FieldID: common.StartOfUserFieldID + 1,
							Fields: []*model.Field{
								{FieldID: common.StartOfUserFieldID + 2},
							},
						},
						{
							FieldID: common.StartOfUserFieldID + 5,
							Fields: []*model.Field{
								{FieldID: common.StartOfUserFieldID + 6},
								{FieldID: common.StartOfUserFieldID + 7},
							},
						},
					},
				},
			},
			want: common.StartOfUserFieldID + 8,
		},
		{
			name: "collection with max_field_id property after field drop",
			args: args{
				coll: &model.Collection{
					Fields: []*model.Field{
						{FieldID: common.StartOfUserFieldID},     // 100
						{FieldID: common.StartOfUserFieldID + 1}, // 101
					},
					Properties: []*commonpb.KeyValuePair{
						{Key: common.MaxFieldIDKey, Value: "102"},
					},
				},
			},
			want: common.StartOfUserFieldID + 3, // 103, not 102
		},
		{
			name: "max_field_id property smaller than current fields",
			args: args{
				coll: &model.Collection{
					Fields: []*model.Field{
						{FieldID: common.StartOfUserFieldID},     // 100
						{FieldID: common.StartOfUserFieldID + 5}, // 105
					},
					Properties: []*commonpb.KeyValuePair{
						{Key: common.MaxFieldIDKey, Value: "102"},
					},
				},
			},
			want: common.StartOfUserFieldID + 6, // 106, current fields dominate
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nextFieldID(tt.args.coll)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_nextFunctionID(t *testing.T) {
	type args struct {
		coll *model.Collection
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "empty functions list returns StartOfUserFunctionID+1",
			args: args{
				coll: &model.Collection{
					Functions: nil,
				},
			},
			want: common.StartOfUserFunctionID + 1,
		},
		{
			name: "single function returns its ID+1",
			args: args{
				coll: &model.Collection{
					Functions: []*model.Function{
						{ID: common.StartOfUserFunctionID + 5},
					},
				},
			},
			want: common.StartOfUserFunctionID + 6,
		},
		{
			name: "multiple functions returns max ID+1",
			args: args{
				coll: &model.Collection{
					Functions: []*model.Function{
						{ID: common.StartOfUserFunctionID + 3},
						{ID: common.StartOfUserFunctionID + 10},
						{ID: common.StartOfUserFunctionID + 7},
					},
				},
			},
			want: common.StartOfUserFunctionID + 11,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nextFunctionID(tt.args.coll)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_updateMaxFieldIDProperty(t *testing.T) {
	t.Run("add to empty properties", func(t *testing.T) {
		props := updateMaxFieldIDProperty(nil, 105)
		assert.Len(t, props, 1)
		assert.Equal(t, common.MaxFieldIDKey, props[0].Key)
		assert.Equal(t, "105", props[0].Value)
	})

	t.Run("update existing property", func(t *testing.T) {
		props := []*commonpb.KeyValuePair{
			{Key: "other_key", Value: "other_value"},
			{Key: common.MaxFieldIDKey, Value: "100"},
		}
		result := updateMaxFieldIDProperty(props, 105)
		assert.Len(t, result, 2)
		assert.Equal(t, "105", result[1].Value)
	})

	t.Run("update existing property does not mutate original", func(t *testing.T) {
		original := &commonpb.KeyValuePair{Key: common.MaxFieldIDKey, Value: "100"}
		props := []*commonpb.KeyValuePair{
			{Key: "other_key", Value: "other_value"},
			original,
		}
		result := updateMaxFieldIDProperty(props, 105)
		assert.Len(t, result, 2)
		assert.Equal(t, "105", result[1].Value)
		// verify original is NOT modified
		assert.Equal(t, "100", original.Value)
	})

	t.Run("append to non-empty properties", func(t *testing.T) {
		props := []*commonpb.KeyValuePair{
			{Key: "other_key", Value: "other_value"},
		}
		result := updateMaxFieldIDProperty(props, 103)
		assert.Len(t, result, 2)
		assert.Equal(t, common.MaxFieldIDKey, result[1].Key)
		assert.Equal(t, "103", result[1].Value)
	})
}
