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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func Test_EqualKeyPairArray(t *testing.T) {
	p1 := []*commonpb.KeyValuePair{
		{
			Key:   "k1",
			Value: "v1",
		},
	}

	p2 := []*commonpb.KeyValuePair{}
	assert.False(t, EqualKeyPairArray(p1, p2))

	p2 = append(p2, &commonpb.KeyValuePair{
		Key:   "k2",
		Value: "v2",
	})
	assert.False(t, EqualKeyPairArray(p1, p2))
	p2 = []*commonpb.KeyValuePair{
		{
			Key:   "k1",
			Value: "v2",
		},
	}
	assert.False(t, EqualKeyPairArray(p1, p2))

	p2 = []*commonpb.KeyValuePair{
		{
			Key:   "k1",
			Value: "v1",
		},
	}
	assert.True(t, EqualKeyPairArray(p1, p2))
}

func Test_GetFieldSchemaByID(t *testing.T) {
	coll := &model.Collection{
		Fields: []*model.Field{
			{
				FieldID: 1,
			},
		},
	}
	_, err := GetFieldSchemaByID(coll, 1)
	assert.NoError(t, err)
	_, err = GetFieldSchemaByID(coll, 2)
	assert.Error(t, err)
}

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
