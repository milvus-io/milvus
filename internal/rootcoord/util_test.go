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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
