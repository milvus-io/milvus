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

package datacoord

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func genNodeChannelInfos(id int64, num int) *NodeChannelInfo {
	channels := make([]*channel, 0, num)
	for i := 0; i < num; i++ {
		name := fmt.Sprintf("ch%d", i)
		channels = append(channels, &channel{Name: name, CollectionID: 1})
	}
	return &NodeChannelInfo{
		NodeID:   id,
		Channels: channels,
	}
}

func genChannelOperations(from, to int64, num int) ChannelOpSet {
	ops := make([]*ChannelOp, 0, 2)
	channels := make([]*channel, 0, num)
	channelWatchInfos := make([]*datapb.ChannelWatchInfo, 0, num)
	for i := 0; i < num; i++ {
		name := fmt.Sprintf("ch%d", i)
		channels = append(channels, &channel{Name: name, CollectionID: 1})
		channelWatchInfos = append(channelWatchInfos, &datapb.ChannelWatchInfo{})
	}

	ops = append(ops, &ChannelOp{
		Type:     Delete,
		NodeID:   from,
		Channels: channels,
	})

	ops = append(ops, &ChannelOp{
		Type:              Add,
		NodeID:            to,
		Channels:          channels,
		ChannelWatchInfos: channelWatchInfos,
	})

	return ops
}

func TestChannelStore_Update(t *testing.T) {
	txnKv := mocks.NewTxnKV(t)
	txnKv.On("MultiSaveAndRemove",
		mock.Anything,
		mock.Anything,
	).Run(func(args mock.Arguments) {
		saves := args.Get(0).(map[string]string)
		removals := args.Get(1).([]string)
		assert.False(t, len(saves)+len(removals) > 128, "too many operations")
	}).Return(nil)

	type fields struct {
		store        kv.TxnKV
		channelsInfo map[int64]*NodeChannelInfo
	}
	type args struct {
		opSet ChannelOpSet
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"test more than 128 operations",
			fields{
				txnKv,
				map[int64]*NodeChannelInfo{
					1: genNodeChannelInfos(1, 500),
					2: {NodeID: 2},
				},
			},
			args{
				genChannelOperations(1, 2, 250),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ChannelStore{
				store:        tt.fields.store,
				channelsInfo: tt.fields.channelsInfo,
			}
			err := c.Update(tt.args.opSet)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}
