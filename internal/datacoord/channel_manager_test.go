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
	"testing"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/stretchr/testify/assert"
	"stathat.com/c/consistent"
)

func TestReload(t *testing.T) {
	t.Run("test reload with data", func(t *testing.T) {
		Params.Init()
		kv := memkv.NewMemoryKV()
		hash := consistent.New()
		cm, err := NewChannelManager(kv, newMockHandler(), withFactory(NewConsistentHashChannelPolicyFactory(hash)))
		assert.Nil(t, err)
		assert.Nil(t, cm.AddNode(1))
		assert.Nil(t, cm.AddNode(2))
		assert.Nil(t, cm.Watch(&channel{"channel1", 1}))
		assert.Nil(t, cm.Watch(&channel{"channel2", 1}))

		hash2 := consistent.New()
		cm2, err := NewChannelManager(kv, newMockHandler(), withFactory(NewConsistentHashChannelPolicyFactory(hash2)))
		assert.Nil(t, err)
		assert.Nil(t, cm2.Startup([]int64{1, 2}))
		assert.Nil(t, cm2.AddNode(3))
		assert.True(t, cm2.Match(3, "channel1"))
		assert.True(t, cm2.Match(3, "channel2"))
	})
}

func TestChannelManager_RemoveChannel(t *testing.T) {
	type fields struct {
		store RWChannelStore
	}
	type args struct {
		channelName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"test remove existed channel",
			fields{
				store: &ChannelStore{
					store: memkv.NewMemoryKV(),
					channelsInfo: map[int64]*NodeChannelInfo{
						1: {
							NodeID: 1,
							Channels: []*channel{
								{"ch1", 1},
							},
						},
					},
				},
			},
			args{
				"ch1",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ChannelManager{
				store: tt.fields.store,
			}
			err := c.RemoveChannel(tt.args.channelName)
			assert.Equal(t, tt.wantErr, err != nil)
			_, ch := c.findChannel(tt.args.channelName)
			assert.Nil(t, ch)
		})
	}
}
