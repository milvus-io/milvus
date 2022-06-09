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
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
	"stathat.com/c/consistent"
)

func fillEmptyPosition(operations ChannelOpSet) {
	for _, op := range operations {
		if op.Type == Add {
			for range op.Channels {
				op.ChannelWatchInfos = append(op.ChannelWatchInfos, nil)
			}
		}
	}
}

func TestBufferChannelAssignPolicy(t *testing.T) {
	kv := memkv.NewMemoryKV()

	channels := []*channel{{"chan1", 1}}
	store := &ChannelStore{
		meta:         newMockMeta(kv),
		channelsInfo: map[int64]*NodeChannelInfo{bufferID: {bufferID, channels}},
	}

	updates := BufferChannelAssignPolicy(store, 1)
	assert.NotNil(t, updates)
	assert.Equal(t, 2, len(updates))
	assert.EqualValues(t, &ChannelOp{Type: Delete, NodeID: bufferID, Channels: channels}, updates[0])
	assert.EqualValues(t, 1, updates[1].NodeID)
	assert.Equal(t, Add, updates[1].Type)
	assert.Equal(t, channels, updates[1].Channels)
}

func TestConsistentHashRegisterPolicy(t *testing.T) {
	t.Run("first register", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		channels := []*channel{
			{"chan1", 1},
			{"chan2", 2},
		}

		store := &ChannelStore{
			meta:         newMockMeta(kv),
			channelsInfo: map[int64]*NodeChannelInfo{bufferID: {bufferID, channels}},
		}

		hashring := consistent.New()
		policy := ConsistentHashRegisterPolicy(hashring)

		updates := policy(store, 1)
		assert.NotNil(t, updates)
		assert.Equal(t, 2, len(updates))
		assert.EqualValues(t, &ChannelOp{Type: Delete, NodeID: bufferID, Channels: channels}, updates[0])
		assert.EqualValues(t, &ChannelOp{Type: Add, NodeID: 1, Channels: channels}, updates[1])
	})

	t.Run("rebalance after register", func(t *testing.T) {
		kv := memkv.NewMemoryKV()

		channels := []*channel{
			{"chan1", 1},
			{"chan2", 2},
		}

		store := &ChannelStore{
			meta:         newMockMeta(kv),
			channelsInfo: map[int64]*NodeChannelInfo{1: {1, channels}, 2: {2, []*channel{}}},
		}

		hashring := consistent.New()
		hashring.Add(formatNodeID(1))
		policy := ConsistentHashRegisterPolicy(hashring)

		updates := policy(store, 2)

		assert.NotNil(t, updates)
		assert.Equal(t, 1, len(updates))
		// No Delete operation will be generated
		assert.EqualValues(t, &ChannelOp{Type: Add, NodeID: 1, Channels: []*channel{channels[0]}}, updates[0])
	})
}

func TestAverageAssignPolicy(t *testing.T) {
	type args struct {
		store    ROChannelStore
		channels []*channel
	}
	tests := []struct {
		name string
		args args
		want ChannelOpSet
	}{
		{
			"test assign empty cluster",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{},
				},
				[]*channel{{"chan1", 1}},
			},
			[]*ChannelOp{{Add, bufferID, []*channel{{"chan1", 1}}, nil}},
		},
		{
			"test watch same channel",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"chan1", 1}}},
					},
				},
				[]*channel{{"chan1", 1}},
			},
			nil,
		},
		{
			"test normal assign",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"chan1", 1}, {"chan2", 1}}},
						2: {2, []*channel{{"chan3", 1}}},
					},
				},
				[]*channel{{"chan4", 1}},
			},
			[]*ChannelOp{{Add, 2, []*channel{{"chan4", 1}}, nil}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AverageAssignPolicy(tt.args.store, tt.args.channels)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func TestConsistentHashChannelAssignPolicy(t *testing.T) {
	type args struct {
		hashring *consistent.Consistent
		store    ROChannelStore
		channels []*channel
	}
	tests := []struct {
		name string
		args args
		want ChannelOpSet
	}{
		{
			"test assign empty cluster",
			args{
				consistent.New(),
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{},
				},
				[]*channel{{"chan1", 1}},
			},
			[]*ChannelOp{{Add, bufferID, []*channel{{"chan1", 1}}, nil}},
		},
		{
			"test watch same channel",
			args{
				consistent.New(),
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"chan1", 1}, {"chan2", 1}}},
					},
				},
				[]*channel{{"chan1", 1}},
			},
			nil,
		},
		{
			"test normal watch",
			args{
				consistent.New(),
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{1: {1, nil}, 2: {2, nil}, 3: {3, nil}},
				},
				[]*channel{{"chan1", 1}, {"chan2", 1}, {"chan3", 1}},
			},
			[]*ChannelOp{{Add, 2, []*channel{{"chan1", 1}}, nil}, {Add, 1, []*channel{{"chan2", 1}}, nil}, {Add, 3, []*channel{{"chan3", 1}}, nil}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := ConsistentHashChannelAssignPolicy(tt.args.hashring)
			got := policy(tt.args.store, tt.args.channels)
			assert.Equal(t, len(tt.want), len(got))
			for _, op := range tt.want {
				assert.Contains(t, got, op)
			}
		})
	}
}

func TestAvgAssignUnregisteredChannels(t *testing.T) {
	type args struct {
		store  ROChannelStore
		nodeID int64
	}
	tests := []struct {
		name string
		args args
		want ChannelOpSet
	}{
		{
			"test deregister the last node",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"chan1", 1}}},
					},
				},
				1,
			},
			[]*ChannelOp{{Delete, 1, []*channel{{"chan1", 1}}, nil}, {Add, bufferID, []*channel{{"chan1", 1}}, nil}},
		},
		{
			"test rebalance channels after deregister",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"chan1", 1}}},
						2: {2, []*channel{{"chan2", 1}}},
						3: {3, []*channel{}},
					},
				},
				2,
			},
			[]*ChannelOp{{Delete, 2, []*channel{{"chan2", 1}}, nil}, {Add, 3, []*channel{{"chan2", 1}}, nil}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AvgAssignUnregisteredChannels(tt.args.store, tt.args.nodeID)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func TestConsistentHashDeregisterPolicy(t *testing.T) {
	type args struct {
		hashring *consistent.Consistent
		store    ROChannelStore
		nodeID   int64
	}
	tests := []struct {
		name string
		args args
		want ChannelOpSet
	}{
		{
			"test deregister the last node",
			args{
				consistent.New(),
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"chan1", 1}}},
					},
				},
				1,
			},
			[]*ChannelOp{{Delete, 1, []*channel{{"chan1", 1}}, nil}, {Add, bufferID, []*channel{{"chan1", 1}}, nil}},
		},
		{
			"rebalance after deregister",
			args{
				consistent.New(),
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"chan2", 1}}},
						2: {2, []*channel{{"chan1", 1}}},
						3: {3, []*channel{{"chan3", 1}}},
					},
				},
				2,
			},
			[]*ChannelOp{{Delete, 2, []*channel{{"chan1", 1}}, nil}, {Add, 1, []*channel{{"chan1", 1}}, nil}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := ConsistentHashDeregisterPolicy(tt.args.hashring)
			got := policy(tt.args.store, tt.args.nodeID)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func TestAverageReassignPolicy(t *testing.T) {
	type args struct {
		store     ROChannelStore
		reassigns []*NodeChannelInfo
	}
	tests := []struct {
		name string
		args args
		want ChannelOpSet
	}{
		{
			"test only one node",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"chan1", 1}}},
					},
				},
				[]*NodeChannelInfo{{1, []*channel{{"chan1", 1}}}},
			},
			nil,
		},
		{
			"test normal reassing",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"chan1", 1}, {"chan2", 1}}},
						2: {2, []*channel{}},
					},
				},
				[]*NodeChannelInfo{{1, []*channel{{"chan1", 1}, {"chan2", 1}}}},
			},
			[]*ChannelOp{{Delete, 1, []*channel{{"chan1", 1}, {"chan2", 1}}, nil}, {Add, 2, []*channel{{"chan1", 1}, {"chan2", 1}}, nil}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AverageReassignPolicy(tt.args.store, tt.args.reassigns)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func TestBgCheckWithMaxWatchDuration(t *testing.T) {
	type watch struct {
		nodeID int64
		name   string
		info   *datapb.ChannelWatchInfo
	}
	getKv := func(watchInfos []*watch) kv.TxnKV {
		kv := memkv.NewMemoryKV()
		for _, info := range watchInfos {
			k := buildNodeChannelKey(info.nodeID, info.name)
			v, _ := proto.Marshal(info.info)
			kv.Save(k, string(v))
		}
		return kv
	}

	type args struct {
		kv        kv.TxnKV
		channels  []*NodeChannelInfo
		timestamp time.Time
	}

	ts := time.Now()
	tests := []struct {
		name    string
		args    args
		want    []*NodeChannelInfo
		wantErr error
	}{
		{
			"test normal expiration",
			args{
				getKv([]*watch{{1, "chan1", &datapb.ChannelWatchInfo{StartTs: ts.Unix(), State: datapb.ChannelWatchState_Uncomplete}},
					{1, "chan2", &datapb.ChannelWatchInfo{StartTs: ts.Unix(), State: datapb.ChannelWatchState_Complete}}}),
				[]*NodeChannelInfo{{1, []*channel{{"chan1", 1}, {"chan2", 1}}}},
				ts.Add(maxWatchDuration),
			},
			[]*NodeChannelInfo{{1, []*channel{{"chan1", 1}}}},
			nil,
		},
		{
			"test no expiration",
			args{
				getKv([]*watch{{1, "chan1", &datapb.ChannelWatchInfo{StartTs: ts.Unix(), State: datapb.ChannelWatchState_Uncomplete}}}),
				[]*NodeChannelInfo{{1, []*channel{{"chan1", 1}}}},
				ts.Add(maxWatchDuration).Add(-time.Second),
			},
			[]*NodeChannelInfo{},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := BgCheckWithMaxWatchDuration(tt.args.kv)
			got, err := policy(tt.args.channels, tt.args.timestamp)
			assert.Equal(t, tt.wantErr, err)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func TestAvgAssignRegisterPolicy(t *testing.T) {
	type args struct {
		store  ROChannelStore
		nodeID int64
	}
	tests := []struct {
		name string
		args args
		want ChannelOpSet
	}{
		{
			"test empty",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{},
				},
				1,
			},
			nil,
		},
		{
			"test with buffer channel",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						bufferID: {bufferID, []*channel{{"ch1", 1}}},
					},
				},
				1,
			},
			[]*ChannelOp{
				{
					Type:     Delete,
					NodeID:   bufferID,
					Channels: []*channel{{"ch1", 1}},
				},
				{
					Type:     Add,
					NodeID:   1,
					Channels: []*channel{{"ch1", 1}},
				},
			},
		},
		{
			"test with avg assign",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"ch1", 1}, {"ch2", 1}}},
					},
				},
				3,
			},
			[]*ChannelOp{
				{
					Type:     Add,
					NodeID:   1,
					Channels: []*channel{{"ch1", 1}},
				},
			},
		},
		{
			"test with avg equals to zero",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"ch1", 1}}},
						2: {2, []*channel{{"ch3", 1}}},
					},
				},
				3,
			},
			nil,
		},
		{
			"test node with empty channel",
			args{
				&ChannelStore{
					newMockMeta(memkv.NewMemoryKV()),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{"ch1", 1}, {"ch2", 1}, {"ch3", 1}}},
						2: {2, []*channel{}},
					},
				},
				3,
			},
			[]*ChannelOp{
				{
					Type:     Add,
					NodeID:   1,
					Channels: []*channel{{"ch1", 1}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AvgAssignRegisterPolicy(tt.args.store, tt.args.nodeID)
			assert.EqualValues(t, tt.want, got)
		})
	}
}
