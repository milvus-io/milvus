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

	"github.com/stretchr/testify/assert"
	"stathat.com/c/consistent"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
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

	channels := []*channel{{Name: "chan1", CollectionID: 1}}
	store := &ChannelStore{
		store:        kv,
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
			{Name: "chan1", CollectionID: 1},
			{Name: "chan2", CollectionID: 2},
		}
		store := &ChannelStore{
			store:        kv,
			channelsInfo: map[int64]*NodeChannelInfo{bufferID: {bufferID, channels}},
		}

		hashring := consistent.New()
		policy := ConsistentHashRegisterPolicy(hashring)

		updates, _ := policy(store, 1)
		assert.NotNil(t, updates)
		assert.Equal(t, 2, len(updates))
		assert.EqualValues(t, &ChannelOp{Type: Delete, NodeID: bufferID, Channels: channels}, updates[0])
		assert.EqualValues(t, &ChannelOp{Type: Add, NodeID: 1, Channels: channels}, updates[1])
	})

	t.Run("rebalance after register", func(t *testing.T) {
		kv := memkv.NewMemoryKV()

		channels := []*channel{
			{Name: "chan1", CollectionID: 1},
			{Name: "chan2", CollectionID: 2},
		}

		store := &ChannelStore{
			store:        kv,
			channelsInfo: map[int64]*NodeChannelInfo{1: {1, channels}, 2: {2, []*channel{}}},
		}

		hashring := consistent.New()
		hashring.Add(formatNodeID(1))
		policy := ConsistentHashRegisterPolicy(hashring)

		_, updates := policy(store, 2)

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
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{},
				},
				[]*channel{{Name: "chan1", CollectionID: 1}},
			},
			[]*ChannelOp{{Add, bufferID, []*channel{{Name: "chan1", CollectionID: 1}}, nil}},
		},
		{
			"test watch same channel",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan1", CollectionID: 1}}},
					},
				},
				[]*channel{{Name: "chan1", CollectionID: 1}},
			},
			nil,
		},
		{
			"test normal assign",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}},
						2: {2, []*channel{{Name: "chan3", CollectionID: 1}}},
					},
				},
				[]*channel{{Name: "chan4", CollectionID: 1}},
			},
			[]*ChannelOp{{Add, 2, []*channel{{Name: "chan4", CollectionID: 1}}, nil}},
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
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{},
				},
				[]*channel{{Name: "chan1", CollectionID: 1}},
			},
			[]*ChannelOp{{Add, bufferID, []*channel{{Name: "chan1", CollectionID: 1}}, nil}},
		},
		{
			"test watch same channel",
			args{
				consistent.New(),
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}},
					},
				},
				[]*channel{{Name: "chan1", CollectionID: 1}},
			},
			nil,
		},
		{
			"test normal watch",
			args{
				consistent.New(),
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{1: {1, nil}, 2: {2, nil}, 3: {3, nil}},
				},
				[]*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}, {Name: "chan3", CollectionID: 1}},
			},
			[]*ChannelOp{{Add, 2, []*channel{{Name: "chan1", CollectionID: 1}}, nil}, {Add, 1, []*channel{{Name: "chan2", CollectionID: 1}}, nil}, {Add, 3, []*channel{{Name: "chan3", CollectionID: 1}}, nil}},
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
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan1", CollectionID: 1}}},
					},
				},
				1,
			},
			[]*ChannelOp{{Delete, 1, []*channel{{Name: "chan1", CollectionID: 1}}, nil}, {Add, bufferID, []*channel{{Name: "chan1", CollectionID: 1}}, nil}},
		},
		{
			"test rebalance channels after deregister",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan1", CollectionID: 1}}},
						2: {2, []*channel{{Name: "chan2", CollectionID: 1}}},
						3: {3, []*channel{}},
					},
				},
				2,
			},
			[]*ChannelOp{{Delete, 2, []*channel{{Name: "chan2", CollectionID: 1}}, nil}, {Add, 3, []*channel{{Name: "chan2", CollectionID: 1}}, nil}},
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
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan1", CollectionID: 1}}},
					},
				},
				1,
			},
			[]*ChannelOp{{Delete, 1, []*channel{{Name: "chan1", CollectionID: 1}}, nil}, {Add, bufferID, []*channel{{Name: "chan1", CollectionID: 1}}, nil}},
		},
		{
			"rebalance after deregister",
			args{
				consistent.New(),
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan2", CollectionID: 1}}},
						2: {2, []*channel{{Name: "chan1", CollectionID: 1}}},
						3: {3, []*channel{{Name: "chan3", CollectionID: 1}}},
					},
				},
				2,
			},
			[]*ChannelOp{{Delete, 2, []*channel{{Name: "chan1", CollectionID: 1}}, nil}, {Add, 1, []*channel{{Name: "chan1", CollectionID: 1}}, nil}},
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

func TestRoundRobinReassignPolicy(t *testing.T) {
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
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan1", CollectionID: 1}}},
					},
				},
				[]*NodeChannelInfo{{1, []*channel{{Name: "chan1", CollectionID: 1}}}},
			},
			[]*ChannelOp{},
		},
		{
			"test normal reassing",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}},
						2: {2, []*channel{}},
					},
				},
				[]*NodeChannelInfo{{1, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}}},
			},
			[]*ChannelOp{{Delete, 1, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}, nil}, {Add, 2, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}, nil}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RoundRobinReassignPolicy(tt.args.store, tt.args.reassigns)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func TestBgCheckForChannelBalance(t *testing.T) {
	type args struct {
		channels  []*NodeChannelInfo
		timestamp time.Time
	}

	tests := []struct {
		name    string
		args    args
		want    []*NodeChannelInfo
		wantErr error
	}{
		{
			"test even distribution",
			args{
				[]*NodeChannelInfo{
					{1, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}},
					{2, []*channel{{Name: "chan1", CollectionID: 2}, {Name: "chan2", CollectionID: 2}}},
					{3, []*channel{{Name: "chan1", CollectionID: 3}, {Name: "chan2", CollectionID: 3}}},
				},
				time.Now(),
			},
			// there should be no reallocate
			[]*NodeChannelInfo{},
			nil,
		},
		{
			"test uneven with conservative effect",
			args{
				[]*NodeChannelInfo{
					{1, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}},
					{2, []*channel{}},
				},
				time.Now(),
			},
			// as we deem that the node having only one channel more than average as even, so there's no reallocation
			// for this test case
			[]*NodeChannelInfo{},
			nil,
		},
		{
			"test uneven with zero",
			args{
				[]*NodeChannelInfo{
					{1, []*channel{
						{Name: "chan1", CollectionID: 1},
						{Name: "chan2", CollectionID: 1},
						{Name: "chan3", CollectionID: 1},
					}},
					{2, []*channel{}},
				},
				time.Now(),
			},
			[]*NodeChannelInfo{{1, []*channel{{Name: "chan1", CollectionID: 1}}}},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := BgBalanceCheck
			got, err := policy(tt.args.channels, tt.args.timestamp)
			assert.Equal(t, tt.wantErr, err)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func TestAvgReassignPolicy(t *testing.T) {
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
			"test_only_one_node",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan1", CollectionID: 1}}},
					},
				},
				[]*NodeChannelInfo{{1, []*channel{{Name: "chan1", CollectionID: 1}}}},
			},
			// as there's no available nodes except the input node, there's no reassign plan generated
			[]*ChannelOp{},
		},
		{
			"test_zero_avg",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan1", CollectionID: 1}}},
						2: {2, []*channel{}},
						3: {2, []*channel{}},
						4: {2, []*channel{}},
					},
				},
				[]*NodeChannelInfo{{1, []*channel{{Name: "chan1", CollectionID: 1}}}},
			},
			[]*ChannelOp{
				// as we use ceil to calculate the wanted average number, there should be one reassign
				// though the average num less than 1
				{Delete, 1, []*channel{{Name: "chan1", CollectionID: 1}}, nil},
				{Add, 2, []*channel{{Name: "chan1", CollectionID: 1}}, nil},
			},
		},
		{
			"test_normal_reassigning_for_one_available_nodes",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}},
						2: {2, []*channel{}},
					},
				},
				[]*NodeChannelInfo{{1, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}}},
			},
			[]*ChannelOp{
				{Delete, 1, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}, nil},
				{Add, 2, []*channel{{Name: "chan1", CollectionID: 1}, {Name: "chan2", CollectionID: 1}}, nil},
			},
		},
		{
			"test_normal_reassigning_for_multiple_available_nodes",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{
							{Name: "chan1", CollectionID: 1},
							{Name: "chan2", CollectionID: 1},
							{Name: "chan3", CollectionID: 1},
							{Name: "chan4", CollectionID: 1},
						}},
						2: {2, []*channel{}},
						3: {3, []*channel{}},
						4: {4, []*channel{}},
					},
				},
				[]*NodeChannelInfo{{1, []*channel{
					{Name: "chan1", CollectionID: 1},
					{Name: "chan2", CollectionID: 1},
					{Name: "chan3", CollectionID: 1},
				}}},
			},
			[]*ChannelOp{
				{
					Delete, 1,
					[]*channel{
						{Name: "chan1", CollectionID: 1},
						{Name: "chan2", CollectionID: 1},
						{Name: "chan3", CollectionID: 1},
					},
					nil,
				},
				{Add, 2, []*channel{{Name: "chan1", CollectionID: 1}}, nil},
				{Add, 3, []*channel{{Name: "chan2", CollectionID: 1}}, nil},
				{Add, 4, []*channel{{Name: "chan3", CollectionID: 1}}, nil},
			},
		},
		{
			"test_reassigning_for_extreme_case",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{
							{Name: "chan1", CollectionID: 1},
							{Name: "chan2", CollectionID: 1},
							{Name: "chan3", CollectionID: 1},
							{Name: "chan4", CollectionID: 1},
							{Name: "chan5", CollectionID: 1},
							{Name: "chan6", CollectionID: 1},
							{Name: "chan7", CollectionID: 1},
							{Name: "chan8", CollectionID: 1},
							{Name: "chan9", CollectionID: 1},
							{Name: "chan10", CollectionID: 1},
							{Name: "chan11", CollectionID: 1},
							{Name: "chan12", CollectionID: 1},
						}},
						2: {2, []*channel{
							{Name: "chan13", CollectionID: 1}, {Name: "chan14", CollectionID: 1},
						}},
						3: {3, []*channel{{Name: "chan15", CollectionID: 1}}},
						4: {4, []*channel{}},
					},
				},
				[]*NodeChannelInfo{{1, []*channel{
					{Name: "chan1", CollectionID: 1},
					{Name: "chan2", CollectionID: 1},
					{Name: "chan3", CollectionID: 1},
					{Name: "chan4", CollectionID: 1},
					{Name: "chan5", CollectionID: 1},
					{Name: "chan6", CollectionID: 1},
					{Name: "chan7", CollectionID: 1},
					{Name: "chan8", CollectionID: 1},
					{Name: "chan9", CollectionID: 1},
					{Name: "chan10", CollectionID: 1},
					{Name: "chan11", CollectionID: 1},
					{Name: "chan12", CollectionID: 1},
				}}},
			},
			[]*ChannelOp{
				{Delete, 1, []*channel{
					{Name: "chan1", CollectionID: 1},
					{Name: "chan2", CollectionID: 1},
					{Name: "chan3", CollectionID: 1},
					{Name: "chan4", CollectionID: 1},
					{Name: "chan5", CollectionID: 1},
					{Name: "chan6", CollectionID: 1},
					{Name: "chan7", CollectionID: 1},
					{Name: "chan8", CollectionID: 1},
					{Name: "chan9", CollectionID: 1},
					{Name: "chan10", CollectionID: 1},
					{Name: "chan11", CollectionID: 1},
					{Name: "chan12", CollectionID: 1},
				}, nil},
				{Add, 4, []*channel{
					{Name: "chan1", CollectionID: 1},
					{Name: "chan2", CollectionID: 1},
					{Name: "chan3", CollectionID: 1},
					{Name: "chan4", CollectionID: 1},
					{Name: "chan5", CollectionID: 1},
				}, nil},
				{Add, 3, []*channel{
					{Name: "chan6", CollectionID: 1},
					{Name: "chan7", CollectionID: 1},
					{Name: "chan8", CollectionID: 1},
					{Name: "chan9", CollectionID: 1},
				}, nil},
				{Add, 2, []*channel{
					{Name: "chan10", CollectionID: 1},
					{Name: "chan11", CollectionID: 1},
					{Name: "chan12", CollectionID: 1},
				}, nil},
			},
		},
	}
	for _, tt := range tests {
		if tt.name == "test_reassigning_for_extreme_case" ||
			tt.name == "test_normal_reassigning_for_multiple_available_nodes" {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			got := AverageReassignPolicy(tt.args.store, tt.args.reassigns)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func TestAvgBalanceChannelPolicy(t *testing.T) {
	type args struct {
		store ROChannelStore
	}
	tests := []struct {
		name string
		args args
		want ChannelOpSet
	}{
		{
			"test_only_one_node",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {
							1, []*channel{
								{Name: "chan1", CollectionID: 1},
								{Name: "chan2", CollectionID: 1},
								{Name: "chan3", CollectionID: 1},
								{Name: "chan4", CollectionID: 1},
							},
						},
						2: {2, []*channel{}},
					},
				},
			},
			[]*ChannelOp{
				{Add, 1, []*channel{
					{Name: "chan1", CollectionID: 1},
				}, nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AvgBalanceChannelPolicy(tt.args.store, time.Now())
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
		name            string
		args            args
		bufferedUpdates ChannelOpSet
		balanceUpdates  ChannelOpSet
	}{
		{
			"test empty",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {NodeID: 1, Channels: make([]*channel, 0)},
					},
				},
				1,
			},
			nil,
			nil,
		},
		{
			"test with buffer channel",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						bufferID: {bufferID, []*channel{{Name: "ch1", CollectionID: 1}}},
						1:        {NodeID: 1, Channels: []*channel{}},
					},
				},
				1,
			},
			[]*ChannelOp{
				{
					Type:     Delete,
					NodeID:   bufferID,
					Channels: []*channel{{Name: "ch1", CollectionID: 1}},
				},
				{
					Type:     Add,
					NodeID:   1,
					Channels: []*channel{{Name: "ch1", CollectionID: 1}},
				},
			},
			nil,
		},
		{
			"test with avg assign",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "ch1", CollectionID: 1}, {Name: "ch2", CollectionID: 1}}},
						3: {3, []*channel{}},
					},
				},
				3,
			},
			nil,
			[]*ChannelOp{
				{
					Type:     Add,
					NodeID:   1,
					Channels: []*channel{{Name: "ch1", CollectionID: 1}},
				},
			},
		},
		{
			"test with avg equals to zero",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "ch1", CollectionID: 1}}},
						2: {2, []*channel{{Name: "ch3", CollectionID: 1}}},
						3: {3, []*channel{}},
					},
				},
				3,
			},
			nil,
			nil,
		},
		{
			"test node with empty channel",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []*channel{{Name: "ch1", CollectionID: 1}, {Name: "ch2", CollectionID: 1}, {Name: "ch3", CollectionID: 1}}},
						2: {2, []*channel{}},
						3: {3, []*channel{}},
					},
				},
				3,
			},
			nil,
			[]*ChannelOp{
				{
					Type:     Add,
					NodeID:   1,
					Channels: []*channel{{Name: "ch1", CollectionID: 1}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bufferedUpdates, balanceUpdates := AvgAssignRegisterPolicy(tt.args.store, tt.args.nodeID)
			assert.EqualValues(t, tt.bufferedUpdates, bufferedUpdates)
			assert.EqualValues(t, tt.balanceUpdates, balanceUpdates)
		})
	}
}
