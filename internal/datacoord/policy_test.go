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

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"stathat.com/c/consistent"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
)

func TestBufferChannelAssignPolicy(t *testing.T) {
	kv := memkv.NewMemoryKV()

	channels := []RWChannel{getChannel("chan1", 1)}
	store := &ChannelStore{
		store: kv,
		channelsInfo: map[int64]*NodeChannelInfo{
			1:        {1, []RWChannel{}},
			bufferID: {bufferID, channels},
		},
	}

	updates := BufferChannelAssignPolicy(store, 1).Collect()
	assert.NotNil(t, updates)
	assert.Equal(t, 2, len(updates))
	assert.ElementsMatch(t,
		NewChannelOpSet(
			NewAddOp(1, channels...),
			NewDeleteOp(bufferID, channels...),
		).Collect(),
		updates)
}

func getChannel(name string, collID int64) *channelMeta {
	return &channelMeta{Name: name, CollectionID: collID}
}

func getChannels(ch2Coll map[string]int64) []RWChannel {
	return lo.MapToSlice(ch2Coll, func(name string, coll int64) RWChannel {
		return &channelMeta{Name: name, CollectionID: coll}
	})
}

func TestConsistentHashRegisterPolicy(t *testing.T) {
	t.Run("first register", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		ch2Coll := map[string]int64{
			"chan1": 1,
			"chan2": 2,
		}
		channels := getChannels(ch2Coll)
		store := &ChannelStore{
			store: kv,
			channelsInfo: map[int64]*NodeChannelInfo{
				bufferID: {bufferID, channels},
				1:        {1, []RWChannel{}},
			},
		}

		hashring := consistent.New()
		policy := ConsistentHashRegisterPolicy(hashring)

		up, _ := policy(store, 1)
		updates := up.Collect()

		assert.NotNil(t, updates)
		assert.Equal(t, 2, len(updates))
		assert.EqualValues(t, &ChannelOp{Type: Delete, NodeID: bufferID, Channels: channels}, updates[0])
		assert.EqualValues(t, &ChannelOp{Type: Add, NodeID: 1, Channels: channels}, updates[1])
	})

	t.Run("rebalance after register", func(t *testing.T) {
		kv := memkv.NewMemoryKV()

		ch2Coll := map[string]int64{
			"chan1": 1,
			"chan2": 2,
		}
		channels := getChannels(ch2Coll)

		store := &ChannelStore{
			store:        kv,
			channelsInfo: map[int64]*NodeChannelInfo{1: {1, channels}, 2: {2, []RWChannel{}}},
		}

		hashring := consistent.New()
		hashring.Add(formatNodeID(1))
		policy := ConsistentHashRegisterPolicy(hashring)

		_, up := policy(store, 2)
		updates := up.Collect()

		assert.NotNil(t, updates)
		assert.Equal(t, 1, len(updates))
		// No Delete operation will be generated

		assert.Equal(t, 1, len(updates[0].GetChannelNames()))
		channel := updates[0].GetChannelNames()[0]

		// Not stable whether to balance chan-1 or chan-2
		if channel == "chan-1" {
			assert.EqualValues(t, &ChannelOp{Type: Add, NodeID: 1, Channels: []RWChannel{channels[0]}}, updates[0])
		}

		if channel == "chan-2" {
			assert.EqualValues(t, &ChannelOp{Type: Add, NodeID: 1, Channels: []RWChannel{channels[1]}}, updates[0])
		}
	})
}

func TestAverageAssignPolicy(t *testing.T) {
	type args struct {
		store    ROChannelStore
		channels []RWChannel
	}
	tests := []struct {
		name string
		args args
		want *ChannelOpSet
	}{
		{
			"test assign empty cluster",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{},
				},
				[]RWChannel{getChannel("chan1", 1)},
			},
			NewChannelOpSet(NewAddOp(bufferID, getChannel("chan1", 1))),
		},
		{
			"test watch same channel",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan1", 1)}},
					},
				},
				[]RWChannel{getChannel("chan1", 1)},
			},
			NewChannelOpSet(),
		},
		{
			"test normal assign",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan1", 1), getChannel("chan2", 1)}},
						2: {2, []RWChannel{getChannel("chan3", 1)}},
					},
				},
				[]RWChannel{getChannel("chan4", 1)},
			},
			NewChannelOpSet(NewAddOp(2, getChannel("chan4", 1))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AverageAssignPolicy(tt.args.store, tt.args.channels)
			assert.EqualValues(t, tt.want.Collect(), got.Collect())
		})
	}
}

func TestConsistentHashChannelAssignPolicy(t *testing.T) {
	type args struct {
		hashring *consistent.Consistent
		store    ROChannelStore
		channels []RWChannel
	}
	tests := []struct {
		name string
		args args
		want *ChannelOpSet
	}{
		{
			"test assign empty cluster",
			args{
				consistent.New(),
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{},
				},
				[]RWChannel{getChannel("chan1", 1)},
			},
			NewChannelOpSet(NewAddOp(bufferID, getChannel("chan1", 1))),
		},
		{
			"test watch same channel",
			args{
				consistent.New(),
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan1", 1), getChannel("chan2", 1)}},
					},
				},
				[]RWChannel{getChannel("chan1", 1)},
			},
			NewChannelOpSet(),
		},
		{
			"test normal watch",
			args{
				consistent.New(),
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{1: {1, nil}, 2: {2, nil}, 3: {3, nil}},
				},
				[]RWChannel{getChannel("chan1", 1), getChannel("chan2", 1), getChannel("chan3", 1)},
			},
			NewChannelOpSet(
				NewAddOp(2, getChannel("chan1", 1)),
				NewAddOp(1, getChannel("chan2", 1)),
				NewAddOp(3, getChannel("chan3", 1)),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := ConsistentHashChannelAssignPolicy(tt.args.hashring)
			got := policy(tt.args.store, tt.args.channels).Collect()
			want := tt.want.Collect()
			assert.Equal(t, len(want), len(got))
			for _, op := range want {
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
		want *ChannelOpSet
	}{
		{
			"test deregister the last node",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan1", 1)}},
					},
				},
				1,
			},
			NewChannelOpSet(
				NewDeleteOp(1, getChannel("chan1", 1)),
				NewAddOp(bufferID, getChannel("chan1", 1)),
			),
		},
		{
			"test rebalance channels after deregister",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan1", 1)}},
						2: {2, []RWChannel{getChannel("chan2", 1)}},
						3: {3, []RWChannel{}},
					},
				},
				2,
			},
			NewChannelOpSet(
				NewDeleteOp(2, getChannel("chan2", 1)),
				NewAddOp(3, getChannel("chan2", 1)),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AvgAssignUnregisteredChannels(tt.args.store, tt.args.nodeID)
			assert.EqualValues(t, tt.want.Collect(), got.Collect())
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
		want *ChannelOpSet
	}{
		{
			"test deregister the last node",
			args{
				consistent.New(),
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan1", 1)}},
					},
				},
				1,
			},
			NewChannelOpSet(
				NewDeleteOp(1, getChannel("chan1", 1)),
				NewAddOp(bufferID, getChannel("chan1", 1)),
			),
		},
		{
			"rebalance after deregister",
			args{
				consistent.New(),
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan2", 1)}},
						2: {2, []RWChannel{getChannel("chan1", 1)}},
						3: {3, []RWChannel{getChannel("chan3", 1)}},
					},
				},
				2,
			},
			NewChannelOpSet(
				NewDeleteOp(2, getChannel("chan1", 1)),
				NewAddOp(1, getChannel("chan1", 1)),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := ConsistentHashDeregisterPolicy(tt.args.hashring)
			got := policy(tt.args.store, tt.args.nodeID)
			assert.EqualValues(t, tt.want.Collect(), got.Collect())
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
		want *ChannelOpSet
	}{
		{
			"test only one node",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan1", 1)}},
					},
				},
				[]*NodeChannelInfo{{1, []RWChannel{getChannel("chan1", 1)}}},
			},
			NewChannelOpSet(),
		},
		{
			"test normal reassigning",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan1", 1), getChannel("chan2", 1)}},
						2: {2, []RWChannel{}},
					},
				},
				[]*NodeChannelInfo{{1, []RWChannel{getChannel("chan1", 1), getChannel("chan2", 1)}}},
			},
			NewChannelOpSet(
				NewDeleteOp(1, getChannel("chan1", 1), getChannel("chan2", 1)),
				NewAddOp(2, getChannel("chan1", 1), getChannel("chan2", 1)),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RoundRobinReassignPolicy(tt.args.store, tt.args.reassigns)
			assert.EqualValues(t, tt.want.Collect(), got.Collect())
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
					{1, []RWChannel{getChannel("chan1", 1), getChannel("chan2", 1)}},
					{2, []RWChannel{getChannel("chan1", 2), getChannel("chan2", 2)}},
					{3, []RWChannel{getChannel("chan1", 3), getChannel("chan2", 3)}},
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
					{1, []RWChannel{getChannel("chan1", 1), getChannel("chan2", 1)}},
					{2, []RWChannel{}},
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
					{1, []RWChannel{
						getChannel("chan1", 1),
						getChannel("chan2", 1),
						getChannel("chan3", 1),
					}},
					{2, []RWChannel{}},
				},
				time.Now(),
			},
			[]*NodeChannelInfo{{1, []RWChannel{getChannel("chan1", 1)}}},
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
		want *ChannelOpSet
	}{
		{
			"test_only_one_node",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan1", 1)}},
					},
				},
				[]*NodeChannelInfo{{1, []RWChannel{getChannel("chan1", 1)}}},
			},
			// as there's no available nodes except the input node, there's no reassign plan generated
			NewChannelOpSet(),
		},
		{
			"test_zero_avg",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan1", 1)}},
						2: {2, []RWChannel{}},
						3: {2, []RWChannel{}},
						4: {2, []RWChannel{}},
					},
				},
				[]*NodeChannelInfo{{1, []RWChannel{getChannel("chan1", 1)}}},
			},
			// as we use ceil to calculate the wanted average number, there should be one reassign
			// though the average num less than 1
			NewChannelOpSet(
				NewDeleteOp(1, getChannel("chan1", 1)),
				NewAddOp(2, getChannel("chan1", 1)),
			),
		},
		{
			"test_normal_reassigning_for_one_available_nodes",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("chan1", 1), getChannel("chan2", 1)}},
						2: {2, []RWChannel{}},
					},
				},
				[]*NodeChannelInfo{{1, []RWChannel{getChannel("chan1", 1), getChannel("chan2", 1)}}},
			},
			NewChannelOpSet(
				NewDeleteOp(1, getChannel("chan1", 1), getChannel("chan2", 1)),
				NewAddOp(2, getChannel("chan1", 1), getChannel("chan2", 1)),
			),
		},
		{
			"test_normal_reassigning_for_multiple_available_nodes",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{
							getChannel("chan1", 1),
							getChannel("chan2", 1),
							getChannel("chan3", 1),
							getChannel("chan4", 1),
						}},
						2: {2, []RWChannel{}},
						3: {3, []RWChannel{}},
						4: {4, []RWChannel{}},
					},
				},
				[]*NodeChannelInfo{{1, []RWChannel{
					getChannel("chan1", 1),
					getChannel("chan2", 1),
					getChannel("chan3", 1),
				}}},
			},
			NewChannelOpSet(
				NewDeleteOp(1, []RWChannel{
					getChannel("chan1", 1),
					getChannel("chan2", 1),
					getChannel("chan3", 1),
				}...),
				NewAddOp(2, getChannel("chan1", 1)),
				NewAddOp(3, getChannel("chan2", 1)),
				NewAddOp(4, getChannel("chan3", 1)),
			),
		},
		{
			"test_reassigning_for_extreme_case",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{
							getChannel("chan1", 1),
							getChannel("chan2", 1),
							getChannel("chan3", 1),
							getChannel("chan4", 1),
							getChannel("chan5", 1),
							getChannel("chan6", 1),
							getChannel("chan7", 1),
							getChannel("chan8", 1),
							getChannel("chan9", 1),
							getChannel("chan10", 1),
							getChannel("chan11", 1),
							getChannel("chan12", 1),
						}},
						2: {2, []RWChannel{
							getChannel("chan13", 1),
							getChannel("chan14", 1),
						}},
						3: {3, []RWChannel{getChannel("chan15", 1)}},
						4: {4, []RWChannel{}},
					},
				},
				[]*NodeChannelInfo{{1, []RWChannel{
					getChannel("chan1", 1),
					getChannel("chan2", 1),
					getChannel("chan3", 1),
					getChannel("chan4", 1),
					getChannel("chan5", 1),
					getChannel("chan6", 1),
					getChannel("chan7", 1),
					getChannel("chan8", 1),
					getChannel("chan9", 1),
					getChannel("chan10", 1),
					getChannel("chan11", 1),
					getChannel("chan12", 1),
				}}},
			},
			NewChannelOpSet(
				NewDeleteOp(1, []RWChannel{
					getChannel("chan1", 1),
					getChannel("chan2", 1),
					getChannel("chan3", 1),
					getChannel("chan4", 1),
					getChannel("chan5", 1),
					getChannel("chan6", 1),
					getChannel("chan7", 1),
					getChannel("chan8", 1),
					getChannel("chan9", 1),
					getChannel("chan10", 1),
					getChannel("chan11", 1),
					getChannel("chan12", 1),
				}...),
				NewAddOp(4, []RWChannel{
					getChannel("chan1", 1),
					getChannel("chan2", 1),
					getChannel("chan3", 1),
					getChannel("chan4", 1),
					getChannel("chan5", 1),
				}...),
				NewAddOp(3, []RWChannel{
					getChannel("chan6", 1),
					getChannel("chan7", 1),
					getChannel("chan8", 1),
					getChannel("chan9", 1),
				}...),
				NewAddOp(2, []RWChannel{
					getChannel("chan10", 1),
					getChannel("chan11", 1),
					getChannel("chan12", 1),
				}...),
			),
		},
	}
	for _, tt := range tests {
		if tt.name == "test_reassigning_for_extreme_case" ||
			tt.name == "test_normal_reassigning_for_multiple_available_nodes" {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			got := AverageReassignPolicy(tt.args.store, tt.args.reassigns)
			assert.ElementsMatch(t, tt.want.Collect(), got.Collect())
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
		want *ChannelOpSet
	}{
		{
			"test_only_one_node",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {
							1, []RWChannel{
								getChannel("chan1", 1),
								getChannel("chan2", 1),
								getChannel("chan3", 1),
								getChannel("chan4", 1),
							},
						},
						2: {2, []RWChannel{}},
					},
				},
			},
			NewChannelOpSet(NewAddOp(1, getChannel("chan1", 1))),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AvgBalanceChannelPolicy(tt.args.store, time.Now())
			assert.EqualValues(t, tt.want.Collect(), got.Collect())
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
		bufferedUpdates *ChannelOpSet
		balanceUpdates  *ChannelOpSet
	}{
		{
			"test empty",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {NodeID: 1, Channels: make([]RWChannel, 0)},
					},
				},
				1,
			},
			NewChannelOpSet(),
			NewChannelOpSet(),
		},
		{
			"test with buffer channel",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						bufferID: {bufferID, []RWChannel{getChannel("ch1", 1)}},
						1:        {NodeID: 1, Channels: []RWChannel{}},
					},
				},
				1,
			},
			NewChannelOpSet(
				NewDeleteOp(bufferID, getChannel("ch1", 1)),
				NewAddOp(1, getChannel("ch1", 1)),
			),
			NewChannelOpSet(),
		},
		{
			"test with avg assign",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("ch1", 1), getChannel("ch2", 1)}},
						3: {3, []RWChannel{}},
					},
				},
				3,
			},
			NewChannelOpSet(),
			NewChannelOpSet(NewAddOp(1, getChannel("ch1", 1))),
		},
		{
			"test with avg equals to zero",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("ch1", 1)}},
						2: {2, []RWChannel{getChannel("ch3", 1)}},
						3: {3, []RWChannel{}},
					},
				},
				3,
			},
			NewChannelOpSet(),
			NewChannelOpSet(),
		},
		{
			"test node with empty channel",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: {1, []RWChannel{getChannel("ch1", 1), getChannel("ch2", 1), getChannel("ch3", 1)}},
						2: {2, []RWChannel{}},
						3: {3, []RWChannel{}},
					},
				},
				3,
			},
			NewChannelOpSet(),
			NewChannelOpSet(NewAddOp(1, getChannel("ch1", 1))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bufferedUpdates, balanceUpdates := AvgAssignRegisterPolicy(tt.args.store, tt.args.nodeID)
			assert.EqualValues(t, tt.bufferedUpdates.Collect(), bufferedUpdates.Collect())
			assert.EqualValues(t, tt.balanceUpdates.Collect(), balanceUpdates.Collect())
		})
	}
}
