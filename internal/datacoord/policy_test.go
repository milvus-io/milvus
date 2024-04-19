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
	"github.com/stretchr/testify/require"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
)

func TestBufferChannelAssignPolicy(t *testing.T) {
	kv := memkv.NewMemoryKV()

	channels := []RWChannel{getChannel("chan1", 1)}
	store := &ChannelStore{
		store: kv,
		channelsInfo: map[int64]*NodeChannelInfo{
			1:        NewNodeChannelInfo(1),
			bufferID: NewNodeChannelInfo(bufferID, channels...),
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
						1: NewNodeChannelInfo(1, getChannel("chan1", 1)),
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
						1: NewNodeChannelInfo(1, getChannel("chan", 1), getChannel("chan2", 1)),
						2: NewNodeChannelInfo(2, getChannel("chan3", 1)),
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
						1: NewNodeChannelInfo(1, getChannel("chan1", 1)),
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
						1: NewNodeChannelInfo(1, getChannel("chan1", 1)),
						2: NewNodeChannelInfo(2, getChannel("chan2", 1)),
						3: NewNodeChannelInfo(3),
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

func TestBgCheckForChannelBalance(t *testing.T) {
	type args struct {
		channels  []*NodeChannelInfo
		timestamp time.Time
	}

	tests := []struct {
		name string
		args args
		// want    []*NodeChannelInfo
		want    int
		wantErr error
	}{
		{
			"test even distribution",
			args{
				[]*NodeChannelInfo{
					NewNodeChannelInfo(1, getChannel("chan1", 1), getChannel("chan2", 1)),
					NewNodeChannelInfo(2, getChannel("chan1", 2), getChannel("chan2", 2)),
					NewNodeChannelInfo(3, getChannel("chan1", 3), getChannel("chan2", 3)),
				},
				time.Now(),
			},
			// there should be no reallocate
			0,
			nil,
		},
		{
			"test uneven with conservative effect",
			args{
				[]*NodeChannelInfo{
					NewNodeChannelInfo(1, getChannel("chan1", 1), getChannel("chan2", 1)),
					NewNodeChannelInfo(2),
				},
				time.Now(),
			},
			// as we deem that the node having only one channel more than average as even, so there's no reallocation
			// for this test case
			0,
			nil,
		},
		{
			"test uneven with zero",
			args{
				[]*NodeChannelInfo{
					NewNodeChannelInfo(1, getChannel("chan1", 1), getChannel("chan2", 1), getChannel("chan3", 1)),
					NewNodeChannelInfo(2),
				},
				time.Now(),
			},
			1,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := BgBalanceCheck
			got, err := policy(tt.args.channels, tt.args.timestamp)
			assert.Equal(t, tt.wantErr, err)
			assert.EqualValues(t, tt.want, len(got))
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
						1: NewNodeChannelInfo(1, getChannel("chan1", 1)),
					},
				},
				[]*NodeChannelInfo{NewNodeChannelInfo(1, getChannel("chan1", 1))},
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
						1: NewNodeChannelInfo(1, getChannel("chan1", 1)),
						2: NewNodeChannelInfo(2),
						3: NewNodeChannelInfo(3),
						4: NewNodeChannelInfo(4),
					},
				},
				[]*NodeChannelInfo{NewNodeChannelInfo(1, getChannel("chan1", 1))},
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
						1: NewNodeChannelInfo(1, getChannel("chan1", 1), getChannel("chan2", 1)),
						2: NewNodeChannelInfo(2),
					},
				},
				[]*NodeChannelInfo{NewNodeChannelInfo(1, getChannel("chan1", 1), getChannel("chan2", 1))},
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
						1: NewNodeChannelInfo(1, getChannel("chan1", 1),
							getChannel("chan2", 1),
							getChannel("chan3", 1),
							getChannel("chan4", 1)),
						2: NewNodeChannelInfo(2),
						3: NewNodeChannelInfo(3),
						4: NewNodeChannelInfo(4),
					},
				},
				[]*NodeChannelInfo{NewNodeChannelInfo(1, getChannel("chan1", 1),
					getChannel("chan2", 1),
					getChannel("chan3", 1),
					getChannel("chan4", 1))},
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
						1: NewNodeChannelInfo(1,
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
							getChannel("chan12", 1)),
						2: NewNodeChannelInfo(2,
							getChannel("chan13", 1),
							getChannel("chan14", 1)),
						3: NewNodeChannelInfo(3, getChannel("chan15", 1)),
						4: NewNodeChannelInfo(4),
					},
				},
				[]*NodeChannelInfo{NewNodeChannelInfo(1,
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
					getChannel("chan12", 1))},
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

			wantMap, gotMap := tt.want.SplitByChannel(), got.SplitByChannel()
			assert.ElementsMatch(t, lo.Keys(wantMap), lo.Keys(gotMap))

			for k, opSet := range wantMap {
				gotOpSet, ok := gotMap[k]
				require.True(t, ok)
				assert.ElementsMatch(t, opSet.Collect(), gotOpSet.Collect())
			}
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
		want int
	}{
		{
			"test_only_one_node",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: NewNodeChannelInfo(1,
							getChannel("chan1", 1),
							getChannel("chan2", 1),
							getChannel("chan3", 1),
							getChannel("chan4", 1),
						),
						2: NewNodeChannelInfo(2),
					},
				},
			},
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AvgBalanceChannelPolicy(tt.args.store, time.Now())
			assert.EqualValues(t, tt.want, len(got.Collect()))
		})
	}
}

func TestAvgAssignRegisterPolicy(t *testing.T) {
	type args struct {
		store  ROChannelStore
		nodeID int64
	}
	tests := []struct {
		name               string
		args               args
		bufferedUpdates    *ChannelOpSet
		balanceUpdates     *ChannelOpSet
		exact              bool
		bufferedUpdatesNum int
		balanceUpdatesNum  int
	}{
		{
			"test empty",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: NewNodeChannelInfo(1),
					},
				},
				1,
			},
			NewChannelOpSet(),
			NewChannelOpSet(),
			true,
			0,
			0,
		},
		{
			"test with buffer channel",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						bufferID: NewNodeChannelInfo(bufferID, getChannel("ch1", 1)),
						1:        NewNodeChannelInfo(1),
					},
				},
				1,
			},
			NewChannelOpSet(
				NewDeleteOp(bufferID, getChannel("ch1", 1)),
				NewAddOp(1, getChannel("ch1", 1)),
			),
			NewChannelOpSet(),
			true,
			0,
			0,
		},
		{
			"test with avg assign",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: NewNodeChannelInfo(1, getChannel("ch1", 1), getChannel("ch2", 1)),
						3: NewNodeChannelInfo(3),
					},
				},
				3,
			},
			NewChannelOpSet(),
			NewChannelOpSet(NewAddOp(1, getChannel("ch1", 1))),
			false,
			0,
			1,
		},
		{
			"test with avg equals to zero",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: NewNodeChannelInfo(1, getChannel("ch1", 1)),
						2: NewNodeChannelInfo(2, getChannel("ch3", 1)),
						3: NewNodeChannelInfo(3),
					},
				},
				3,
			},
			NewChannelOpSet(),
			NewChannelOpSet(),
			true,
			0,
			0,
		},
		{
			"test_node_with_empty_channel",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
					map[int64]*NodeChannelInfo{
						1: NewNodeChannelInfo(1, getChannel("ch1", 1), getChannel("ch2", 1), getChannel("ch3", 1)),
						2: NewNodeChannelInfo(2),
						3: NewNodeChannelInfo(3),
					},
				},
				3,
			},
			NewChannelOpSet(),
			NewChannelOpSet(NewAddOp(1, getChannel("ch1", 1))),
			false,
			0,
			1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bufferedUpdates, balanceUpdates := AvgAssignRegisterPolicy(tt.args.store, tt.args.nodeID)
			if tt.exact {
				assert.EqualValues(t, tt.bufferedUpdates.Collect(), bufferedUpdates.Collect())
				assert.EqualValues(t, tt.balanceUpdates.Collect(), balanceUpdates.Collect())
			} else {
				assert.Equal(t, tt.bufferedUpdatesNum, len(bufferedUpdates.Collect()))
				assert.Equal(t, tt.balanceUpdatesNum, len(balanceUpdates.Collect()))
			}
		})
	}
}
