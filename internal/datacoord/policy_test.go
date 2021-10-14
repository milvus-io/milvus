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
			{"chan1", 1},
			{"chan2", 2},
		}
		store := &ChannelStore{
			store:        kv,
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
			store:        kv,
			channelsInfo: map[int64]*NodeChannelInfo{1: {1, channels}, 2: {2, []*channel{}}},
		}

		hashring := consistent.New()
		hashring.Add(formatNodeID(1))
		policy := ConsistentHashRegisterPolicy(hashring)

		updates := policy(store, 2)

		// chan1 will be hash to 2, chan2 will be hash to 1
		assert.NotNil(t, updates)
		assert.Equal(t, 2, len(updates))
		assert.EqualValues(t, &ChannelOp{Type: Delete, NodeID: 1, Channels: []*channel{channels[0]}}, updates[0])
		assert.EqualValues(t, &ChannelOp{Type: Add, NodeID: 2, Channels: []*channel{channels[0]}}, updates[1])
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
				[]*channel{{"chan1", 1}},
			},
			[]*ChannelOp{{Add, bufferID, []*channel{{"chan1", 1}}, nil}},
		},
		{
			"test watch same channel",
			args{
				&ChannelStore{
					memkv.NewMemoryKV(),
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
					memkv.NewMemoryKV(),
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
					memkv.NewMemoryKV(),
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
					memkv.NewMemoryKV(),
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
					memkv.NewMemoryKV(),
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
					memkv.NewMemoryKV(),
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
					memkv.NewMemoryKV(),
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
					memkv.NewMemoryKV(),
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
					memkv.NewMemoryKV(),
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
					memkv.NewMemoryKV(),
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
					memkv.NewMemoryKV(),
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
			k := buildChannelKey(info.nodeID, info.name)
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
