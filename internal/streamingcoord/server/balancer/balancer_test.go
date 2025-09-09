package balancer_test

import (
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_manager"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	_ "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/policy"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestBalancer(t *testing.T) {
	paramtable.Init()
	etcdClient, _ := kvfactory.GetEtcdAndPath()
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager([]string{})

	streamingNodeManager := mock_manager.NewMockManagerClient(t)
	streamingNodeManager.EXPECT().WatchNodeChanged(mock.Anything).Return(make(chan struct{}), nil)
	streamingNodeManager.EXPECT().Assign(mock.Anything, mock.Anything).Return(nil)
	streamingNodeManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	streamingNodeManager.EXPECT().CollectAllStatus(mock.Anything).Return(map[int64]*types.StreamingNodeStatus{
		1: {
			StreamingNodeInfo: types.StreamingNodeInfo{
				ServerID: 1,
				Address:  "localhost:1",
			},
		},
		2: {
			StreamingNodeInfo: types.StreamingNodeInfo{
				ServerID: 2,
				Address:  "localhost:2",
			},
		},
		3: {
			StreamingNodeInfo: types.StreamingNodeInfo{
				ServerID: 3,
				Address:  "localhost:3",
			},
		},
		4: {
			StreamingNodeInfo: types.StreamingNodeInfo{
				ServerID: 4,
				Address:  "localhost:3",
			},
			Err: types.ErrStopping,
		},
	}, nil)

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptETCD(etcdClient), resource.OptStreamingCatalog(catalog), resource.OptStreamingManagerClient(streamingNodeManager))
	catalog.EXPECT().GetCChannel(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveCChannel(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveVersion(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Unset()
	catalog.EXPECT().ListPChannel(mock.Anything).RunAndReturn(func(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
		return []*streamingpb.PChannelMeta{
			{
				Channel: &streamingpb.PChannelInfo{
					Name:       "test-channel-1",
					Term:       1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				Node:  &streamingpb.StreamingNodeInfo{ServerId: 1},
			},
			{
				Channel: &streamingpb.PChannelInfo{
					Name:       "test-channel-2",
					Term:       1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE,
				Node:  &streamingpb.StreamingNodeInfo{ServerId: 4},
			},
			{
				Channel: &streamingpb.PChannelInfo{
					Name:       "test-channel-3",
					Term:       2,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				Node:  &streamingpb.StreamingNodeInfo{ServerId: 2},
			},
		}, nil
	})
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	// Test for lower datanode and proxy version protection.
	metaRoot := paramtable.Get().EtcdCfg.MetaRootPath.GetValue()
	proxyPath1 := path.Join(metaRoot, sessionutil.DefaultServiceRoot, typeutil.ProxyRole+"-1")
	r := sessionutil.SessionRaw{Version: "2.5.11", ServerID: 1}
	data, _ := json.Marshal(r)
	resource.Resource().ETCD().Put(context.Background(), proxyPath1, string(data))
	proxyPath2 := path.Join(metaRoot, sessionutil.DefaultServiceRoot, typeutil.ProxyRole+"-2")
	r = sessionutil.SessionRaw{Version: "2.5.11", ServerID: 2}
	data, _ = json.Marshal(r)
	resource.Resource().ETCD().Put(context.Background(), proxyPath2, string(data))
	metaRoot = paramtable.Get().EtcdCfg.MetaRootPath.GetValue()
	dataNodePath := path.Join(metaRoot, sessionutil.DefaultServiceRoot, typeutil.DataNodeRole)
	resource.Resource().ETCD().Put(context.Background(), dataNodePath, string(data))

	ctx := context.Background()
	b, err := balancer.RecoverBalancer(ctx, "test-channel-1")
	assert.NoError(t, err)
	assert.NotNil(t, b)

	doneErr := errors.New("done")
	err = b.WatchChannelAssignments(context.Background(), func(param balancer.WatchChannelAssignmentsCallbackParam) error {
		for _, relation := range param.Relations {
			assert.Equal(t, relation.Channel.AccessMode, types.AccessModeRO)
		}
		if len(param.Relations) == 3 {
			return doneErr
		}
		return nil
	})
	assert.ErrorIs(t, err, doneErr)
	resource.Resource().ETCD().Delete(context.Background(), proxyPath1)
	resource.Resource().ETCD().Delete(context.Background(), proxyPath2)
	resource.Resource().ETCD().Delete(context.Background(), dataNodePath)

	checkReady := func() {
		err = b.WatchChannelAssignments(ctx, func(param balancer.WatchChannelAssignmentsCallbackParam) error {
			// should one pchannel be assigned to per nodes
			nodeIDs := typeutil.NewSet[int64]()
			if len(param.Relations) == 3 {
				rwCount := types.AccessModeRW
				for _, relation := range param.Relations {
					if relation.Channel.AccessMode == types.AccessModeRW {
						rwCount++
					}
					nodeIDs.Insert(relation.Node.ServerID)
				}
				if rwCount == 3 {
					assert.Equal(t, 3, nodeIDs.Len())
					return doneErr
				}
			}
			return nil
		})
		assert.ErrorIs(t, err, doneErr)
	}
	checkReady()

	b.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "test-channel-1",
		Term: 1,
	}})
	b.Trigger(ctx)
	checkReady()

	// create a inifite block watcher and can be interrupted by close of balancer.
	f := syncutil.NewFuture[error]()
	go func() {
		err := b.WatchChannelAssignments(context.Background(), func(param balancer.WatchChannelAssignmentsCallbackParam) error {
			return nil
		})
		f.Set(err)
	}()
	time.Sleep(20 * time.Millisecond)
	assert.False(t, f.Ready())

	assert.True(t, paramtable.Get().StreamingCfg.WALBalancerPolicyAllowRebalance.GetAsBool())
	resp, err := b.UpdateBalancePolicy(ctx, &streamingpb.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{
			AllowRebalance: false,
		},
		Nodes: &streamingpb.WALBalancePolicyNodes{
			FreezeNodeIds:   []int64{1},
			DefreezeNodeIds: []int64{},
		},
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []int64{1}, resp.FreezeNodeIds)
	assert.False(t, resp.Config.AllowRebalance)
	assert.False(t, paramtable.Get().StreamingCfg.WALBalancerPolicyAllowRebalance.GetAsBool())
	b.Trigger(ctx)
	err = b.WatchChannelAssignments(ctx, func(param balancer.WatchChannelAssignmentsCallbackParam) error {
		for _, relation := range param.Relations {
			if relation.Node.ServerID == 1 {
				return nil
			}
		}
		return doneErr
	})
	assert.ErrorIs(t, err, doneErr)

	resp, err = b.UpdateBalancePolicy(ctx, &streamingpb.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{
			AllowRebalance: true,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{types.UpdateMaskPathWALBalancePolicyAllowRebalance},
		},
	})
	assert.True(t, resp.Config.AllowRebalance)
	assert.True(t, paramtable.Get().StreamingCfg.WALBalancerPolicyAllowRebalance.GetAsBool())
	assert.NoError(t, err)
	b.Trigger(ctx)

	resp, err = b.UpdateBalancePolicy(ctx, &streamingpb.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{
			AllowRebalance: false,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{},
		},
		Nodes: &streamingpb.WALBalancePolicyNodes{
			FreezeNodeIds:   []int64{},
			DefreezeNodeIds: []int64{1},
		},
	})
	assert.True(t, resp.Config.AllowRebalance)
	assert.Empty(t, resp.FreezeNodeIds)
	assert.True(t, paramtable.Get().StreamingCfg.WALBalancerPolicyAllowRebalance.GetAsBool())
	assert.NoError(t, err)
	b.Trigger(ctx)

	b.Close()
	assert.ErrorIs(t, f.Get(), balancer.ErrBalancerClosed)
}

func TestBalancer_WithRecoveryLag(t *testing.T) {
	paramtable.Init()
	etcdClient, _ := kvfactory.GetEtcdAndPath()
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager([]string{})

	lag := atomic.NewBool(true)
	streamingNodeManager := mock_manager.NewMockManagerClient(t)
	streamingNodeManager.EXPECT().WatchNodeChanged(mock.Anything).Return(make(chan struct{}), nil)
	streamingNodeManager.EXPECT().Assign(mock.Anything, mock.Anything).Return(nil)
	streamingNodeManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	streamingNodeManager.EXPECT().CollectAllStatus(mock.Anything).RunAndReturn(func(ctx context.Context) (map[int64]*types.StreamingNodeStatus, error) {
		now := time.Now()
		mvccTimeTick := tsoutil.ComposeTSByTime(now, 0)
		recoveryTimeTick := tsoutil.ComposeTSByTime(now.Add(-time.Second*10), 0)
		if !lag.Load() {
			recoveryTimeTick = mvccTimeTick
		}
		return map[int64]*types.StreamingNodeStatus{
			1: {
				StreamingNodeInfo: types.StreamingNodeInfo{
					ServerID: 1,
					Address:  "localhost:1",
				},
				Metrics: types.StreamingNodeMetrics{
					WALMetrics: map[types.ChannelID]types.WALMetrics{
						channel.ChannelID{Name: "test-channel-1"}: types.RWWALMetrics{MVCCTimeTick: mvccTimeTick, RecoveryTimeTick: recoveryTimeTick},
						channel.ChannelID{Name: "test-channel-2"}: types.RWWALMetrics{MVCCTimeTick: mvccTimeTick, RecoveryTimeTick: recoveryTimeTick},
						channel.ChannelID{Name: "test-channel-3"}: types.RWWALMetrics{MVCCTimeTick: mvccTimeTick, RecoveryTimeTick: recoveryTimeTick},
					},
				},
			},
			2: {
				StreamingNodeInfo: types.StreamingNodeInfo{
					ServerID: 2,
					Address:  "localhost:2",
				},
			},
		}, nil
	})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptETCD(etcdClient), resource.OptStreamingCatalog(catalog), resource.OptStreamingManagerClient(streamingNodeManager))
	catalog.EXPECT().GetCChannel(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveCChannel(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveVersion(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Unset()
	catalog.EXPECT().ListPChannel(mock.Anything).RunAndReturn(func(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
		return []*streamingpb.PChannelMeta{
			{
				Channel: &streamingpb.PChannelInfo{
					Name:       "test-channel-1",
					Term:       1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				Node:  &streamingpb.StreamingNodeInfo{ServerId: 1},
			},
			{
				Channel: &streamingpb.PChannelInfo{
					Name:       "test-channel-2",
					Term:       1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				Node:  &streamingpb.StreamingNodeInfo{ServerId: 1},
			},
			{
				Channel: &streamingpb.PChannelInfo{
					Name:       "test-channel-3",
					Term:       1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				Node:  &streamingpb.StreamingNodeInfo{ServerId: 1},
			},
			{
				Channel: &streamingpb.PChannelInfo{
					Name:       "test-channel-4",
					Term:       1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				Node:  &streamingpb.StreamingNodeInfo{ServerId: 2},
			},
		}, nil
	})
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	ctx := context.Background()
	b, err := balancer.RecoverBalancer(ctx, "test-channel-1")
	assert.NoError(t, err)
	assert.NotNil(t, b)

	b.Trigger(context.Background())
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	b.WatchChannelAssignments(ctx2, func(param balancer.WatchChannelAssignmentsCallbackParam) error {
		counts := map[int64]int{}
		for _, relation := range param.Relations {
			assert.Equal(t, relation.Channel.AccessMode, types.AccessModeRW)
			counts[relation.Node.ServerID]++
		}
		assert.Equal(t, 2, len(counts))
		assert.Equal(t, 3, counts[1])
		assert.Equal(t, 1, counts[2])
		return nil
	})

	lag.Store(false)
	b.Trigger(context.Background())
	doneErr := errors.New("done")
	b.WatchChannelAssignments(context.Background(), func(param balancer.WatchChannelAssignmentsCallbackParam) error {
		counts := map[int64]int{}
		for _, relation := range param.Relations {
			assert.Equal(t, relation.Channel.AccessMode, types.AccessModeRW)
			counts[relation.Node.ServerID]++
		}
		if len(counts) == 2 && counts[1] == 2 && counts[2] == 2 {
			return doneErr
		}
		return nil
	})
}
