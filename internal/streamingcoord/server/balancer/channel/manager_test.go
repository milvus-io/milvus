package channel

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestChannelManager(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))

	ctx := context.Background()
	// Test recover failure.
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return(nil, errors.New("recover failure"))
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	m, err := RecoverChannelManager(ctx)
	assert.Nil(t, m)
	assert.Error(t, err)

	catalog.EXPECT().ListPChannel(mock.Anything).Unset()
	catalog.EXPECT().ListPChannel(mock.Anything).RunAndReturn(func(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
		return []*streamingpb.PChannelMeta{
			{
				Channel: &streamingpb.PChannelInfo{
					Name: "test-channel",
					Term: 1,
				},
				Node: &streamingpb.StreamingNodeInfo{
					ServerId: 1,
				},
			},
		}, nil
	})
	m, err = RecoverChannelManager(ctx)
	assert.NotNil(t, m)
	assert.NoError(t, err)

	// Test update non exist pchannel
	modified, err := m.AssignPChannels(ctx, map[ChannelID]types.PChannelInfoAssigned{newChannelID("non-exist-channel"): {
		Channel: types.PChannelInfo{
			Name:       "non-exist-channel",
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
		Node: types.StreamingNodeInfo{ServerID: 2},
	}})
	assert.Nil(t, modified)
	assert.ErrorIs(t, err, ErrChannelNotExist)
	err = m.AssignPChannelsDone(ctx, []ChannelID{newChannelID("non-exist-channel")})
	assert.ErrorIs(t, err, ErrChannelNotExist)
	err = m.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "non-exist-channel",
		Term: 2,
	}})
	assert.ErrorIs(t, err, ErrChannelNotExist)

	// Test success.
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Unset()
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pm []*streamingpb.PChannelMeta) error {
		return nil
	})
	modified, err = m.AssignPChannels(ctx, map[ChannelID]types.PChannelInfoAssigned{newChannelID("test-channel"): {
		Channel: types.PChannelInfo{
			Name:       "test-channel",
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
		Node: types.StreamingNodeInfo{ServerID: 2},
	}})
	assert.NotNil(t, modified)
	assert.NoError(t, err)
	assert.Len(t, modified, 1)
	err = m.AssignPChannelsDone(ctx, []ChannelID{newChannelID("test-channel")})
	assert.NoError(t, err)

	nodeID, ok := m.GetLatestWALLocated(ctx, "test-channel")
	assert.True(t, ok)
	assert.NotZero(t, nodeID)

	err = m.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "test-channel",
		Term: 2,
	}})
	assert.NoError(t, err)

	view := m.CurrentPChannelsView()
	assert.NotNil(t, view)
	assert.Equal(t, len(view.Channels), 1)
	channel, ok := view.Channels[newChannelID("test-channel")]
	assert.True(t, ok)
	assert.NotNil(t, channel)

	nodeID, ok = m.GetLatestWALLocated(ctx, "test-channel")
	assert.False(t, ok)
	assert.Zero(t, nodeID)

	t.Run("UpdateReplicateConfiguration", func(t *testing.T) {
		param, err := m.GetLatestChannelAssignment()
		oldLocalVersion := param.Version.Local
		assert.NoError(t, err)
		assert.Equal(t, m.ReplicateRole(), replicateutil.RolePrimary)

		// Test update replicate configurations
		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-test-channel-1", "by-dev-test-channel-2"}},
				{ClusterId: "by-dev2", Pchannels: []string{"by-dev2-test-channel-1", "by-dev2-test-channel-2"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "by-dev", TargetClusterId: "by-dev2"},
			},
		}
		msg := message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ReplicateConfiguration: cfg,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"by-dev-test-channel-1", "by-dev-test-channel-2"}).
			MustBuildBroadcast()

		result := message.BroadcastResultAlterReplicateConfigMessageV2{
			Message: message.MustAsBroadcastAlterReplicateConfigMessageV2(msg),
			Results: map[string]*message.AppendResult{
				"by-dev-test-channel-1": {
					MessageID:              walimplstest.NewTestMessageID(1),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(2),
					TimeTick:               1,
				},
				"by-dev-test-channel-2": {
					MessageID:              walimplstest.NewTestMessageID(3),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(4),
					TimeTick:               1,
				},
			},
		}

		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
				assert.True(t, proto.Equal(config.ReplicateConfiguration, cfg))
				assert.Len(t, replicatingTasks, 2)
				for _, task := range replicatingTasks {
					result := result.Results[task.GetSourceChannelName()]
					assert.True(t, result.LastConfirmedMessageID.EQ(message.MustUnmarshalMessageID(task.InitializedCheckpoint.MessageId)))
					assert.Equal(t, result.TimeTick, task.InitializedCheckpoint.TimeTick)
					assert.Equal(t, task.GetTargetChannelName(), strings.Replace(task.GetSourceChannelName(), "by-dev", "by-dev2", 1))
					assert.Equal(t, task.GetTargetCluster().GetClusterId(), "by-dev2")
				}
				return nil
			})
		err = m.UpdateReplicateConfiguration(ctx, result)
		assert.NoError(t, err)
		param, err = m.GetLatestChannelAssignment()
		assert.Equal(t, param.Version.Local, oldLocalVersion+1)
		assert.NoError(t, err)
		assert.Equal(t, m.ReplicateRole(), replicateutil.RolePrimary)

		// test idempotency
		err = m.UpdateReplicateConfiguration(ctx, result)
		assert.NoError(t, err)
		param, err = m.GetLatestChannelAssignment()
		assert.Equal(t, param.Version.Local, oldLocalVersion+1)
		assert.NoError(t, err)
		assert.Equal(t, m.ReplicateRole(), replicateutil.RolePrimary)

		// TODO: support add new pchannels into existing clusters.
		// Add more pchannels into existing clusters.
		// 	Clusters: []*commonpb.MilvusCluster{
		// 		{ClusterId: "by-dev", Pchannels: []string{"by-dev-test-channel-1", "by-dev-test-channel-2", "by-dev-test-channel-3"}},
		// 		{ClusterId: "by-dev2", Pchannels: []string{"by-dev2-test-channel-1", "by-dev2-test-channel-2", "by-dev2-test-channel-3"}},
		// 	},
		// 	CrossClusterTopology: []*commonpb.CrossClusterTopology{
		// 		{SourceClusterId: "by-dev", TargetClusterId: "by-dev2"},
		// 	},
		// }
		// msg = message.NewAlterReplicateConfigMessageBuilderV2().
		// 	WithHeader(&message.AlterReplicateConfigMessageHeader{
		// 		ReplicateConfiguration: cfg,
		// 	}).
		// 	WithBody(&message.AlterReplicateConfigMessageBody{}).
		// 	WithBroadcast([]string{"by-dev-test-channel-1", "by-dev-test-channel-2", "by-dev-test-channel-3"}).
		// 	MustBuildBroadcast()
		// result = message.BroadcastResultAlterReplicateConfigMessageV2{
		// 	Message: message.MustAsBroadcastAlterReplicateConfigMessageV2(msg),
		// 	Results: map[string]*message.AppendResult{
		// 		"by-dev-test-channel-1": {
		// 			MessageID:              walimplstest.NewTestMessageID(1),
		// 			LastConfirmedMessageID: walimplstest.NewTestMessageID(2),
		// 			TimeTick:               1,
		// 		},
		// 		"by-dev-test-channel-2": {
		// 			MessageID:              walimplstest.NewTestMessageID(3),
		// 			LastConfirmedMessageID: walimplstest.NewTestMessageID(4),
		// 			TimeTick:               1,
		// 		},
		// 		"by-dev-test-channel-3": {
		// 			MessageID:              walimplstest.NewTestMessageID(5),
		// 			LastConfirmedMessageID: walimplstest.NewTestMessageID(6),
		// 			TimeTick:               1,
		// 		},
		// 	},
		// }
		// catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).Unset()
		// catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		// 	func(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
		// 		assert.True(t, proto.Equal(config.ReplicateConfiguration, cfg))
		// 		assert.Len(t, replicatingTasks, 1) // here should be two new incoming tasks.
		// 		for _, task := range replicatingTasks {
		// 			assert.Equal(t, task.GetSourceChannelName(), "by-dev-test-channel-3")
		// 			result := result.Results[task.GetSourceChannelName()]
		// 			assert.True(t, result.LastConfirmedMessageID.EQ(message.MustUnmarshalMessageID(task.InitializedCheckpoint.MessageId)))
		// 			assert.Equal(t, result.TimeTick, task.InitializedCheckpoint.TimeTick)
		// 			assert.Equal(t, task.GetTargetChannelName(), strings.Replace(task.GetSourceChannelName(), "by-dev", "by-dev2", 1))
		// 			assert.Equal(t, task.GetTargetCluster().GetClusterId(), "by-dev2")
		// 		}
		// 		return nil
		// 	})

		// err = m.UpdateReplicateConfiguration(ctx, result)
		// assert.NoError(t, err)

		// Add new cluster into existing config.
		cfg = &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-test-channel-1", "by-dev-test-channel-2"}},
				{ClusterId: "by-dev2", Pchannels: []string{"by-dev2-test-channel-1", "by-dev2-test-channel-2"}},
				{ClusterId: "by-dev3", Pchannels: []string{"by-dev3-test-channel-1", "by-dev3-test-channel-2"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "by-dev", TargetClusterId: "by-dev2"},
				{SourceClusterId: "by-dev", TargetClusterId: "by-dev3"},
			},
		}
		msg = message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ReplicateConfiguration: cfg,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"by-dev-test-channel-1", "by-dev-test-channel-2"}).
			MustBuildBroadcast()
		result = message.BroadcastResultAlterReplicateConfigMessageV2{
			Message: message.MustAsBroadcastAlterReplicateConfigMessageV2(msg),
			Results: map[string]*message.AppendResult{
				"by-dev-test-channel-1": {
					MessageID:              walimplstest.NewTestMessageID(1),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(2),
					TimeTick:               1,
				},
				"by-dev-test-channel-2": {
					MessageID:              walimplstest.NewTestMessageID(3),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(4),
					TimeTick:               1,
				},
			},
		}
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).Unset()
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
				assert.True(t, proto.Equal(config.ReplicateConfiguration, cfg))
				assert.Len(t, replicatingTasks, 2) // here should be two new incoming tasks.
				for _, task := range replicatingTasks {
					assert.Equal(t, task.GetTargetCluster().GetClusterId(), "by-dev3")
					result := result.Results[task.GetSourceChannelName()]
					assert.True(t, result.LastConfirmedMessageID.EQ(message.MustUnmarshalMessageID(task.InitializedCheckpoint.MessageId)))
					assert.Equal(t, result.TimeTick, task.InitializedCheckpoint.TimeTick)
					assert.Equal(t, task.GetTargetChannelName(), strings.Replace(task.GetSourceChannelName(), "by-dev", "by-dev3", 1))
					assert.Equal(t, task.GetTargetCluster().GetClusterId(), "by-dev3")
				}
				return nil
			})

		err = m.UpdateReplicateConfiguration(ctx, result)
		assert.NoError(t, err)

		param, err = m.GetLatestChannelAssignment()
		assert.NoError(t, err)
		assert.Equal(t, param.Version.Local, oldLocalVersion+2)
		assert.True(t, proto.Equal(param.ReplicateConfiguration, cfg))
		assert.Equal(t, m.ReplicateRole(), replicateutil.RolePrimary)

		// switch into secondary
		cfg = &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-test-channel-1", "by-dev-test-channel-2"}},
				{ClusterId: "by-dev2", Pchannels: []string{"by-dev2-test-channel-1", "by-dev2-test-channel-2"}},
				{ClusterId: "by-dev3", Pchannels: []string{"by-dev3-test-channel-1", "by-dev3-test-channel-2"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "by-dev2", TargetClusterId: "by-dev"},
				{SourceClusterId: "by-dev2", TargetClusterId: "by-dev3"},
			},
		}
		msg = message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ReplicateConfiguration: cfg,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"by-dev-test-channel-1", "by-dev-test-channel-2"}).
			MustBuildBroadcast()
		result = message.BroadcastResultAlterReplicateConfigMessageV2{
			Message: message.MustAsBroadcastAlterReplicateConfigMessageV2(msg),
			Results: map[string]*message.AppendResult{
				"by-dev-test-channel-1": {
					MessageID:              walimplstest.NewTestMessageID(1),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(2),
					TimeTick:               1,
				},
				"by-dev-test-channel-2": {
					MessageID:              walimplstest.NewTestMessageID(3),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(4),
					TimeTick:               1,
				},
			},
		}
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).Unset()
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
				assert.True(t, proto.Equal(config.ReplicateConfiguration, cfg))
				assert.Len(t, replicatingTasks, 0) // here should be two new incoming tasks.
				return nil
			})
		err = m.UpdateReplicateConfiguration(ctx, result)
		assert.NoError(t, err)
		err = m.UpdateReplicateConfiguration(ctx, result)
		assert.NoError(t, err)

		param, err = m.GetLatestChannelAssignment()
		assert.NoError(t, err)
		assert.Equal(t, param.Version.Local, oldLocalVersion+3)
		assert.True(t, proto.Equal(param.ReplicateConfiguration, cfg))
		assert.Equal(t, m.ReplicateRole(), replicateutil.RoleSecondary)
	})
}

func TestAllocVirtualChannels(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	// Test recover failure.
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test-channel",
	}, nil).Maybe()
	catalog.EXPECT().GetVersion(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveVersion(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().ListPChannel(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil).Maybe()

	ctx := context.Background()
	newIncomingTopics := util.GetAllTopicsFromConfiguration()
	m, err := RecoverChannelManager(ctx, newIncomingTopics.Collect()...)
	assert.NoError(t, err)
	assert.NotNil(t, m)

	allocVChannels, err := m.AllocVirtualChannels(ctx, AllocVChannelParam{
		CollectionID: 1,
		Num:          256,
	})
	assert.Error(t, err)
	assert.Nil(t, allocVChannels, 0)

	StaticPChannelStatsManager.Get().AddVChannel("by-dev-rootcoord-dml_0_100v0", "by-dev-rootcoord-dml_0_101v0", "by-dev-rootcoord-dml_1_100v1")

	allocVChannels, err = m.AllocVirtualChannels(ctx, AllocVChannelParam{
		CollectionID: 1,
		Num:          4,
	})
	assert.NoError(t, err)
	assert.Len(t, allocVChannels, 4)
	assert.Equal(t, allocVChannels[0], "by-dev-rootcoord-dml_10_1v0")
	assert.Equal(t, allocVChannels[1], "by-dev-rootcoord-dml_11_1v1")
	assert.Equal(t, allocVChannels[2], "by-dev-rootcoord-dml_12_1v2")
	assert.Equal(t, allocVChannels[3], "by-dev-rootcoord-dml_13_1v3")
}

func TestStreamingEnableChecker(t *testing.T) {
	ctx := context.Background()
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	// Test recover failure.
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test-channel",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveVersion(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return(nil, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	m, err := RecoverChannelManager(ctx, "test-channel")
	assert.NoError(t, err)

	assert.False(t, m.IsStreamingEnabledOnce())

	n := syncutil.NewAsyncTaskNotifier[struct{}]()
	m.RegisterStreamingEnabledNotifier(n)
	assert.NoError(t, n.Context().Err())

	go func() {
		defer n.Finish(struct{}{})
		<-n.Context().Done()
	}()

	err = m.MarkStreamingHasEnabled(ctx)
	assert.NoError(t, err)

	n2 := syncutil.NewAsyncTaskNotifier[struct{}]()
	m.RegisterStreamingEnabledNotifier(n2)
	assert.Error(t, n.Context().Err())
	assert.Error(t, n2.Context().Err())
}

func TestChannelManagerWatch(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test-channel",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Unset()
	catalog.EXPECT().ListPChannel(mock.Anything).RunAndReturn(func(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
		return []*streamingpb.PChannelMeta{
			{
				Channel: &streamingpb.PChannelInfo{
					Name: "test-channel",
					Term: 1,
				},
				Node: &streamingpb.StreamingNodeInfo{
					ServerId: 1,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		}, nil
	})
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	manager, err := RecoverChannelManager(context.Background())
	assert.NoError(t, err)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	called := make(chan struct{}, 1)
	go func() {
		defer close(done)
		err := manager.WatchAssignmentResult(ctx, func(param WatchChannelAssignmentsCallbackParam) error {
			select {
			case called <- struct{}{}:
			default:
			}
			return nil
		})
		assert.ErrorIs(t, err, context.Canceled)
	}()

	manager.AssignPChannels(ctx, map[ChannelID]types.PChannelInfoAssigned{newChannelID("test-channel"): {
		Channel: types.PChannelInfo{
			Name:       "test-channel",
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
		Node: types.StreamingNodeInfo{ServerID: 2},
	}})
	manager.AssignPChannelsDone(ctx, []ChannelID{newChannelID("test-channel")})

	<-called
	manager.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "test-channel",
		Term: 2,
	}})
	<-called
	cancel()
	<-done
}

func newChannelID(name string) ChannelID {
	return ChannelID{
		Name: name,
	}
}
