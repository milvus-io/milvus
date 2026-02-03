package service

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestAssignmentService(t *testing.T) {
	resource.InitForTest()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan")
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	// Set up the balancer
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	})
	b.EXPECT().Close().Return().Maybe()
	balance.Register(b)

	// Set up the broadcaster
	fb := syncutil.NewFuture[broadcaster.Broadcaster]()
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mb := mock_broadcaster.NewMockBroadcaster(t)
	fb.Set(mb)
	mba.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{}, nil).Maybe()
	mba.EXPECT().Close().Return().Maybe()
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(mba, nil).Maybe()
	mb.EXPECT().Ack(mock.Anything, mock.Anything).Return(nil).Maybe()
	mb.EXPECT().LegacyAck(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	// Test assignment discover
	as := NewAssignmentService()
	ss := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	ss.EXPECT().Context().Return(context.Background()).Maybe()
	ss.EXPECT().Recv().Return(nil, io.EOF).Maybe()
	ss.EXPECT().Send(mock.Anything).Return(io.EOF).Maybe()

	err := as.AssignmentDiscover(ss)
	assert.Error(t, err)

	// Test update WAL balance policy
	b.EXPECT().UpdateBalancePolicy(context.Background(), mock.Anything).Return(&streamingpb.UpdateWALBalancePolicyResponse{}, nil).Maybe()
	as.UpdateWALBalancePolicy(context.Background(), &streamingpb.UpdateWALBalancePolicyRequest{})

	// Test update replicate configuration

	// Test illegal replicate configuration
	cfg := &commonpb.ReplicateConfiguration{}
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
	}, nil).Maybe()
	_, err = as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
	})
	assert.Error(t, err)

	//
	cfg = &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			{ClusterId: "test2", Pchannels: []string{"test2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test2:19530", Token: "test2"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev", TargetClusterId: "test2"},
		},
	}

	// Test update pass.
	_, err = as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
	})
	assert.NoError(t, err)

	// Test idempotent
	b.EXPECT().GetLatestChannelAssignment().Unset()
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: cfg,
	}, nil).Maybe()
	_, err = as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
	})
	assert.NoError(t, err)

	// Test secondary path.
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Unset()
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rk ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
		return nil, broadcaster.ErrNotPrimary
	})
	// Still idempotent.
	_, err = as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
	})
	assert.NoError(t, err)

	// Test update on secondary path, it should be block until the replicate configuration is changed.
	b.EXPECT().GetLatestChannelAssignment().Unset()
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
				{ClusterId: "test2", Pchannels: []string{"test2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test2:19530", Token: "test2"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "test2", TargetClusterId: "by-dev"},
			},
		},
	}, nil).Maybe()

	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).Unset()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		select {
		case <-time.After(500 * time.Millisecond):
			return cb(balancer.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: cfg,
			})
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = as.UpdateReplicateConfiguration(ctx, &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
	})
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	_, err = as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
	})
	assert.NoError(t, err)

	// Test callback
	b.EXPECT().UpdateReplicateConfiguration(mock.Anything, mock.Anything).Return(nil)
	msg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: cfg,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	assert.NoError(t, registry.CallMessageAckCallback(context.Background(), msg, map[string]*message.AppendResult{}))
}

func TestForcePromoteValidation(t *testing.T) {
	// Test validateForcePromoteConfiguration function directly

	t.Run("valid_single_cluster_no_topology", func(t *testing.T) {
		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{},
		}
		err := validateForcePromoteConfiguration(cfg, "by-dev")
		assert.NoError(t, err)
	})

	t.Run("valid_single_cluster_nil_topology", func(t *testing.T) {
		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			},
		}
		err := validateForcePromoteConfiguration(cfg, "by-dev")
		assert.NoError(t, err)
	})

	t.Run("invalid_empty_clusters", func(t *testing.T) {
		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{},
		}
		err := validateForcePromoteConfiguration(cfg, "by-dev")
		assert.Error(t, err)
	})

	t.Run("invalid_multiple_clusters_no_topology", func(t *testing.T) {
		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
				{ClusterId: "test2", Pchannels: []string{"test2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test2:19530", Token: "test2"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{},
		}
		err := validateForcePromoteConfiguration(cfg, "by-dev")
		assert.Error(t, err)
		// Config helper validates this as "primary count is not 1"
		assert.Contains(t, err.Error(), "primary count is not 1")
	})

	t.Run("invalid_multiple_clusters_with_topology", func(t *testing.T) {
		// Config with valid topology (passes NewConfigHelper) but multiple clusters
		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
				{ClusterId: "test2", Pchannels: []string{"test2-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test2:19530", Token: "test2"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "by-dev", TargetClusterId: "test2"},
			},
		}
		err := validateForcePromoteConfiguration(cfg, "by-dev")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one cluster")
	})

	t.Run("invalid_with_topology", func(t *testing.T) {
		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "by-dev", TargetClusterId: "test2"},
			},
		}
		err := validateForcePromoteConfiguration(cfg, "by-dev")
		assert.Error(t, err)
		// Config helper validates this as invalid topology/wrong configuration
		assert.Contains(t, err.Error(), "wrong replicate configuration")
	})

	t.Run("invalid_wrong_cluster", func(t *testing.T) {
		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "test2", Pchannels: []string{"test2-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test2:19530", Token: "test2"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{},
		}
		err := validateForcePromoteConfiguration(cfg, "by-dev")
		assert.Error(t, err)
		// Config helper validates this as "current cluster not found"
		assert.Contains(t, err.Error(), "current cluster not found")
	})
}

func TestForcePromoteOnPrimaryCluster(t *testing.T) {
	resource.InitForTest()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	// Current cluster is a secondary, with existing replicate config
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"primary-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530", Token: "primary"}},
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "by-dev"},
			},
		},
	}, nil).Maybe()
	balance.Register(b)

	// Set up broadcaster as PRIMARY (WithResourceKeys succeeds, meaning we are primary)
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mba.EXPECT().Close().Return().Maybe()
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(mba, nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()

	// Force promote on a primary cluster should fail
	forcePromoteCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: forcePromoteCfg,
		ForcePromote:  true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "force promote can only be used on secondary clusters")
}

func TestForcePromoteSuccess(t *testing.T) {
	resource.InitForTest()

	// Set up WAL mock with Broadcast
	mockBroadcastService := mock_streaming.NewMockBroadcast(t)
	mockBroadcastService.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{
		BroadcastID: 1,
		AppendResults: map[string]*types.AppendResult{
			"by-dev-1": {
				MessageID: nil,
				TimeTick:  100,
			},
		},
	}, nil).Maybe()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	mw.EXPECT().Broadcast().Return(mockBroadcastService).Maybe()
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()

	// Current cluster is secondary (has topology from primary to by-dev)
	currentReplicateConfig := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "primary", Pchannels: []string{"primary-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530", Token: "primary"}},
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "primary", TargetClusterId: "by-dev"},
		},
	}
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: currentReplicateConfig,
	}, nil).Maybe()
	b.EXPECT().UpdateReplicateConfiguration(mock.Anything, mock.Anything).Return(nil).Maybe()
	balance.Register(b)

	// Set up broadcaster that returns ErrNotPrimary (we are secondary)
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rk ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
		return nil, broadcaster.ErrNotPrimary
	}).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()

	// Force promote with valid single-cluster config (no topology)
	forcePromoteCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	resp, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: forcePromoteCfg,
		ForcePromote:  true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestForcePromoteIdempotent(t *testing.T) {
	resource.InitForTest()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()

	// Config already applied (same as force promote config)
	sameConfig := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: sameConfig,
	}, nil).Maybe()
	balance.Register(b)

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()

	// Force promote with same configuration should be idempotent
	resp, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: sameConfig,
		ForcePromote:  true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestForcePromoteInvalidConfig(t *testing.T) {
	resource.InitForTest()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()

	// Current cluster is secondary
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"primary-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530", Token: "primary"}},
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "by-dev"},
			},
		},
	}, nil).Maybe()
	balance.Register(b)

	// Set up broadcaster that returns ErrNotPrimary (we are secondary)
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rk ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
		return nil, broadcaster.ErrNotPrimary
	}).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()

	// Force promote with multiple clusters (invalid for force promote)
	invalidCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			{ClusterId: "other", Pchannels: []string{"other-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://other:19530", Token: "other"}},
		},
	}
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: invalidCfg,
		ForcePromote:  true,
	})
	assert.Error(t, err)
}

func TestAlterReplicateConfigCallbackForcePromote(t *testing.T) {
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	// Set up balancer
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	b.EXPECT().UpdateReplicateConfiguration(mock.Anything, mock.Anything).Return(nil).Maybe()
	balance.Register(b)

	// Set up broadcaster that returns no pending messages
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().GetPendingBroadcastMessages().Return(nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	as := NewAssignmentService()
	_ = as

	cfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}

	// Test callback with force promote flag set
	msg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: cfg,
			ForcePromote:           true,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	assert.NoError(t, registry.CallMessageAckCallback(context.Background(), msg, map[string]*message.AppendResult{}))

	// Test callback without force promote (should not call supplement)
	msg2 := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: cfg,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	assert.NoError(t, registry.CallMessageAckCallback(context.Background(), msg2, map[string]*message.AppendResult{}))
}

func TestSupplementIncompleteBroadcasts(t *testing.T) {
	resource.InitForTest()

	t.Run("no_pending_messages", func(t *testing.T) {
		broadcast.ResetBroadcaster()
		snmanager.ResetStreamingNodeManager()

		b := mock_balancer.NewMockBalancer(t)
		b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
		b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		}).Maybe()
		b.EXPECT().Close().Return().Maybe()
		b.EXPECT().UpdateReplicateConfiguration(mock.Anything, mock.Anything).Return(nil).Maybe()
		balance.Register(b)

		mb := mock_broadcaster.NewMockBroadcaster(t)
		mb.EXPECT().GetPendingBroadcastMessages().Return(nil)
		mb.EXPECT().Close().Return().Maybe()
		broadcast.Register(mb)

		mw := mock_streaming.NewMockWALAccesser(t)
		mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
		streaming.SetWALForTest(mw)

		as := &assignmentServiceImpl{}
		err := as.supplementIncompleteBroadcasts(context.Background())
		assert.NoError(t, err)
	})

	t.Run("with_pending_messages_success", func(t *testing.T) {
		broadcast.ResetBroadcaster()
		snmanager.ResetStreamingNodeManager()

		b := mock_balancer.NewMockBalancer(t)
		b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
		b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		}).Maybe()
		b.EXPECT().Close().Return().Maybe()
		balance.Register(b)

		// Create a pending message
		pendingMsg := message.NewCreateDatabaseMessageBuilderV2().
			WithHeader(&message.CreateDatabaseMessageHeader{}).
			WithBody(&message.CreateDatabaseMessageBody{}).
			WithVChannel("v1").
			MustBuildMutable()

		mb := mock_broadcaster.NewMockBroadcaster(t)
		mb.EXPECT().GetPendingBroadcastMessages().Return([]message.MutableMessage{pendingMsg})
		mb.EXPECT().Close().Return().Maybe()
		broadcast.Register(mb)

		mw := mock_streaming.NewMockWALAccesser(t)
		mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).Return(streaming.AppendResponses{
			Responses: []streaming.AppendResponse{
				{
					AppendResult: &types.AppendResult{TimeTick: 100},
				},
			},
		}).Maybe()
		streaming.SetWALForTest(mw)

		as := &assignmentServiceImpl{}
		err := as.supplementIncompleteBroadcasts(context.Background())
		assert.NoError(t, err)
	})

	t.Run("with_pending_messages_failure", func(t *testing.T) {
		broadcast.ResetBroadcaster()
		snmanager.ResetStreamingNodeManager()

		b := mock_balancer.NewMockBalancer(t)
		b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
		b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		}).Maybe()
		b.EXPECT().Close().Return().Maybe()
		balance.Register(b)

		// Create pending messages
		pendingMsg := message.NewCreateDatabaseMessageBuilderV2().
			WithHeader(&message.CreateDatabaseMessageHeader{}).
			WithBody(&message.CreateDatabaseMessageBody{}).
			WithVChannel("v1").
			MustBuildMutable()

		mb := mock_broadcaster.NewMockBroadcaster(t)
		mb.EXPECT().GetPendingBroadcastMessages().Return([]message.MutableMessage{pendingMsg})
		mb.EXPECT().Close().Return().Maybe()
		broadcast.Register(mb)

		appendErr := errors.New("append failed")
		mw := mock_streaming.NewMockWALAccesser(t)
		mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).Return(streaming.AppendResponses{
			Responses: []streaming.AppendResponse{
				{
					Error: appendErr,
				},
			},
		}).Maybe()
		streaming.SetWALForTest(mw)

		as := &assignmentServiceImpl{}
		err := as.supplementIncompleteBroadcasts(context.Background())
		assert.Error(t, err)
	})
}

func TestForcePromoteBroadcastOtherError(t *testing.T) {
	resource.InitForTest()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()

	// Current cluster is secondary
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"primary-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530", Token: "primary"}},
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "by-dev"},
			},
		},
	}, nil).Maybe()
	balance.Register(b)

	// Broadcaster returns a non-ErrNotPrimary error
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(nil, errors.New("broadcaster unavailable"))
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	forcePromoteCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: forcePromoteCfg,
		ForcePromote:  true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "broadcaster unavailable")
}

func TestForcePromoteBroadcastAppendError(t *testing.T) {
	resource.InitForTest()

	// Set up WAL mock where Broadcast().Append() fails
	mockBroadcastService := mock_streaming.NewMockBroadcast(t)
	mockBroadcastService.EXPECT().Append(mock.Anything, mock.Anything).Return(nil, errors.New("broadcast append failed"))

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	mw.EXPECT().Broadcast().Return(mockBroadcastService).Maybe()
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()

	currentReplicateConfig := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "primary", Pchannels: []string{"primary-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530", Token: "primary"}},
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "primary", TargetClusterId: "by-dev"},
		},
	}
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: currentReplicateConfig,
	}, nil).Maybe()
	balance.Register(b)

	// Broadcaster returns ErrNotPrimary (we are secondary)
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rk ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
		return nil, broadcaster.ErrNotPrimary
	}).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	forcePromoteCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: forcePromoteCfg,
		ForcePromote:  true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "broadcast append failed")
}

func TestForcePromoteUpdateConfigError(t *testing.T) {
	resource.InitForTest()

	// Set up WAL mock with Broadcast that succeeds
	mockBroadcastService := mock_streaming.NewMockBroadcast(t)
	mockBroadcastService.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{
		BroadcastID: 1,
		AppendResults: map[string]*types.AppendResult{
			"by-dev-1": {TimeTick: 100},
		},
	}, nil)

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	mw.EXPECT().Broadcast().Return(mockBroadcastService).Maybe()
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()

	currentReplicateConfig := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "primary", Pchannels: []string{"primary-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530", Token: "primary"}},
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "primary", TargetClusterId: "by-dev"},
		},
	}
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: currentReplicateConfig,
	}, nil).Maybe()
	// UpdateReplicateConfiguration fails
	b.EXPECT().UpdateReplicateConfiguration(mock.Anything, mock.Anything).Return(errors.New("update config failed"))
	balance.Register(b)

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rk ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
		return nil, broadcaster.ErrNotPrimary
	}).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	forcePromoteCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: forcePromoteCfg,
		ForcePromote:  true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update config failed")
}

func TestAlterReplicateConfigCallbackErrors(t *testing.T) {
	resource.InitForTest()

	t.Run("update_config_error", func(t *testing.T) {
		broadcast.ResetBroadcaster()
		snmanager.ResetStreamingNodeManager()

		b := mock_balancer.NewMockBalancer(t)
		b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
		b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		}).Maybe()
		b.EXPECT().Close().Return().Maybe()
		b.EXPECT().UpdateReplicateConfiguration(mock.Anything, mock.Anything).Return(errors.New("update failed"))
		balance.Register(b)

		mb := mock_broadcaster.NewMockBroadcaster(t)
		mb.EXPECT().Close().Return().Maybe()
		broadcast.Register(mb)

		mw := mock_streaming.NewMockWALAccesser(t)
		mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
		streaming.SetWALForTest(mw)

		as := NewAssignmentService()
		_ = as

		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			},
		}
		msg := message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ReplicateConfiguration: cfg,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"v1"}).
			MustBuildBroadcast()
		err := registry.CallMessageAckCallback(context.Background(), msg, map[string]*message.AppendResult{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "update failed")
	})

	t.Run("force_promote_supplement_error", func(t *testing.T) {
		broadcast.ResetBroadcaster()
		snmanager.ResetStreamingNodeManager()

		b := mock_balancer.NewMockBalancer(t)
		b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
		b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		}).Maybe()
		b.EXPECT().Close().Return().Maybe()
		b.EXPECT().UpdateReplicateConfiguration(mock.Anything, mock.Anything).Return(nil).Maybe()
		balance.Register(b)

		// Create pending message that fails
		pendingMsg := message.NewCreateDatabaseMessageBuilderV2().
			WithHeader(&message.CreateDatabaseMessageHeader{}).
			WithBody(&message.CreateDatabaseMessageBody{}).
			WithVChannel("v1").
			MustBuildMutable()

		mb := mock_broadcaster.NewMockBroadcaster(t)
		mb.EXPECT().GetPendingBroadcastMessages().Return([]message.MutableMessage{pendingMsg})
		mb.EXPECT().Close().Return().Maybe()
		broadcast.Register(mb)

		mw := mock_streaming.NewMockWALAccesser(t)
		mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).Return(streaming.AppendResponses{
			Responses: []streaming.AppendResponse{
				{Error: errors.New("supplement failed")},
			},
		})
		streaming.SetWALForTest(mw)

		as := NewAssignmentService()
		_ = as

		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			},
		}
		// Force promote callback
		msg := message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ReplicateConfiguration: cfg,
				ForcePromote:           true,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"v1"}).
			MustBuildBroadcast()
		err := registry.CallMessageAckCallback(context.Background(), msg, map[string]*message.AppendResult{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "supplement failed")
	})
}

func TestUpdateReplicateConfigNonPrimaryBroadcastError(t *testing.T) {
	resource.InitForTest()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
	}, nil).Maybe()
	balance.Register(b)

	// Broadcaster returns a non-ErrNotPrimary error (not force promote path)
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(nil, errors.New("unexpected error"))
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	cfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			{ClusterId: "test2", Pchannels: []string{"test2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test2:19530", Token: "test2"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev", TargetClusterId: "test2"},
		},
	}
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected error")
}

func TestUpdateReplicateConfigBroadcastError(t *testing.T) {
	resource.InitForTest()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
	}, nil).Maybe()
	balance.Register(b)

	// Broadcaster acquires lock but Broadcast() fails
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, errors.New("broadcast failed"))
	mba.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(mba, nil)
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	cfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			{ClusterId: "test2", Pchannels: []string{"test2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test2:19530", Token: "test2"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev", TargetClusterId: "test2"},
		},
	}
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "broadcast failed")
}

func TestUpdateReplicateConfigSecondValidateSameConfig(t *testing.T) {
	// Tests the path where the second validateReplicateConfiguration returns errReplicateConfigurationSame
	// (config became same between first check and getting the lock)
	resource.InitForTest()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	callCount := 0
	cfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			{ClusterId: "test2", Pchannels: []string{"test2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test2:19530", Token: "test2"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev", TargetClusterId: "test2"},
		},
	}

	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	b.EXPECT().GetLatestChannelAssignment().RunAndReturn(func() (*balancer.WatchChannelAssignmentsCallbackParam, error) {
		callCount++
		if callCount <= 1 {
			// First call: config is different (nil)
			return &balancer.WatchChannelAssignmentsCallbackParam{
				PChannelView: &channel.PChannelView{
					Channels: map[channel.ChannelID]*channel.PChannelMeta{
						{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
					},
				},
			}, nil
		}
		// Second call: config is now the same (was applied by another path)
		return &balancer.WatchChannelAssignmentsCallbackParam{
			PChannelView: &channel.PChannelView{
				Channels: map[channel.ChannelID]*channel.PChannelMeta{
					{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
				},
			},
			ReplicateConfiguration: cfg,
		}, nil
	})
	balance.Register(b)

	// Broadcaster acquires lock successfully
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(mba, nil)
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	resp, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestAssignmentDiscoverBalanceError(t *testing.T) {
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager() // Reset so GetWithContext fails with canceled context

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	ss := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	ss.EXPECT().Context().Return(ctx).Maybe()

	err := as.AssignmentDiscover(ss)
	assert.Error(t, err)
}

func TestUpdateWALBalancePolicyError(t *testing.T) {
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager() // Reset so GetWithContext fails with canceled context

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so GetWithContext fails

	_, err := as.UpdateWALBalancePolicy(ctx, &streamingpb.UpdateWALBalancePolicyRequest{})
	assert.Error(t, err)
}

func TestWaitUntilPrimaryChangeBalanceError(t *testing.T) {
	// Covers waitUntilPrimaryChangeOrConfigurationSame balance.GetWithContext error (line 123-125)
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager() // Reset so GetWithContext fails

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := &assignmentServiceImpl{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	err := as.waitUntilPrimaryChangeOrConfigurationSame(ctx, cfg)
	assert.Error(t, err)
}

func TestHandleForcePromoteBalanceError(t *testing.T) {
	// Covers handleForcePromote balance.GetWithContext error (lines 241-243)
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager() // Reset so GetWithContext fails

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	_, err := as.UpdateReplicateConfiguration(ctx, &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
		ForcePromote:  true,
	})
	assert.Error(t, err)
}

func TestHandleForcePromoteGetAssignmentError(t *testing.T) {
	// Covers handleForcePromote GetLatestChannelAssignment error (lines 246-248)
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	// First call for validateReplicateConfiguration succeeds, second call in handleForcePromote fails
	callCount := 0
	b.EXPECT().GetLatestChannelAssignment().RunAndReturn(func() (*balancer.WatchChannelAssignmentsCallbackParam, error) {
		callCount++
		if callCount <= 1 {
			return &balancer.WatchChannelAssignmentsCallbackParam{
				PChannelView: &channel.PChannelView{
					Channels: map[channel.ChannelID]*channel.PChannelMeta{
						{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
					},
				},
			}, nil
		}
		return nil, errors.New("assignment error")
	})
	balance.Register(b)

	// Broadcaster returns ErrNotPrimary (we are secondary)
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rk ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
		return nil, broadcaster.ErrNotPrimary
	}).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	cfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
		ForcePromote:  true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "assignment error")
}

func TestHandleForcePromoteSameConfigAfterBroadcasterCheck(t *testing.T) {
	// Covers handleForcePromote double-check same config (lines 273-276)
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	forcePromoteCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	callCount := 0
	b.EXPECT().GetLatestChannelAssignment().RunAndReturn(func() (*balancer.WatchChannelAssignmentsCallbackParam, error) {
		callCount++
		if callCount <= 1 {
			// First call: config is different (has secondary setup)
			return &balancer.WatchChannelAssignmentsCallbackParam{
				PChannelView: &channel.PChannelView{
					Channels: map[channel.ChannelID]*channel.PChannelMeta{
						{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
					},
				},
				ReplicateConfiguration: &commonpb.ReplicateConfiguration{
					Clusters: []*commonpb.MilvusCluster{
						{ClusterId: "primary", Pchannels: []string{"primary-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530", Token: "primary"}},
						{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
					},
					CrossClusterTopology: []*commonpb.CrossClusterTopology{
						{SourceClusterId: "primary", TargetClusterId: "by-dev"},
					},
				},
			}, nil
		}
		// Second call (in handleForcePromote): config is now the same
		return &balancer.WatchChannelAssignmentsCallbackParam{
			PChannelView: &channel.PChannelView{
				Channels: map[channel.ChannelID]*channel.PChannelMeta{
					{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
				},
			},
			ReplicateConfiguration: forcePromoteCfg,
		}, nil
	})
	balance.Register(b)

	// Broadcaster returns ErrNotPrimary
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rk ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
		return nil, broadcaster.ErrNotPrimary
	}).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	resp, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: forcePromoteCfg,
		ForcePromote:  true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestHandleForcePromoteValidatorError(t *testing.T) {
	// Covers handleForcePromote validator.Validate() error (lines 285-288)
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	callCount := 0
	b.EXPECT().GetLatestChannelAssignment().RunAndReturn(func() (*balancer.WatchChannelAssignmentsCallbackParam, error) {
		callCount++
		if callCount <= 1 {
			// First call: different config (secondary setup)
			return &balancer.WatchChannelAssignmentsCallbackParam{
				PChannelView: &channel.PChannelView{
					Channels: map[channel.ChannelID]*channel.PChannelMeta{
						{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
					},
				},
				ReplicateConfiguration: &commonpb.ReplicateConfiguration{
					Clusters: []*commonpb.MilvusCluster{
						{ClusterId: "primary", Pchannels: []string{"primary-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530", Token: "primary"}},
						{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
					},
					CrossClusterTopology: []*commonpb.CrossClusterTopology{
						{SourceClusterId: "primary", TargetClusterId: "by-dev"},
					},
				},
			}, nil
		}
		// Second call in handleForcePromote: return config with missing pchannel mapping
		// so that validator fails (pchannel "by-dev-1" not mapped to new config's cluster pchannels)
		return &balancer.WatchChannelAssignmentsCallbackParam{
			PChannelView: &channel.PChannelView{
				Channels: map[channel.ChannelID]*channel.PChannelMeta{
					{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
					{Name: "other-1"}:  channel.NewPChannelMeta("other-1", types.AccessModeRW),
				},
			},
			ReplicateConfiguration: &commonpb.ReplicateConfiguration{
				Clusters: []*commonpb.MilvusCluster{
					{ClusterId: "primary", Pchannels: []string{"primary-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530", Token: "primary"}},
					{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
				},
				CrossClusterTopology: []*commonpb.CrossClusterTopology{
					{SourceClusterId: "primary", TargetClusterId: "by-dev"},
				},
			},
		}, nil
	})
	balance.Register(b)

	// Broadcaster returns ErrNotPrimary
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rk ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
		return nil, broadcaster.ErrNotPrimary
	}).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	cfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
		ForcePromote:  true,
	})
	assert.Error(t, err)
}

func TestSecondValidateNonSameError(t *testing.T) {
	// Covers line 111: second validateReplicateConfiguration returns non-same error
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	callCount := 0
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	b.EXPECT().GetLatestChannelAssignment().RunAndReturn(func() (*balancer.WatchChannelAssignmentsCallbackParam, error) {
		callCount++
		if callCount <= 1 {
			return &balancer.WatchChannelAssignmentsCallbackParam{
				PChannelView: &channel.PChannelView{
					Channels: map[channel.ChannelID]*channel.PChannelMeta{
						{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
					},
				},
			}, nil
		}
		// Second call after lock: return error
		return nil, errors.New("assignment unavailable")
	})
	balance.Register(b)

	// Broadcaster lock succeeds
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(mba, nil)
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	cfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
			{ClusterId: "test2", Pchannels: []string{"test2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test2:19530", Token: "test2"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev", TargetClusterId: "test2"},
		},
	}
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: cfg,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "assignment unavailable")
}

func TestAlterReplicateConfigCallbackBalanceError(t *testing.T) {
	// Covers alterReplicateConfiguration balance.GetWithContext error (lines 345-347)
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	_ = as

	cfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	msg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: cfg,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := registry.CallMessageAckCallback(ctx, msg, map[string]*message.AppendResult{})
	assert.Error(t, err)
}

func TestSupplementIncompleteBroadcastsGetBroadcasterError(t *testing.T) {
	// Covers supplementIncompleteBroadcasts broadcast.GetWithContext error (lines 379-381)
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	streaming.SetWALForTest(mw)

	as := &assignmentServiceImpl{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := as.supplementIncompleteBroadcasts(ctx)
	assert.Error(t, err)
}

func TestForcePromoteMultiplePChannels(t *testing.T) {
	// Covers line 295: non-control-channel path in handleForcePromote broadcastPChannels
	// and line 166: non-control-channel path in validateReplicateConfiguration
	resource.InitForTest()

	mockBroadcastService := mock_streaming.NewMockBroadcast(t)
	mockBroadcastService.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{
		BroadcastID: 1,
		AppendResults: map[string]*types.AppendResult{
			"by-dev-1": {TimeTick: 100},
			"by-dev-2": {TimeTick: 101},
		},
	}, nil).Maybe()

	mw := mock_streaming.NewMockWALAccesser(t)
	mw.EXPECT().ControlChannel().Return("by-dev-1_vcchan").Maybe()
	mw.EXPECT().Broadcast().Return(mockBroadcastService).Maybe()
	streaming.SetWALForTest(mw)

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()

	currentReplicateConfig := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "primary", Pchannels: []string{"primary-1", "primary-2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530", Token: "primary"}},
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1", "by-dev-2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "primary", TargetClusterId: "by-dev"},
		},
	}
	// Multiple pchannels - by-dev-1 matches control channel, by-dev-2 does not
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
				{Name: "by-dev-2"}: channel.NewPChannelMeta("by-dev-2", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: currentReplicateConfig,
	}, nil).Maybe()
	b.EXPECT().UpdateReplicateConfiguration(mock.Anything, mock.Anything).Return(nil).Maybe()
	balance.Register(b)

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rk ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
		return nil, broadcaster.ErrNotPrimary
	}).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	forcePromoteCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-1", "by-dev-2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://test:19530", Token: "by-dev"}},
		},
	}
	resp, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: forcePromoteCfg,
		ForcePromote:  true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}
