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
	// Test force promote validation - force promote requires empty cluster and topology fields
	// The configuration is auto-constructed from the current cluster's existing meta

	t.Run("invalid_non_empty_clusters", func(t *testing.T) {
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

		mb := mock_broadcaster.NewMockBroadcaster(t)
		mb.EXPECT().Close().Return().Maybe()
		broadcast.Register(mb)

		as := NewAssignmentService()

		// Force promote with non-empty clusters should fail
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
		assert.Contains(t, err.Error(), "force promote requires empty cluster and topology fields")
	})

	t.Run("invalid_non_empty_topology", func(t *testing.T) {
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

		mb := mock_broadcaster.NewMockBroadcaster(t)
		mb.EXPECT().Close().Return().Maybe()
		broadcast.Register(mb)

		as := NewAssignmentService()

		// Force promote with non-empty topology should fail
		cfg := &commonpb.ReplicateConfiguration{
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "by-dev", TargetClusterId: "test2"},
			},
		}
		_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
			Configuration: cfg,
			ForcePromote:  true,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "force promote requires empty cluster and topology fields")
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
	// Current cluster is a primary
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
	}, nil).Maybe()
	balance.Register(b)

	// Set up broadcaster to return ErrNotSecondary (we are primary, force promote should fail)
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithSecondaryClusterResourceKey(mock.Anything).Return(nil, broadcaster.ErrNotSecondary).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()

	// Force promote on a primary cluster should fail - use empty config
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: &commonpb.ReplicateConfiguration{},
		ForcePromote:  true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "force promote can only be used on secondary clusters")
}

func TestForcePromoteSuccess(t *testing.T) {
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
	balance.Register(b)

	// Set up broadcaster with WithSecondaryClusterResourceKey (we are secondary)
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{
		BroadcastID: 1,
		AppendResults: map[string]*types.AppendResult{
			"by-dev-1": {TimeTick: 100},
		},
	}, nil).Maybe()
	mba.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithSecondaryClusterResourceKey(mock.Anything).Return(mba, nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()

	// Force promote with empty config (configuration is auto-constructed)
	resp, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: &commonpb.ReplicateConfiguration{},
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

	// Config already applied - standalone primary with matching pchannel
	// This will be matched against the auto-constructed config
	alreadyPromotedConfig := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "by-dev",
				Pchannels: []string{"by-dev-1"},
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "http://localhost:19530",
					Token: "",
				},
			},
		},
	}
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: alreadyPromotedConfig,
	}, nil).Maybe()
	balance.Register(b)

	// Set up broadcaster with WithSecondaryClusterResourceKey
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithSecondaryClusterResourceKey(mock.Anything).Return(mba, nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()

	// Force promote with empty config - auto-constructed config matches existing
	resp, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: &commonpb.ReplicateConfiguration{},
		ForcePromote:  true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestForcePromoteInvalidConfig(t *testing.T) {
	// Test that force promote rejects non-empty cluster/topology fields
	// This test is now covered by TestForcePromoteValidation
	// Keep this for additional coverage

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

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()

	// Force promote with multiple clusters (invalid for force promote - non-empty clusters)
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
	assert.Contains(t, err.Error(), "force promote requires empty cluster and topology fields")
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

	// Set up broadcaster that returns success for FixIncompleteBroadcastsForForcePromote
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().FixIncompleteBroadcastsForForcePromote(mock.Anything).Return(nil).Maybe()
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

	// Test callback with force promote flag set - should call FixIncompleteBroadcastsForForcePromote
	msg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: cfg,
			ForcePromote:           true,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	assert.NoError(t, registry.CallMessageAckCallback(context.Background(), msg, map[string]*message.AppendResult{}))

	// Test callback without force promote (should not call FixIncompleteBroadcastsForForcePromote)
	msg2 := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: cfg,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	assert.NoError(t, registry.CallMessageAckCallback(context.Background(), msg2, map[string]*message.AppendResult{}))
}

func TestAlterReplicateConfigCallbackIgnore(t *testing.T) {
	// Test callback with ignore flag set - should skip all processing
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	// Set up balancer - should NOT be called
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	// Note: UpdateReplicateConfiguration should NOT be called because ignore=true
	balance.Register(b)

	// Set up broadcaster - should NOT call FixIncompleteBroadcastsForForcePromote
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().Close().Return().Maybe()
	// Note: FixIncompleteBroadcastsForForcePromote should NOT be called because ignore=true
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

	// Test callback with ignore flag set - should skip all processing
	msg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: cfg,
			ForcePromote:           true,
			Ignore:                 true,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	assert.NoError(t, registry.CallMessageAckCallback(context.Background(), msg, map[string]*message.AppendResult{}))
}

func TestFixIncompleteBroadcastsForForcePromote(t *testing.T) {
	// Test FixIncompleteBroadcastsForForcePromote is called during force promote callback
	resource.InitForTest()

	t.Run("success", func(t *testing.T) {
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
		mb.EXPECT().FixIncompleteBroadcastsForForcePromote(mock.Anything).Return(nil)
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
				ForcePromote:           true,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"v1"}).
			MustBuildBroadcast()
		err := registry.CallMessageAckCallback(context.Background(), msg, map[string]*message.AppendResult{})
		assert.NoError(t, err)
	})

	t.Run("failure", func(t *testing.T) {
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

		fixErr := errors.New("fix incomplete broadcasts failed")
		mb := mock_broadcaster.NewMockBroadcaster(t)
		mb.EXPECT().FixIncompleteBroadcastsForForcePromote(mock.Anything).Return(fixErr)
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
				ForcePromote:           true,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"v1"}).
			MustBuildBroadcast()
		err := registry.CallMessageAckCallback(context.Background(), msg, map[string]*message.AppendResult{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fix incomplete broadcasts failed")
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

	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
	}, nil).Maybe()
	balance.Register(b)

	// Broadcaster returns a non-ErrNotSecondary error
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithSecondaryClusterResourceKey(mock.Anything).Return(nil, errors.New("broadcaster unavailable"))
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	// Force promote with empty config
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: &commonpb.ReplicateConfiguration{},
		ForcePromote:  true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "broadcaster unavailable")
}

func TestForcePromoteBroadcastAppendError(t *testing.T) {
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

	// Set up broadcaster with WithSecondaryClusterResourceKey returning broadcast API that fails on Broadcast
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, errors.New("broadcast append failed"))
	mba.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithSecondaryClusterResourceKey(mock.Anything).Return(mba, nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	// Force promote with empty config
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: &commonpb.ReplicateConfiguration{},
		ForcePromote:  true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "broadcast append failed")
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

	t.Run("force_promote_fix_incomplete_broadcasts_error", func(t *testing.T) {
		broadcast.ResetBroadcaster()
		snmanager.ResetStreamingNodeManager()

		b := mock_balancer.NewMockBalancer(t)
		b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
		b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		}).Maybe()
		b.EXPECT().Close().Return().Maybe()
		// Note: UpdateReplicateConfiguration should NOT be called because FixIncompleteBroadcastsForForcePromote fails first
		balance.Register(b)

		mb := mock_broadcaster.NewMockBroadcaster(t)
		mb.EXPECT().FixIncompleteBroadcastsForForcePromote(mock.Anything).Return(errors.New("fix incomplete broadcasts failed"))
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
		assert.Contains(t, err.Error(), "fix incomplete broadcasts failed")
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
	// Covers handleForcePromote balance.GetWithContext error
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

	// Force promote with empty config
	_, err := as.UpdateReplicateConfiguration(ctx, &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: &commonpb.ReplicateConfiguration{},
		ForcePromote:  true,
	})
	assert.Error(t, err)
}

func TestHandleForcePromoteGetAssignmentError(t *testing.T) {
	// Covers handleForcePromote GetLatestChannelAssignment error
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
	// GetLatestChannelAssignment in handleForcePromote fails
	b.EXPECT().GetLatestChannelAssignment().Return(nil, errors.New("assignment error")).Maybe()
	balance.Register(b)

	// WithSecondaryClusterResourceKey succeeds (we are secondary)
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithSecondaryClusterResourceKey(mock.Anything).Return(mba, nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	// Force promote with empty config
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: &commonpb.ReplicateConfiguration{},
		ForcePromote:  true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "assignment error")
}

func TestHandleForcePromoteSameConfigAfterBroadcasterCheck(t *testing.T) {
	// Covers handleForcePromote double-check same config - auto-constructed config matches existing
	resource.InitForTest()

	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	// The auto-constructed standalone primary config
	// ConnectionParam must match what exists in the secondary config
	forcePromoteCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "by-dev",
				Pchannels: []string{"by-dev-1"},
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "http://test:19530",
					Token: "by-dev",
				},
			},
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
	// Return the standalone primary config directly
	// This tests the idempotent path where config already matches
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{
			Channels: map[channel.ChannelID]*channel.PChannelMeta{
				{Name: "by-dev-1"}: channel.NewPChannelMeta("by-dev-1", types.AccessModeRW),
			},
		},
		ReplicateConfiguration: forcePromoteCfg,
	}, nil).Maybe()
	balance.Register(b)

	// Broadcaster returns success for secondary check
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithSecondaryClusterResourceKey(mock.Anything).Return(mba, nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	// Force promote with empty config
	resp, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: &commonpb.ReplicateConfiguration{},
		ForcePromote:  true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestHandleForcePromoteValidatorError(t *testing.T) {
	// Covers handleForcePromote validator.Validate() error
	// The validator may fail if pchannels in cluster view don't match cluster config
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
	// Return config with extra pchannel in PChannelView that causes validator to fail
	// PChannelView has 2 pchannels but current config's cluster has only 1
	b.EXPECT().GetLatestChannelAssignment().Return(&balancer.WatchChannelAssignmentsCallbackParam{
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
	}, nil).Maybe()
	balance.Register(b)

	// WithSecondaryClusterResourceKey succeeds
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithSecondaryClusterResourceKey(mock.Anything).Return(mba, nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	// Force promote with empty config
	_, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: &commonpb.ReplicateConfiguration{},
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

func TestForcePromoteMultiplePChannels(t *testing.T) {
	// Tests force promote with multiple pchannels - auto-constructed config includes all pchannels
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
	balance.Register(b)

	// Set up broadcaster with WithSecondaryClusterResourceKey
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{
		BroadcastID: 1,
		AppendResults: map[string]*types.AppendResult{
			"by-dev-1": {TimeTick: 100},
			"by-dev-2": {TimeTick: 101},
		},
	}, nil).Maybe()
	mba.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithSecondaryClusterResourceKey(mock.Anything).Return(mba, nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	as := NewAssignmentService()
	// Force promote with empty config - config auto-constructed from meta with all pchannels
	resp, err := as.UpdateReplicateConfiguration(context.Background(), &streamingpb.UpdateReplicateConfigurationRequest{
		Configuration: &commonpb.ReplicateConfiguration{},
		ForcePromote:  true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}
