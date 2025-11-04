package service

import (
	"context"
	"io"
	"testing"
	"time"

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
