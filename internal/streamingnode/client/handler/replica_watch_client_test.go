package handler

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestReplicaWatchClientRediscoversRejectedOwner(t *testing.T) {
	mockey.PatchConvey("owner rejection follows HandlerClient assignment discovery", t, func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		mockey.Mock(registry.GetLocalAvailableWAL).Return(nil, registry.ErrNoStreamingNodeDeployed).Build()
		var term, reports atomic.Int64
		term.Store(1)
		mockey.Mock((*analyzerWatcher).Get).To(func(_ *analyzerWatcher, _ context.Context, _ string) *types.PChannelInfoAssigned {
			return &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: term.Load()}, Node: types.StreamingNodeInfo{ServerID: 10}}
		}).Build()
		mockey.Mock((*analyzerWatcher).Watch).Return(nil).Build()
		mockey.Mock((*analyzerTrigger).ReportAssignmentError).To(func(_ *analyzerTrigger, ctx context.Context, channel types.PChannelInfo, err error) error {
			require.NoError(t, ctx.Err())
			require.True(t, status.AsStreamingError(err).IsWrongStreamingNode())
			require.EqualValues(t, 1, channel.Term)
			reports.Add(1)
			term.Store(2)
			return nil
		}).Build()
		directory := replicadiscovery.NewDirectory()
		defer directory.Close()
		directory.Set("p_1v0", 9, &qviews.QueryViewVersion{})
		mockey.Mock((*viewpb.UnimplementedViewQueryServiceServer).WatchVChannelReplicas).To(func(_ *viewpb.UnimplementedViewQueryServiceServer, stream viewpb.ViewQueryService_WatchVChannelReplicasServer) error {
			pc, err := worknodehandler.DecodeQueryViewPChannelFromIncomingContext(stream.Context())
			if err != nil {
				return err
			}
			if pc.Term == 1 {
				return status.NewGRPCStatusFromStreamingError(status.NewChannelNotExist(pc.Name)).Err()
			}
			watch := directory.Watch()
			defer watch.Close()
			err = replicadiscovery.Serve(stream, pc.Name, watch, nil)
			if err != nil {
				return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
			}
			return nil
		}).Build()
		listener := bufconn.Listen(64 * 1024)
		server := grpc.NewServer()
		viewpb.RegisterViewQueryServiceServer(server, &viewpb.UnimplementedViewQueryServiceServer{})
		go func() { _ = server.Serve(listener) }()
		defer server.Stop()
		defer listener.Close()
		conn, err := grpc.NewClient("passthrough:///replica-watch", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
		require.NoError(t, err)
		defer conn.Close()
		owner := &handlerClientImpl{lifetime: typeutil.NewLifetime(), watcher: &analyzerWatcher{}, rebalanceTrigger: &analyzerTrigger{}}
		lazyConn := lazygrpc.NewConn(func(context.Context) (*grpc.ClientConn, error) { return conn, nil })
		client := newQueryViewClient(owner, lazyConn)
		defer client.(*queryViewClient).replicas.Close()
		snapshots := make(chan replicadiscovery.Snapshot, 8)
		sub, err := client.SubscribeReplicas("p_1v0", func(s replicadiscovery.Snapshot) { snapshots <- s })
		require.NoError(t, err)
		defer sub.Close()
		for {
			select {
			case snapshot := <-snapshots:
				if snapshot.Valid {
					require.Equal(t, []replicadiscovery.Replica{{ReplicaID: 9}}, snapshot.Replicas)
					goto ready
				}
			case <-ctx.Done():
				t.Fatal("watch failed to discover new owner")
			}
		}
	ready:

		require.EqualValues(t, 1, reports.Load())
		_, err = client.SubscribeReplicas("", func(replicadiscovery.Snapshot) {})
		require.Error(t, err)
		owner.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = client.SubscribeReplicas("p_1v0", func(replicadiscovery.Snapshot) {})
		require.ErrorIs(t, err, ErrClientClosed)
	})
}

// Optional WAL methods are runtime-mocked; transport and dispatch run unchanged.
type replicaLocalTestWAL struct{ wal.WAL }

func (*replicaLocalTestWAL) WatchReplicas() (*replicadiscovery.Watch, error) { panic("mockey") }
func (*replicaLocalTestWAL) Unavailable() <-chan struct{}                    { panic("mockey") }

func TestReplicaDiscoveryLocalRemoteLocal(t *testing.T) {
	mockey.PatchConvey("one logical subscription survives local/remote owner migration", t, func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		local1, remote, local2 := replicadiscovery.NewDirectory(), replicadiscovery.NewDirectory(), replicadiscovery.NewDirectory()
		defer local1.Close()
		defer remote.Close()
		defer local2.Close()
		local1.Set("p_1v0", 1, &qviews.QueryViewVersion{})
		remote.Set("p_1v0", 2, &qviews.QueryViewVersion{})
		local2.Set("p_1v0", 3, &qviews.QueryViewVersion{})
		unavailable1, unavailable2 := make(chan struct{}), make(chan struct{})
		var phase atomic.Int64
		phase.Store(1)
		var localCalls, remoteCalls atomic.Int64
		mockey.Mock((*analyzerWatcher).Get).To(func(_ *analyzerWatcher, _ context.Context, _ string) *types.PChannelInfoAssigned {
			return &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: phase.Load()}, Node: types.StreamingNodeInfo{ServerID: 10}}
		}).Build()
		localWAL := &replicaLocalTestWAL{}
		mockey.Mock(registry.GetLocalAvailableWAL).To(func(channel types.PChannelInfo) (wal.WAL, error) {
			localCalls.Add(1)
			if channel.Term == 2 {
				return nil, registry.ErrNoStreamingNodeDeployed
			}
			return localWAL, nil
		}).Build()
		mockey.Mock((*replicaLocalTestWAL).WatchReplicas).To(func(*replicaLocalTestWAL) (*replicadiscovery.Watch, error) {
			if phase.Load() == 1 {
				return local1.Watch(), nil
			}
			return local2.Watch(), nil
		}).Build()
		mockey.Mock((*replicaLocalTestWAL).Unavailable).To(func(*replicaLocalTestWAL) <-chan struct{} {
			if phase.Load() == 1 {
				return unavailable1
			}
			return unavailable2
		}).Build()
		mockey.Mock((*viewpb.UnimplementedViewQueryServiceServer).WatchVChannelReplicas).To(func(_ *viewpb.UnimplementedViewQueryServiceServer, stream viewpb.ViewQueryService_WatchVChannelReplicasServer) error {
			remoteCalls.Add(1)
			watch := remote.Watch()
			defer watch.Close()
			err := replicadiscovery.Serve(stream, "p", watch, nil)
			if err != nil {
				return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
			}
			return nil
		}).Build()
		listener := bufconn.Listen(64 * 1024)
		server := grpc.NewServer()
		viewpb.RegisterViewQueryServiceServer(server, &viewpb.UnimplementedViewQueryServiceServer{})
		go func() { _ = server.Serve(listener) }()
		defer server.Stop()
		defer listener.Close()
		conn, err := grpc.NewClient("passthrough:///local-remote-replicas", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
		require.NoError(t, err)
		defer conn.Close()
		owner := &handlerClientImpl{lifetime: typeutil.NewLifetime(), watcher: &analyzerWatcher{}}
		client := newQueryViewClient(owner, lazygrpc.NewConn(func(context.Context) (*grpc.ClientConn, error) { return conn, nil }))
		defer client.(*queryViewClient).replicas.Close()
		events := make(chan replicadiscovery.Snapshot, 16)
		sub, err := client.SubscribeReplicas("p_1v0", func(s replicadiscovery.Snapshot) { events <- s })
		require.NoError(t, err)
		defer sub.Close()
		wait := func(id int64) {
			for {
				select {
				case event := <-events:
					if event.Valid && len(event.Replicas) == 1 && event.Replicas[0].ReplicaID == id {
						return
					}
				case <-ctx.Done():
					t.Fatalf("did not discover replica %d", id)
				}
			}
		}
		wait(1)
		require.Zero(t, remoteCalls.Load(), "LocalWAL must not create a gRPC watch")
		phase.Store(2)
		close(unavailable1)
		wait(2)
		require.EqualValues(t, 1, remoteCalls.Load())
		phase.Store(3)
		remote.Close()
		wait(3)
		require.EqualValues(t, 1, remoteCalls.Load(), "migration back to LocalWAL must avoid another RPC")
		require.GreaterOrEqual(t, localCalls.Load(), int64(3))
	})
}
