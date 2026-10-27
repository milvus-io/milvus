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
	remote "github.com/milvus-io/milvus/internal/streamingnode/client/handler/replicadiscovery"
	serveradapter "github.com/milvus-io/milvus/internal/streamingnode/server/service/handler/replicadiscovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type replicaLocalTestWAL struct{ wal.WAL }

func (*replicaLocalTestWAL) AcquireReplicaWatchStream(context.Context) (wal.ReplicaWatchStream, error) {
	panic("mockey")
}

func TestReplicaWatchClientSelectsOwner(t *testing.T) {
	for _, mode := range []string{"local", "remote", "retiring local", "terminal local"} {
		mockey.PatchConvey(mode, t, func() {
			assigned := &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: 7}, Node: types.StreamingNodeInfo{ServerID: 17}}
			mockey.Mock((*analyzerWatcher).Get).Return(assigned).Build()
			var lookupErr error
			switch mode {
			case "remote":
				lookupErr = registry.ErrNoStreamingNodeDeployed
			case "retiring local":
				lookupErr = status.NewOnShutdownError("retiring")
			case "terminal local":
				lookupErr = status.NewUnrecoverableError("broken")
			}
			mockey.Mock(registry.GetLocalAvailableWAL).To(func(pc types.PChannelInfo) (wal.WAL, error) {
				require.Equal(t, assigned.Channel, pc)
				return &replicaLocalTestWAL{}, lookupErr
			}).Build()
			d := snview.NewReplicaDirectoryForTest()
			defer d.Close()
			local := d.AcquireStream(t.Context(), "p", nil)
			defer local.Close()
			localCall := mockey.Mock((*replicaLocalTestWAL).AcquireReplicaWatchStream).Return(local, nil).Build()
			serviceCall := mockey.Mock((*analyzerService).GetService).Return(streamingpb.NewStreamingNodeHandlerServiceClient(nil), nil).Build()
			remoteStream := &remote.EventStream{}
			remoteCall := mockey.Mock(remote.CreateEventStream).To(func(_ context.Context, opt *remote.EventStreamOptions, _ streamingpb.StreamingNodeHandlerServiceClient) (*remote.EventStream, error) {
				require.Equal(t, assigned, opt.Assignment)
				return remoteStream, nil
			}).Build()
			hc := &handlerClientImpl{lifetime: typeutil.NewLifetime(), watcher: &analyzerWatcher{}, service: &analyzerService{}}
			stream, err := hc.AcquireReplicaWatchStream(t.Context(), "p")
			switch mode {
			case "local":
				require.NoError(t, err)
				require.Same(t, local, stream)
				require.Equal(t, 1, localCall.Times())
				require.Zero(t, serviceCall.Times())
				require.Zero(t, remoteCall.Times())
			case "terminal local":
				require.ErrorIs(t, err, lookupErr)
				require.Zero(t, serviceCall.Times())
			default:
				require.NoError(t, err)
				require.Same(t, remoteStream, stream)
				require.Equal(t, 1, remoteCall.Times())
			}
			_, err = hc.AcquireReplicaWatchStream(t.Context(), "")
			require.Error(t, err)
			hc.lifetime.SetState(typeutil.LifetimeStateStopped)
			_, err = hc.AcquireReplicaWatchStream(t.Context(), "p")
			require.ErrorIs(t, err, ErrClientClosed)
		})
	}
}

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
		directory := snview.NewReplicaDirectoryForTest()
		defer directory.Close()
		directory.Set("p_1v0", 9, &qviews.QueryViewVersion{})
		mockey.Mock(streamingpb.UnimplementedStreamingNodeHandlerServiceServer.SubscribeReplicas).To(func(_ streamingpb.UnimplementedStreamingNodeHandlerServiceServer, stream streamingpb.StreamingNodeHandlerService_SubscribeReplicasServer) error {
			create, err := contextutil.GetCreateReplicaWatchStream(stream.Context())
			pc := types.NewPChannelInfoFromProto(create.GetPchannel())
			if err != nil {
				return err
			}
			if pc.Term == 1 {
				return status.NewGRPCStatusFromStreamingError(status.NewChannelNotExist(pc.Name)).Err()
			}
			watch := directory.AcquireStream(stream.Context(), pc.Name, nil)
			defer watch.Close()
			err = serveradapter.ServeStream(stream, pc.Name, watch)
			if err != nil {
				return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
			}
			return nil
		}).Build()
		listener := bufconn.Listen(64 * 1024)
		server := grpc.NewServer()
		streamingpb.RegisterStreamingNodeHandlerServiceServer(server, streamingpb.UnimplementedStreamingNodeHandlerServiceServer{})
		go func() { _ = server.Serve(listener) }()
		defer server.Stop()
		defer listener.Close()
		conn, err := grpc.NewClient("passthrough:///replica-watch", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
		require.NoError(t, err)
		defer conn.Close()
		owner := &handlerClientImpl{lifetime: typeutil.NewLifetime(), watcher: &analyzerWatcher{}, rebalanceTrigger: &analyzerTrigger{}}
		owner.service = &analyzerService{}
		mockey.Mock((*analyzerService).GetService).Return(streamingpb.NewStreamingNodeHandlerServiceClient(conn), nil).Build()
		stream, err := owner.AcquireReplicaWatchStream(ctx, "p")
		require.NoError(t, err)
		defer stream.Close()

		snapshots := make(chan qviews.ReplicaSnapshot, 8)
		sub, err := stream.Subscribe(ctx, "p_1v0", func(s qviews.ReplicaSnapshot) { snapshots <- s })
		require.NoError(t, err)
		defer sub.Close()
		for {
			select {
			case snapshot := <-snapshots:
				if snapshot.Valid {
					require.Equal(t, []qviews.Replica{{ReplicaID: 9}}, snapshot.Replicas)
					goto ready
				}
			case <-ctx.Done():
				t.Fatal("watch failed to discover new owner")
			}
		}
	ready:

		require.EqualValues(t, 1, reports.Load())
		_, err = owner.AcquireReplicaWatchStream(ctx, "")
		require.Error(t, err)
		owner.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = owner.AcquireReplicaWatchStream(ctx, "p")
		require.ErrorIs(t, err, ErrClientClosed)
	})
}
