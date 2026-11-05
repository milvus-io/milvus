package handler

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_assignment"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	remote "github.com/milvus-io/milvus/internal/streamingnode/client/handler/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestTransformLogClientSelectsOwner(t *testing.T) {
	for _, mode := range []string{"local", "remote", "retiring local", "terminal local", "service error"} {
		mockey.PatchConvey(mode, t, func() {
			assignment := &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: 7}, Node: types.StreamingNodeInfo{ServerID: 17}}
			mockey.Mock((*mock_assignment.MockWatcher).Get).Return(assignment).Build()
			var lookupErr error
			switch mode {
			case "remote", "service error":
				lookupErr = status.NewChannelNotExist("p")
			case "retiring local":
				lookupErr = status.NewOnShutdownError("retiring")
			case "terminal local":
				lookupErr = status.NewUnrecoverableError("failed WAL")
			}
			mockey.Mock(registry.GetLocalAvailableWAL).To(func(channel types.PChannelInfo) (wal.WAL, error) {
				require.Equal(t, assignment.Channel, channel)
				return &mock_wal.MockWAL{}, lookupErr
			}).Build()
			localStream := walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{}))
			defer localStream.Close()
			accessor := wal.NewTransformLogErrorAccesser(nil)
			mockey.Mock((*mock_wal.MockWAL).TransformLog).Return(accessor).Build()
			mockey.Mock(mockey.GetMethod(accessor, "AcquireStream")).Return(localStream, nil).Build()
			var serviceErr error
			if mode == "service error" {
				serviceErr = status.NewUnrecoverableError("service unavailable permanently")
			}
			service := mockey.Mock((*analyzerService).GetService).Return(streamingpb.NewStreamingNodeHandlerServiceClient(nil), serviceErr).Build()
			remoteStream := &remote.EventStream{}
			calls := 0
			client := &handlerClientImpl{lifetime: typeutil.NewLifetime(), watcher: &mock_assignment.MockWatcher{}, service: &analyzerService{}, newTransformLogEventStream: func(ctx context.Context, opt *remote.EventStreamOptions, _ streamingpb.StreamingNodeHandlerServiceClient) (*remote.EventStream, error) {
				calls++
				require.Equal(t, assignment, opt.Assignment)
				server, ok := contextutil.GetPickServerID(ctx)
				require.True(t, ok)
				require.EqualValues(t, 17, server)
				return remoteStream, nil
			}}
			got, err := client.AcquireTransformLogStream(t.Context(), "p")
			switch mode {
			case "local":
				require.NoError(t, err)
				require.Same(t, localStream, got)
				require.Zero(t, service.Times())
				require.Zero(t, calls)
			case "terminal local":
				require.ErrorIs(t, err, lookupErr)
				require.Zero(t, service.Times())
				require.Zero(t, calls)
			case "service error":
				require.ErrorIs(t, err, serviceErr)
				require.Zero(t, calls)
			default:
				require.NoError(t, err)
				require.Same(t, remoteStream, got)
				require.Equal(t, 1, calls)
			}
			_, err = client.AcquireTransformLogStream(t.Context(), "")
			require.True(t, status.AsStreamingError(err).IsInvalidArgument())
			client.lifetime.SetState(typeutil.LifetimeStateStopped)
			_, err = client.AcquireTransformLogStream(t.Context(), "p")
			require.ErrorIs(t, err, ErrClientClosed)
		})
	}
}
