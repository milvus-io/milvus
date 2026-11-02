package transformlog

import (
	"context"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_walmanager"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestCreateSubscribeServerAcknowledgesAcquiredStream(t *testing.T) {
	for _, sendErr := range []error{nil, io.ErrClosedPipe} {
		mockey.PatchConvey("creation acknowledgement and cleanup", t, func() {
			ctx := contextutil.WithCreateTransformStream(t.Context(), &streamingpb.CreateTransformStreamRequest{
				Pchannel: &streamingpb.PChannelInfo{Name: "p", Term: 7},
			})
			md, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			wire := newFakeSubscribeTransformServer(metadata.NewIncomingContext(ctx, md))
			stream := walsummary.NewDispatchStream(walsummary.NewManager(walsummary.ManagerConfig{}))
			defer stream.Close()
			accessor := wal.NewTransformLogErrorAccesser(nil)
			mockey.Mock((*mock_walmanager.MockManager).GetAvailableWAL).Return(&mock_wal.MockWAL{}, nil).Build()
			mockey.Mock((*mock_wal.MockWAL).TransformLog).Return(accessor).Build()
			mockey.Mock(mockey.GetMethod(accessor, "AcquireStream")).To(func(context.Context, string) (wal.TransformLogStream, error) {
				return stream, nil
			}).Build()
			if sendErr != nil {
				mockey.Mock((*fakeSubscribeTransformServer).Send).Return(sendErr).Build()
			}
			server, err := CreateSubscribeServer(&mock_walmanager.MockManager{}, wire)
			if sendErr != nil {
				require.ErrorIs(t, err, sendErr)
				require.Nil(t, server)
				require.ErrorIs(t, stream.Error(), context.Canceled)
			} else {
				require.NoError(t, err)
				require.NotNil(t, wire.sent(t).GetCreateStream(), "acknowledge without receiving a subscription")
				require.NoError(t, stream.Error())
				server.closeAll()
			}
		})
	}
}
