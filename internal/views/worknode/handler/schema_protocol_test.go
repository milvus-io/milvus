package handler

import (
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestViewSyncServerSchemaProtocol(t *testing.T) {
	for _, headerError := range []bool{false, true} {
		t.Run(map[bool]string{false: "advertise", true: "failed-header"}[headerError], func(t *testing.T) {
			mockey.PatchConvey("attest before applying any views", t, func() {
				called := false
				mockey.Mock((*mockServerStream).SendHeader).To(func(_ *mockServerStream, header metadata.MD) error {
					require.Equal(t, []string{common.QueryViewSchemaProtocol}, header.Get(common.QueryViewSchemaProtocolHeader))
					called = true
					if headerError {
						return io.ErrClosedPipe
					}
					return nil
				}).Build()
				stream := newMockServerStream(t.Context())
				handler := newMockHandler()
				handler.applyFn = func(views []ApplyView) { require.True(t, called) }
				stream.injectViewsRequest(newTestQNView(1, 1, 0).IntoProto())
				close(stream.recvCh)
				err := NewViewSyncServer(handler).SyncQueryView(stream)
				require.True(t, called)
				if headerError {
					require.ErrorIs(t, err, io.ErrClosedPipe)
					require.Zero(t, handler.getApplyCalls())
				} else {
					require.NoError(t, err)
					require.Equal(t, 1, handler.getApplyCalls())
				}
			})
		})
	}
}
