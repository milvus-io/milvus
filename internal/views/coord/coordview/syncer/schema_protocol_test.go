package syncer

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestResumableSchemaProtocolRecheckedAfterReconnect(t *testing.T) {
	for _, protocol := range []string{"", "unsupported", "header-error"} {
		t.Run(protocol, func(t *testing.T) {
			mockey.PatchConvey("unsupported streams cannot satisfy pending readiness", t, func() {
				var calls atomic.Int32
				mockey.Mock((*mockStream).Header).To(func(_ *mockStream) (metadata.MD, error) {
					if calls.Add(1) == 1 {
						if protocol == "header-error" {
							return nil, io.ErrUnexpectedEOF
						}
						if protocol == "" {
							return nil, nil
						}
						return metadata.Pairs(common.QueryViewSchemaProtocolHeader, protocol), nil
					}
					return metadata.Pairs(common.QueryViewSchemaProtocolHeader, common.QueryViewSchemaProtocol), nil
				}).Build()
				rs, streams, cancel := newResumableTestSetup(t)
				defer cancel()
				defer rs.Close()
				first := waitStream(t, streams)
				var acknowledged atomic.Bool
				sv := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool { acknowledged.Store(true); return true }, nil)
				rs.Sync([]SyncView{sv})
				_, ok := first.waitSend(time.Second)
				require.True(t, ok)
				first.injectResponse(sv.View.IntoProto())
				second := waitStream(t, streams)
				_, ok = second.waitSend(time.Second)
				require.True(t, ok, "unacknowledged obligation must survive reconnection")
				require.False(t, acknowledged.Load())
				second.injectResponse(sv.View.IntoProto())
				require.Eventually(t, acknowledged.Load, time.Second, time.Millisecond)
				require.EqualValues(t, 2, calls.Load())
			})
		})
	}
}

func TestResumableSchemaProtocolRejectsDowngradedStream(t *testing.T) {
	mockey.PatchConvey("a previous successful stream does not authorize a new worker", t, func() {
		var calls atomic.Int32
		mockey.Mock((*mockStream).Header).To(func(_ *mockStream) (metadata.MD, error) {
			if calls.Add(1) == 1 {
				return metadata.Pairs(common.QueryViewSchemaProtocolHeader, common.QueryViewSchemaProtocol), nil
			}
			return nil, nil
		}).Build()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		rs := &resumableSyncer{node: qviews.NewQueryNode(1), pending: newPendingSyncQueryViews()}
		// Empty views still perform the stream handshake, independently of pending callbacks.
		first := newMockStream(ctx)
		first.injectResponse()
		close(first.recvCh)
		require.True(t, rs.recvLoop(ctx, first))
		second := newMockStream(ctx)
		second.injectResponse()
		require.False(t, rs.recvLoop(ctx, second))
		require.EqualValues(t, 2, calls.Load())
	})
}
