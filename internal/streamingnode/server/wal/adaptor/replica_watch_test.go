package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestReplicaWatchWALAvailability(t *testing.T) {
	mockey.PatchConvey("local watch follows WAL availability independently of directory draining", t, func() {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: ctx, lifetime: typeutil.NewLifetime()}}
		_, err := w.AcquireReplicaWatchStream(t.Context())
		require.True(t, status.AsStreamingError(err).IsOnShutdown())
		d := replicadiscovery.NewDirectory()
		defer d.Close()
		w.queryViewHandler = &snview.SNQueryViewHandler{}
		mockey.Mock((*roWALAdaptorImpl).Channel).Return(types.PChannelInfo{Name: "p", Term: 1}).Build()
		mockey.Mock((*snview.SNQueryViewHandler).AcquireReplicaWatchStream).To(func(_ *snview.SNQueryViewHandler, callCtx context.Context, pc string, unavailable <-chan struct{}) replicadiscovery.Stream {
			require.Equal(t, "p", pc)
			require.Equal(t, ctx.Done(), unavailable)
			return d.AcquireStream(callCtx, pc, unavailable)
		}).Build()
		stream, err := w.AcquireReplicaWatchStream(t.Context())
		require.NoError(t, err)
		defer stream.Close()
		sub, err := stream.Subscribe(t.Context(), "p_1v0", func(replicadiscovery.Snapshot) {})
		require.NoError(t, err)
		require.Eventually(t, func() bool { return sub.Snapshot().Valid }, time.Second, time.Millisecond)
		cancel()
		select {
		case <-stream.Done():
		case <-time.After(time.Second):
			t.Fatal("WAL unavailability did not end direct stream")
		}
		require.True(t, status.AsStreamingError(stream.Error()).IsOnShutdown())
		w.lifetime.SetState(typeutil.LifetimeStateStopped)
		w.lifetime.Wait()
		_, err = w.AcquireReplicaWatchStream(t.Context())
		require.True(t, status.AsStreamingError(err).IsOnShutdown())
	})
}
