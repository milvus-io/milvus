package autowalimpls

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
)

func TestSlowHistoricalOpenDoesNotBlockWriter(t *testing.T) {
	mockey.PatchConvey("slow history is independent of current appends", t, func() {
		p, w, _ := installBackends(t)
		entered, release := make(chan struct{}), make(chan struct{})
		auto := New(p, func(context.Context, message.WALName, types.AccessMode) (walimpls.WALImpls, error) {
			close(entered)
			<-release
			return w, nil
		})
		done := make(chan error, 1)
		go func() { _, err := auto.backend(context.Background(), w.name, false); done <- err }()
		<-entered
		written := make(chan error, 1)
		go func() {
			_, err := auto.Append(context.Background(), ordinary(10, nil))
			written <- err
		}()
		select {
		case err := <-written:
			require.NoError(t, err)
		case <-time.After(time.Second):
			close(release)
			t.Fatal("historical Open blocked the active writer")
		}
		require.Equal(t, p.name, auto.WALName())
		close(release)
		require.NoError(t, <-done)
		auto.Close()
	})
}

func TestBackendOpenOwnership(t *testing.T) {
	for _, closeDuringOpen := range []bool{false, true} {
		t.Run(map[bool]string{false: "one writer open", true: "close during open"}[closeDuringOpen], func(t *testing.T) {
			mockey.PatchConvey("one owner per physical handle", t, func() {
				p, w, _ := installBackends(t)
				entered, release := make(chan struct{}), make(chan struct{})
				var opens atomic.Int32
				auto := New(p, func(context.Context, message.WALName, types.AccessMode) (walimpls.WALImpls, error) {
					opens.Add(1)
					close(entered)
					<-release
					return w, nil
				})
				done := make(chan error, 2)
				open := func() { _, err := auto.backend(context.Background(), w.name, true); done <- err }
				go open()
				<-entered
				go open()
				if closeDuringOpen {
					auto.Close()
				}
				close(release)
				for range 2 {
					err := <-done
					if closeDuringOpen {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				}
				require.EqualValues(t, 1, opens.Load(), "duplicate RW opens would fence each other")
				auto.Close()
				require.True(t, w.closed)
			})
		})
	}
}

func TestBackendOpenWaitCancellation(t *testing.T) {
	for _, write := range []bool{false, true} {
		t.Run(map[bool]string{false: "reader", true: "writer"}[write], func(t *testing.T) {
			mockey.PatchConvey("canceling a waiter does not depend on another open finishing", t, func() {
				p, w, _ := installBackends(t)
				entered, release := make(chan struct{}), make(chan struct{})
				var opens atomic.Int32
				auto := New(p, func(context.Context, message.WALName, types.AccessMode) (walimpls.WALImpls, error) {
					opens.Add(1)
					close(entered)
					<-release
					return w, nil
				})
				ownerDone := make(chan error, 1)
				go func() {
					_, err := auto.backend(context.Background(), w.name, true)
					ownerDone <- err
				}()
				<-entered
				defer func() {
					close(release)
					require.NoError(t, <-ownerDone)
					auto.Close()
				}()

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				started, done := make(chan struct{}), make(chan error, 1)
				go func() {
					close(started)
					_, err := auto.backend(ctx, w.name, write)
					done <- err
				}()
				<-started
				cancel()
				select {
				case err := <-done:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(time.Second):
					t.Fatal("canceled waiter is blocked behind another backend open")
				}
				require.EqualValues(t, 1, opens.Load())
				require.Equal(t, p.name, auto.WALName())
			})
		})
	}
}
