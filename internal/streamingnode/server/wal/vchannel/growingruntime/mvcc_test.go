//go:build test && dynamic

package growingruntime

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
)

func TestRuntimeWaitMVCCVisibleBlocksUntilBothFrontiersReachTarget(t *testing.T) {
	runtime := newRuntime()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- runtime.WaitMVCCVisible(ctx, 20, 10)
	}()

	require.Never(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 30*time.Millisecond, 5*time.Millisecond)

	runtime.markGrowingTimeTick(20)
	require.Never(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 30*time.Millisecond, 5*time.Millisecond)

	runtime.markTransformTimeTick(10)
	require.NoError(t, <-done)
}

func TestRuntimeCloseInvalidatesWaitingQueries(t *testing.T) {
	runtime := newRuntime()
	waiting := make(chan struct{})
	// Identify the actual wait without a sleep or racing Close with admission.
	patch := mockey.Mock((*Runtime).mvccVisibleLocked).To(func(*Runtime, uint64, uint64) bool {
		select {
		case <-waiting:
		default:
			close(waiting)
		}
		return false
	}).Build()
	defer patch.UnPatch()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.WaitMVCCVisible(ctx, 100, 100) }()
	<-waiting
	runtime.Close()
	err := <-done
	require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
	wire := viewerror.NewGRPCStatusFromViewError(viewerror.AsViewError(err)).Err()
	require.True(t, viewerror.AsViewError(viewerror.ConvertViewError("SearchOnView", wire)).IsRetryable())
	_, err = runtime.AcquireGrowingSegmentHandles(ctx, qviews.DataVersion{}, nil)
	require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
}

func TestRuntimeWaitMVCCVisibleReturnsContextError(t *testing.T) {
	runtime := newRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := runtime.WaitMVCCVisible(ctx, 1, 1)

	require.ErrorIs(t, err, context.Canceled)
}
