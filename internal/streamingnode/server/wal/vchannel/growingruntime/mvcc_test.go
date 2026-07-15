//go:build test && dynamic

package growingruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func TestRuntimeWaitMVCCVisibleReturnsContextError(t *testing.T) {
	runtime := newRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := runtime.WaitMVCCVisible(ctx, 1, 1)

	require.ErrorIs(t, err, context.Canceled)
}
