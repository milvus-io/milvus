package ack

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type retryTestTSOAllocator struct{ idalloc.Allocator }

func newAllocationRetryManager(t *testing.T) *AckManager {
	t.Helper()
	paramtable.Init()
	resource.InitForTest(t)
	patch := mockey.Mock(mockey.GetMethod(resource.Resource(), "TSOAllocator")).Return(&retryTestTSOAllocator{}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	metrics := metricsutil.NewTimeTickMetrics(t.Name())
	t.Cleanup(metrics.Close)
	return NewAckManager(0, nil, metrics)
}

func TestTimestampAllocationRetriesUntilSuccess(t *testing.T) {
	for _, barrier := range []uint64{0, 90} {
		t.Run(map[uint64]string{0: "allocate", 90: "barrier and allocate"}[barrier], func(t *testing.T) {
			manager := newAllocationRetryManager(t)
			allocationAttempts, barrierAttempts := 0, 0
			allocate := mockey.Mock((*retryTestTSOAllocator).Allocate).To(func(_ *retryTestTSOAllocator, ctx context.Context) (uint64, error) {
				require.NoError(t, ctx.Err())
				allocationAttempts++
				switch allocationAttempts {
				case 1:
					return 0, context.Canceled // Only the underlying RPC was canceled.
				case 2:
					return 0, merr.WrapErrServiceUnavailable("coordinator recovering")
				default:
					return 100, nil
				}
			}).Build()
			defer allocate.UnPatch()
			waitBarrier := mockey.Mock((*retryTestTSOAllocator).BarrierUntil).To(func(_ *retryTestTSOAllocator, ctx context.Context, ts uint64) error {
				require.NoError(t, ctx.Err())
				require.Equal(t, barrier, ts)
				barrierAttempts++
				if barrierAttempts == 1 {
					return context.DeadlineExceeded // Retry the RPC, not the WAL append.
				}
				return nil
			}).Build()
			defer waitBarrier.UnPatch()
			acker, err := manager.AllocateForAppend(context.Background(), barrier)
			if barrier == 0 {
				require.Zero(t, barrierAttempts)
			} else {
				require.Equal(t, 4, barrierAttempts)
			}
			require.NoError(t, err)
			require.Equal(t, 3, allocationAttempts)
			require.EqualValues(t, 100, acker.Timestamp())
			require.Equal(t, 1, manager.notAckHeap.Len(), "failed attempts must not publish ack state")
			acker.Ack(OptSync())
		})
	}
}

func TestTimestampAllocationStopsOnlyOnCallerCancellation(t *testing.T) {
	for _, barrier := range []uint64{0, 90} {
		t.Run(map[uint64]string{0: "allocate", 90: "barrier"}[barrier], func(t *testing.T) {
			manager := newAllocationRetryManager(t)
			cause := errors.New("WAL stopped")
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			attempts := 0
			allocate := mockey.Mock((*retryTestTSOAllocator).Allocate).To(func(_ *retryTestTSOAllocator, _ context.Context) (uint64, error) {
				attempts++
				cancel(cause)
				return 0, merr.WrapErrServiceUnavailable("allocation failed")
			}).Build()
			defer allocate.UnPatch()
			waitBarrier := mockey.Mock((*retryTestTSOAllocator).BarrierUntil).To(func(_ *retryTestTSOAllocator, _ context.Context, _ uint64) error {
				attempts++
				cancel(cause)
				return context.DeadlineExceeded
			}).Build()
			defer waitBarrier.UnPatch()
			acker, err := manager.AllocateForAppend(ctx, barrier)
			require.ErrorIs(t, err, cause)
			require.Nil(t, acker)
			require.Equal(t, 1, attempts)
			require.Zero(t, manager.lastAllocatedTimeTick)
			require.Zero(t, manager.notAckHeap.Len())
		})
	}
}

func TestBackgroundTimestampSyncReturnsAllocationFailure(t *testing.T) {
	manager := newAllocationRetryManager(t)
	attempts := 0
	failure := merr.WrapErrServiceUnavailable("coordinator recovering")
	allocate := mockey.Mock((*retryTestTSOAllocator).Allocate).To(func(_ *retryTestTSOAllocator, _ context.Context) (uint64, error) {
		attempts++
		return 0, failure
	}).Build()
	defer allocate.UnPatch()
	sync := mockey.Mock((*retryTestTSOAllocator).SyncIfExpired).To(func(_ *retryTestTSOAllocator, _ time.Duration) {}).Build()
	defer sync.UnPatch()
	waitRefresh := mockey.Mock((*retryTestTSOAllocator).WaitRefresh).To(func(_ *retryTestTSOAllocator, _ context.Context) error { return nil }).Build()
	defer waitRefresh.UnPatch()

	// The inspector retries on its next sync, without holding a WAL's ack lock
	// until the process-wide inspector context is canceled.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	details, err := manager.SyncAndGetAcknowledged(ctx)
	require.ErrorIs(t, err, failure)
	require.Nil(t, details)
	require.Equal(t, 1, attempts)
	require.Zero(t, manager.lastAllocatedTimeTick)
	require.Zero(t, manager.notAckHeap.Len())
}

func TestBackgroundTimestampSyncWaitsForRefreshOutsideAckLock(t *testing.T) {
	for _, refreshErr := range []error{nil, merr.WrapErrServiceUnavailable("coordinator recovering")} {
		t.Run(map[bool]string{true: "refreshed", false: "refresh failed"}[refreshErr == nil], func(t *testing.T) {
			manager := newAllocationRetryManager(t)
			next := uint64(100)
			allocate := mockey.Mock((*retryTestTSOAllocator).Allocate).To(func(_ *retryTestTSOAllocator, _ context.Context) (uint64, error) {
				next++
				return next, nil
			}).Build()
			defer allocate.UnPatch()
			syncCalls := 0
			sync := mockey.Mock((*retryTestTSOAllocator).SyncIfExpired).To(func(_ *retryTestTSOAllocator, _ time.Duration) { syncCalls++ }).Build()
			defer sync.UnPatch()
			waiting := make(chan struct{})
			release := make(chan struct{})
			waitRefresh := mockey.Mock((*retryTestTSOAllocator).WaitRefresh).To(func(_ *retryTestTSOAllocator, _ context.Context) error {
				assert.Equal(t, 1, syncCalls, "the refresh must be triggered before waiting for it")
				close(waiting)
				<-release
				return refreshErr
			}).Build()
			defer waitRefresh.UnPatch()

			done := make(chan error, 1)
			go func() {
				_, err := manager.SyncAndGetAcknowledged(context.Background())
				done <- err
			}()
			select {
			case <-waiting:
			case <-time.After(5 * time.Second):
				require.Fail(t, "the time tick sync doesn't wait for the refresh")
			}

			// An append must not wait for the refresh behind the ack lock.
			appended := make(chan *Acker, 1)
			go func() {
				acker, err := manager.Allocate(context.Background())
				assert.NoError(t, err)
				appended <- acker
			}()
			var acker *Acker
			select {
			case acker = <-appended:
			case <-time.After(5 * time.Second):
				// Let both goroutines finish while the allocator is still patched.
				close(release)
				<-done
				<-appended
				require.Fail(t, "append blocked by the time tick sync")
			}
			require.EqualValues(t, 101, acker.Timestamp())

			// The time tick is allocated only after the refresh is finished, even if it failed.
			close(release)
			require.NoError(t, <-done)
			require.EqualValues(t, 102, manager.lastAllocatedTimeTick)
			acker.Ack(OptSync())
		})
	}
}
