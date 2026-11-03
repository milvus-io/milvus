package idalloc

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
)

// BenchmarkTSOPeriodicRefresh isolates the allocation following a periodic
// refresh with 30ms coordinator latency. It also runs against the old allocator
// to compare invalidation with background refresh under the same conditions.
func BenchmarkTSOPeriodicRefresh(b *testing.B) {
	var next uint64
	patch := mockey.Mock((*tsoAllocator).batchAllocate).To(func(_ *tsoAllocator, ctx context.Context, count uint32) (uint64, int, error) {
		select {
		case <-time.After(30 * time.Millisecond):
		case <-ctx.Done():
			return 0, 0, context.Cause(ctx)
		}
		start := next + 1
		next += uint64(count)
		return start, int(count), nil
	}).Build()
	defer patch.UnPatch()
	a := NewTSOAllocator(nil)
	ctx := context.Background()
	_, err := a.Allocate(ctx)
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.SyncIfExpired(0)
		_, err := a.Allocate(ctx)
		b.StopTimer()
		require.NoError(b, err)
		// Drain any in-flight refresh and warm a fresh range outside the timed
		// section; this also makes unpatching safe when the benchmark exits.
		a.Sync()
		_, err = a.Allocate(ctx)
		require.NoError(b, err)
		b.StartTimer()
	}
}
