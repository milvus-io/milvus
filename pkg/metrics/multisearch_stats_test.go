package metrics

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiSearchStatsIsolation(t *testing.T) {
	parent := attachStats(context.Background(), &RPCStats{collectionName: "envelope"})
	a, b := WithIndependentRPCStats(parent), WithIndependentRPCStats(parent)
	var wg sync.WaitGroup
	for _, item := range []struct {
		ctx  context.Context
		name string
	}{{a, "a"}, {b, "b"}} {
		wg.Add(1)
		go func(ctx context.Context, name string) {
			defer wg.Done()
			for range 100 {
				GetStats(ctx).SetCollectionName(name).SetDatabaseName(name)
			}
		}(item.ctx, item.name)
	}
	wg.Wait()
	require.Equal(t, "envelope", GetStats(parent).collectionName)
	require.Equal(t, "a", GetStats(a).collectionName)
	require.Equal(t, "b", GetStats(b).collectionName)
}
