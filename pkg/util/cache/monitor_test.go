package cache

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisterLRUCacheMetrics(t *testing.T) {
	cacheBuilder := NewCacheBuilder[int, int]().WithLoader(func(ctx context.Context, key int) (int, error) {
		return key, nil
	})
	RegisterLRUCacheMetrics[int, int](
		prometheus.NewRegistry(),
		cacheBuilder.Build(),
		"namespace",
		"subsystem",
		"prefix",
		prometheus.Labels{
			"label": "value",
		},
	)
}
