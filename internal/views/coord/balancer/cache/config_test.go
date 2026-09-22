package cache

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
)

func TestConfigPublicationValidationAndOwnership(t *testing.T) {
	c := New(nil)
	var notifications int
	c.SetNotifier(func(api.TriggerScope) { notifications++ })
	original := c.GetBalanceConfig()
	for _, mutate := range []func(*api.BalanceConfig){
		func(v *api.BalanceConfig) { v.NodeLoadWeight = math.NaN() },
		func(v *api.BalanceConfig) { v.FanoutWeight = math.Inf(1) },
		func(v *api.BalanceConfig) { v.StickinessWeight = -1 },
		func(v *api.BalanceConfig) { v.NodeLoadWeight, v.FanoutWeight, v.StickinessWeight = 0, 0, 0 },
		func(v *api.BalanceConfig) { v.StickyRowsScale = 0 },
		func(v *api.BalanceConfig) { v.TargetRowsPerShardNode = -1 },
		func(v *api.BalanceConfig) { v.TickerInterval = 0 },
	} {
		value := *original
		mutate(&value)
		require.False(t, c.UpdateBalanceConfig(&value))
		require.Same(t, original, c.GetBalanceConfig())
	}
	require.Zero(t, notifications)
	value := *original
	value.TickerInterval += time.Minute
	require.True(t, c.UpdateBalanceConfig(&value))
	require.Zero(t, notifications)
	value.NodeLoadWeight = 2
	require.True(t, c.UpdateBalanceConfig(&value))
	require.Equal(t, 1, notifications)
	value.NodeLoadWeight = 100
	require.Equal(t, 2.0, c.GetBalanceConfig().NodeLoadWeight)
	require.Equal(t, 1.0, original.NodeLoadWeight)
	require.True(t, c.UpdateBalanceConfig(nil))
	require.Equal(t, original, c.GetBalanceConfig())
	require.False(t, c.UpdateBalanceConfig(nil))
	require.Equal(t, 2, notifications)
}
