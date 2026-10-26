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
		func(v *api.BalanceConfig) { v.ResourceGroupWeight = math.NaN() },
		func(v *api.BalanceConfig) { v.FanoutWeight = math.Inf(1) },
		func(v *api.BalanceConfig) { v.MoveCostPerUnit = -1 },
		func(v *api.BalanceConfig) {
			v.ResourceGroupWeight, v.FanoutWeight, v.ShardConcentrationWeight, v.CollectionConcentrationWeight = 0, 0, 0, 0
		},
		func(v *api.BalanceConfig) { v.ResourceGroupAbsoluteTolerance = 0 },
		func(v *api.BalanceConfig) { v.TargetLogicalSizePerNode = -1 },
		func(v *api.BalanceConfig) { v.ReconcileInterval = 0 },
	} {
		value := *original
		mutate(&value)
		require.False(t, c.UpdateBalanceConfig(&value))
		require.Same(t, original, c.GetBalanceConfig())
	}
	require.Zero(t, notifications)
	value := *original
	value.ReconcileInterval += time.Minute
	require.True(t, c.UpdateBalanceConfig(&value))
	require.Zero(t, notifications)
	value.ResourceGroupWeight = 2
	require.True(t, c.UpdateBalanceConfig(&value))
	require.Equal(t, 1, notifications)
	value.ResourceGroupWeight = 100
	require.Equal(t, 2.0, c.GetBalanceConfig().ResourceGroupWeight)
	require.Equal(t, 1.0, original.ResourceGroupWeight)
	require.True(t, c.UpdateBalanceConfig(nil))
	require.Equal(t, original, c.GetBalanceConfig())
	require.False(t, c.UpdateBalanceConfig(nil))
	require.Equal(t, 2, notifications)
}
