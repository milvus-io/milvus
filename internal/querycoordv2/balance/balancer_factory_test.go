package balance

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestBalancerFactory_GetBalancer(t *testing.T) {
	paramtable.Init()
	// Init global assign policy factory to avoid panic in ScoreBasedBalancer creation
	assign.ResetGlobalAssignPolicyFactoryForTest()
	assign.InitGlobalAssignPolicyFactory(nil, nil, nil, nil, nil)

	// Create a factory with nil dependencies (constructors don't use them immediately)
	f := NewBalancerFactory(nil, nil, nil, nil, nil)

	t.Run("Test Default Balancer", func(t *testing.T) {
		// Ensure config is set to default
		paramtable.Get().Save("queryCoord.balancer", "ChannelLevelScoreBalancer")

		balancer := f.GetBalancer()
		assert.IsType(t, &ChannelLevelScoreBalancer{}, balancer)
	})

	t.Run("Test Fallback Logic", func(t *testing.T) {
		// Set to an unknown balancer name
		paramtable.Get().Save("queryCoord.balancer", "UnknownBalancerType")

		balancer := f.GetBalancer()
		// Should fall back to ChannelLevelScoreBalancer
		assert.IsType(t, &ChannelLevelScoreBalancer{}, balancer)
	})

	t.Run("Test Explicit ScoreBasedBalancer", func(t *testing.T) {
		paramtable.Get().Save("queryCoord.balancer", "ScoreBasedBalancer")

		balancer := f.GetBalancer()
		assert.IsType(t, &ScoreBasedBalancer{}, balancer)
	})

	t.Run("Test RoundRobinBalancer", func(t *testing.T) {
		paramtable.Get().Save("queryCoord.balancer", "RoundRobinBalancer")
		balancer := f.GetBalancer()
		assert.IsType(t, &RoundRobinBalancer{}, balancer)
	})
}
