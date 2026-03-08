package vchannelfair

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	policyName = "vchannelFair"
)

// PolicyBuilder is a builder to build vchannel fair policy.
type PolicyBuilder struct{}

// Name returns the name of the vchannel fair policy.
func (b *PolicyBuilder) Name() string {
	return policyName
}

// Build creates a new vchannel fair policy.
func (b *PolicyBuilder) Build() balancer.Policy {
	cfg := newVChannelFairPolicyConfig()
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	return &policy{
		cfg: cfg,
	}
}

// newVChannelFairPolicyConfig creates a new vchannel fair policy config.
func newVChannelFairPolicyConfig() policyConfig {
	params := paramtable.Get()
	return policyConfig{
		PChannelWeight:     params.StreamingCfg.WALBalancerPolicyVChannelFairPChannelWeight.GetAsFloat(),
		VChannelWeight:     params.StreamingCfg.WALBalancerPolicyVChannelFairVChannelWeight.GetAsFloat(),
		AntiAffinityWeight: params.StreamingCfg.WALBalancerPolicyVChannelFairAntiAffinityWeight.GetAsFloat(),
		RebalanceTolerance: params.StreamingCfg.WALBalancerPolicyVChannelFairRebalanceTolerance.GetAsFloat(),
		RebalanceMaxStep:   params.StreamingCfg.WALBalancerPolicyVChannelFairRebalanceMaxStep.GetAsInt(),
	}
}

// policyConfig is the config for vchannel fair policy.
type policyConfig struct {
	PChannelWeight     float64
	VChannelWeight     float64
	AntiAffinityWeight float64
	RebalanceTolerance float64
	RebalanceMaxStep   int
}

// Vaildate validates the vchannel fair policy config.
func (c policyConfig) Validate() error {
	if c.PChannelWeight < 0 || c.VChannelWeight < 0 || c.AntiAffinityWeight < 0 || c.RebalanceTolerance < 0 || c.RebalanceMaxStep < 0 {
		return errors.Errorf("invalid vchannel fair policy config, %+v", c)
	}
	return nil
}
