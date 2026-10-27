package balancer

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// readBalanceConfig validates raw values before publishing a refreshed profile.
// Validation is for the complete group, including the sum of scoring weights.
func readBalanceConfig(params *paramtable.ComponentParam) (*BalanceConfig, bool) {
	p := &params.QueryViewCfg
	value := &BalanceConfig{}
	for _, field := range []struct {
		item *paramtable.ParamItem
		dest *float64
	}{
		{&p.BalancerResourceGroupWeight, &value.ResourceGroupWeight},
		{&p.BalancerShardConcentrationWeight, &value.ShardConcentrationWeight},
		{&p.BalancerCollectionConcentrationWeight, &value.CollectionConcentrationWeight},
		{&p.BalancerFanoutWeight, &value.FanoutWeight},
		{&p.BalancerMoveCostPerUnit, &value.MoveCostPerUnit},
		{&p.BalancerLoadCostPerUnit, &value.LoadCostPerUnit},
		{&p.BalancerResourceGroupRelativeTolerance, &value.ResourceGroupRelativeTolerance},
		{&p.BalancerConcentrationRelativeTolerance, &value.ConcentrationRelativeTolerance},
		{&p.BalancerFanoutHysteresis, &value.FanoutHysteresis},
		{&p.BalancerMinScoreGain, &value.MinScoreGain},
	} {
		parsed, err := strconv.ParseFloat(field.item.GetValue(), 64)
		if err != nil {
			return nil, false
		}
		*field.dest = parsed
	}
	for _, field := range []struct {
		item *paramtable.ParamItem
		dest *int64
	}{
		{&p.BalancerResourceGroupAbsoluteTolerance, &value.ResourceGroupAbsoluteTolerance},
		{&p.BalancerTargetLogicalSizePerNode, &value.TargetLogicalSizePerNode},
		{&p.BalancerMaxCandidateEvaluationsPerShard, &value.MaxCandidateEvaluationsPerShard},
	} {
		parsed, err := strconv.ParseInt(field.item.GetValue(), 10, 64)
		if err != nil {
			return nil, false
		}
		*field.dest = parsed
	}
	interval, err := time.ParseDuration(p.BalancerReconcileInterval.GetValue())
	if err != nil {
		return nil, false
	}
	// The typed getter falls back to the default on malformed input. Validate
	// first, and reject a concurrent change; its callback will refresh again.
	value.ReconcileInterval = p.BalancerReconcileInterval.GetAsDurationByParse()
	if value.ReconcileInterval != interval {
		return nil, false
	}
	enabled, err := strconv.ParseBool(p.BalancerOptimizationEnabled.GetValue())
	if err != nil {
		return nil, false
	}
	value.OptimizationEnabled = enabled
	return value, value.Valid()
}

// watchBalanceConfig registers before reading initial values. Refreshes are
// serialized so a delayed callback cannot overwrite newer effective config.
// Stop unregisters every handler and waits for in-flight refreshes to finish.
func (b *DefaultBalancer) watchBalanceConfig(ctx context.Context, params *paramtable.ComponentParam) func() {
	if b.cache == nil {
		return func() {}
	}
	p := &params.QueryViewCfg
	keys := []string{
		p.BalancerOptimizationEnabled.Key,
		p.BalancerReconcileInterval.Key,
		p.BalancerResourceGroupWeight.Key,
		p.BalancerShardConcentrationWeight.Key,
		p.BalancerCollectionConcentrationWeight.Key,
		p.BalancerFanoutWeight.Key,
		p.BalancerMoveCostPerUnit.Key,
		p.BalancerLoadCostPerUnit.Key,
		p.BalancerResourceGroupRelativeTolerance.Key,
		p.BalancerConcentrationRelativeTolerance.Key,
		p.BalancerFanoutHysteresis.Key,
		p.BalancerResourceGroupAbsoluteTolerance.Key,
		p.BalancerTargetLogicalSizePerNode.Key,
		p.BalancerMinScoreGain.Key,
		p.BalancerMaxCandidateEvaluationsPerShard.Key,
	}

	var mu sync.Mutex
	refresh := func() {
		mu.Lock()
		defer mu.Unlock()
		value, valid := readBalanceConfig(params)
		if !valid {
			mlog.Warn(ctx, "invalid QueryView balance configuration; retaining last valid configuration")
			return
		}
		if b.cache.UpdateBalanceConfig(value) {
			select {
			case b.configChanged <- struct{}{}:
			default:
			}
		}
	}
	handler := config.NewHandler(fmt.Sprintf("query-view-balance-%p", b), func(*config.Event) { refresh() })
	for _, key := range keys {
		params.Watch(key, handler)
	}
	refresh()
	return func() {
		for _, key := range keys {
			params.Unwatch(key, handler)
		}
	}
}
