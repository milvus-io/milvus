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

// readBalanceConfig avoids the panic-on-invalid typed ParamItem accessors.
// Validation is for the complete group, including the sum of scoring weights.
func readBalanceConfig(params *paramtable.ComponentParam) (*BalanceConfig, bool) {
	p := &params.QueryViewCfg
	value := &BalanceConfig{}
	for _, field := range []struct {
		item *paramtable.ParamItem
		dest *float64
	}{
		{&p.BalancerStickinessWeight, &value.StickinessWeight},
		{&p.BalancerNodeLoadWeight, &value.NodeLoadWeight},
		{&p.BalancerFanoutWeight, &value.FanoutWeight},
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
		{&p.BalancerStickyRowsScale, &value.StickyRowsScale},
		{&p.BalancerTargetRowsPerShardNode, &value.TargetRowsPerShardNode},
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
	value.TickerInterval = p.BalancerReconcileInterval.GetAsDurationByParse()
	if value.TickerInterval != interval {
		return nil, false
	}
	enabled, err := strconv.ParseBool(p.BalancerAutoBalance.GetValue())
	if err != nil {
		return nil, false
	}
	value.AutoBalance = enabled
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
		p.BalancerAutoBalance.Key, p.BalancerReconcileInterval.Key,
		p.BalancerStickinessWeight.Key, p.BalancerNodeLoadWeight.Key,
		p.BalancerFanoutWeight.Key, p.BalancerStickyRowsScale.Key,
		p.BalancerTargetRowsPerShardNode.Key,
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
