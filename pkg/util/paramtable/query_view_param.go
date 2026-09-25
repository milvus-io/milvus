// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

// queryViewConfig contains the shared QueryView subsystem configuration.
type queryViewConfig struct {
	BalancerAutoBalance            ParamItem `refreshable:"true"`
	BalancerReconcileInterval      ParamItem `refreshable:"true"`
	BalancerStickinessWeight       ParamItem `refreshable:"true"`
	BalancerNodeLoadWeight         ParamItem `refreshable:"true"`
	BalancerFanoutWeight           ParamItem `refreshable:"true"`
	BalancerStickyRowsScale        ParamItem `refreshable:"true"`
	BalancerTargetRowsPerShardNode ParamItem `refreshable:"true"`
}

func (p *queryViewConfig) init(base *BaseTable) {
	p.BalancerAutoBalance = ParamItem{
		Key:          "queryView.balancer.autoBalance",
		Version:      "3.0.0",
		DefaultValue: "true",
		Export:       true,
		Doc:          "Enables optional QueryView placement optimization on both event-triggered and periodic reconciles; does not disable reconciliation. Disable to reduce background segment movement; mandatory recovery, view updates, replica isolation, and releases continue. Independent of queryCoord.autoBalance.",
	}
	p.BalancerAutoBalance.Init(base.mgr)

	p.BalancerReconcileInterval = ParamItem{
		Key:          "queryView.balancer.reconcileInterval",
		Version:      "3.0.0",
		DefaultValue: "1m",
		Export:       true,
		Doc:          "Positive duration with a unit between periodic QueryView reconciles (e.g. 500ms, 10s, 1m). Decrease for more frequent optimization and lifecycle checks at higher coordinator CPU cost; increase to reduce scan overhead. Event-triggered reconciliation continues independently, even when autoBalance is false.",
	}
	p.BalancerReconcileInterval.Init(base.mgr)

	p.BalancerStickinessWeight = ParamItem{
		Key:          "queryView.balancer.stickinessWeight",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Relative weight for reusing loaded or preparing segments. Increase to discourage movement and repeated loading; decrease to favor row-load balance or lower fanout. Must be finite and nonnegative; at least one scoring weight must be positive.",
	}
	p.BalancerStickinessWeight.Init(base.mgr)

	p.BalancerNodeLoadWeight = ParamItem{
		Key:          "queryView.balancer.nodeLoadWeight",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Relative weight for preferring nodes with fewer projected rows. Increase to favor row-load balance, potentially causing more segment movement or shard fanout; decrease to favor resource reuse or lower fanout. Must be finite and nonnegative; at least one scoring weight must be positive.",
	}
	p.BalancerNodeLoadWeight.Init(base.mgr)

	p.BalancerFanoutWeight = ParamItem{
		Key:          "queryView.balancer.fanoutWeight",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Relative weight for avoiding additional nodes beyond the shard free fanout allowance. Increase to favor fewer nodes per shard, potentially accepting less even row loads; decrease to allow wider spreading when other scores favor it. Must be finite and nonnegative; at least one scoring weight must be positive.",
	}
	p.BalancerFanoutWeight.Init(base.mgr)

	p.BalancerStickyRowsScale = ParamItem{
		Key:          "queryView.balancer.stickyRowsScale",
		Version:      "3.0.0",
		DefaultValue: "1000000",
		Export:       true,
		Doc:          "Positive row count at which movement penalty saturates: min(segmentRows / stickyRowsScale, 1). Increase to reduce the penalty for moving a given segment; decrease to discourage movement until the penalty saturates. Only affects the stickiness score.",
	}
	p.BalancerStickyRowsScale.Init(base.mgr)

	p.BalancerTargetRowsPerShardNode = ParamItem{
		Key:          "queryView.balancer.targetRowsPerShardNode",
		Version:      "3.0.0",
		DefaultValue: "100000",
		Export:       true,
		Doc:          "Positive rows used to derive the free fanout allowance: ceil(shardRows / targetRowsPerShardNode), capped by eligible nodes and segment count. Increase to favor concentrating each shard on fewer nodes; decrease to allow more nodes before the fanout penalty applies. This is a soft scoring allowance, not a node capacity limit.",
	}
	p.BalancerTargetRowsPerShardNode.Init(base.mgr)
}
