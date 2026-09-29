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

// queryViewConfig owns the refreshable QueryView placement profile.
type queryViewConfig struct {
	BalancerAutoBalance             ParamItem `refreshable:"true"`
	BalancerReconcileInterval       ParamItem `refreshable:"true"`
	BalancerGlobalWeight            ParamItem `refreshable:"true"`
	BalancerShardWeight             ParamItem `refreshable:"true"`
	BalancerCollectionWeight        ParamItem `refreshable:"true"`
	BalancerFanoutPenaltyWeight     ParamItem `refreshable:"true"`
	BalancerMovePrice               ParamItem `refreshable:"true"`
	BalancerLoadPrice               ParamItem `refreshable:"true"`
	BalancerRelativeTolerance       ParamItem `refreshable:"true"`
	BalancerLocalTolerance          ParamItem `refreshable:"true"`
	BalancerFanoutHysteresis        ParamItem `refreshable:"true"`
	BalancerAbsoluteToleranceRows   ParamItem `refreshable:"true"`
	BalancerTargetRowsPerShardNode  ParamItem `refreshable:"true"`
	BalancerMinGainRows             ParamItem `refreshable:"true"`
	BalancerMaxCandidateEvaluations ParamItem `refreshable:"true"`
}

func (p *queryViewConfig) init(base *BaseTable) {
	p.BalancerAutoBalance = ParamItem{
		Key:          "queryView.balancer.autoBalance",
		Version:      "3.0.0",
		DefaultValue: "true",
		Export:       true,
		Doc:          "Enables optional placement optimization. Disable to stop optional movement; mandatory recovery, version updates and releases continue independently of queryCoord.autoBalance.",
	}
	p.BalancerAutoBalance.Init(base.mgr)
	p.BalancerReconcileInterval = ParamItem{
		Key:          "queryView.balancer.reconcileInterval",
		Version:      "3.0.0",
		DefaultValue: "1m",
		Export:       true,
		Doc:          "Positive duration with a unit between periodic reconciles. Decrease for faster convergence and more coordinator work; increase to reduce scan frequency. Events reconcile independently.",
	}
	p.BalancerReconcileInterval.Init(base.mgr)
	p.BalancerGlobalWeight = ParamItem{
		Key:          "queryView.balancer.scoring.globalWeight",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Weight of RG load-band improvement. Increase to favor reducing aggregate row skew over local concentration and loading cost.",
	}
	p.BalancerGlobalWeight.Init(base.mgr)
	p.BalancerShardWeight = ParamItem{
		Key:          "queryView.balancer.scoring.shardWeight",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Weight of excess shard rows on one node. Increase to spread large shards more strongly; small shards within their preferred footprint receive no spreading reward.",
	}
	p.BalancerShardWeight.Init(base.mgr)
	p.BalancerCollectionWeight = ParamItem{
		Key:          "queryView.balancer.scoring.collectionWeight",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Weight of excess collection-replica rows on one node. Increase to distribute large collections made of many small shards more evenly.",
	}
	p.BalancerCollectionWeight.Init(base.mgr)
	p.BalancerFanoutPenaltyWeight = ParamItem{
		Key:          "queryView.balancer.scoring.fanoutPenaltyWeight",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Weight of net excess shard fanout. Increase to favor consolidation and fewer query participants; decrease to permit wider spreading.",
	}
	p.BalancerFanoutPenaltyWeight.Init(base.mgr)
	p.BalancerMovePrice = ParamItem{
		Key:          "queryView.balancer.scoring.movePrice",
		Version:      "3.0.0",
		DefaultValue: "0.02",
		Export:       true,
		Doc:          "Row-equivalent cost per row whose node changes. Increase to reduce movement; decrease to accept smaller placement improvements, even when destination resources already exist.",
	}
	p.BalancerMovePrice.Init(base.mgr)
	p.BalancerLoadPrice = ParamItem{
		Key:          "queryView.balancer.scoring.loadPrice",
		Version:      "3.0.0",
		DefaultValue: "0.08",
		Export:       true,
		Doc:          "Additional row-equivalent cost per row needing a compatible destination load. Increase to favor protected ready-resource reuse; decrease to favor balance despite additional loading.",
	}
	p.BalancerLoadPrice.Init(base.mgr)
	p.BalancerRelativeTolerance = ParamItem{
		Key:          "queryView.balancer.scoring.relativeTolerance",
		Version:      "3.0.0",
		DefaultValue: "0.1",
		Export:       true,
		Doc:          "Fractional RG load tolerance, in [0,1). Increase to accept more relative skew and reduce movement; decrease to pursue closer row balance.",
	}
	p.BalancerRelativeTolerance.Init(base.mgr)
	p.BalancerLocalTolerance = ParamItem{
		Key:          "queryView.balancer.scoring.localTolerance",
		Version:      "3.0.0",
		DefaultValue: "0.1",
		Export:       true,
		Doc:          "Fractional allowance above shard and collection concentration targets. Increase to retain more concentrated layouts; decrease to spread large data more strongly.",
	}
	p.BalancerLocalTolerance.Init(base.mgr)
	p.BalancerFanoutHysteresis = ParamItem{
		Key:          "queryView.balancer.scoring.fanoutHysteresis",
		Version:      "3.0.0",
		DefaultValue: "0.1",
		Export:       true,
		Doc:          "Fractional growth/shrink margin for preferred fanout, in [0,1). Increase to reduce fanout changes near size boundaries; decrease to react sooner to data growth and shrinkage.",
	}
	p.BalancerFanoutHysteresis.Init(base.mgr)
	p.BalancerAbsoluteToleranceRows = ParamItem{
		Key:          "queryView.balancer.scoring.absoluteToleranceRows",
		Version:      "3.0.0",
		DefaultValue: "100000",
		Export:       true,
		Doc:          "Positive minimum RG tolerance in rows. Increase to keep tiny shards concentrated at low total volume; decrease to rebalance smaller absolute load differences.",
	}
	p.BalancerAbsoluteToleranceRows.Init(base.mgr)
	p.BalancerTargetRowsPerShardNode = ParamItem{
		Key:          "queryView.balancer.targetRowsPerShardNode",
		Version:      "3.0.0",
		DefaultValue: "100000",
		Export:       true,
		Doc:          "Positive concentration scale in rows for preferred shard fanout and the collection concentration floor. Increase to favor fewer participants; decrease to favor spreading. Not a capacity limit.",
	}
	p.BalancerTargetRowsPerShardNode.Init(base.mgr)
	p.BalancerMinGainRows = ParamItem{
		Key:          "queryView.balancer.scoring.minGainRows",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Positive minimum net optional gain in row-equivalent units. Increase to reject marginal moves; decrease to accept smaller improvements.",
	}
	p.BalancerMinGainRows.Init(base.mgr)
	p.BalancerMaxCandidateEvaluations = ParamItem{
		Key:          "queryView.balancer.scoring.maxCandidateEvaluations",
		Version:      "3.0.0",
		DefaultValue: "100000",
		Export:       true,
		Doc:          "Positive maximum optional candidate evaluations per shard pass. Increase to search more candidates at higher CPU cost; decrease to yield earlier and continue on a later reconcile. Mandatory placement is not capped.",
	}
	p.BalancerMaxCandidateEvaluations.Init(base.mgr)
}
