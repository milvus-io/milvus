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

// queryViewConfig owns QueryView execution and placement configuration.
type queryViewConfig struct {
	IDFLazyLoadSealedStats                  ParamItem `refreshable:"true"`
	IDFSealedStatsLoadConcurrencyRatio      ParamItem `refreshable:"true"`
	LeaseDuration                           ParamItem `refreshable:"false"`
	LogicalSizeSource                       ParamItem `refreshable:"false"`
	LogicalSizeBytesPerUnit                 ParamItem `refreshable:"false"`
	LogicalSizeSegmentBaseCost              ParamItem `refreshable:"true"`
	BalancerReconcileInterval               ParamItem `refreshable:"true"`
	BalancerOptimizationEnabled             ParamItem `refreshable:"true"`
	BalancerMinScoreGain                    ParamItem `refreshable:"true"`
	BalancerMaxCandidateEvaluationsPerShard ParamItem `refreshable:"true"`
	BalancerTargetLogicalSizePerNode        ParamItem `refreshable:"true"`
	BalancerResourceGroupWeight             ParamItem `refreshable:"true"`
	BalancerResourceGroupRelativeTolerance  ParamItem `refreshable:"true"`
	BalancerResourceGroupAbsoluteTolerance  ParamItem `refreshable:"true"`
	BalancerShardConcentrationWeight        ParamItem `refreshable:"true"`
	BalancerCollectionConcentrationWeight   ParamItem `refreshable:"true"`
	BalancerConcentrationRelativeTolerance  ParamItem `refreshable:"true"`
	BalancerFanoutWeight                    ParamItem `refreshable:"true"`
	BalancerFanoutHysteresis                ParamItem `refreshable:"true"`
	BalancerMoveCostPerUnit                 ParamItem `refreshable:"true"`
	BalancerLoadCostPerUnit                 ParamItem `refreshable:"true"`
}

func (p *queryViewConfig) init(base *BaseTable) {
	p.IDFLazyLoadSealedStats = ParamItem{
		Key:          "queryView.idfOracle.lazyLoadSealedStats",
		Version:      "3.1.0",
		Export:       true,
		DefaultValue: "false",
		Doc:          "Whether QueryView IDF runtimes defer sealed BM25 resource discovery and stats materialization until the first BM25 search.",
	}
	p.IDFLazyLoadSealedStats.Init(base.mgr)

	p.IDFSealedStatsLoadConcurrencyRatio = ParamItem{
		Key:          "queryView.idfOracle.sealedStatsLoadConcurrencyRatio",
		Version:      "3.1.0",
		Export:       true,
		DefaultValue: "4",
		Doc:          "Maximum process-wide concurrency for loading sealed BM25 stats, expressed as a ratio of CPU cores.",
		Formatter: func(v string) string {
			if getAsFloat(v) <= 0 {
				return "1"
			}
			return v
		},
	}
	p.IDFSealedStatsLoadConcurrencyRatio.Init(base.mgr)

	p.LeaseDuration = ParamItem{
		Key:          "queryView.leaseDuration",
		Version:      "3.1.0",
		DefaultValue: "60s",
		Doc:          "Renewable SN Up-view retention after query access. Delays normal Down; non-positive durations disable timed retention. Not refreshable.",
		Export:       true,
	}
	p.LeaseDuration.Init(base.mgr)

	p.LogicalSizeSource = ParamItem{
		Key:          "queryView.logicalSize.source",
		Version:      "3.0.0",
		DefaultValue: "rowCount",
		Export:       true,
		Doc:          "LogicalSize source: rowCount uses physical rows; insertBytes uses ceil(segment insert_binlog_size / bytesPerUnit). Unsupported values use rowCount. Missing size falls back to rows, then segmentBaseCost is added. Sampled once when the DataView manager is initialized; requires coordinator restart.",
	}
	p.LogicalSizeSource.Init(base.mgr)

	p.LogicalSizeBytesPerUnit = ParamItem{
		Key:          "queryView.logicalSize.bytesPerUnit",
		Version:      "3.0.0",
		DefaultValue: "64KB",
		Export:       true,
		Doc:          "Bytes per logical unit for the insertBytes source, parsed as a size (for example 64KB or 1MB). Non-positive or invalid values use 64KB. Sampled once when the DataView manager is initialized; requires coordinator restart. Missing segment size estimates one unit per physical row.",
	}
	p.LogicalSizeBytesPerUnit.Init(base.mgr)

	p.LogicalSizeSegmentBaseCost = ParamItem{
		Key:          "queryView.logicalSize.segmentBaseCost",
		Version:      "3.0.0",
		DefaultValue: "16",
		Export:       true,
		Doc:          "Non-negative fixed cost in logical units added to physical rows or normalized segment bytes when building each new DataView LogicalSize. Changes apply on the next DataView update, never to existing snapshots. Invalid values retain the last valid sampled cost; zero disables the fixed cost.",
	}
	p.LogicalSizeSegmentBaseCost.Init(base.mgr)

	p.BalancerReconcileInterval = ParamItem{
		Key:          "queryView.balancer.reconcileInterval",
		Version:      "3.0.0",
		DefaultValue: "1m",
		Export:       true,
		Doc:          "Positive duration with a unit between periodic reconciles. Decrease for faster convergence and more coordinator work; increase to reduce scan frequency. Events reconcile independently.",
	}
	p.BalancerReconcileInterval.Init(base.mgr)

	p.BalancerOptimizationEnabled = ParamItem{
		Key:          "queryView.balancer.optimization.enabled",
		Version:      "3.0.0",
		DefaultValue: "true",
		Export:       true,
		Doc:          "Enables optional placement optimization. Disable to stop optional movement; mandatory recovery, version updates and releases continue independently of queryCoord.autoBalance.",
	}
	p.BalancerOptimizationEnabled.Init(base.mgr)

	p.BalancerMinScoreGain = ParamItem{
		Key:          "queryView.balancer.optimization.minScoreGain",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Positive minimum net weighted score gain after movement and loading costs for optional optimization. Increase to reject marginal moves; decrease to accept smaller improvements.",
	}
	p.BalancerMinScoreGain.Init(base.mgr)

	p.BalancerMaxCandidateEvaluationsPerShard = ParamItem{
		Key:          "queryView.balancer.optimization.maxCandidateEvaluationsPerShard",
		Version:      "3.0.0",
		DefaultValue: "100000",
		Export:       true,
		Doc:          "Positive maximum optional candidate evaluations per shard pass. Increase to search more candidates at higher CPU cost; decrease to yield earlier and continue on a later reconcile. Mandatory placement is not capped.",
	}
	p.BalancerMaxCandidateEvaluationsPerShard.Init(base.mgr)

	p.BalancerTargetLogicalSizePerNode = ParamItem{
		Key:          "queryView.balancer.scoring.targetLogicalSizePerNode",
		Version:      "3.0.0",
		DefaultValue: "100000",
		Export:       true,
		Doc:          "Positive concentration scale in logical size units for preferred shard fanout and the collection concentration floor. Increase to favor fewer participants; decrease to favor spreading. Not a capacity limit.",
	}
	p.BalancerTargetLogicalSizePerNode.Init(base.mgr)

	p.BalancerResourceGroupWeight = ParamItem{
		Key:          "queryView.balancer.scoring.resourceGroup.weight",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Weight of RG load-band improvement. Increase to favor reducing aggregate logical-size skew over local concentration and loading cost.",
	}
	p.BalancerResourceGroupWeight.Init(base.mgr)

	p.BalancerResourceGroupRelativeTolerance = ParamItem{
		Key:          "queryView.balancer.scoring.resourceGroup.relativeTolerance",
		Version:      "3.0.0",
		DefaultValue: "0.1",
		Export:       true,
		Doc:          "Fractional RG load tolerance, in [0,1). Increase to accept more relative skew and reduce movement; decrease to pursue closer logical-size balance.",
	}
	p.BalancerResourceGroupRelativeTolerance.Init(base.mgr)

	p.BalancerResourceGroupAbsoluteTolerance = ParamItem{
		Key:          "queryView.balancer.scoring.resourceGroup.absoluteTolerance",
		Version:      "3.0.0",
		DefaultValue: "100000",
		Export:       true,
		Doc:          "Positive minimum RG tolerance in logical size units. Increase to keep tiny shards concentrated at low total volume; decrease to rebalance smaller absolute load differences.",
	}
	p.BalancerResourceGroupAbsoluteTolerance.Init(base.mgr)

	p.BalancerShardConcentrationWeight = ParamItem{
		Key:          "queryView.balancer.scoring.concentration.shardWeight",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Weight of excess shard logical size units on one node. Increase to spread large shards more strongly; small shards within their preferred footprint receive no spreading reward.",
	}
	p.BalancerShardConcentrationWeight.Init(base.mgr)

	p.BalancerCollectionConcentrationWeight = ParamItem{
		Key:          "queryView.balancer.scoring.concentration.collectionWeight",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Weight of excess collection-replica logical size units on one node. Increase to distribute large collections made of many small shards more evenly.",
	}
	p.BalancerCollectionConcentrationWeight.Init(base.mgr)

	p.BalancerConcentrationRelativeTolerance = ParamItem{
		Key:          "queryView.balancer.scoring.concentration.relativeTolerance",
		Version:      "3.0.0",
		DefaultValue: "0.1",
		Export:       true,
		Doc:          "Fractional allowance above shard and collection concentration targets. Increase to retain more concentrated layouts; decrease to spread large data more strongly.",
	}
	p.BalancerConcentrationRelativeTolerance.Init(base.mgr)

	p.BalancerFanoutWeight = ParamItem{
		Key:          "queryView.balancer.scoring.fanout.weight",
		Version:      "3.0.0",
		DefaultValue: "1",
		Export:       true,
		Doc:          "Weight of net excess shard fanout. Increase to favor consolidation and fewer query participants; decrease to permit wider spreading.",
	}
	p.BalancerFanoutWeight.Init(base.mgr)

	p.BalancerFanoutHysteresis = ParamItem{
		Key:          "queryView.balancer.scoring.fanout.hysteresis",
		Version:      "3.0.0",
		DefaultValue: "0.1",
		Export:       true,
		Doc:          "Fractional growth/shrink margin for preferred fanout, in [0,1). Increase to reduce fanout changes near size boundaries; decrease to react sooner to data growth and shrinkage.",
	}
	p.BalancerFanoutHysteresis.Init(base.mgr)

	p.BalancerMoveCostPerUnit = ParamItem{
		Key:          "queryView.balancer.scoring.movement.moveCostPerUnit",
		Version:      "3.0.0",
		DefaultValue: "0.02",
		Export:       true,
		Doc:          "Cost multiplier on published LogicalSize whose node changes. Increase to reduce movement; decrease to accept smaller placement improvements, even when destination resources already exist.",
	}
	p.BalancerMoveCostPerUnit.Init(base.mgr)

	p.BalancerLoadCostPerUnit = ParamItem{
		Key:          "queryView.balancer.scoring.movement.loadCostPerUnit",
		Version:      "3.0.0",
		DefaultValue: "0.08",
		Export:       true,
		Doc:          "Additional cost multiplier on logical size units needing a compatible destination load. Increase to favor protected ready-resource reuse; decrease to favor balance despite additional loading.",
	}
	p.BalancerLoadCostPerUnit.Init(base.mgr)
}
