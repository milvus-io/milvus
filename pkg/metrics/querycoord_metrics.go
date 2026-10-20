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

package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	SegmentGrowTaskLabel   = "segment_grow"
	SegmentReduceTaskLabel = "segment_reduce"
	SegmentMoveTaskLabel   = "segment_move"
	SegmentUpdateTaskLabel = "segment_update"

	ChannelGrowTaskLabel   = "channel_grow"
	ChannelReduceTaskLabel = "channel_reduce"
	ChannelMoveTaskLabel   = "channel_move"

	LeaderGrowTaskLabel   = "leader_grow"
	LeaderReduceTaskLabel = "leader_reduce"
	LeaderUpdateTaskLabel = "leader_update"

	UnknownTaskLabel = "unknown"

	QueryCoordTaskType = "querycoord_task_type"
)

var (
	QueryCoordNumCollections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "collection_num",
			Help:      "number of collections",
		}, []string{})

	QueryCoordNumPartitions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "partition_num",
			Help:      "number of partitions",
		}, []string{})

	QueryCoordLoadCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "load_req_count",
			Help:      "count of load request",
		}, []string{
			statusLabelName,
		})

	QueryCoordReleaseCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "release_req_count",
			Help:      "count of release request",
		}, []string{
			statusLabelName,
		})

	QueryCoordLoadLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "load_latency",
			Help:      "latency of load the entire collection",
			Buckets:   []float64{0, 500, 1000, 2000, 5000, 10000, 20000, 50000, 60000, 300000, 600000, 1800000},
		}, []string{})

	QueryCoordReleaseLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "release_latency",
			Help:      "latency of release request",
			Buckets:   []float64{0, 5, 10, 20, 40, 100, 200, 400, 1000, 10000},
		}, []string{})

	QueryCoordTaskNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "task_num",
			Help:      "the number of tasks in QueryCoord's scheduler",
		}, []string{QueryCoordTaskType})

	QueryCoordLoadDemandMemoryBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "load_demand_memory_bytes",
			Help:      "cumulative estimated memory bytes required by load configuration changes successfully broadcast after resource precheck",
		}, []string{ResourceGroupLabelName})

	QueryCoordLoadDemandDiskBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "load_demand_disk_bytes",
			Help:      "cumulative estimated disk bytes required by load configuration changes successfully broadcast after resource precheck",
		}, []string{ResourceGroupLabelName})

	QueryCoordNumQueryNodes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "querynode_num",
			Help:      "number of QueryNodes managered by QueryCoord",
		}, []string{})

	QueryCoordCurrentTargetCheckpointUnixSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "current_target_checkpoint_unix_seconds",
			Help:      "current target checkpoint timestamp in unix seconds",
		}, []string{
			nodeIDLabelName,
			channelNameLabelName,
		})

	QueryCoordCurrentTargetAllReplicasCheckpointUnixSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "current_target_all_replicas_checkpoint_unix_seconds",
			Help:      "current target checkpoint unix seconds, only advances when all replicas are ready",
		}, []string{
			nodeIDLabelName,
			channelNameLabelName,
		})

	QueryCoordTaskLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "task_latency",
			Help:      "latency of all kind of task in query coord scheduler scheduler",
			Buckets:   longTaskBuckets,
		}, []string{collectionIDLabelName, TaskTypeLabel, channelNameLabelName})

	QueryCoordResourceGroupInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "resource_group_info",
			Help:      "all resource group detail info in query coord",
		}, []string{ResourceGroupLabelName, NodeIDLabelName})

	QueryCoordResourceGroupReplicaTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "resource_group_replica_total",
			Help:      "total replica number of resource group",
		}, []string{ResourceGroupLabelName})

	QueryCoordReplicaRONodeTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "replica_ro_node_total",
			Help:      "total read only node number of replica",
		})

	QueryCoordLastHeartbeatTimeStamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "last_heartbeat_timestamp",
			Help:      "heartbeat timestamp of query node",
		}, []string{nodeIDLabelName})
)

// Two-level balancer, placement and load pacing. Reasons and causes are
// the planner's own words (level, table_move, table_exchange, ...;
// table_stuck, level_hurt_table, node_term, ...), so a dashboard reads
// like the log.
var (
	QueryCoordBalancePlanTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "balance_plan_total",
			Help:      "segment moves the balancer planned, by reason",
		}, []string{"reason"})

	QueryCoordBalanceSkipTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "balance_skip_total",
			Help:      "actions the balancer considered and did not take, by cause",
		}, []string{"cause"})

	QueryCoordBalancePlanLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "balance_plan_latency",
			Help:      "time one balance planning round took (ms)",
			Buckets:   buckets,
		})

	QueryCoordBalanceNodeDeviation = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "balance_node_deviation",
			Help:      "a node's final memory fill minus its resource group's mean, in bands (|x| > 0.5 is outside the band)",
		}, []string{nodeIDLabelName})

	QueryCoordBalanceNodeSegments = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "balance_node_segments",
			Help:      "sealed segment copies a node holds or is receiving, as the balancer sees it",
		}, []string{nodeIDLabelName})

	QueryCoordBalanceNodeInflightLoads = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "balance_node_inflight_loads",
			Help:      "segment loads in flight to a node, from any source",
		}, []string{nodeIDLabelName})

	QueryCoordBalanceTablesOffShare = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "balance_tables_off_share",
			Help:      "tables (per replica) the last round found outside their share, in segments or bytes",
		})

	QueryCoordPlacementTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "placement_total",
			Help:      "segments placed by the two-level policy: pass=slot went to a node with a free load slot, pass=queued to the shortest queue",
		}, []string{"pass"})

	QueryCoordLoadPacerCap = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "load_pacer_cap",
			Help:      "segment loads the pacer lets a node run at once",
		}, []string{nodeIDLabelName})

	QueryCoordLoadPacerWaitTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "load_pacer_wait_total",
			Help:      "segment loads the pacer held back in QueryCoord because the node was at its cap",
		}, []string{nodeIDLabelName})

	QueryCoordLoadRejectedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "load_rejected_total",
			Help:      "segment loads a QueryNode refused for memory",
		}, []string{nodeIDLabelName})

	QueryCoordForceLoadTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryCoordRole,
			Name:      "force_load_total",
			Help:      "segment loads sent with force_load after repeated memory refusals",
		})
)

// RegisterQueryCoord registers QueryCoord metrics
func RegisterQueryCoord(registry *prometheus.Registry) {
	QueryCoordLoadDemandMemoryBytes.WithLabelValues(common.DefaultResourceGroupName).Add(0)
	QueryCoordLoadDemandDiskBytes.WithLabelValues(common.DefaultResourceGroupName).Add(0)

	registry.MustRegister(QueryCoordNumCollections)
	registry.MustRegister(QueryCoordNumPartitions)
	registry.MustRegister(QueryCoordLoadCount)
	registry.MustRegister(QueryCoordReleaseCount)
	registry.MustRegister(QueryCoordLoadLatency)
	registry.MustRegister(QueryCoordReleaseLatency)
	registry.MustRegister(QueryCoordTaskNum)
	registry.MustRegister(QueryCoordLoadDemandMemoryBytes)
	registry.MustRegister(QueryCoordLoadDemandDiskBytes)
	registry.MustRegister(QueryCoordNumQueryNodes)
	registry.MustRegister(QueryCoordCurrentTargetCheckpointUnixSeconds)
	registry.MustRegister(QueryCoordCurrentTargetAllReplicasCheckpointUnixSeconds)
	registry.MustRegister(QueryCoordTaskLatency)
	registry.MustRegister(QueryCoordResourceGroupInfo)
	registry.MustRegister(QueryCoordResourceGroupReplicaTotal)
	registry.MustRegister(QueryCoordReplicaRONodeTotal)
	registry.MustRegister(QueryCoordLastHeartbeatTimeStamp)
	registry.MustRegister(QueryCoordBalancePlanTotal)
	registry.MustRegister(QueryCoordBalanceSkipTotal)
	registry.MustRegister(QueryCoordBalancePlanLatency)
	registry.MustRegister(QueryCoordBalanceNodeDeviation)
	registry.MustRegister(QueryCoordBalanceNodeSegments)
	registry.MustRegister(QueryCoordBalanceNodeInflightLoads)
	registry.MustRegister(QueryCoordBalanceTablesOffShare)
	registry.MustRegister(QueryCoordPlacementTotal)
	registry.MustRegister(QueryCoordLoadPacerCap)
	registry.MustRegister(QueryCoordLoadPacerWaitTotal)
	registry.MustRegister(QueryCoordLoadRejectedTotal)
	registry.MustRegister(QueryCoordForceLoadTotal)
}

// CleanQueryCoordMetricsWithNodeID drops a gone node's per-node gauges.
func CleanQueryCoordMetricsWithNodeID(nodeID int64) {
	for _, g := range []*prometheus.GaugeVec{QueryCoordBalanceNodeDeviation, QueryCoordBalanceNodeSegments, QueryCoordBalanceNodeInflightLoads, QueryCoordLoadPacerCap} {
		g.DeleteLabelValues(fmt.Sprint(nodeID))
	}
}

func CleanQueryCoordMetricsWithCollectionID(collectionID int64) {
	QueryCoordTaskLatency.DeletePartialMatch(prometheus.Labels{
		collectionIDLabelName: fmt.Sprint(collectionID),
	})
}
