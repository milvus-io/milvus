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

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
)

// RegisterQueryCoord registers QueryCoord metrics
func RegisterQueryCoord(registry *prometheus.Registry) {
	registry.MustRegister(QueryCoordNumCollections)
	registry.MustRegister(QueryCoordNumPartitions)
	registry.MustRegister(QueryCoordLoadCount)
	registry.MustRegister(QueryCoordReleaseCount)
	registry.MustRegister(QueryCoordLoadLatency)
	registry.MustRegister(QueryCoordReleaseLatency)
	registry.MustRegister(QueryCoordTaskNum)
	registry.MustRegister(QueryCoordNumQueryNodes)
	registry.MustRegister(QueryCoordCurrentTargetCheckpointUnixSeconds)
	registry.MustRegister(QueryCoordTaskLatency)
	registry.MustRegister(QueryCoordResourceGroupInfo)
	registry.MustRegister(QueryCoordResourceGroupReplicaTotal)
	registry.MustRegister(QueryCoordReplicaRONodeTotal)
}

func CleanQueryCoordMetricsWithCollectionID(collectionID int64) {
	QueryCoordTaskLatency.DeletePartialMatch(prometheus.Labels{
		collectionIDLabelName: fmt.Sprint(collectionID),
	})
}
