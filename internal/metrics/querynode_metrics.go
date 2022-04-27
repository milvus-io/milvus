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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var (
	QueryNodeNumCollections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "collection_num",
			Help:      "number of collections loaded",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumPartitions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "partition_num",
			Help:      "number of partitions loaded",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumSegments = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "segment_num",
			Help:      "number of segments loaded",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumDmlChannels = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "dml_vchannel_num",
			Help:      "number of dmlChannels watched",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumDeltaChannels = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "delta_vchannel_num",
			Help:      "number of deltaChannels watched",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumConsumers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "consumer_num",
			Help:      "number of consumers",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeSQCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_req_count",
			Help:      "count of search / query request",
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
			statusLabelName,
		})

	QueryNodeSQReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_req_latency",
			Help:      "latency of Search or query requests",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
		})

	QueryNodeSQLatencyInQueue = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_queue_lantency",
			Help:      "latency of search or query in queue",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
		})

	QueryNodeSQSegmentLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_segment_latency",
			Help:      "latency of search or query per segment",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
			segmentStateLabelName,
		})

	QueryNodeSQSegmentLatencyInCore = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_core_latency",
			Help:      "latency of search or query latency in segcore",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
		})

	QueryNodeReduceLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_reduce_latency",
			Help:      "latency of reduce search or query result",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
		})

	QueryNodeLoadSegmentLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "load_segment_latency",
			Help:      "latency of load per segment",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
		})

	/* Todo reimplement in query_shard.go
	QueryNodeServiceTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sync_utc_time",
			Help:      "ServiceTimes of collections in QueryNode.",
		}, []string{
			nodeIDLabelName,
		})
	*/

	QueryNodeNumFlowGraphs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "flowgraph_num",
			Help:      "number of flowgraphs",
		}, []string{
			nodeIDLabelName,
		})
)

//RegisterQueryNode registers QueryNode metrics
func RegisterQueryNode(registry *prometheus.Registry) {
	registry.MustRegister(QueryNodeNumCollections)
	registry.MustRegister(QueryNodeNumPartitions)
	registry.MustRegister(QueryNodeNumSegments)
	registry.MustRegister(QueryNodeNumDmlChannels)
	registry.MustRegister(QueryNodeNumDeltaChannels)
	registry.MustRegister(QueryNodeNumConsumers)
	registry.MustRegister(QueryNodeSQCount)
	registry.MustRegister(QueryNodeSQReqLatency)
	registry.MustRegister(QueryNodeSQLatencyInQueue)
	registry.MustRegister(QueryNodeSQSegmentLatency)
	registry.MustRegister(QueryNodeSQSegmentLatencyInCore)
	registry.MustRegister(QueryNodeReduceLatency)
	registry.MustRegister(QueryNodeLoadSegmentLatency)
	//	registry.MustRegister(QueryNodeServiceTime)
	registry.MustRegister(QueryNodeNumFlowGraphs)
}
