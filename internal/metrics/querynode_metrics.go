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
			Name:      "num_collections",
			Help:      "Number of collections in QueryNode.",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumPartitions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_partitions",
			Help:      "Number of partitions per collection in QueryNode.",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumSegments = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_segments",
			Help:      "Number of segments per collection in QueryNode.",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumDmlChannels = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_dml_channels",
			Help:      "Number of dmlChannels per collection in QueryNode.",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumDeltaChannels = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_delta_channels",
			Help:      "Number of deltaChannels per collection in QueryNode.",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumConsumers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_consumers",
			Help:      "Number of consumers per collection in QueryNode.",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeSQCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_count",
			Help:      "Search and query requests statistic in QueryNode.",
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
			statusLabelName,
		})

	QueryNodeSQReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency",
			Help:      "Search and query requests latency in QueryNode.",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
		})

	QueryNodeSQLatencyInQueue = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency_in_queue",
			Help:      "The search and query latency in queue(unsolved buffer) in QueryNode.",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
		})

	QueryNodeSQSegmentLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency_per_segment",
			Help:      "The search and query on segments(sealed/growing segments).",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
			segmentTypeLabelName,
		})

	QueryNodeSQSegmentLatencyInCore = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency_in_core",
			Help:      "The search and query latency in core.",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
		})

	QueryNodeTranslateHitsLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "translate_hits_latency",
			Help:      "The search and query latency in translate hits.",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeReduceLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "reduce_latency",
			Help:      "The search and query latency in reduce(local reduce) in QueryNode.",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			segmentTypeLabelName,
		})

	QueryNodeLoadSegmentLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "load_latency_per_segment",
			Help:      "The load latency per segment in QueryNode.",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeServiceTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "service_time",
			Help:      "ServiceTimes of collections in QueryNode.",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumFlowGraphs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_flow_graphs",
			Help:      "Number of flow graphs in QueryNode.",
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
	registry.MustRegister(QueryNodeTranslateHitsLatency)
	registry.MustRegister(QueryNodeReduceLatency)
	registry.MustRegister(QueryNodeLoadSegmentLatency)
	registry.MustRegister(QueryNodeServiceTime)
	registry.MustRegister(QueryNodeNumFlowGraphs)
}
