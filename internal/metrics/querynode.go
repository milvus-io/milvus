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

const (
	// TODO: use the common status label
	queryNodeStatusLabel        = "status"
	QueryNodeMetricLabelSuccess = "success"
	QueryNodeMetricLabelFail    = "fail"
	QueryNodeMetricLabelTotal   = "total"

	// TODO: use the common status label
	nodeIDLabel       = "node_id"
	collectionIDLabel = "collection_id"
)

const (
	// query type
	queryTypeLabel           = "query_type"
	QueryNodeQueryTypeSearch = "search"
	QueryNodeQueryTypeQuery  = "query"

	// segment type
	segmentTypeLabel        = "segment_type"
	QueryNodeSegTypeSealed  = "sealed"
	QueryNodeSegTypeGrowing = "growing"
)

// queryNodeDurationBuckets involves durations in milliseconds,
// [10 20 40 80 160 320 640 1280 2560 5120 10240 20480 40960 81920 163840 327680 655360 1.31072e+06]
var queryNodeDurationBuckets = prometheus.ExponentialBuckets(10, 2, 18)

var (
	QueryNodeNumCollections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_collections",
			Help:      "Number of collections in QueryNode.",
		}, []string{
			nodeIDLabel,
		})

	QueryNodeNumPartitions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_partitions",
			Help:      "Number of partitions per collection in QueryNode.",
		}, []string{
			collectionIDLabel,
			nodeIDLabel,
		})

	QueryNodeNumSegments = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_segments",
			Help:      "Number of segments per collection in QueryNode.",
		}, []string{
			collectionIDLabel,
			nodeIDLabel,
		})

	QueryNodeNumDmlChannels = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_dml_channels",
			Help:      "Number of dmlChannels per collection in QueryNode.",
		}, []string{
			collectionIDLabel,
			nodeIDLabel,
		})

	QueryNodeNumDeltaChannels = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_delta_channels",
			Help:      "Number of deltaChannels per collection in QueryNode.",
		}, []string{
			collectionIDLabel,
			nodeIDLabel,
		})

	QueryNodeNumConsumers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_consumers",
			Help:      "Number of consumers per collection in QueryNode.",
		}, []string{
			collectionIDLabel,
			nodeIDLabel,
		})

	QueryNodeNumReaders = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_readers",
			Help:      "Number of readers per collection in QueryNode.",
		}, []string{
			collectionIDLabel,
			nodeIDLabel,
		})

	QueryNodeSQCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_count",
			Help:      "Search and query requests statistic in QueryNode.",
		}, []string{
			queryNodeStatusLabel,
			queryTypeLabel,
			nodeIDLabel,
		})

	QueryNodeSQReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency",
			Help:      "Search and query requests latency in QueryNode.",
			Buckets:   queryNodeDurationBuckets,
		}, []string{
			queryTypeLabel,
			nodeIDLabel,
		})

	QueryNodeSQLatencyInQueue = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency_in_queue",
			Help:      "The search and query latency in queue(unsolved buffer) in QueryNode.",
			Buckets:   queryNodeDurationBuckets,
		}, []string{
			queryTypeLabel,
			nodeIDLabel,
		})

	QueryNodeSQSegmentLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency_per_segment",
			Help:      "The search and query on segments(sealed/growing segments).",
			Buckets:   queryNodeDurationBuckets,
		}, []string{
			queryTypeLabel,
			segmentTypeLabel,
			nodeIDLabel,
		})

	QueryNodeSQSegmentLatencyInCore = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency_in_core",
			Help:      "The search and query latency in core.",
			Buckets:   queryNodeDurationBuckets,
		}, []string{
			queryTypeLabel,
			nodeIDLabel,
		})

	QueryNodeTranslateHitsLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "translate_hits_latency",
			Help:      "The search and query latency in translate hits.",
			Buckets:   queryNodeDurationBuckets,
		}, []string{
			nodeIDLabel,
		})

	QueryNodeReduceLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "reduce_latency",
			Help:      "The search and query latency in reduce(local reduce) in QueryNode.",
			Buckets:   queryNodeDurationBuckets,
		}, []string{
			segmentTypeLabel,
			nodeIDLabel,
		})

	QueryNodeLoadSegmentLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "load_latency_per_segment",
			Help:      "The load latency per segment in QueryNode.",
			Buckets:   queryNodeDurationBuckets,
		}, []string{
			nodeIDLabel,
		})

	QueryNodeServiceTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "service_time",
			Help:      "ServiceTimes of collections in QueryNode.",
		}, []string{
			collectionIDLabel,
			nodeIDLabel,
		})

	QueryNodeNumFlowGraphs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_flow_graphs",
			Help:      "Number of flow graphs in QueryNode.",
		}, []string{
			nodeIDLabel,
		})
)

//RegisterQueryNode registers QueryNode metrics
func RegisterQueryNode() {
	prometheus.MustRegister(QueryNodeNumCollections)
	prometheus.MustRegister(QueryNodeNumPartitions)
	prometheus.MustRegister(QueryNodeNumSegments)
	prometheus.MustRegister(QueryNodeNumDmlChannels)
	prometheus.MustRegister(QueryNodeNumDeltaChannels)
	prometheus.MustRegister(QueryNodeNumConsumers)
	prometheus.MustRegister(QueryNodeNumReaders)
	prometheus.MustRegister(QueryNodeSQCount)
	prometheus.MustRegister(QueryNodeSQReqLatency)
	prometheus.MustRegister(QueryNodeSQLatencyInQueue)
	prometheus.MustRegister(QueryNodeSQSegmentLatency)
	prometheus.MustRegister(QueryNodeSQSegmentLatencyInCore)
	prometheus.MustRegister(QueryNodeTranslateHitsLatency)
	prometheus.MustRegister(QueryNodeReduceLatency)
	prometheus.MustRegister(QueryNodeLoadSegmentLatency)
	prometheus.MustRegister(QueryNodeServiceTime)
	prometheus.MustRegister(QueryNodeNumFlowGraphs)
}
