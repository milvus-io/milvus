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
			collectionIDLabelName,
			nodeIDLabelName,
		})

	QueryNodeNumSegments = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_segments",
			Help:      "Number of segments per collection in QueryNode.",
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	QueryNodeNumDmlChannels = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_dml_channels",
			Help:      "Number of dmlChannels per collection in QueryNode.",
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	QueryNodeNumDeltaChannels = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_delta_channels",
			Help:      "Number of deltaChannels per collection in QueryNode.",
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	QueryNodeNumConsumers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_consumers",
			Help:      "Number of consumers per collection in QueryNode.",
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	QueryNodeNumReaders = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "num_readers",
			Help:      "Number of readers per collection in QueryNode.",
		}, []string{
			collectionIDLabelName,
			nodeIDLabelName,
		})

	QueryNodeSQCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_count",
			Help:      "Search and query requests statistic in QueryNode.",
		}, []string{
			statusLabelName,
			queryTypeLabelName,
			nodeIDLabelName,
		})

	QueryNodeSQReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency",
			Help:      "Search and query requests latency in QueryNode.",
			Buckets:   buckets,
		}, []string{
			queryTypeLabelName,
			nodeIDLabelName,
		})

	QueryNodeSQLatencyInQueue = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency_in_queue",
			Help:      "The search and query latency in queue(unsolved buffer) in QueryNode.",
			Buckets:   buckets,
		}, []string{
			queryTypeLabelName,
			nodeIDLabelName,
		})

	QueryNodeSQSegmentLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency_per_segment",
			Help:      "The search and query on segments(sealed/growing segments).",
			Buckets:   buckets,
		}, []string{
			queryTypeLabelName,
			segmentTypeLabelName,
			nodeIDLabelName,
		})

	QueryNodeSQSegmentLatencyInCore = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_latency_in_core",
			Help:      "The search and query latency in core.",
			Buckets:   buckets,
		}, []string{
			queryTypeLabelName,
			nodeIDLabelName,
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
			segmentTypeLabelName,
			nodeIDLabelName,
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
			collectionIDLabelName,
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

	QueryNodeSearchNQ = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "nq_for_merged_req",
			Help:      "Number of NQ after merging reqs",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeWaitForExecuteReqs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "reqs_wait_for_execute",
			Help:      "the number of requests waiting for execute",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeWaitForMergeReqs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "reqs_wait_for_merge",
			Help:      "the number of requests waiting for merge",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeReceiveReqs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "receive_reqs",
			Help:      "the number of requests querynode receive",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeExecuteReqs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "executed_reqs",
			Help:      "the number of requests querynode executed",
		}, []string{
			nodeIDLabelName,
		})
	QueryNodeSearch = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "query_node_search",
		}, []string{nodeIDLabelName})

	QueryNodeSearchHistorical = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "query_node_search_historical",
		}, []string{nodeIDLabelName})

	QueryNodeSearchReduce = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "query_node_search_reduce",
		}, []string{nodeIDLabelName})
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
	prometheus.MustRegister(QueryNodeSearchNQ)
	prometheus.MustRegister(QueryNodeWaitForExecuteReqs)
	prometheus.MustRegister(QueryNodeWaitForMergeReqs)
	prometheus.MustRegister(QueryNodeReceiveReqs)
	prometheus.MustRegister(QueryNodeExecuteReqs)

	prometheus.MustRegister(QueryNodeSearch)
	prometheus.MustRegister(QueryNodeSearchHistorical)
	prometheus.MustRegister(QueryNodeSearchReduce)
}
