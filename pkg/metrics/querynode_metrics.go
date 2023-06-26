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

	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

	QueryNodeConsumeTimeTickLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "consume_tt_lag_ms",
			Help:      "now time minus tt per physical channel",
		}, []string{
			nodeIDLabelName,
			msgTypeLabelName,
			collectionIDLabelName,
		})

	QueryNodeConsumerMsgCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "consume_msg_count",
			Help:      "count of consumed msg",
		}, []string{
			nodeIDLabelName,
			msgTypeLabelName,
			collectionIDLabelName,
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
			Help:      "number of segments loaded, clustered by its collection, partition, state and # of indexed fields",
		}, []string{
			nodeIDLabelName,
			collectionIDLabelName,
			partitionIDLabelName,
			segmentStateLabelName,
			indexCountLabelName,
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
			requestScope,
		})

	QueryNodeSQLatencyWaitTSafe = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_wait_tsafe_latency",
			Help:      "latency of search or query to wait for tsafe",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
		})

	QueryNodeSQLatencyInQueue = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_queue_latency",
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

	QueryNodeReadTaskUnsolveLen = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "read_task_unsolved_len",
			Help:      "number of unsolved read tasks in unsolvedQueue",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeReadTaskReadyLen = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "read_task_ready_len",
			Help:      "number of ready read tasks in readyQueue",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeReadTaskConcurrency = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "read_task_concurrency",
			Help:      "number of concurrent executing read tasks in QueryNode",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeEstimateCPUUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "estimate_cpu_usage",
			Help:      "estimated cpu usage by the scheduler in QueryNode",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeSearchGroupNQ = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "search_group_nq",
			Help:      "the number of queries of each grouped search task",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeSearchNQ = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "search_nq",
			Help:      "the number of queries of each search task",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeSearchGroupTopK = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "search_group_topk",
			Help:      "the topK of each grouped search task",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeSearchTopK = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "search_topk",
			Help:      "the top of each search task",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeSearchGroupSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "search_group_size",
			Help:      "the number of tasks of each grouped search task",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeEvictedReadReqCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "read_evicted_count",
			Help:      "count of evicted search / query request",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumFlowGraphs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "flowgraph_num",
			Help:      "number of flowgraphs",
		}, []string{
			nodeIDLabelName,
		})

	QueryNodeNumEntities = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "entity_num",
			Help:      "number of entities which can be searched/queried, clustered by collection, partition and state",
		}, []string{
			nodeIDLabelName,
			collectionIDLabelName,
			partitionIDLabelName,
			segmentStateLabelName,
			indexCountLabelName,
		})

	// QueryNodeConsumeCounter counts the bytes QueryNode consumed from message storage.
	QueryNodeConsumeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "consume_bytes_counter",
			Help:      "",
		}, []string{nodeIDLabelName, msgTypeLabelName})

	// QueryNodeExecuteCounter counts the bytes of requests in QueryNode.
	QueryNodeExecuteCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "execute_bytes_counter",
			Help:      "",
		}, []string{nodeIDLabelName, msgTypeLabelName})

	QueryNodeMsgDispatcherTtLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "msg_dispatcher_tt_lag_ms",
			Help:      "time.Now() sub dispatcher's current consume time",
		}, []string{
			nodeIDLabelName,
			channelNameLabelName,
		})

	QueryNodeSegmentSearchLatencyPerVector = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "segment_latency_per_vector",
			Help:      "one vector's search latency per segment",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
			segmentStateLabelName,
		})

	QueryNodeWatchDmlChannelLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "watch_dml_channel_latency",
			Help:      "latency of watch dml channel",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
		})
)

// RegisterQueryNode registers QueryNode metrics
func RegisterQueryNode(registry *prometheus.Registry) {
	registry.MustRegister(QueryNodeNumCollections)
	registry.MustRegister(QueryNodeNumPartitions)
	registry.MustRegister(QueryNodeNumSegments)
	registry.MustRegister(QueryNodeNumDmlChannels)
	registry.MustRegister(QueryNodeNumDeltaChannels)
	registry.MustRegister(QueryNodeSQCount)
	registry.MustRegister(QueryNodeSQReqLatency)
	registry.MustRegister(QueryNodeSQLatencyWaitTSafe)
	registry.MustRegister(QueryNodeSQLatencyInQueue)
	registry.MustRegister(QueryNodeSQSegmentLatency)
	registry.MustRegister(QueryNodeSQSegmentLatencyInCore)
	registry.MustRegister(QueryNodeReduceLatency)
	registry.MustRegister(QueryNodeLoadSegmentLatency)
	registry.MustRegister(QueryNodeReadTaskUnsolveLen)
	registry.MustRegister(QueryNodeReadTaskReadyLen)
	registry.MustRegister(QueryNodeReadTaskConcurrency)
	registry.MustRegister(QueryNodeEstimateCPUUsage)
	registry.MustRegister(QueryNodeSearchGroupNQ)
	registry.MustRegister(QueryNodeSearchNQ)
	registry.MustRegister(QueryNodeSearchGroupSize)
	registry.MustRegister(QueryNodeEvictedReadReqCount)
	registry.MustRegister(QueryNodeSearchGroupTopK)
	registry.MustRegister(QueryNodeSearchTopK)
	registry.MustRegister(QueryNodeNumFlowGraphs)
	registry.MustRegister(QueryNodeNumEntities)
	registry.MustRegister(QueryNodeConsumeCounter)
	registry.MustRegister(QueryNodeExecuteCounter)
	registry.MustRegister(QueryNodeConsumerMsgCount)
	registry.MustRegister(QueryNodeConsumeTimeTickLag)
	registry.MustRegister(QueryNodeMsgDispatcherTtLag)
	registry.MustRegister(QueryNodeSegmentSearchLatencyPerVector)
	registry.MustRegister(QueryNodeWatchDmlChannelLatency)
}

func CleanupQueryNodeCollectionMetrics(nodeID int64, collectionID int64) {
	for _, label := range []string{DeleteLabel, InsertLabel} {
		QueryNodeConsumerMsgCount.
			Delete(
				prometheus.Labels{
					nodeIDLabelName:       fmt.Sprint(nodeID),
					msgTypeLabelName:      label,
					collectionIDLabelName: fmt.Sprint(collectionID),
				})

		QueryNodeConsumeTimeTickLag.
			Delete(
				prometheus.Labels{
					nodeIDLabelName:       fmt.Sprint(nodeID),
					msgTypeLabelName:      label,
					collectionIDLabelName: fmt.Sprint(collectionID),
				})
	}

}
