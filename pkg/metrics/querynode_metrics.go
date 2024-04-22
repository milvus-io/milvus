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

	QueryNodeProcessCost = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "process_insert_or_delete_latency",
			Help:      "process insert or delete cost in ms",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			msgTypeLabelName,
		})

	QueryNodeWaitProcessingMsgCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "wait_processing_msg_count",
			Help:      "count of wait processing msg",
		}, []string{
			nodeIDLabelName,
			msgTypeLabelName,
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
			segmentLevelLabelName,
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
			requestScope,
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

	QueryNodeSQPerUserLatencyInQueue = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "sq_queue_user_latency",
			Help:      "latency per user of search or query in queue",
			Buckets:   buckets,
		}, []string{
			nodeIDLabelName,
			queryTypeLabelName,
			usernameLabelName,
		},
	)

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
			reduceLevelName,
		})

	QueryNodeLoadSegmentLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "load_segment_latency",
			Help:      "latency of load per segment",
			Buckets:   longTaskBuckets, // unit milliseconds
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
			databaseLabelName,
			nodeIDLabelName,
			collectionIDLabelName,
			partitionIDLabelName,
			segmentStateLabelName,
			indexCountLabelName,
		})

	QueryNodeEntitiesSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "entity_size",
			Help:      "entities' memory size, clustered by collection and state",
		}, []string{
			nodeIDLabelName,
			collectionIDLabelName,
			segmentStateLabelName,
		})

	QueryNodeLevelZeroSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "level_zero_size",
			Help:      "level zero segments' delete records memory size, clustered by collection and state",
		}, []string{
			nodeIDLabelName,
			collectionIDLabelName,
			channelNameLabelName,
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

	QueryNodeDiskUsedSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "disk_used_size",
			Help:      "disk used size(MB)",
		}, []string{
			nodeIDLabelName,
		})

	StoppingBalanceNodeNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "stopping_balance_node_num",
			Help:      "the number of node which executing stopping balance",
		}, []string{})

	StoppingBalanceChannelNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "stopping_balance_channel_num",
			Help:      "the number of channel which executing stopping balance",
		}, []string{nodeIDLabelName})

	StoppingBalanceSegmentNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "stopping_balance_segment_num",
			Help:      "the number of segment which executing stopping balance",
		}, []string{nodeIDLabelName})

	QueryNodeLoadSegmentConcurrency = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "load_segment_concurrency",
			Help:      "number of concurrent loading segments in QueryNode",
		}, []string{
			nodeIDLabelName,
			loadTypeName,
		})

	QueryNodeLoadIndexLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "load_index_latency",
			Help:      "latency of load per segment's index, in milliseconds",
			Buckets:   longTaskBuckets, // unit milliseconds
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
	registry.MustRegister(QueryNodeSQPerUserLatencyInQueue)
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
	registry.MustRegister(QueryNodeEntitiesSize)
	registry.MustRegister(QueryNodeLevelZeroSize)
	registry.MustRegister(QueryNodeConsumeCounter)
	registry.MustRegister(QueryNodeExecuteCounter)
	registry.MustRegister(QueryNodeConsumerMsgCount)
	registry.MustRegister(QueryNodeConsumeTimeTickLag)
	registry.MustRegister(QueryNodeMsgDispatcherTtLag)
	registry.MustRegister(QueryNodeSegmentSearchLatencyPerVector)
	registry.MustRegister(QueryNodeWatchDmlChannelLatency)
	registry.MustRegister(QueryNodeDiskUsedSize)
	registry.MustRegister(QueryNodeProcessCost)
	registry.MustRegister(QueryNodeWaitProcessingMsgCount)
	registry.MustRegister(StoppingBalanceNodeNum)
	registry.MustRegister(StoppingBalanceChannelNum)
	registry.MustRegister(StoppingBalanceSegmentNum)
	registry.MustRegister(QueryNodeLoadSegmentConcurrency)
	registry.MustRegister(QueryNodeLoadIndexLatency)
}

func CleanupQueryNode() {
	QueryNodeNumCollections.Reset()
	QueryNodeNumPartitions.Reset()
	QueryNodeNumSegments.Reset()
	QueryNodeNumDmlChannels.Reset()
	QueryNodeNumDeltaChannels.Reset()
	QueryNodeSQCount.Reset()
	QueryNodeSQReqLatency.Reset()
	QueryNodeSQLatencyWaitTSafe.Reset()
	QueryNodeSQLatencyInQueue.Reset()
	QueryNodeSQPerUserLatencyInQueue.Reset()
	QueryNodeSQSegmentLatency.Reset()
	QueryNodeSQSegmentLatencyInCore.Reset()
	QueryNodeReduceLatency.Reset()
	QueryNodeLoadSegmentLatency.Reset()
	QueryNodeReadTaskUnsolveLen.Reset()
	QueryNodeReadTaskReadyLen.Reset()
	QueryNodeReadTaskConcurrency.Reset()
	QueryNodeEstimateCPUUsage.Reset()
	QueryNodeSearchGroupNQ.Reset()
	QueryNodeSearchNQ.Reset()
	QueryNodeSearchGroupSize.Reset()
	QueryNodeEvictedReadReqCount.Reset()
	QueryNodeSearchGroupTopK.Reset()
	QueryNodeSearchTopK.Reset()
	QueryNodeNumFlowGraphs.Reset()
	QueryNodeNumEntities.Reset()
	QueryNodeEntitiesSize.Reset()
	QueryNodeLevelZeroSize.Reset()
	QueryNodeConsumeCounter.Reset()
	QueryNodeExecuteCounter.Reset()
	QueryNodeConsumerMsgCount.Reset()
	QueryNodeConsumeTimeTickLag.Reset()
	QueryNodeMsgDispatcherTtLag.Reset()
	QueryNodeSegmentSearchLatencyPerVector.Reset()
	QueryNodeWatchDmlChannelLatency.Reset()
	QueryNodeDiskUsedSize.Reset()
	QueryNodeProcessCost.Reset()
	QueryNodeWaitProcessingMsgCount.Reset()
	StoppingBalanceNodeNum.Reset()
	StoppingBalanceChannelNum.Reset()
	StoppingBalanceSegmentNum.Reset()
	QueryNodeLoadSegmentConcurrency.Reset()
	QueryNodeLoadIndexLatency.Reset()
	QueryNodeSegmentAccessTotal.Reset()
	QueryNodeSegmentAccessDuration.Reset()
	QueryNodeSegmentAccessGlobalDuration.Reset()
	QueryNodeSegmentAccessWaitCacheTotal.Reset()
	QueryNodeSegmentAccessWaitCacheDuration.Reset()
	QueryNodeSegmentAccessWaitCacheGlobalDuration.Reset()
	QueryNodeDiskCacheLoadTotal.Reset()
	QueryNodeDiskCacheLoadBytes.Reset()
	QueryNodeDiskCacheLoadDuration.Reset()
	QueryNodeDiskCacheLoadGlobalDuration.Reset()
	QueryNodeDiskCacheEvictTotal.Reset()
	QueryNodeDiskCacheEvictBytes.Reset()
	QueryNodeDiskCacheEvictDuration.Reset()
	QueryNodeDiskCacheEvictGlobalDuration.Reset()
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
		QueryNodeNumEntities.
			DeletePartialMatch(
				prometheus.Labels{
					nodeIDLabelName:       fmt.Sprint(nodeID),
					collectionIDLabelName: fmt.Sprint(collectionID),
				})
	}
}
