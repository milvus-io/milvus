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
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	ProxyReceivedNQ = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "received_nq",
			Help:      "counter of nq of received search and query requests",
		}, []string{nodeIDLabelName, queryTypeLabelName, collectionName})

	// ProxySearchVectors record the number of vectors search successfully.
	ProxySearchVectors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "search_vectors_count",
			Help:      "counter of vectors successfully searched",
		}, []string{nodeIDLabelName, databaseLabelName, collectionName})

	// ProxyInsertVectors record the number of vectors insert successfully.
	ProxyInsertVectors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "insert_vectors_count",
			Help:      "counter of vectors successfully inserted",
		}, []string{nodeIDLabelName, databaseLabelName, collectionName})

	// ProxyUpsertVectors record the number of vectors upsert successfully.
	ProxyUpsertVectors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "upsert_vectors_count",
			Help:      "counter of vectors successfully upserted",
		}, []string{nodeIDLabelName, databaseLabelName, collectionName})

	ProxyDeleteVectors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "delete_vectors_count",
			Help:      "counter of vectors successfully deleted",
		}, []string{nodeIDLabelName, databaseLabelName, collectionName})

	// ProxySQLatency record the latency of search successfully.
	ProxySQLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "sq_latency",
			Help:      "latency of search or query successfully",
			Buckets:   buckets,
		}, []string{nodeIDLabelName, queryTypeLabelName, databaseLabelName, collectionName})

	// ProxyCollectionSQLatency record the latency of search successfully, per collection
	// Deprecated, ProxySQLatency instead of it
	ProxyCollectionSQLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "collection_sq_latency",
			Help:      "latency of search or query successfully, per collection",
			Buckets:   buckets,
		}, []string{nodeIDLabelName, queryTypeLabelName, collectionName})

	// ProxyMutationLatency record the latency that mutate successfully.
	ProxyMutationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "mutation_latency",
			Help:      "latency of insert or delete successfully",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, msgTypeLabelName, databaseLabelName, collectionName})

	// ProxyCollectionMutationLatency record the latency that mutate successfully, per collection
	// Deprecated, ProxyMutationLatency instead of it
	ProxyCollectionMutationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "collection_mutation_latency",
			Help:      "latency of insert or delete successfully, per collection",
			Buckets:   buckets,
		}, []string{nodeIDLabelName, msgTypeLabelName, collectionName})

	// ProxyWaitForSearchResultLatency record the time that the proxy waits for the search result.
	ProxyWaitForSearchResultLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "sq_wait_result_latency",
			Help:      "latency that proxy waits for the result",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, queryTypeLabelName})
	// ProxyReduceResultLatency record the time that the proxy reduces search result.
	ProxyReduceResultLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "sq_reduce_result_latency",
			Help:      "latency that proxy reduces search result",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, queryTypeLabelName})

	// ProxyDecodeResultLatency record the time that the proxy decodes the search result.
	ProxyDecodeResultLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "sq_decode_result_latency",
			Help:      "latency that proxy decodes the search result",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, queryTypeLabelName})

	// ProxyMsgStreamObjectsForPChan record the number of MsgStream objects per PChannel on each collection_id on Proxy.
	ProxyMsgStreamObjectsForPChan = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "msgstream_obj_num",
			Help:      "number of MsgStream objects per physical channel",
		}, []string{nodeIDLabelName, channelNameLabelName})

	// ProxySendMutationReqLatency record the latency that Proxy send insert request to MsgStream.
	ProxySendMutationReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "mutation_send_latency",
			Help:      "latency that proxy send insert request to MsgStream",
			Buckets:   longTaskBuckets, // unit: ms
		}, []string{nodeIDLabelName, msgTypeLabelName})

	// ProxyAssignSegmentIDLatency record the latency that Proxy get segmentID from dataCoord.
	ProxyAssignSegmentIDLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "assign_segmentID_latency",
			Help:      "latency that proxy get segmentID from dataCoord",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName})

	// ProxySyncSegmentRequestLength the length of SegmentIDRequests when assigning segments for insert.
	ProxySyncSegmentRequestLength = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "sync_segment_request_length",
			Help:      "the length of SegmentIDRequests when assigning segments for insert",
			Buckets:   buckets,
		}, []string{nodeIDLabelName})

	// ProxyCacheStatsCounter record the number of Proxy cache hits or miss.
	ProxyCacheStatsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "cache_hit_count",
			Help:      "count of cache hits/miss",
		}, []string{nodeIDLabelName, cacheNameLabelName, cacheStateLabelName})

	// ProxyUpdateCacheLatency record the time that proxy update cache when cache miss.
	ProxyUpdateCacheLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "cache_update_latency",
			Help:      "latency that proxy update cache when cache miss",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, cacheNameLabelName})

	// ProxySyncTimeTickLag record Proxy synchronization timestamp statistics, differentiated by Channel.
	ProxySyncTimeTickLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "tt_lag_ms",
			Help:      "now time minus tt per physical channel",
		}, []string{nodeIDLabelName, channelNameLabelName})

	// ProxyApplyPrimaryKeyLatency record the latency that apply primary key.
	ProxyApplyPrimaryKeyLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "apply_pk_latency",
			Help:      "latency that apply primary key",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName})

	// ProxyApplyTimestampLatency record the latency that proxy apply timestamp.
	ProxyApplyTimestampLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "apply_timestamp_latency",
			Help:      "latency that proxy apply timestamp",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName})

	// ProxyFunctionCall records the number of times the function of the DDL operation was executed, like `CreateCollection`.
	ProxyFunctionCall = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "req_count",
			Help:      "count of operation executed",
		}, []string{nodeIDLabelName, functionLabelName, statusLabelName, databaseLabelName, collectionName})

	// ProxyReqLatency records the latency that for all requests, like "CreateCollection".
	ProxyReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "req_latency",
			Help:      "latency of each request",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, functionLabelName})

	// ProxyReceiveBytes record the received bytes of messages in Proxy
	ProxyReceiveBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "receive_bytes_count",
			Help:      "count of bytes received  from sdk",
		}, []string{nodeIDLabelName, msgTypeLabelName, collectionName})

	// ProxyReadReqSendBytes record the bytes sent back to client by Proxy
	ProxyReadReqSendBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "send_bytes_count",
			Help:      "count of bytes sent back to sdk",
		}, []string{nodeIDLabelName})

	// RestfulFunctionCall records the number of times the restful apis was called.
	RestfulFunctionCall = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "restful_api_req_count",
			Help:      "count of operation executed",
		}, []string{nodeIDLabelName, pathLabelName})

	// RestfulReqLatency records the latency that for all requests.
	RestfulReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "restful_api_req_latency",
			Help:      "latency of each request",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, pathLabelName})

	// RestfulReceiveBytes record the received bytes of messages in Proxy
	RestfulReceiveBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "restful_api_receive_bytes_count",
			Help:      "count of bytes received  from sdk",
		}, []string{nodeIDLabelName, pathLabelName})

	// RestfulSendBytes record the bytes sent back to client by Proxy
	RestfulSendBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "restful_api_send_bytes_count",
			Help:      "count of bytes sent back to sdk",
		}, []string{nodeIDLabelName, pathLabelName})

	// ProxyReportValue records value about the request
	ProxyReportValue = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "report_value",
			Help:      "report value about the request",
		}, []string{nodeIDLabelName, msgTypeLabelName, databaseLabelName, usernameLabelName})

	// ProxyLimiterRate records rates of rateLimiter in Proxy.
	ProxyLimiterRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "limiter_rate",
			Help:      "",
		}, []string{nodeIDLabelName, collectionIDLabelName, msgTypeLabelName})

	ProxyHookFunc = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "hook_func_count",
			Help:      "the hook function count",
		}, []string{functionLabelName, fullMethodLabelName})

	UserRPCCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "user_rpc_count",
			Help:      "the rpc count of a user",
		}, []string{usernameLabelName})

	// ProxyWorkLoadScore record the score that measured query node's workload.
	ProxyWorkLoadScore = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "workload_score",
			Help:      "score that measured query node's workload",
		}, []string{
			nodeIDLabelName,
		})

	ProxyExecutingTotalNq = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "executing_total_nq",
			Help:      "total nq of executing search/query",
		}, []string{
			nodeIDLabelName,
		})

	// ProxyRateLimitReqCount integrates a counter monitoring metric for the rate-limit rpc requests.
	ProxyRateLimitReqCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "rate_limit_req_count",
			Help:      "count of operation executed",
		}, []string{nodeIDLabelName, msgTypeLabelName, statusLabelName})

	ProxySlowQueryCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "slow_query_count",
			Help:      "count of slow query executed",
		}, []string{nodeIDLabelName, msgTypeLabelName})

	// ProxyReqInQueueLatency records the latency that requests wait in the queue, like "CreateCollection".
	ProxyReqInQueueLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "req_in_queue_latency",
			Help:      "latency which request waits in the queue",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, functionLabelName})

	MaxInsertRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "max_insert_rate",
			Help:      "max insert rate",
		}, []string{"node_id", "scope"})

	// ProxyRetrySearchCount records the retry search count when result count does not meet limit and topk reduce is on
	ProxyRetrySearchCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "retry_search_cnt",
			Help:      "counter of retry search",
		}, []string{nodeIDLabelName, queryTypeLabelName, collectionName})

	// ProxyRetrySearchResultInsufficientCount records the retry search without reducing topk that still not meet result limit
	// there are more likely some non-index-related reasons like we do not have enough entities for very big k, duplicate pks, etc
	ProxyRetrySearchResultInsufficientCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "retry_search_result_insufficient_cnt",
			Help:      "counter of retry search which does not have enough results",
		}, []string{nodeIDLabelName, queryTypeLabelName, collectionName})

	// ProxyRecallSearchCount records the counter that users issue recall evaluation requests, which are cpu-intensive
	ProxyRecallSearchCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "recall_search_cnt",
			Help:      "counter of recall search",
		}, []string{nodeIDLabelName, queryTypeLabelName, collectionName})

	// ProxySearchSparseNumNonZeros records the estimated number of non-zeros in each sparse search task
	ProxySearchSparseNumNonZeros = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "search_sparse_num_non_zeros",
			Help:      "the number of non-zeros in each sparse search task",
			Buckets:   buckets,
		}, []string{nodeIDLabelName, collectionName})

	ProxyParseExpressionLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "parse_expr_latency",
			Help:      "the latency of parse expression",
			Buckets:   buckets,
		}, []string{nodeIDLabelName, functionLabelName, statusLabelName})

	// ProxyQueueTaskNum records task number of queue in Proxy.
	ProxyQueueTaskNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "queue_task_num",
			Help:      "",
		}, []string{nodeIDLabelName, queueTypeLabelName, TaskStateLabel})
)

// RegisterProxy registers Proxy metrics
func RegisterProxy(registry *prometheus.Registry) {
	registry.MustRegister(ProxyReceivedNQ)
	registry.MustRegister(ProxySearchVectors)
	registry.MustRegister(ProxyInsertVectors)
	registry.MustRegister(ProxyUpsertVectors)
	registry.MustRegister(ProxyDeleteVectors)

	registry.MustRegister(ProxySQLatency)
	registry.MustRegister(ProxyCollectionSQLatency)
	registry.MustRegister(ProxyMutationLatency)
	registry.MustRegister(ProxyCollectionMutationLatency)

	registry.MustRegister(ProxyWaitForSearchResultLatency)
	registry.MustRegister(ProxyReduceResultLatency)
	registry.MustRegister(ProxyDecodeResultLatency)

	registry.MustRegister(ProxyMsgStreamObjectsForPChan)

	registry.MustRegister(ProxySendMutationReqLatency)

	registry.MustRegister(ProxyAssignSegmentIDLatency)
	registry.MustRegister(ProxySyncSegmentRequestLength)

	registry.MustRegister(ProxyCacheStatsCounter)
	registry.MustRegister(ProxyUpdateCacheLatency)

	registry.MustRegister(ProxySyncTimeTickLag)
	registry.MustRegister(ProxyApplyPrimaryKeyLatency)
	registry.MustRegister(ProxyApplyTimestampLatency)

	registry.MustRegister(ProxyFunctionCall)
	registry.MustRegister(ProxyReqLatency)

	registry.MustRegister(ProxyReceiveBytes)
	registry.MustRegister(ProxyReadReqSendBytes)

	registry.MustRegister(RestfulFunctionCall)
	registry.MustRegister(RestfulReqLatency)

	registry.MustRegister(RestfulReceiveBytes)
	registry.MustRegister(RestfulSendBytes)

	registry.MustRegister(ProxyLimiterRate)
	registry.MustRegister(ProxyHookFunc)
	registry.MustRegister(UserRPCCounter)

	registry.MustRegister(ProxyWorkLoadScore)
	registry.MustRegister(ProxyExecutingTotalNq)
	registry.MustRegister(ProxyRateLimitReqCount)

	registry.MustRegister(ProxySlowQueryCount)
	registry.MustRegister(ProxyReportValue)
	registry.MustRegister(ProxyReqInQueueLatency)

	registry.MustRegister(MaxInsertRate)
	registry.MustRegister(ProxyRetrySearchCount)
	registry.MustRegister(ProxyRetrySearchResultInsufficientCount)
	registry.MustRegister(ProxyRecallSearchCount)

	registry.MustRegister(ProxySearchSparseNumNonZeros)
	registry.MustRegister(ProxyQueueTaskNum)

	registry.MustRegister(ProxyParseExpressionLatency)

	RegisterStreamingServiceClient(registry)
}

func CleanupProxyDBMetrics(nodeID int64, dbName string) {
	ProxySearchVectors.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName:   strconv.FormatInt(nodeID, 10),
		databaseLabelName: dbName,
	})
	ProxyInsertVectors.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName:   strconv.FormatInt(nodeID, 10),
		databaseLabelName: dbName,
	})
	ProxyUpsertVectors.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName:   strconv.FormatInt(nodeID, 10),
		databaseLabelName: dbName,
	})
	ProxyDeleteVectors.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName:   strconv.FormatInt(nodeID, 10),
		databaseLabelName: dbName,
	})
	ProxySQLatency.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName:   strconv.FormatInt(nodeID, 10),
		databaseLabelName: dbName,
	})
	ProxyMutationLatency.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName:   strconv.FormatInt(nodeID, 10),
		databaseLabelName: dbName,
	})
	ProxyFunctionCall.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName:   strconv.FormatInt(nodeID, 10),
		databaseLabelName: dbName,
	})
}

func CleanupProxyCollectionMetrics(nodeID int64, collection string) {
	ProxySearchVectors.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		collectionName:  collection,
	})
	ProxyInsertVectors.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		collectionName:  collection,
	})
	ProxyUpsertVectors.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		collectionName:  collection,
	})
	ProxySQLatency.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		collectionName:  collection,
	})
	ProxyMutationLatency.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		collectionName:  collection,
	})
	ProxyFunctionCall.DeletePartialMatch(prometheus.Labels{
		nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		collectionName:  collection,
	})

	ProxyCollectionSQLatency.Delete(prometheus.Labels{
		nodeIDLabelName:    strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: SearchLabel, collectionName: collection,
	})
	ProxyCollectionSQLatency.Delete(prometheus.Labels{
		nodeIDLabelName:    strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: QueryLabel, collectionName: collection,
	})
	ProxyCollectionMutationLatency.Delete(prometheus.Labels{
		nodeIDLabelName:  strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: InsertLabel, collectionName: collection,
	})
	ProxyCollectionMutationLatency.Delete(prometheus.Labels{
		nodeIDLabelName:  strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: DeleteLabel, collectionName: collection,
	})
	ProxyReceivedNQ.Delete(prometheus.Labels{
		nodeIDLabelName:    strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: SearchLabel, collectionName: collection,
	})
	ProxyReceivedNQ.Delete(prometheus.Labels{
		nodeIDLabelName:    strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: QueryLabel, collectionName: collection,
	})
	ProxyReceiveBytes.Delete(prometheus.Labels{
		nodeIDLabelName:  strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: SearchLabel, collectionName: collection,
	})
	ProxyReceiveBytes.Delete(prometheus.Labels{
		nodeIDLabelName:  strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: QueryLabel, collectionName: collection,
	})
	ProxyReceiveBytes.Delete(prometheus.Labels{
		nodeIDLabelName:  strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: InsertLabel, collectionName: collection,
	})
	ProxyReceiveBytes.Delete(prometheus.Labels{
		nodeIDLabelName:  strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: DeleteLabel, collectionName: collection,
	})
	ProxyReceiveBytes.Delete(prometheus.Labels{
		nodeIDLabelName:  strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: UpsertLabel, collectionName: collection,
	})
	ProxyRetrySearchCount.Delete(prometheus.Labels{
		nodeIDLabelName:    strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: SearchLabel,
		collectionName:     collection,
	})
	ProxyRetrySearchCount.Delete(prometheus.Labels{
		nodeIDLabelName:    strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: HybridSearchLabel,
		collectionName:     collection,
	})
	ProxyRetrySearchResultInsufficientCount.Delete(prometheus.Labels{
		nodeIDLabelName:    strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: SearchLabel,
		collectionName:     collection,
	})
	ProxyRetrySearchResultInsufficientCount.Delete(prometheus.Labels{
		nodeIDLabelName:    strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: HybridSearchLabel,
		collectionName:     collection,
	})
	ProxyRecallSearchCount.Delete(prometheus.Labels{
		nodeIDLabelName:    strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: SearchLabel,
		collectionName:     collection,
	})
}
