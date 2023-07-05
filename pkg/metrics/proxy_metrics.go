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

	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
		}, []string{nodeIDLabelName})

	// ProxyInsertVectors record the number of vectors insert successfully.
	ProxyInsertVectors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "insert_vectors_count",
			Help:      "counter of vectors successfully inserted",
		}, []string{nodeIDLabelName})

	// ProxySQLatency record the latency of search successfully.
	ProxySQLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "sq_latency",
			Help:      "latency of search or query successfully",
			Buckets:   buckets,
		}, []string{nodeIDLabelName, queryTypeLabelName})

	// ProxyCollectionSQLatency record the latency of search successfully, per collection
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
		}, []string{nodeIDLabelName, msgTypeLabelName})

	// ProxyMutationLatency record the latency that mutate successfully, per collection
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
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, msgTypeLabelName})

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
		}, []string{nodeIDLabelName, functionLabelName, statusLabelName})

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
)

// RegisterProxy registers Proxy metrics
func RegisterProxy(registry *prometheus.Registry) {
	registry.MustRegister(ProxyReceivedNQ)
	registry.MustRegister(ProxySearchVectors)
	registry.MustRegister(ProxyInsertVectors)

	registry.MustRegister(ProxySQLatency)
	registry.MustRegister(ProxyCollectionSQLatency)
	registry.MustRegister(ProxyMutationLatency)
	registry.MustRegister(ProxyCollectionMutationLatency)

	registry.MustRegister(ProxyWaitForSearchResultLatency)
	registry.MustRegister(ProxyReduceResultLatency)
	registry.MustRegister(ProxyDecodeResultLatency)

	registry.MustRegister(ProxyMsgStreamObjectsForPChan)

	registry.MustRegister(ProxySendMutationReqLatency)

	registry.MustRegister(ProxyCacheStatsCounter)
	registry.MustRegister(ProxyUpdateCacheLatency)

	registry.MustRegister(ProxySyncTimeTickLag)
	registry.MustRegister(ProxyApplyPrimaryKeyLatency)
	registry.MustRegister(ProxyApplyTimestampLatency)

	registry.MustRegister(ProxyFunctionCall)
	registry.MustRegister(ProxyReqLatency)

	registry.MustRegister(ProxyReceiveBytes)
	registry.MustRegister(ProxyReadReqSendBytes)

	registry.MustRegister(ProxyLimiterRate)
	registry.MustRegister(ProxyHookFunc)
	registry.MustRegister(UserRPCCounter)

	registry.MustRegister(ProxyWorkLoadScore)
}

func CleanupCollectionMetrics(nodeID int64, collection string) {
	ProxyCollectionSQLatency.Delete(prometheus.Labels{nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: SearchLabel, collectionName: collection})
	ProxyCollectionSQLatency.Delete(prometheus.Labels{nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: QueryLabel, collectionName: collection})
	ProxyCollectionMutationLatency.Delete(prometheus.Labels{nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: InsertLabel, collectionName: collection})
	ProxyCollectionMutationLatency.Delete(prometheus.Labels{nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: DeleteLabel, collectionName: collection})
	ProxyReceivedNQ.Delete(prometheus.Labels{nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: SearchLabel, collectionName: collection})
	ProxyReceivedNQ.Delete(prometheus.Labels{nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		queryTypeLabelName: QueryLabel, collectionName: collection})
	ProxyReceiveBytes.Delete(prometheus.Labels{nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: SearchLabel, collectionName: collection})
	ProxyReceiveBytes.Delete(prometheus.Labels{nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: QueryLabel, collectionName: collection})
	ProxyReceiveBytes.Delete(prometheus.Labels{nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: InsertLabel, collectionName: collection})
	ProxyReceiveBytes.Delete(prometheus.Labels{nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: DeleteLabel, collectionName: collection})
	ProxyReceiveBytes.Delete(prometheus.Labels{nodeIDLabelName: strconv.FormatInt(nodeID, 10),
		msgTypeLabelName: UpsertLabel, collectionName: collection})
}
