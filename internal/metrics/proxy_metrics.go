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
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
)

var (
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

	// ProxySearchLatency record the latency of search successfully.
	ProxySearchLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "sq_lantency",
			Help:      "latency of search",
			Buckets:   buckets,
		}, []string{nodeIDLabelName, queryTypeLabelName})

	// ProxySendSQReqLatency record the latency that the proxy sent the search request to the message stream.
	ProxySendSQReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "sq_send_latency",
			Help:      "latency that proxy sent the search request to the message stream",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, queryTypeLabelName})

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

	// ProxyMutationLatency record the latency that insert successfully.
	ProxyMutationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "mutation_latency",
			Help:      "latency of insert or delete successfully",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, msgTypeLabelName})

	// ProxySendMutationReqLatency record the latency that Proxy send insert request to MsgStream.
	ProxySendMutationReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "mutation_send_latency",
			Help:      "latency that proxy send insert request to MsgStream",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, msgTypeLabelName})

	// ProxyCacheHitCounter record the number of Proxy cache hits or miss.
	ProxyCacheHitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "cache_hit_count",
			Help:      "count of cache hits",
		}, []string{nodeIDLabelName, cacheNameLabelName, cacheStateLabelName})

	// ProxyUpdateCacheLatency record the time that proxy update cache when cache miss.
	ProxyUpdateCacheLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "cache_update_latency",
			Help:      "latency that proxy update cache when cache miss",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName})

	// ProxySyncTimeTick record Proxy synchronization timestamp statistics, differentiated by Channel.
	ProxySyncTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "sync_epoch_time",
			Help:      "synchronized unix epoch per physical channel and default channel",
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

	// ProxyDDLFunctionCall records the number of times the function of the DDL operation was executed, like `CreateCollection`.
	ProxyDDLFunctionCall = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "ddl_req_count",
			Help:      "count of DDL operation executed",
		}, []string{nodeIDLabelName, functionLabelName, statusLabelName})

	// ProxyDQLFunctionCall records the number of times the function of the DQL operation was executed, like `HasCollection`.
	ProxyDQLFunctionCall = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "dql_req_count",
			Help:      "count of DQL operation executed",
		}, []string{nodeIDLabelName, functionLabelName, statusLabelName})

	// ProxyDMLFunctionCall records the number of times the function of the DML operation was executed, like `LoadCollection`.
	ProxyDMLFunctionCall = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "dml_req_count",
			Help:      "count of DML operation executed",
		}, []string{nodeIDLabelName, functionLabelName, statusLabelName})

	// ProxyDDLReqLatency records the latency that for DML request, like "CreateCollection".
	ProxyDDLReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "ddl_req_latency",
			Help:      "latency of each DDL request",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, functionLabelName})

	// ProxyDMLReqLatency records the latency that for DML request.
	ProxyDMLReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "dml_req_latency",
			Help:      "latency of each DML request excluding insert and delete",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, functionLabelName})

	// ProxyDQLReqLatency record the latency that for DQL request, like "HasCollection".
	ProxyDQLReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "dql_req_latency",
			Help:      "latency of each DQL request excluding search and query",
			Buckets:   buckets, // unit: ms
		}, []string{nodeIDLabelName, functionLabelName})
)

//RegisterProxy registers Proxy metrics
func RegisterProxy(registry *prometheus.Registry) {
	registry.MustRegister(ProxySearchVectors)
	registry.MustRegister(ProxyInsertVectors)

	registry.MustRegister(ProxySearchLatency)
	registry.MustRegister(ProxySendSQReqLatency)
	registry.MustRegister(ProxyWaitForSearchResultLatency)
	registry.MustRegister(ProxyReduceResultLatency)
	registry.MustRegister(ProxyDecodeResultLatency)

	registry.MustRegister(ProxyMsgStreamObjectsForPChan)

	registry.MustRegister(ProxyMutationLatency)
	registry.MustRegister(ProxySendMutationReqLatency)

	registry.MustRegister(ProxyCacheHitCounter)
	registry.MustRegister(ProxyUpdateCacheLatency)

	registry.MustRegister(ProxySyncTimeTick)
	registry.MustRegister(ProxyApplyPrimaryKeyLatency)
	registry.MustRegister(ProxyApplyTimestampLatency)

	registry.MustRegister(ProxyDDLFunctionCall)
	registry.MustRegister(ProxyDQLFunctionCall)
	registry.MustRegister(ProxyDMLFunctionCall)
	registry.MustRegister(ProxyDDLReqLatency)
	registry.MustRegister(ProxyDMLReqLatency)
	registry.MustRegister(ProxyDQLReqLatency)

}
